#include "matrix_client.h"

MATRIXClient::MATRIXClient() {
}

MATRIXClient::~MATRIXClient() {
}

timespec start_tasks, end_tasks;
static int cl_LOGGING = 0;
int start_flag = 0;
uint32_t total_num_tasks = 0;
//uint32_t total_submitted = 0;

static pthread_attr_t attr; // thread attribute
pthread_mutex_t submit_q = PTHREAD_MUTEX_INITIALIZER;

string outputfile;
string loadfile_name;
ofstream client_logfile;
ofstream loadfile;

//initialize client parameters
int MATRIXClient::init(int num_tasks, int numSleep, ZHTClient &clientRet, int log, int index) {
	//cout << "mc: prefix = " << prefix << " shared = " << shared << endl;
	if(set_ip(client_id))
	{
		printf("Could not get the IP address of this machine!\n");
		return -1;
	}

	total_num_tasks = num_tasks;
        //total_submitted = num_tasks;

	// string host("localhost");
	selfindex = getSelfIndex(client_id, clientRet.memberList);
	stringstream numSleep_ss, num_nodes_ss;
	numSleep_ss << numSleep;
	string suffix("_");
	suffix.append(numSleep_ss.str());
	suffix.append("_");
	num_nodes_ss << clientRet.memberList.size();
	suffix.append(num_nodes_ss.str());
	cl_LOGGING = log;
	stringstream index_ss;
	index_ss << index;
	if(cl_LOGGING) {
		outputfile.append(prefix);
		outputfile.append("client_log.txt");
		outputfile.append(index_ss.str());
		outputfile.append(suffix);
		client_logfile.open(outputfile.c_str());
	}
	if(index == 1){
		loadfile_name.append(prefix);
		loadfile_name.append("worker_load");
		loadfile_name.append(suffix);
		loadfile.open(loadfile_name.c_str(), ios_base::app);
	}

	pthread_attr_init(&attr); // set thread detachstate attribute to DETACHED
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
	
	return 0;
}

struct submit_args {
	vector<string> *task_str_list;
	ZHTClient clientRet;
	int per_client_task;
};

void get_adjlist(int num_tasks, AdjList &adj_list) {

        #define MIN_PER_RANK 1 // Nodes/Rank: How 'fat' the DAG should be
        #define MAX_PER_RANK 5
        #define MIN_RANKS 3    // Ranks: How 'tall' the DAG should be
        #define MAX_RANKS 5
        #define PERCENT 30     // Chance of having an Edge

        srand (time (NULL));
        int height = floor(sqrt(num_tasks));    //height = MIN_RANKS + (rand () % (MAX_RANKS - MIN_RANKS + 1));
        int new_nodes = ceil(sqrt(num_tasks));  //new_nodes = MIN_PER_RANK + (rand () % (MAX_PER_RANK - MIN_PER_RANK + 1));
        int nodes = 0;

        for (int i = 0; i < height; i++) { // New nodes of 'higher' rank than all nodes generated till now
                // Edges from old nodes ('nodes') to new ones ('new_nodes')
                for (int j = 0; j < nodes; j++) {
                        for (int k = 0; k < new_nodes; k++) {
                                if ( (rand () % 100) < PERCENT) {
                                        if(adj_list.find(j) == adj_list.end()) {
                                                vector<int> new_list;
                                                new_list.push_back(k + nodes);
                                                adj_list.insert(make_pair(j, new_list));
                                        }
                                        else {
                                                vector<int> &exist_list = adj_list[j];
                                                exist_list.push_back(k + nodes);
                                        }
                                        if(adj_list.find(k + nodes) == adj_list.end()) {
                                                adj_list.insert(make_pair(k + nodes, vector<int>()));
                                        }
                                }
                        }
                }
                nodes += new_nodes; // Accumulate into old node set
        }
}

void print_AdjList(AdjList &adj_list) {
        for(AdjList::iterator it = adj_list.begin(); it != adj_list.end(); ++it) {
                vector<int> exist_list = it->second;
                cout << " " << it->first << " -> ";
                for(int i = 0; i < exist_list.size(); i++) {
                        cout << " " << exist_list[i] << ",";
                }
                cout << endl;
        }
}

int get_DAG(AdjList &adj_list, TaskDAG &dag) {
        InDegree indegree;
        for(AdjList::iterator it = adj_list.begin(); it != adj_list.end(); ++it) {
                vector<int> exist_list = it->second;
                int source_vertex = it->first;
                if(indegree.find(source_vertex) == indegree.end()) {
                        indegree[source_vertex] = 0;
                }

                stringstream adj_ss;
                for(int i = 0; i < exist_list.size(); i++) {
                        int dest_vertex = exist_list[i];

                        // add each vertex to string
                        adj_ss << exist_list[i] << "\'";

                        // update indegree count of each vertex in adjacency list
                        if(indegree.find(dest_vertex) == indegree.end()) {
                                indegree[dest_vertex] = 1;
                        }
                        else {
                                indegree[dest_vertex] = indegree[dest_vertex] + 1;
                        }
                }
                adj_ss << "\"";
                string adjliststring(adj_ss.str()); // list of vertices delimited by \' with a final \"

                // store info into DAG
                TaskDAG_Value value(indegree[source_vertex], adjliststring);
                dag[source_vertex] = value;
        }

        return indegree.size();
}

void print_DAG(TaskDAG &dag) {
        for(TaskDAG::iterator it = dag.begin(); it != dag.end(); ++it) {
                int vertex = it->first;
                TaskDAG_Value value(it->second);
                cout << "Vertex = " << vertex << " Indegree = " << value.first << " AdjList = " << value.second << endl;
        }
}

TaskDAG generate_DAG(int &num_tasks, int &num_nodes) {
        AdjList adj_list;
        get_adjlist(num_tasks, adj_list);
        //print_AdjList(adj_list);
        TaskDAG dag;
        num_nodes = get_DAG(adj_list, dag);
        //cout << "Num nodes = " << num_nodes << endl;
        //print_DAG(dag); exit(1);
	return dag;
}


void* submit(void *args) {

	//vector<string>* task_str_list = (vector<string>*)args;
	uint32_t count = 0;
	submit_args* thread_args = (submit_args*)args; //cout << "Thread: per_client_task = " << thread_args->per_client_task << endl;
	while(count != thread_args->per_client_task) {
//cout << "Thread: task_str_list empty " << thread_args->task_str_list->size() << endl;
	while(thread_args->task_str_list->size() > 0) {
		string str;
		pthread_mutex_lock(&submit_q);
		if(thread_args->task_str_list->size() > 0) {
			str = thread_args->task_str_list->back();
			//cout << "Thread: Task " << count << ": " << str << endl;
			thread_args->task_str_list->pop_back();
		}
		else {
			//cout << "Thread: task_str_list empty " << thread_args->task_str_list->size() << endl;
			pthread_mutex_unlock(&submit_q);
			continue;
		}
		pthread_mutex_unlock(&submit_q);
		int32_t ret = thread_args->clientRet.insert(str); cout << "Insert status = " << ret << endl;//<< " string = " << str << endl;
		//string result;
		//ret = thread_args->clientRet.lookup(str, result); cout << "Lookup status = " << ret << " string = " << result << endl;
		count++; cout << "Thread: Task " << count << ": sent" << endl;
	}
	}
}

int index_start = 0;
//initialize all tasks
int MATRIXClient::initializeTasks(int num_tasks_req, int numSleep, int mode, int max_tasks_per_package, ZHTClient &clientRet){	

	srand(time(NULL)); // Random seed for all time measurements

	// Task description
        char task[10];  
        memset(task, '\0', 10);
        sprintf(task, "%d", numSleep); // sleep time - 1, 2, 4, 8, 16, 32
	string task_desc(task);

	int num_worker = clientRet.memberList.size();
        int toserver = (num_worker-1)/(selfindex+1); // Server index where the tasks will be initially submitted to the Wait queue
	//cout << "to server = " << toserver << endl;

	// Initialize a random DAG based on the number of tasks requested by client
	// Note: The number of tasks in the generated DAG may be actually less than what is requested
	//	 This is fine for now, as in real systems the actual DAG would be supplied rather than we generate one.
	int num_tasks; // number of tasks actually generated
	TaskDAG dag = generate_DAG(num_tasks_req, num_tasks); cout << "total tasks = " << num_tasks << endl;
	total_num_tasks = num_tasks * 2;
	//print_DAG(dag);

	// Submission time for the task; for simplicity it is kept same for all tasks
	timespec sub_time;
	clock_gettime(CLOCK_REALTIME, &sub_time);
	uint64_t sub_time_ns;
	sub_time_ns = (uint64_t)sub_time.tv_sec*1000000000 + (uint64_t)sub_time.tv_nsec;

	// Arguments to be passed to submission thread:
	// 1. Vector that holds serialized packages for each individual task, 
	// 2. ZHT Client for network communication, and 
	// 3. Number of tasks to be submitted
	submit_args thread_args;
	thread_args.task_str_list = &task_str_list;
	thread_args.clientRet = clientRet;
	thread_args.per_client_task = num_tasks;

	// Spin the submission thread with the structure containing the above mentioned arguments
	pthread_t submit_thread;
	pthread_create(&submit_thread, NULL, submit, &thread_args);

	// Reserve space for the vector to hold serialized packages for each individual task
	task_str_list.reserve(num_tasks);

	// Measure the start time for task submission into NoVoHT
	clock_gettime(CLOCK_REALTIME, &start_tasks);

	// For all tasks in the DAG, package it and store it in NoVOHT
        //for(i = 0; i < num_tasks; i++){ 
	for(TaskDAG::iterator it = dag.begin(); it != dag.end(); ++it) {
		int task_id = it->first;
                TaskDAG_Value value(it->second);

		stringstream task_id_ss;
		task_id_ss << task_id << client_id; // Task ID + Client ID
		//cout << "id = " << task_id_ss.str();
		//cout << "numwait = " << value.first << " notlist = " << value.second << endl;

		Package package;
		package.set_virtualpath(task_id_ss.str()); // Key is task ID + client ID
		package.set_operation(3);		   // Insert task and its decription into NoVoHT
//		cout << "key = " << package.virtualpath();
//		cout << " op = " << package.operation() << endl;
//		string str1 = package.SerializeAsString(); // Serialize the package
//cout << " str1 = " << str1 << endl;

/*		stringstream to_index_ss;
		to_index_ss << toserver << "\'" << "\"";
		package.set_nodehistory(to_index_ss.str()); // History of migrations delimited by \' with a final \"
//		cout << "nodehistory = " << package.nodehistory();
		package.set_currnode(toserver); 	    // Current node
//		cout << " currnode = " << package.currnode();
		package.set_nummoves(0); 		    // Number of migrations, initially it is zero
//		cout << " nummoves = " << package.nummoves();
		package.set_numwait(value.first); 	    // Number of notifications to receive before it can be moved to ready queue
//		cout << " numwait = " << package.numwait() << endl;
		//package.set_notlist(value.second);	    // List of tasks to be notified after finishing execution
		//cout << "notlist = " << package.notlist() << endl;
//		string str2 = package.SerializeAsString();  // Serialize the package
//cout << " str2 = " << str2 << endl;
*/
		stringstream package_content_ss;
		//package_content_ss << value.second; // List of tasks to be notified after finishing execution
		package_content_ss << "NULL"; package_content_ss << "\'"; 				// Task completion status
		package_content_ss << task_desc; package_content_ss << "\'"; 				// Task Description
		package_content_ss << task_id_ss.str();	package_content_ss << "\'"; 			// Task ID
		package_content_ss << sub_time_ns; package_content_ss << "\'"; package_content_ss << "\""; // Task Submission Time
		//package_content_ss << value.second; // List of tasks to be notified after finishing execution
		package.set_realfullpath(package_content_ss.str());
		string str = package.SerializeAsString(); // Serialize the package
//cout << " str = " << str << endl;
		pthread_mutex_lock(&submit_q);
		// Push the serialized task into a vector which is shared by another thread that handles the submission over the network
		task_str_list.push_back(str);
		pthread_mutex_unlock(&submit_q);
       	}
	pthread_join(submit_thread, NULL); // Wait for the submission thread to finish sending the tasks over the network
	clock_gettime(CLOCK_REALTIME, &end_tasks); // Measure the end time to insert all tasks into NoVoHT
	timespec diff = timediff(start_tasks, end_tasks); // Measure the total time to insert all tasks into NoVoHT
	cout << num_tasks << " tasks inserted into NoVoHT" <<  endl;
	if (client_logfile.is_open() && cl_LOGGING) {
		client_logfile << num_tasks << "tasks inserted into NoVoHT" <<  endl;
		client_logfile << "TIME TAKEN: " << diff.tv_sec << "  SECONDS  " << diff.tv_nsec << "  NANOSECONDS" << endl;
	}
	//exit(1);
	// Some temp parameters
	int num_packages = 0;
	int total_submitted1 = 0;
	static int id = 0;
	// Measure the start time for task submission into Wait queue
	clock_gettime(CLOCK_REALTIME, &start_tasks);
	TaskDAG::iterator it = dag.begin();
	while (total_submitted1 != num_tasks) {
		Package package; string alltasks;
		package.set_virtualpath(client_id); // Here key is just the client ID
		//package.set_operation(21);			
		package.set_operation(22);
		num_packages++;
			
		int num_tasks_this_package = max_tasks_per_package;
		int num_tasks_left = num_tasks - total_submitted1;
		if (num_tasks_left < max_tasks_per_package) {
			num_tasks_this_package = num_tasks_left;
		}
		for(int j = 0; j < num_tasks_this_package; j++) {
			int task_id = it->first; ++it;

        	        stringstream task_id_ss;
                	task_id_ss << task_id << client_id; // Task ID + Client ID
			alltasks.append(task_id_ss.str()); alltasks.append("\'"); // Task ID
		}
		total_submitted1 = total_submitted1 + num_tasks_this_package;
		package.set_realfullpath(alltasks);
		string str = package.SerializeAsString();
		//cout << "String size = " << str.size() << " str length = " << strlen(str.c_str());
		int32_t ret = clientRet.svrtosvr(str, str.length(), toserver);
                //int32_t ret = clientRet.svrtosvr(str, str.length(), selfindex);
	}
	//cout << "No. of packages = " << num_packages;
	cout << "\tTotal Jobs submitted = " << total_submitted1 << endl;
	clock_gettime(CLOCK_REALTIME, &end_tasks); // Measure the end time to insert all tasks into Wait queue
	diff = timediff(start_tasks, end_tasks); // Measure the total time to insert all tasks into Wait queue
	//cout << "TIME TAKEN: " << diff.tv_sec << "  SECONDS  " << diff.tv_nsec << "  NANOSECONDS" << endl;
	if (client_logfile.is_open() && cl_LOGGING) {
		client_logfile << "to server = " << toserver << endl;
		client_logfile << "Total Jobs submitted = " << total_submitted1 << endl;
		client_logfile << "TIME TAKEN: " << diff.tv_sec << "  SECONDS  " << diff.tv_nsec << "  NANOSECONDS" << endl;
	}
	//exit(1);
}

//monitor the submitted tasks
// by polling each server for its load information. If a server is idle then it will return load = -4 (queue length - num of idle cores)
// so if every server returns load = -4 it implies that all submitted tasks are complete
void *monitor_function(void* args) {

	ZHTClient *clientRet = (ZHTClient*)args;
	
	Package loadPackage, shutdownPackage;	
	string loadmessage("Monitoring Information!");
	loadPackage.set_virtualpath(loadmessage);
	loadPackage.set_operation(15);
	string loadstr = loadPackage.SerializeAsString();

	string endmessage("Shutdown!");
	loadPackage.set_virtualpath(endmessage);
	loadPackage.set_operation(98);
	string endstr = loadPackage.SerializeAsString();

	//int num_worker = clientRet.memberList.size();
	int num_worker = clientRet->memberList.size();
	int num_cores = 4;
	int index = 0;
	long termination_value = num_worker * num_cores * -1;

	int total_avail_cores = num_cores * num_worker;

	int32_t total_queued = 0;
	int32_t total_idle = 0;
	int32_t queued_busy = 0;

	int32_t queued_idle = 0;
	int32_t queued = 0;
	int32_t num_idle = 0;
	int32_t num_busy = 0;
	int32_t load = 0;
	int32_t total_busy = 0;
	//int32_t status = 0;
	int32_t finished = 0;

	int32_t total_msg_count = 0;
	int32_t ret = 0;
	//sleep(60);

	int min_lines = num_worker;
	int num = num_worker - 1;
	stringstream num_ss;
	num_ss << num;
	//min_lines++;
	string filename(shared);
        filename = filename + "startinfo" + num_ss.str();
        string cmd("cat ");
        cmd = cmd + filename + " | wc -l";
        string result = executeShell(cmd);
	//cout << cmd << " " << result << endl;
	//cout << "client: minlines = " << min_lines << " cmd = " << cmd << " result = " << result << endl;
	/*string filename(shared);
	filename = filename + "start_info";
	string cmd("wc -l ");
	cmd = cmd + filename + " | awk {\'print $1\'}";
	string result = executeShell(cmd);*/
	
	while(atoi(result.c_str()) < 1) {
		sleep(5);
		result = executeShell(cmd); cout << " temp result = " << result << endl;
	} 
	cout << "client: minlines = 1 " << " cmd = " << cmd << " result = " << result << endl;
	//cout << "starting to monitor" << endl;
	cout << "TIME START: " << start_tasks.tv_sec << "  SECONDS  " << start_tasks.tv_nsec << "  NANOSECONDS" << endl;
	while(1) {

		total_queued = 0;
		total_idle = 0;
		queued_busy = 0;

		stringstream worker_load;
		for(index = 0; index < num_worker; index++) {
                        //int32_t queued_idle = clientRet.svrtosvr(loadstr, loadstr.length(), index);
			queued_idle = clientRet->svrtosvr(loadstr, loadstr.length(), index);
                        queued  = queued_idle/10;
                        num_idle = queued_idle%10;   
                        total_queued = total_queued + queued;
                        total_idle   = total_idle + num_idle;
			num_busy = num_cores - num_idle;
			load = queued + num_busy;
			worker_load << load << " ";                 
                }
		loadfile << worker_load.str() << endl;
		total_busy = total_avail_cores - total_idle;
		queued_busy = total_queued + total_busy;
		finished = total_num_tasks - queued_busy;
		clock_gettime(CLOCK_REALTIME, &end_tasks);
		cout << "Total busy cores " << total_busy << " Total Load on all workers = " << queued_busy << " No. of tasks finished = " << finished << " Total tasks submitted = " << total_num_tasks << endl;//" time = " << end_tasks.tv_sec << " " << end_tasks.tv_nsec << endl;
		if (client_logfile.is_open() && cl_LOGGING) {
			client_logfile << "Total busy cores " << total_busy << "  Total Load on all workers = " << queued_busy << " No. of tasks finished = " << finished << " Total tasks submitted = " << total_num_tasks << endl;
		}
                if(finished == total_num_tasks) {

                        clock_gettime(CLOCK_REALTIME, &end_tasks);
                        cout << "\n\n\n\n==============================All tasks finished===========================\n\n\n\n";
                        break;
                }

		sleep(2);
	}

	total_msg_count = 0;
	for(index = 0; index < num_worker; index++) {
		//clientRet.svrtosvr(endstr, endstr.length(), index);
		ret = clientRet->svrtosvr(endstr, endstr.length(), index);
		total_msg_count += ret;
	}

	cout << "TIME START: " << start_tasks.tv_sec << "  SECONDS  " << start_tasks.tv_nsec << "  NANOSECONDS" << "\n";
	cout << "TIME END: " << end_tasks.tv_sec << "  SECONDS  " << end_tasks.tv_nsec << "  NANOSECONDS" << "\n";
	timespec diff = timediff(start_tasks, end_tasks);
	cout << "TIME TAKEN: " << diff.tv_sec << "  SECONDS  " << diff.tv_nsec << "  NANOSECONDS" << "\n";
	cout << "Total messages between all servers = " << total_msg_count << endl;
	if (client_logfile.is_open() && cl_LOGGING) {		

		client_logfile << "\n\n\n\n==============================All tasks finished===========================\n\n\n\n";
		client_logfile << "TIME START: " << start_tasks.tv_sec << "  SECONDS  " << start_tasks.tv_nsec << "  NANOSECONDS" << "\n";
		client_logfile << "TIME END: " << end_tasks.tv_sec << "  SECONDS  " << end_tasks.tv_nsec << "  NANOSECONDS" << "\n";
		client_logfile << "TIME TAKEN: " << diff.tv_sec << "  SECONDS  " << diff.tv_nsec << "  NANOSECONDS" << endl;
		client_logfile << "Total messages between all servers = " << total_msg_count << endl;

		client_logfile.close();
		//return 1;
	}
}


pthread_t MATRIXClient::monitor(int num_tasks, ZHTClient &clientRet) {
	pthread_t monitor_thread;
	pthread_create(&monitor_thread, NULL, monitor_function, &clientRet);
	return monitor_thread;
}




























