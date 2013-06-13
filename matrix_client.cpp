#include "matrix_client.h"
#include <map>
#include <vector>
#include <deque>
#include <math.h>

MATRIXClient::MATRIXClient() {
}

MATRIXClient::~MATRIXClient() {
}

timespec start_tasks, end_tasks;
static int cl_LOGGING = 0;
int start_flag = 0;
uint32_t total_num_tasks = 0;

static pthread_attr_t attr; // thread attribute
pthread_mutex_t submit_q = PTHREAD_MUTEX_INITIALIZER;

string outputfile;
string loadfile_name;
ofstream client_logfile;
ofstream loadfile;

#define SIXTY_KILOBYTES 61440
#define STRING_THRESHOLD SIXTY_KILOBYTES

typedef deque<string> NodeList;
map<uint32_t, NodeList> update_map;
static uint32_t tcount = 0;
void get_map(vector<string> &mqueue, uint32_t num_nodes) {
        for(vector<string>::iterator it = mqueue.begin(); it != mqueue.end(); ++it) {
                Package package;
                package.ParseFromString(*it);
                uint32_t serverid = myhash(package.virtualpath().c_str(), num_nodes);
                string str(*it); str.append("\?");
                if(update_map.find(serverid) == update_map.end()) {
                        str.append("\\");
                        NodeList new_list;
                        new_list.push_back(str);
                        update_map.insert(make_pair(serverid, new_list));
                }
                else {
                        NodeList &exist_list = update_map[serverid];
                        string last_str(exist_list.back());
                        if((last_str.size() + str.size()) > STRING_THRESHOLD) {
                                str.append("\\");
                                exist_list.push_back(str);
                        }
                        else {
                                exist_list.pop_back();
                                str.append(last_str);
                                exist_list.push_back(str);
                        }
                }
        }
}

//initialize client parameters
int MATRIXClient::init(int num_tasks, int numSleep, ZHTClient &clientRet, int log, int index) {
	if(set_ip(client_id))
	{
		printf("Could not get the IP address of this machine!\n");
		return -1;
	}

	total_num_tasks = num_tasks;

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

void get_adjlist(int num_tasks, AdjList &adj_list, int DAG_choice) {

	#define MAX_CHILDREN 100

	if(DAG_choice == 0) { // bag of tasks
		for (int i = 0; i < num_tasks; i++) { // New nodes of 'higher' rank than all nodes generated till now
                	// Edges from old nodes ('nodes') to new ones ('new_nodes')
	                vector<int> new_list;
        	        adj_list.insert(make_pair(i, new_list));
        	}
	}

	else if(DAG_choice == 1) { // random DAG
	        #define PERCENT 30     // Chance of having an Edge

        	srand (time (NULL));
	        int height = floor(sqrt(num_tasks));
        	int new_nodes = ceil(sqrt(num_tasks));
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
							if(exist_list.size() < 10)
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

	else if(DAG_choice == 2) { // pipeline DAG
		int nodes = 0;

		int num_pipeline = num_tasks/10;
		int pipeline_height = 10;

        	for (int i = 0; i < num_pipeline; i++) {
                	for (int j = 0; j < pipeline_height; j++) { // New nodes of 'higher' rank than all nodes generated till now
	                // Edges from old nodes ('nodes') to new ones ('new_nodes')
        	                if(adj_list.find(nodes) == adj_list.end()) {
                	                vector<int> new_list;
                        	        new_list.push_back(nodes+1);
                                	adj_list.insert(make_pair(nodes, new_list));
	                        }
        	                else {
                	                vector<int> &exist_list = adj_list[nodes];
                        	        exist_list.push_back(nodes+1);
	                        }
        	                if(adj_list.find(nodes+1) == adj_list.end()) {
                	                adj_list.insert(make_pair(nodes+1, vector<int>()));
                        	}
	                        nodes++; // Accumulate into old node set
        	        }
                	nodes++;
        	}
	}

	else if(DAG_choice == 3) { // fan in DAG
	        AdjList adj_list1;
	        adj_list1.insert(make_pair(0, vector<int>()));
	        int index = 0; int count = pow(MAX_CHILDREN, 0);
        	int num_level = floor(log(num_tasks)/log(MAX_CHILDREN));
	        int j = 0; int num = 1;
	        while(j <= num_level) {
                	while(index < count) {
        	                vector<int> &exist_list = adj_list1[index];
	                        for (int i = num; i < num+MAX_CHILDREN; i++) {
                                	exist_list.push_back(i);
                        	        if(adj_list1.find(i) == adj_list1.end()) {
                	                        adj_list1.insert(make_pair(i, vector<int>()));
        	                        }
	                                if(i >= num_tasks) {
                                	        index = count; j = num_level+1; break;
                        	        }
                	        } num += MAX_CHILDREN;
        	                index++;
	                }
                	count += pow(MAX_CHILDREN, ++j);
        	}

		for(AdjList::iterator it = adj_list1.begin(); it != adj_list1.end(); ++it) {
        	        int vertex = it->first;
	                if(adj_list.find(vertex) == adj_list.end()) {
                        	adj_list.insert(make_pair(vertex, vector<int>()));
                	}
        	        vector<int> &alist = it->second; int alist_size = alist.size();
	                for(int i = 0; i < alist_size; i++) {
                        	int v = alist[i];
                	        if(adj_list.find(v) == adj_list.end()) {
        	                        adj_list.insert(make_pair(v, vector<int>()));
	                        }
	                        vector<int> &exist_list = adj_list[v]; exist_list.push_back(vertex);
                	}
        	}
	}

	else if(DAG_choice == 4) { // fan out DAG
	        adj_list.insert(make_pair(0, vector<int>()));
        	int index = 0; int count = pow(MAX_CHILDREN, 0);
	        int num_level = floor(log(num_tasks)/log(MAX_CHILDREN));
        	int j = 0; int num = 1;
	        while(j <= num_level) {
        	        while(index < count) {
                	        vector<int> &exist_list = adj_list[index];
                        	for (int i = num; i < num+MAX_CHILDREN; i++) {
                                	exist_list.push_back(i);
	                                if(adj_list.find(i) == adj_list.end()) {
        	                                adj_list.insert(make_pair(i, vector<int>()));
                	                }
                        	        if(i >= num_tasks)
                                	        return;
	                        } num += MAX_CHILDREN;
        	                index++;
                	}
        	        count += pow(MAX_CHILDREN, ++j);
	        }
	}

	else {
		cout << "Enter proper choice for DAG" << endl;
		exit(1);
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

int get_DAG(AdjList &adj_list, TaskDAG &dag, string clientid) {
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
			adj_ss << exist_list[i] << clientid << "\'";

                        // update indegree count of each vertex in adjacency list
                        if(indegree.find(dest_vertex) == indegree.end()) {
                                indegree[dest_vertex] = 1;
                        }
                        else {
                                indegree[dest_vertex] = indegree[dest_vertex] + 1;
                        }
			if(dag.find(dest_vertex) != dag.end()) {
                                TaskDAG_Value &value = dag[dest_vertex];
                                value.first = indegree[dest_vertex];
                        }
                }
                string adjliststring(adj_ss.str()); // list of vertices delimited by \' with a final \"

                // store info into DAG
                TaskDAG_Value value(indegree[source_vertex], adjliststring);
                dag[source_vertex] = value;
        }

        return indegree.size();
}

void print_DAG(TaskDAG &dag) {
	uint32_t expected_notifications = 0;
        for(TaskDAG::iterator it = dag.begin(); it != dag.end(); ++it) {
                int vertex = it->first;
                TaskDAG_Value value(it->second);
		expected_notifications += value.first;
		client_logfile << "Vertex = " << vertex << " Indegree = " << value.first << " AdjList = " << value.second << endl;
        }
	cout << "expected_notifications = " << expected_notifications << endl;
	client_logfile << "expected_notifications = " << expected_notifications << endl;
}

TaskDAG generate_DAG(int &num_tasks, int &num_nodes, string clientid, int choice) {
        AdjList adj_list;
        get_adjlist(num_tasks, adj_list, choice);
        //print_AdjList(adj_list);
        TaskDAG dag;
        num_nodes = get_DAG(adj_list, dag, clientid);
        //cout << "Num nodes = " << num_nodes << endl;
        //print_DAG(dag); exit(1);
	return dag;
}


void* submit(void *args) {

	uint32_t count = 0;
	submit_args* thread_args = (submit_args*)args; //cout << "Thread: per_client_task = " << thread_args->per_client_task << endl;
	while(count != thread_args->per_client_task) {
	while(thread_args->task_str_list->size() > 0) {
		string str;
		pthread_mutex_lock(&submit_q);
		if(thread_args->task_str_list->size() > 0) {
			str = thread_args->task_str_list->back();
			thread_args->task_str_list->pop_back();
		}
		else {
			pthread_mutex_unlock(&submit_q);
			continue;
		}
		pthread_mutex_unlock(&submit_q);
		int32_t ret = thread_args->clientRet.insert(str); //cout << "Insert status = " << ret << endl;//<< " string = " << str << endl;
		//string result;
		//ret = thread_args->clientRet.lookup(str, result); cout << "Lookup status = " << ret << " string = " << result << endl;
		count++; //cout << "Thread: Task " << count << ": sent" << endl;
	}
	}
}

void submittasks(int selfIndex, string client_id, ZHTClient &clientRet) {
	map<uint32_t, uint32_t> ret_map;
        int ret = 0;
        Package package;
        package.set_operation(20);
        package.set_virtualpath(client_id);
        for(map<uint32_t, NodeList>::iterator map_it = update_map.begin(); map_it != update_map.end(); ++map_it) {
                uint32_t index = map_it->first; //cout << selfIndex << " Sending to server: " << index << endl; int a = 1;
                NodeList &update_list = map_it->second; ret_map[index] = update_list.size();
                while(!update_list.empty()) { //cout << "str = " << update_list.front() << endl;
                        package.set_realfullpath(update_list.front());
			//package.set_serialized(update_list.front());
                        update_list.pop_front();
                        string update_str = package.SerializeAsString(); //cout << selfIndex << " " << a << " started" << endl;
                        ret += clientRet.svrtosvr(update_str, update_str.size(), index); //cout << selfIndex << " " << a << " done" << endl;
			//cout << "ret so far = " << ret << " sent size = " << update_str.size() << endl; a++;
                }
        }
	
	Package check_pkg;
	check_pkg.set_operation(19);
        check_pkg.set_virtualpath(client_id);
	string check_str = check_pkg.SerializeAsString();
	while(ret_map.size() > 0) {
	for(map<uint32_t, uint32_t>::iterator map_it = ret_map.begin(); map_it != ret_map.end(); ++map_it) {
		uint32_t index = map_it->first; uint32_t expected_ret = map_it->second;
		ret = clientRet.svrtosvr(check_str, check_str.size(), index);
		if(ret == expected_ret) {
			ret_map.erase(index);
		}
	}
	sleep(2);
	}
        //cout << " no. tasks inserted = " << ret << endl;
}

int index_start = 0;
//initialize all tasks
int MATRIXClient::initializeTasks(int num_tasks_req, int numSleep, int max_tasks_per_package, ZHTClient &clientRet, int DAG_choice){

	srand(time(NULL)); // Random seed for all time measurements

	// Task description
        char task[10];  
        memset(task, '\0', 10);
        sprintf(task, "%d", numSleep); // sleep time - 1, 2, 4, 8, 16, 32
	string task_desc(task);

	int num_worker = clientRet.memberList.size();
        int toserver = (num_worker-1)/(selfindex+1); // Server index where the tasks will be initially submitted to the Wait queue

	// Initialize a random DAG based on the number of tasks requested by client
	// Note: The number of tasks in the generated DAG may not be actually equal to what is requested
	//	 This is fine for now, as in real systems the actual DAG would be supplied rather than we generate one.
	int num_tasks; // number of tasks actually generated
	TaskDAG dag = generate_DAG(num_tasks_req, num_tasks, client_id, DAG_choice); //cout << "total tasks = " << num_tasks << endl;
	total_num_tasks = num_tasks * ceil(sqrt(num_worker));
	print_DAG(dag);

	// Submission time for the task; for simplicity it is kept same for all tasks
	timespec sub_time;
	clock_gettime(CLOCK_REALTIME, &sub_time);
	uint64_t sub_time_ns;
	sub_time_ns = (uint64_t)sub_time.tv_sec*1000000000 + (uint64_t)sub_time.tv_nsec;

	clock_gettime(CLOCK_REALTIME, &start_tasks);

	for(TaskDAG::iterator it = dag.begin(); it != dag.end(); ++it) {
                int task_id = it->first;
                TaskDAG_Value value(it->second);

		stringstream taskd;
                taskd << task_id << client_id << ";"; uint32_t serverid = myhash(taskd.str().c_str(), num_worker);
		taskd << toserver << ";";
		taskd << value.first << ";";
		taskd << task_desc << ";";
		taskd << sub_time_ns << ";";
		taskd << value.second << ";" << "\"";

		string str(taskd.str());

		if(update_map.find(serverid) == update_map.end()) {
                        NodeList new_list;
                        new_list.push_back(str);
                        update_map.insert(make_pair(serverid, new_list));
                }
		else {
			NodeList &exist_list = update_map[serverid];
                        string last_str(exist_list.back());
                        if((last_str.size() + str.size()) > STRING_THRESHOLD) {
                                exist_list.push_back(str);
                        }
                        else {
                                exist_list.pop_back();
                                str.append(last_str);
                                exist_list.push_back(str);
                        }
		}
	}

	clock_gettime(CLOCK_REALTIME, &end_tasks); timespec diff1 = timediff(start_tasks, end_tasks);
        cout << num_tasks << " novoht jobs created. TIME TAKEN: " << diff1.tv_sec << "  SECONDS  " << diff1.tv_nsec << "  NANOSECONDS" << endl;

        clock_gettime(CLOCK_REALTIME, &start_tasks);
        submittasks(selfindex, client_id, clientRet);
	clock_gettime(CLOCK_REALTIME, &end_tasks); // Measure the end time to insert all tasks into NoVoHT
	
	timespec diff = timediff(start_tasks, end_tasks); // Measure the total time to insert all tasks into NoVoHT
	cout << num_tasks << " tasks inserted into NoVoHT" <<  endl;
	cout << "TIME TAKEN: " << diff.tv_sec << "  SECONDS  " << diff.tv_nsec << "  NANOSECONDS" << endl;
	if (client_logfile.is_open() && cl_LOGGING) {
		client_logfile << num_tasks << "tasks inserted into NoVoHT" <<  endl;
		client_logfile << "TIME TAKEN: " << diff.tv_sec << "  SECONDS  " << diff.tv_nsec << "  NANOSECONDS" << endl;
	}
	
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
			int task_id = it->first;
			TaskDAG_Value value(it->second);
			if(value.first > 0) {
				alltasks.append("w"); alltasks.append("\'");
			} else {
				alltasks.append("r"); alltasks.append("\'");
			}

        	        stringstream task_id_ss;
                	task_id_ss << task_id << client_id; // Task ID + Client ID
			alltasks.append(task_id_ss.str()); alltasks.append("\'\""); // Task ID
			++it;
		}
		total_submitted1 = total_submitted1 + num_tasks_this_package;
		package.set_realfullpath(alltasks);
		string str = package.SerializeAsString();
		int32_t ret = clientRet.svrtosvr(str, str.length(), toserver);
	}
	cout << "\tTotal Jobs submitted = " << total_submitted1 << endl;
	clock_gettime(CLOCK_REALTIME, &end_tasks); // Measure the end time to insert all tasks into Wait queue
	diff = timediff(start_tasks, end_tasks); // Measure the total time to insert all tasks into Wait queue
	//cout << "TIME TAKEN: " << diff.tv_sec << "  SECONDS  " << diff.tv_nsec << "  NANOSECONDS" << endl;
	if (client_logfile.is_open() && cl_LOGGING) {
		client_logfile << "to server = " << toserver << endl;
		client_logfile << "Total Jobs submitted = " << total_submitted1 << endl;
		client_logfile << "TIME TAKEN: " << diff.tv_sec << "  SECONDS  " << diff.tv_nsec << "  NANOSECONDS" << endl;
	}
}

//monitor the submitted tasks
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
	int32_t finished = 0;

	int32_t total_msg_count = 0;
	int32_t ret = 0;

	int min_lines = num_worker;
	int num = num_worker - 1;
	stringstream num_ss;
	num_ss << num;
	string filename(shared);
        filename = filename + "startinfo" + num_ss.str();
        string cmd("cat ");
        cmd = cmd + filename + " | wc -l";
        string result = executeShell(cmd);
	
	while(atoi(result.c_str()) < 1) {
		sleep(5);
		result = executeShell(cmd); cout << " temp result = " << result << endl;
	} 
	cout << "client: minlines = 1 " << " cmd = " << cmd << " result = " << result << endl;
	cout << "TIME START: " << start_tasks.tv_sec << "  SECONDS  " << start_tasks.tv_nsec << "  NANOSECONDS" << endl;
	while(1) {

		total_queued = 0;
		total_idle = 0;
		queued_busy = 0;

		stringstream worker_load;
		for(index = 0; index < num_worker; index++) {
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
		cout << "Total busy cores " << total_busy << " Total Load on all workers = " << queued_busy << " No. of tasks finished = " << finished << " Total tasks submitted = " << total_num_tasks << endl;
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
	}
}

pthread_t MATRIXClient::monitor(int num_tasks, ZHTClient &clientRet) {
	pthread_t monitor_thread;
	pthread_create(&monitor_thread, NULL, monitor_function, &clientRet);
	return monitor_thread;
}

