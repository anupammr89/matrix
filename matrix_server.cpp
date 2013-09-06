
#include "matrix_server.h"

Worker::Worker() {
	
}

Worker::~Worker() {

}

map<string, uint32_t> client_map;

WaitQueue wqueue;
TaskQueue rqueue;
TaskQueue mqueue;
CompQueue cqueue;
static pthread_mutex_t w_lock = PTHREAD_MUTEX_INITIALIZER;              // Lock for the "wait queue"
static pthread_mutex_t r_lock = PTHREAD_MUTEX_INITIALIZER;              // Lock for the "ready queue"
static pthread_cond_t readyqueue_notempty = PTHREAD_COND_INITIALIZER;	// Condition variable for ready queue
pthread_cond_t zhtinsqueue_notempty = PTHREAD_COND_INITIALIZER;
pthread_cond_t insertqueue_notempty = PTHREAD_COND_INITIALIZER;
pthread_cond_t notqueue_notempty = PTHREAD_COND_INITIALIZER;

static pthread_mutex_t c_lock = PTHREAD_MUTEX_INITIALIZER;              // Lock for the "complete queue"
static pthread_cond_t compqueue_notempty = PTHREAD_COND_INITIALIZER;	// Condition variable for complete queue

static pthread_mutex_t m_lock = PTHREAD_MUTEX_INITIALIZER;              // Lock for the "migrate queue"
static pthread_mutex_t mutex_idle = PTHREAD_MUTEX_INITIALIZER;          // Lock for the "num_idle_cores"
static pthread_mutex_t mutex_finish = PTHREAD_MUTEX_INITIALIZER;        // Lock for the "finish file"

static pthread_mutex_t zht_lock = PTHREAD_MUTEX_INITIALIZER;        // Lock for the "zht"

queue<string*> zht_ins;
queue<string*> waitq;
queue<string*> insertq;
queue<int*> migrateq;
bitvec migratev;
queue<string*> notifyq;

pthread_mutex_t zht_ins_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t waitq_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t iq_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mq_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t notq_lock = PTHREAD_MUTEX_INITIALIZER;

struct package_thread_args {
	queue<string*> *source;
	TaskQueue *dest;
	pthread_mutex_t *slock;
	pthread_mutex_t *dlock;
	Worker *worker;
};

struct exec_thread_args {
	int tid;
	Worker *worker;
};

static pthread_mutex_t msg_lock = PTHREAD_MUTEX_INITIALIZER;

ofstream fin_fp;
ofstream log_fp;
long msg_count[NUM_MSG];

int ON = 1; int ZHT_INSQ = 1;
long task_comp_count = 0;

uint32_t nots = 0, notr = 0;

long start_poll = 1000; long start_thresh = 1000000;
long end_poll = 100000;	long end_thresh = 10000000;
int diff_thresh = 500;
timespec poll_start, poll_end;
uint64_t failed_attempts = 0;
uint64_t fail_threshold = 1000;
int work_steal_signal = 1;

//Worker *worker;
static NoVoHT *pmap;

pthread_attr_t attr; // thread attribute
static ofstream worker_start;
static ofstream task_fp;
static ofstream load_fp;
static ofstream migrate_fp;

/* filenames */
static string file_worker_start;	
static string file_task_fp;			
static string file_migrate_fp;		
static string file_fin_fp;			
static string file_log_fp;		

static string loadstr, taskstr;

Worker::Worker(char *parameters[], NoVoHT *novoht) {
	/* set thread detachstate attribute to DETACHED */
	pthread_attr_init(&attr);
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
	pthread_attr_setscope(&attr,PTHREAD_SCOPE_SYSTEM);
	/* filename definitions */
	set_dir(parameters[9],parameters[10]);
	file_worker_start.append(shared);	file_worker_start.append("startinfo");
	file_task_fp.append(prefix);		file_task_fp.append("pkgs");
	file_migrate_fp.append(prefix);		file_migrate_fp.append("log_migrate");
	file_fin_fp.append(prefix);		file_fin_fp.append("finish");
	file_log_fp.append(prefix);		file_log_fp.append("log_worker");
	
	pmap = novoht;
	Env_var::set_env_var(parameters);
	svrclient.initialize(Env_var::cfgFile, Env_var::membershipFile, Env_var::TCP);

	if(set_ip(ip)) {
		printf("Could not get the IP address of this machine!\n");
		exit(1);
	}
	
	for(int i = 0; i < NUM_MSG; i++) {
		msg_count[i] = 0;
	}
	
	poll_interval = start_poll;
	poll_threshold = start_thresh;
	num_nodes = svrclient.memberList.size();
	num_cores = NUM_COMPUTE_SLOTS;
	num_idle_cores = num_cores;
	neigh_mode = 'd';
	//worker.num_neigh = (int)(sqrt(worker.num_nodes));
	num_neigh = (int)(log(num_nodes)/log(2));
	neigh_index = new int[num_neigh];
	selfIndex = getSelfIndex(ip, atoi(parameters[1]), svrclient.memberList);
	ostringstream oss;
        oss << selfIndex;
	
	string f1 = file_fin_fp;
	f1 = f1 + oss.str();
	fin_fp.open(f1.c_str(), ios_base::app);

	if(LOGGING) {

		string f2 = file_task_fp;
		f2 = f2 + oss.str();
		task_fp.open(f2.c_str(), ios_base::app);

		string f3 = file_log_fp;
		f3 = f3 + oss.str();
		log_fp.open(f3.c_str(), ios_base::app);
		
		string f4 = file_migrate_fp;
		f4 = f4 + oss.str();
		migrate_fp.open(f4.c_str(), ios_base::app);
	}
	
	migratev = bitvec(num_nodes);
	
	Package loadPackage, tasksPackage;
	string loadmessage("Load Information!");
	loadPackage.set_virtualpath(loadmessage);
	loadPackage.set_operation(13);
	loadstr = loadPackage.SerializeAsString();
	
	stringstream selfIndexstream;
	selfIndexstream << selfIndex;
	string taskmessage(selfIndexstream.str());
	tasksPackage.set_virtualpath(taskmessage);
	tasksPackage.set_operation(14);
	taskstr = tasksPackage.SerializeAsString();

	srand((selfIndex+1)*(selfIndex+5));
	int rand_wait = rand() % 20;
	cout << "Worker ip = " << ip << " selfIndex = " << selfIndex << endl;
	sleep(rand_wait);

	file_worker_start.append(oss.str());
        string cmd("touch ");
        cmd.append(file_worker_start);
        executeShell(cmd);
        
        worker_start.open(file_worker_start.c_str(), ios_base::app);
        if(worker_start.is_open()) {
        	worker_start << ip << ":" << selfIndex << " " << std::flush;
        	worker_start.close();
        	worker_start.open(file_worker_start.c_str(), ios_base::app);
        }

        clock_gettime(CLOCK_REALTIME, &poll_start);

	int err;
	try {

	exec_thread_args eta[NUM_COMPUTE_SLOTS];
	pthread_t *exec_thread = new pthread_t[NUM_COMPUTE_SLOTS];
	for(int i = 0; i < NUM_COMPUTE_SLOTS; i++) {
		eta[i].tid = i; eta[i].worker = this;
		err = pthread_create(&exec_thread[i], &attr, execute_thread, (void*) &eta);
		if(err){
                	printf("work_steal_init: pthread_create: ready_queue_thread: %s\n", strerror(errno));
                        exit(1);
                }
	}

	pthread_t *zht_ins_thread = new pthread_t();
        err = pthread_create(zht_ins_thread, &attr, zht_ins_queue, (void*) this);
        if(err){
                printf("work_steal_init: pthread_create: zht_ins_thread: %s\n", strerror(errno));
                exit(1);
        }

	pthread_t *complete_queue_thread = new pthread_t();
        err = pthread_create(complete_queue_thread, &attr, check_complete_queue, (void*) this);
        if(err){
                printf("work_steal_init: pthread_create: complete_queue_thread: %s\n", strerror(errno));
                exit(1);
        }

	pthread_t *queueinsert_thread = new pthread_t();
	err = pthread_create(queueinsert_thread, &attr, QueueInsert, (void*) this);
	if(err){
                printf("work_steal_init: pthread_create: waitq_thread: %s\n", strerror(errno));
                exit(1);
        }

	pthread_t *migrateq_thread = new pthread_t();
        err = pthread_create(migrateq_thread, &attr, migrateTasks, (void*) this);
        if(err){
                printf("work_steal_init: pthread_create: migrateq_thread: %s\n", strerror(errno));
                exit(1);
        }

	pthread_t *notq_thread = new pthread_t();
        err = pthread_create(notq_thread, &attr, notQueue, (void*) this);
        if(err){
                printf("work_steal_init: pthread_create: notq_thread: %s\n", strerror(errno));
                exit(1);
        }

	int min_lines = svrclient.memberList.size();
        string filename(file_worker_start);
	
	string cmd2("ls -l "); 	cmd2.append(shared);	cmd2.append("startinfo*");	cmd2.append(" | wc -l");
        string result = executeShell(cmd2);
	//cout << "server: minlines = " << min_lines << " cmd = " << cmd << " result = " << result << endl;
        while(atoi(result.c_str()) < min_lines) {
		sleep(2);
		 //cout << "server: " << worker.selfIndex << " minlines = " << min_lines << " cmd = " << cmd << " result = " << result << endl;
                result = executeShell(cmd2);
        }
	//cout << "server: " << selfIndex << " minlines = " << min_lines << " cmd = " << cmd2 << " result = " << result << endl;
	
	/*int num = min_lines - 1;
        stringstream num_ss;
        num_ss << num;
        //min_lines++;
	string cmd1("cat ");    cmd1.append(shared);    cmd1.append("startinfo"); 	cmd1.append(num_ss.str());     cmd1.append(" | wc -l");
	string result1 = executeShell(cmd1);
	//cout << "server: minlines = " << min_lines << " cmd = " << cmd << " result = " << result << endl;
	while(atoi(result1.c_str()) < 1) {
		sleep(2);
		result1 = executeShell(cmd1);
	}
	cout << "worksteal started: server: " << selfIndex << " minlines = " << 1 << " cmd = " << cmd1 << " result = " << result1 << endl;*/
	
    pthread_t *work_steal_thread = new pthread_t();//(pthread_t*)malloc(sizeof(pthread_t));
	err = pthread_create(work_steal_thread, &attr, worksteal, (void*) this);
	if(err){
                printf("work_steal_init: pthread_create: work_steal_thread: %s\n", strerror(errno));
        	exit(1);
        }

	delete zht_ins_thread;
	delete exec_thread;
	delete complete_queue_thread;
	delete work_steal_thread;
	delete queueinsert_thread;
	delete migrateq_thread;
	delete notq_thread;
	}
	catch (std::bad_alloc& exc) {
		cout << "work_steal_init: failed to allocate memory while creating threads" << endl;
		exit(1);
	}
}

int32_t Worker::get_ret_status(string client_id) {
	if(client_map.find(client_id) == client_map.end()) {
		return 0;
	}
	else {
		return client_map[client_id];
	}
}

void* zht_ins_queue(void* args) {
	Worker *worker = (Worker*)args;
	string *st;
	Package package;

	while(ZHT_INSQ) {
		pthread_mutex_lock(&zht_ins_lock);
		while(zht_ins.size() == 0) {
			pthread_cond_wait(&zhtinsqueue_notempty, &zht_ins_lock);
		}
		try {
			st = zht_ins.front();
			zht_ins.pop();
		}
		catch (exception& e) {
			cout << "void* zht_ins: " << e.what() << endl;
			exit(1);
		}
		pthread_mutex_unlock(&zht_ins_lock);
		package.ParseFromString(*st);
		delete st;
		int ret = worker->zht_ins_mul(package);
	}
}

int Worker::zht_ins_mul(Package &recv_package) {
        int num_vector_count, per_vector_count;
        vector< vector<string> > tokenize_string = tokenize(recv_package.realfullpath(), '\"', ';', num_vector_count, per_vector_count);
	if(LOGGING) {
		log_fp << " num_vector_count = " << num_vector_count << " per_vector_count = " << per_vector_count << endl;
	}
	string client_id(recv_package.virtualpath());
	Package package;
        package.set_operation(3); int i;
	for(i = 0; i < num_vector_count; i++) {
		package.set_virtualpath(tokenize_string.at(i).at(0));	// Key is task ID + client ID

		stringstream to_index_ss;
		to_index_ss << tokenize_string.at(i).at(1); int toserver; to_index_ss >> toserver;
		to_index_ss << "\'" << "\"";
		package.set_nodehistory(to_index_ss.str()); 	// History of migrations delimited by \' with a final \"
		package.set_currnode(toserver);			// Current node

		package.set_nummoves(0);		    	// Number of migrations, initially it is zero

		stringstream numwait_ss;
		numwait_ss << tokenize_string.at(i).at(2);
		int numwait; numwait_ss >> numwait;
		package.set_numwait(numwait);			// Number of notifications to receive before it can be moved to ready queue

		stringstream package_content_ss;
		package_content_ss << "NULL"; package_content_ss << "\'";
		package_content_ss << tokenize_string.at(i).at(3); package_content_ss << "\'";
		package_content_ss << tokenize_string.at(i).at(0); package_content_ss << "\'";
		package_content_ss << tokenize_string.at(i).at(4); package_content_ss << "\'"; package_content_ss << "\"";
		if(tokenize_string.at(i).size() == 5) {
			package_content_ss << "\"";
		}
		else {
			package_content_ss << tokenize_string.at(i).at(5) << "\"";
		}
		package.set_realfullpath(package_content_ss.str());
		string str = package.SerializeAsString();
                zht_insert(str);
        }

	if(client_map.find(client_id) == client_map.end()) {
                client_map[client_id] = 1;
        }
        else {
                client_map[client_id] = client_map[client_id] + 1;
        }

        return num_vector_count;
}

// parse the input string containing two delimiters
vector< vector<string> > tokenize(string input, char delim1, char delim2, int &num_vector, int &per_vector_count) {
	vector< vector<string> > token_vector;
	stringstream whole_stream(input);
	num_vector = 0; per_vector_count = 0;
	string perstring;
	while(getline(whole_stream, perstring, delim1)) { //cout << "pertask = " << pertask << endl;
		num_vector++;
                vector<string> per_vector;
                size_t prev = 0, pos;
		while ((pos = perstring.find_first_of(delim2, prev)) != string::npos) {
                       	if (pos > prev) {
				try {
                               		per_vector.push_back(perstring.substr(prev, pos-prev));
				}
				catch (exception& e) {
					cout << "tokenize: (prev, pos-prev) " << " " << e.what() << endl;
					exit(1);
				}
                       	}
              		prev = pos+1;
               	}
               	if (prev < perstring.length()) {
			try {
                       		per_vector.push_back(perstring.substr(prev, string::npos));
			}
			catch (exception& e) {
                       	        cout << "tokenize: (prev, string::npos) " << " " << e.what() << endl;
                               	exit(1);
                        }
               	}
		try {
			token_vector.push_back(per_vector);
		}
		catch (exception& e) {
                        cout << "tokenize: token_vector.push_back " << " " << e.what() << endl;
                	exit(1);
                }
	}
	if(token_vector.size() > 0) {
		per_vector_count = token_vector.at(0).size();
	}
	return token_vector;
}

void* notQueue(void* args) {
	Worker *worker = (Worker*)args;
	string *st;
	Package package;

	while(ON) {
		pthread_mutex_lock(&notq_lock);
		while(notifyq.size() == 0) {
			pthread_cond_wait(&notqueue_notempty, &notq_lock);
		}
		try {
			st = notifyq.front();
			notifyq.pop();
		}
		catch (exception& e) {
			cout << "void* notifyq: " << e.what() << endl;
			exit(1);
		}
		pthread_mutex_unlock(&notq_lock);
		package.ParseFromString(*st);
		delete st;
		worker->update(package);
	}
}

// Insert tasks into queue without repeated fields
void* QueueInsert(void* args) {

	queue<string*> *source = &insertq;
	pthread_mutex_t *slock = &iq_lock;

	Worker *worker = (Worker*)args;

	string *st;
	Package package;

	while(ON) {
		pthread_mutex_lock(slock);
		while(source->size() == 0) {
			pthread_cond_wait(&insertqueue_notempty, slock);
		}
		try {
			st = source->front();
			source->pop();
		}
		catch (exception& e) {
			cout << "void* QueueInsertQ: " << e.what() << endl;
			exit(1);
		}
		pthread_mutex_unlock(slock);

		package.ParseFromString(*st);	delete st;
		TaskQueue_Item *qi;
		uint32_t task_recd_count = 0;
		string alltasks(package.realfullpath());
		int num_vector_count, per_vector_count;
		vector< vector<string> > tokenize_string = tokenize(alltasks, '\"', '\'', num_vector_count, per_vector_count);
		//cout << "num_vector_count = " << num_vector_count << " per_vector_count = " << per_vector_count << endl;
		task_recd_count = num_vector_count;
		for(int i = 0; i < num_vector_count; i++) {
			qi = new TaskQueue_Item();
			try {
				qi->task_id = tokenize_string.at(i).at(1);
			}
			catch (exception& e) {
				cout << "void* QueueInsert: (tokenize_string.at(i).at(1)) " << " " << e.what() << endl;
				exit(1);
			}
			string type;
			try {
				type = tokenize_string.at(i).at(0);
			}
			catch (exception& e) {
				cout << "void* QueueInsert: (tokenize_string.at(i).at(0)) " << " " << e.what() << endl;
				exit(1);
			}
			if(LOGGING) {
				task_fp << " taskid = " << qi->task_id; task_fp << " type = " << type << endl;
			}
			if(type.compare("w") == 0) {
				pthread_mutex_lock(&w_lock);
				try {
					wqueue.push_back(qi);
				}
				catch (std::bad_alloc& exc) {
					cout << "QueueInsert: cannot allocate memory while adding element to wait queue" << endl;
					pthread_exit(NULL);
				}
				pthread_mutex_unlock(&w_lock);
			} else {
				pthread_mutex_lock(&r_lock);
				try {
					rqueue.push_back(qi);
				}
				catch (std::bad_alloc& exc) {
					cout << "QueueInsert: cannot allocate memory while adding element to ready queue" << endl;
					pthread_exit(NULL);
				}
				pthread_cond_signal(&readyqueue_notempty);
				pthread_mutex_unlock(&r_lock);
			}
			if(LOGGING) {
				log_fp << "Num tasks received = " << task_recd_count << " Wait Queue length = " << wqueue.size() << " Ready Queue length = " << rqueue.size() << endl;
			}
		}
	}
}

map<uint32_t, NodeList> Worker::get_map(TaskQueue &mqueue) {
	map<uint32_t, NodeList> update_map;
	uint32_t num_nodes = svrclient.memberList.size();
	for(TaskQueue::iterator it = mqueue.begin(); it != mqueue.end(); ++it) {
		uint32_t serverid = myhash(((*it)->task_id).c_str(), num_nodes);
		string str((*it)->task_id); str.append("\'");
		if(update_map.find(serverid) == update_map.end()) {
			str.append("\"");
			NodeList new_list;
			new_list.push_back(str);
			update_map.insert(make_pair(serverid, new_list));
		}
		else {
			NodeList &exist_list = update_map[serverid];
			string last_str(exist_list.back());
			if((last_str.size() + str.size()) > STRING_THRESHOLD) {
				str.append("\"");
				exist_list.push_back(str);
			}
			else {
				exist_list.pop_back();
				str.append(last_str);
				exist_list.push_back(str);
			}
		}
	}
	return update_map;
}

map<uint32_t, NodeList> Worker::get_map(vector<string> &mqueue) {
        map<uint32_t, NodeList> update_map;
        uint32_t num_nodes = svrclient.memberList.size();
        for(vector<string>::iterator it = mqueue.begin(); it != mqueue.end(); ++it) {
                uint32_t serverid = myhash((*it).c_str(), num_nodes);
                string str(*it); str.append("\'");
                if(update_map.find(serverid) == update_map.end()) {
                        str.append("\"");
                        NodeList new_list;
                        new_list.push_back(str);
                        update_map.insert(make_pair(serverid, new_list));
                }
                else {
                        NodeList &exist_list = update_map[serverid];
                        string last_str(exist_list.back());
                        if((last_str.size() + str.size()) > STRING_THRESHOLD) {
                                str.append("\"");
                                exist_list.push_back(str);
                        }
                        else {
                                exist_list.pop_back();
                                str.append(last_str);
                                exist_list.push_back(str);
                        }
                }
        }
        return update_map;
}

// pack the jobs into multiple packages - 2000 jobs per package
// and insert it into the ready queue of server that requested to steal tasks
void* migrateTasks(void *args) {

	Worker *worker = (Worker*)args;
	int index;
    while(ON) {
	while(migratev.any()) {
			pthread_mutex_lock(&mq_lock);
			if(migratev.any()) {
				index = migratev.pop();
			}		        
			else {
				pthread_mutex_unlock(&mq_lock);
				continue;
			}
			if(index < 0 || index >= worker->num_nodes) {
                                pthread_mutex_unlock(&mq_lock);
                        	continue;
                        }
			pthread_mutex_unlock(&mq_lock);
			pthread_mutex_lock (&m_lock); pthread_mutex_lock (&r_lock);
			int32_t num_tasks = rqueue.size()/2;
			if(num_tasks < 1) {
				pthread_mutex_unlock (&r_lock); pthread_mutex_unlock (&m_lock);
				continue;;
			}
			try {
				mqueue.assign(rqueue.end()-num_tasks, rqueue.end());
				rqueue.erase(rqueue.end()-num_tasks, rqueue.end());
			}
			catch (...) {
				cout << "migrateTasks: cannot allocate memory while copying tasks to migrate queue" << endl;
				pthread_exit(NULL);
			}
			pthread_mutex_unlock (&r_lock);

			map<uint32_t, NodeList> update_map = worker->get_map(mqueue);
			int update_ret = worker->zht_update(update_map, "nodehistory", index);
			/*if(index == worker->selfIndex) {
				cout << "ALERT: MIGRATING TO ITSELF" << endl;
			}*/
			int num_packages = 0;
			long total_submitted = 0;

			num_tasks = mqueue.size();
			while (total_submitted != num_tasks) {
				Package package; string alltasks;
				package.set_virtualpath(worker->ip);
				package.set_operation(22);
				num_packages++;
				int num_tasks_this_package = max_tasks_per_package;
				int num_tasks_left = num_tasks - total_submitted;
				if (num_tasks_left < max_tasks_per_package) {
                	 	       num_tasks_this_package = num_tasks_left;
				}
				for(int j = 0; j < num_tasks_this_package; j++) {
                			if(mqueue.size() < 1) {
		                	        if(j > 0) {
                			        	total_submitted = total_submitted + j;
							package.set_realfullpath(alltasks);
			                	        string str = package.SerializeAsString();
							pthread_mutex_lock(&msg_lock);
        	        		        	int32_t ret = worker->svrclient.svrtosvr(str, str.length(), index);
							pthread_mutex_unlock(&msg_lock);
	        	                	}
						total_submitted = num_tasks;
						break;
        	            		}
                			try {
						alltasks.append("r"); alltasks.append("\'");
						alltasks.append(mqueue.front()->task_id); alltasks.append("\'\""); // Task ID
		
                			        if(LOGGING) {
				            		migrate_fp << " taskid = " << mqueue.front()->task_id;
                    		   		}
						delete mqueue.front();
		                   		mqueue.pop_front();
                	    		}
                    	    		catch (...) {
		                    		cout << "migrateTasks: Exception occurred while processing mqueue" << endl;
                	    		}
				}
				if(total_submitted == num_tasks) {
					break;
				}
				total_submitted = total_submitted + num_tasks_this_package;
				package.set_realfullpath(alltasks);
				string str = package.SerializeAsString();
				pthread_mutex_lock(&msg_lock);
				int32_t ret = worker->svrclient.svrtosvr(str, str.length(), index);
				pthread_mutex_unlock(&msg_lock);
			}
			pthread_mutex_unlock (&m_lock);
		}
	}
}

//request the worker with given index to send tasks
int32_t Worker::recv_tasks() {
	int32_t num_task;
	pthread_mutex_lock(&msg_lock);
	num_task = svrclient.svrtosvr(taskstr, taskstr.length(), max_loaded_node);
	pthread_mutex_unlock(&msg_lock); 
	/*if(LOGGING) {
		log_fp << "expected num tasks = " << num_task << endl;
	}*/

	clock_gettime(CLOCK_REALTIME, &poll_end);
        timespec diff = timediff(poll_start, poll_end);
	if(diff.tv_sec > diff_thresh) {
		poll_threshold = end_thresh;
	}

	if(num_task > 0) { // there are tasks to receive
		if(diff.tv_sec > diff_thresh) {
			poll_interval = end_poll; // reset the poll interval to 1ms
		}
		else {
			poll_interval = start_poll;
		}
		return num_task;
	}

	else {
		// no tasks
		if(poll_interval < poll_threshold) { // increase poll interval only if it is under 1 sec
			poll_interval *= 2;
		}
		return 0;
	}
}

/*
 * Find the neighbor which has the heaviest load
 */
int32_t Worker::get_max_load() {
	int i;
	int32_t max_load = -1000000, load;
	
	for(i = 0; i < num_neigh; i++) {
		// Get Load information
		pthread_mutex_lock(&msg_lock);
		load = svrclient.svrtosvr(loadstr, loadstr.length(), neigh_index[i]);
		pthread_mutex_unlock(&msg_lock);
		if(load > max_load) {
			max_load = load;
			max_loaded_node = neigh_index[i];
		}
	}
	return max_load;
}

/*
 * Randomly choose neighbors dynamically
 */
void Worker::choose_neigh()
{	
	srand(time(NULL));
	int i, ran;
	int x;
	char *flag = new char[num_nodes];
	memset(flag, '0', num_nodes);
	for(i = 0; i < num_neigh; i++) {
		ran = rand() % num_nodes;		
		x = hostComp(svrclient.memberList.at(ran), svrclient.memberList.at(selfIndex));
		while(flag[ran] == '1' || !x) {
			ran = rand() % num_nodes;
			x = hostComp(svrclient.memberList.at(ran), svrclient.memberList.at(selfIndex));
		}
		flag[ran] = '1';
		neigh_index[i] = ran;
	}
	delete flag;
}

/*
 * Steal tasks from the neighbors
 */
int32_t Worker::steal_task()
{
	choose_neigh();
	//cout << "Done choose_neigh" << endl;
	int32_t max_load = get_max_load();
	/*if(LOGGING) {
                log_fp << "Max loaded node = " << max_loaded_node << endl;
        }*/
	while(max_load <= 0 && work_steal_signal) {
		usleep(poll_interval);	failed_attempts++;
		clock_gettime(CLOCK_REALTIME, &poll_end);
        	timespec diff = timediff(poll_start, poll_end);
        	if(diff.tv_sec > diff_thresh) {
                	poll_threshold = end_thresh;
        	}

		if(poll_interval < poll_threshold) { // increase poll interval only if it is under 1 sec
			poll_interval *= 2;
		}

		if(failed_attempts >= fail_threshold) {
			work_steal_signal = 0;
			return 0;
		}
		choose_neigh();
		max_load = get_max_load();
		/*if(LOGGING) {
                	log_fp << "Max loaded node = " << max_loaded_node << endl;
        	}*/
	}
	/*if(LOGGING) {
		log_fp << "Max loaded node = " << max_loaded_node << endl;
	}*/
	failed_attempts = 0;
	return recv_tasks();
}

// thread to steal tasks it ready queue length becomes zero
void* worksteal(void* args){
	//cout << "entered worksteal thread" << endl;
	Worker *worker = (Worker*)args;

	
	int num = worker->svrclient.memberList.size() - 1;
        stringstream num_ss;
        num_ss << num;
	string cmd1("cat ");    cmd1.append(shared);    cmd1.append("startinfo"); 	cmd1.append(num_ss.str());     cmd1.append(" | wc -l");
	string result1 = executeShell(cmd1);
	//cout << "server: minlines = " << min_lines << " cmd = " << cmd << " result = " << result << endl;
	while(atoi(result1.c_str()) < 1) {
		sleep(2);
		result1 = executeShell(cmd1);
	}
	//cout << "worksteal started: server: " << worker->selfIndex << " minlines = " << 1 << " cmd = " << cmd1 << " result = " << result1 << endl;
	
	while(work_steal_signal) {
		while(rqueue.size() > 0) { }
		
		// If there are no waiting ready tasks, do work stealing
		if (worker->num_nodes > 1 && rqueue.size() < 1) {
			int32_t success = worker->steal_task();			
			// Do work stealing until succeed
			while(success == 0) {
				failed_attempts++;
				if(failed_attempts >= fail_threshold) {
                        		work_steal_signal = 0;
					cout << worker->selfIndex << " stopping worksteal" << endl;
					break;
                		}
				usleep(worker->poll_interval);
				success = worker->steal_task();				
			}
			failed_attempts = 0;
		}
	}
}

int Worker::check_if_task_is_ready(string key) {
	int index = myhash(key.c_str(), svrclient.memberList.size());
	if(index != selfIndex) {
		Package check_package;
		check_package.set_virtualpath(key);
		check_package.set_operation(23);
		string check_str = check_package.SerializeAsString();
		pthread_mutex_lock(&msg_lock);
		int ret = svrclient.svrtosvr(check_str, check_str.size(), index);
		pthread_mutex_unlock(&msg_lock);
		return ret;
	}
	else {
		string value = zht_lookup(key);
		Package check_package;
		check_package.ParseFromString(value);
		return check_package.numwait();
	}
}

int Worker::move_task_to_ready_queue(TaskQueue_Item **qi) {
	pthread_mutex_lock(&r_lock);
	rqueue.push_back(*qi);
	pthread_cond_signal(&readyqueue_notempty);
	pthread_mutex_unlock(&r_lock);
}

bool check(TaskQueue_Item *qi) {
	return qi==NULL;
}

static int work_exec_flag = 0;
int Worker::execute(TaskQueue_Item *qi) {
	pthread_mutex_lock(&mutex_idle);
	worker->num_idle_cores--;
	pthread_mutex_unlock(&mutex_idle);

	if(!work_exec_flag) {
		work_exec_flag = 1;
		worker_start << ip << ":" << selfIndex << " Got jobs..Started excuting" << endl;
	}

	uint32_t duration = worker->get_task_desc(qi->task_id);

	timespec task_start_time, task_end_time;
	clock_gettime(CLOCK_REALTIME, &task_start_time);
	uint32_t exit_code = usleep(duration);
	clock_gettime(CLOCK_REALTIME, &task_end_time);

	pthread_mutex_lock(&mutex_idle);
        worker->num_idle_cores++; task_comp_count++;
        pthread_mutex_unlock(&mutex_idle);

	uint64_t st = (uint64_t)task_start_time.tv_sec * 1000000000 + (uint64_t)task_start_time.tv_nsec;
	uint64_t et = (uint64_t)task_end_time.tv_sec * 1000000000 + (uint64_t)task_end_time.tv_nsec;
	timespec diff = timediff(task_start_time, task_end_time);

	if(LOGGING) {
		string fin_str; stringstream out;
		out << qi->task_id << " exitcode " << exit_code << " Interval " << diff.tv_sec << " S  " << diff.tv_nsec << " NS" << " server " << worker->ip;
		fin_str = out.str();
		pthread_mutex_lock(&mutex_finish);
		fin_fp << fin_str << endl;
		pthread_mutex_unlock(&mutex_finish);
	}

	return exit_code;
}

// thread to monitor ready queue and execute tasks based on num of cores availability
void* execute_thread(void* args) {
	exec_thread_args exec_arg = *((exec_thread_args*)args);
        Worker *worker = exec_arg.worker;
        int tid = exec_arg.tid;

	TaskQueue_Item *qi;
	while(ON) {
		pthread_mutex_lock(&r_lock);
                while(rqueue.size() == 0) {
                        pthread_cond_wait(&readyqueue_notempty, &r_lock);
                }
		qi = rqueue.front();
		rqueue.pop_front();
		pthread_mutex_unlock(&r_lock);

		worker->execute(qi);
		worker->notify(qi->task_id);

		try {
			delete qi;
		}
		catch (exception& e) {
			cout << "void* execute_thread: delete qi " << " " << e.what() << endl;
			exit(1);
		}
	}
}

int Worker::notify(ComPair &compair) {
	map<uint32_t, NodeList> update_map = worker->get_map(compair.second);
	int update_ret = worker->zht_update(update_map, "numwait", selfIndex);
	nots += compair.second.size(); log_fp << "nots = " << nots << endl;
	return update_ret;
}

int Worker::notify(string key) {
	int index = myhash(key.c_str(), svrclient.memberList.size());
	if(index != selfIndex) {
		Package package;
	        package.set_virtualpath(key);
        	package.set_operation(17);
	        string notify_str = package.SerializeAsString();

		pthread_mutex_lock(&msg_lock);
		int ret = svrclient.svrtosvr(notify_str, notify_str.size(), index);
		pthread_mutex_unlock(&msg_lock);
		return 0;
	}
	else {
		string value = worker->zht_lookup(key);
		Package recv_pkg;
	        recv_pkg.ParseFromString(value);
		int num_vector_count, per_vector_count;
	        vector< vector<string> > tokenize_string = tokenize(recv_pkg.realfullpath(), '\"', '\'', num_vector_count, per_vector_count);
		// push completed task into complete queue
		pthread_mutex_lock(&c_lock);
		try {
			cqueue.push_back(make_pair(key, tokenize_string.at(1)));
		}
		catch (exception& e) {
                        cout << "Worker::notify: num_vector_count = " << num_vector_count << " per_vector_count = " << per_vector_count << endl;
                        cout << "Worker::notify: (tokenize_string.at(1)) " << " " << e.what() << endl;
			exit(1);
		}
		pthread_cond_signal(&compqueue_notempty);
		pthread_mutex_unlock(&c_lock);
		return 0;
	}
}

void* check_complete_queue(void* args) {
	Worker *worker = (Worker*)args;

        ComPair compair;
        while(ON) {
		pthread_mutex_lock(&c_lock);
		while(cqueue.size() == 0) {
			pthread_cond_wait(&compqueue_notempty, &c_lock);
		}
		compair = cqueue.front();
		cqueue.pop_front();
		pthread_mutex_unlock(&c_lock);
		worker->notify(compair);
	}
}

int32_t Worker::get_load_info() {
	return (rqueue.size() - num_idle_cores);
}

int32_t Worker::get_monitoring_info() {
	if (LOGGING) {
		log_fp << "rqueue = " << rqueue.size() << " mqueue = " << mqueue.size() << " wqueue = " << wqueue.size() << " cqueue = " << cqueue.size() << endl;
	}
	return (((rqueue.size() + mqueue.size() + wqueue.size()) * 10) + num_idle_cores);
}

int32_t Worker::get_numtasks_to_steal() {
	return ((rqueue.size() - num_idle_cores)/2);
}

//string Worker::get_task_desc(string key) {
uint32_t Worker::get_task_desc(string key) {
	int index = myhash(key.c_str(), svrclient.memberList.size());
	if(index != selfIndex) {
		Package package;
                package.set_virtualpath(key);
		package.set_realfullpath("task_desc");
                package.set_operation(16);
                string get_task_desc_str = package.SerializeAsString();

                pthread_mutex_lock(&msg_lock);
                string task_desc;
		//svrclient.task_lookup(get_task_desc_str, get_task_desc_str.size(), index, task_desc);
		uint32_t duration = svrclient.svrtosvr(get_task_desc_str, get_task_desc_str.size(), index);
                pthread_mutex_unlock(&msg_lock);
		//return task_desc;
		return duration;
	} else {
		uint32_t duration;
		string *result = pmap->get(key);
		if (result == NULL) {
			cout << "lookup find nothing. key = " << key << endl;
			string nullString = "Empty";
			//return nullString;
			return 0;
		}
		string task_str = *result;
		Package recv_pkg;
	        recv_pkg.ParseFromString(task_str);
		int num_vector_count, per_vector_count;
	        vector< vector<string> > tokenize_string = tokenize(recv_pkg.realfullpath(), '\"', '\'', num_vector_count, per_vector_count);
		string task_desc;
		try {
			task_desc = tokenize_string.at(0).at(1);
			stringstream duration_ss;
		        duration_ss << task_desc;
		        duration_ss >> duration;
		}
		catch (exception& e) {
			cout << "get_task_desc: num_vector_count = " << num_vector_count << " per_vector_count = " << per_vector_count << endl;
                        cout << "get_task_desc: (tokenize_string.at(0).at(1)) " << " " << e.what() << endl;
			cout << "get_task_desc: result = " << task_str;
                        exit(1);
		}
		//return task_desc;
        	return duration;
	}
}

string Worker::zht_lookup(string key) {
	string task_str;

	/*Package package;
	package.set_virtualpath(key);
	package.set_operation(1);
	string lookup_str = package.SerializeAsString();
	int index;
	svrclient.str2Host(lookup_str, index);*/

	int index = myhash(key.c_str(), svrclient.memberList.size());

	if(index != selfIndex) {
		Package package;
	        package.set_virtualpath(key);
        	package.set_operation(1);
	        string lookup_str = package.SerializeAsString();

		pthread_mutex_lock(&msg_lock);
		int ret = svrclient.lookup(lookup_str, task_str);
		pthread_mutex_unlock(&msg_lock);

		Package task_pkg;
        	task_pkg.ParseFromString(task_str);
		//cout << "remote lookup: string = " << task_str << " str size = " << task_str.size() << endl;
	        //cout << "remote lookup: task = " << task_pkg.virtualpath() << " nodehistory = " << task_pkg.nodehistory() << endl;
	}
	else {
		string *result = pmap->get(key);
		if (result == NULL) {
			//cout << "lookup find nothing. key = " << key << endl;
			string nullString = "Empty";
			return nullString;
		}
		task_str = *result;
		//Package task_pkg;
        	//task_pkg.ParseFromString(task_str);
	        //cout << "local lookup: task = " << task_pkg.virtualpath() << " nodehistory = " << task_pkg.nodehistory() << endl;
	}
	return task_str;
	/*Package task_pkg;
	task_pkg.ParseFromString(task_str);
	return task_pkg.realfullpath();*/
}

int Worker::zht_insert(string str) {
	Package package;
	package.ParseFromString(str);
        package.set_operation(3);
	str = package.SerializeAsString();

	int index;
        svrclient.str2Host(str, index);

	if(index != selfIndex) { //cout << "NOOOOO..index = " << index << endl;
                pthread_mutex_lock(&msg_lock);
                int ret = svrclient.insert(str); 
                pthread_mutex_unlock(&msg_lock);
		return ret;
        }
	else {
		string key = package.virtualpath(); //cout << "key = " << key << endl;
		//pthread_mutex_lock(&zht_lock);
		int ret = pmap->put(key, str);
		//pthread_mutex_unlock(&zht_lock);
		if (ret != 0) {
			cerr << "insert error: key = " << key << " ret = " << ret << endl;
			return -3;
		}
		else
			return 0;
	}
}

int Worker::zht_remove(string key) {

	/*Package package;
	package.set_virtualpath(key);
	package.set_operation(2);
	string str = package.SerializeAsString();

	int index;
        svrclient.str2Host(str, index);*/

	int index = myhash(key.c_str(), svrclient.memberList.size());

        if(index != selfIndex) {
		Package package;
	        package.set_virtualpath(key);
        	package.set_operation(2);
	        string str = package.SerializeAsString();
                pthread_mutex_lock(&msg_lock);
                int ret = svrclient.remove(str);
                pthread_mutex_unlock(&msg_lock);
                return ret;
        }
        else {
                int ret = pmap->remove(key);
		if (ret != 0) {
			cerr << "DB Error: fail to remove :ret= " << ret << endl;
			return -2;
		} else
			return 0; //succeed.
        }

}

int Worker::zht_append(string str) {
	Package package;
	package.ParseFromString(str);
	package.set_operation(4);
	str = package.SerializeAsString();

	int index;
        svrclient.str2Host(str, index);

	if(index != selfIndex) {
                pthread_mutex_lock(&msg_lock);
                int ret = svrclient.append(str);
                pthread_mutex_unlock(&msg_lock);
                return ret;
        }
        else {
		string key = package.virtualpath();
		int ret = pmap->append(key, str);
		if (ret != 0) {
			cerr << "Append error: ret = " << ret << endl;
			return -4;
		} else
			return 0;
	}
}

int Worker::update_nodehistory(uint32_t currnode, string alltasks) {
	int num_vector_count, per_vector_count;
	vector< vector<string> > tokenize_string = tokenize(alltasks, '\"', '\'', num_vector_count, per_vector_count);
	uint32_t num_nodes = svrclient.memberList.size();
	//cout << "Worker = " << selfIndex << " num_vector_count = " << num_vector_count << " per_vector_count = " << per_vector_count << endl;
	for(int i = 0; i < num_vector_count; i++) {
		for(int j = 0; j < per_vector_count; j++) {
                try {
                	string &taskid = tokenize_string.at(i).at(j);
			string value = zht_lookup(taskid);
			Package recv_pkg;
                        recv_pkg.ParseFromString(value);

			int index = myhash(taskid.c_str(), num_nodes);
                        if(index != selfIndex) {
                                cout << "something wrong..doing remote update_nodehistory index = " << index << " selfIndex = " << selfIndex << endl;
                        }

			// update number of moves (increment)
			uint32_t old_nummoves = recv_pkg.nummoves();
			recv_pkg.set_nummoves(old_nummoves+1);

			// update current location of task
			recv_pkg.set_currnode(currnode);

			// update task migration history
			stringstream nodehistory_ss;
			nodehistory_ss << currnode << "\'";
			string new_nodehistory(nodehistory_ss.str());
			new_nodehistory.append(recv_pkg.nodehistory()); //cout << "update_nodehistory: task " << recv_pkg.virtualpath() << " node history = " << recv_pkg.nodehistory();
			recv_pkg.set_nodehistory(new_nodehistory); //cout << " node history = " << recv_pkg.nodehistory() << endl;

			// insert updated task into ZHT
			int ret = zht_insert(recv_pkg.SerializeAsString());
			if (ret != 0) {
				cout << "update_nodehistory: zht_insert error ret = " << ret << endl;
				exit(1);
			}
                }
                catch (exception& e) {
                	cout << "update_nodehistory: (tokenize_string.at(i).at(0)) " << " " << e.what() << endl;
                        exit(1);
                }
		}
	}
	return 0;
}

int Worker::move(string key) {
        TaskQueue_Item *qi;
	for(WaitQueue::iterator wq = wqueue.begin(); wq != wqueue.end(); ++wq) {
		qi = *wq;
		if(qi->task_id.compare(key) == 0) { //cout << "moving task " << key << " to ready queue" << endl;
                        pthread_mutex_lock(&w_lock);
                        int ret = worker->move_task_to_ready_queue(&qi);
			wqueue.erase(wq);
			pthread_mutex_unlock(&w_lock);
			return 0;
		}
	}
        /*for(int i = 0; i < wqueue.size(); i++) {
                qi = wqueue[i];
                if(qi == NULL)
                        continue;
                if(qi->task_id.compare(key) == 0) { cout << "moving task " << key << " to ready queue" << endl;
                        pthread_mutex_lock(&w_lock);
                        int ret = worker->move_task_to_ready_queue(&qi);
                        wqueue[i] = NULL;
                        TaskQueue::iterator last = remove_if(wqueue.begin(), wqueue.end(), check);
                        wqueue.erase(last, wqueue.end());
                        pthread_mutex_unlock(&w_lock);
                        return 0;
                }
        }*/
        cout << "Worker::move task " << key << " not found" << endl;
        return -1;
}

int Worker::move_task_to_ready_queue(string key, int index) {
        //int index = myhash(key.c_str(), svrclient.memberList.size());

        if(index != selfIndex) {
                Package package;
                package.set_virtualpath(key);
                package.set_operation(18);
                string str = package.SerializeAsString();
                pthread_mutex_lock(&msg_lock);
                int ret = svrclient.svrtosvr(str, str.length(), index);
                pthread_mutex_unlock(&msg_lock);
                return ret;
        }
        else {
                return move(key);
        }
}

int Worker::update_numwait(string alltasks) {
	int num_vector_count, per_vector_count;
        vector< vector<string> > tokenize_string = tokenize(alltasks, '\"', '\'', num_vector_count, per_vector_count);
	uint32_t num_nodes = svrclient.memberList.size();
	for(int i = 0; i < num_vector_count; i++) {
		for(int j = 0; j < per_vector_count; j++) {
                try {
                        string &taskid = tokenize_string.at(i).at(j);
			string value = zht_lookup(taskid);
			Package recv_pkg;
                        recv_pkg.ParseFromString(value);
	
			int index = myhash(taskid.c_str(), num_nodes);
			if(index != selfIndex) {
				cout << "something wrong..doing remote update_numwait: index = " << index << " selfIndex = " << selfIndex << endl;
			}

			// update number of tasks to wait (decrement)
			uint32_t old_numwait = recv_pkg.numwait();
                        recv_pkg.set_numwait(old_numwait-1);
			notr++;
			if(old_numwait-1 == 0) {
                                if(LOGGING) {
                                        log_fp << "task = " << taskid << " is ready" << endl;
                                }
                                int ret = move_task_to_ready_queue(taskid, recv_pkg.currnode());
                        }

			// insert updated task into ZHT
                        int ret = zht_insert(recv_pkg.SerializeAsString());
			if (ret != 0) {
				cout << "update_numwait: old_numwait = " << old_numwait << endl;
                                cout << "update_numwait: zht_insert error ret = " << ret << " key = " << taskid << " index = " << index << " selfindex = " << selfIndex << endl;
                                exit(1);
                        }
                }
                catch (exception& e) {
                        cout << "update_numwait: (tokenize_string.at(i).at(0)) " << " " << e.what() << endl;
                        exit(1);
                }
		}
        }
	log_fp << "notr = " << notr << endl;
	return 0;
}

int Worker::update(Package &package) {
	string field(package.virtualpath());
	if(field.compare("nodehistory") == 0) {
		return update_nodehistory(package.currnode(), package.realfullpath());
	}
	else if(field.compare("numwait") == 0) {
		return update_numwait(package.realfullpath());
	}
}

int Worker::zht_update(map<uint32_t, NodeList> &update_map, string field, uint32_t toid = 0) {

	Package package;
        package.set_operation(25);
	package.set_virtualpath(field);
        if(!field.compare("nodehistory")) {
                package.set_currnode(toid);
        } //cout << "deque size = ";
	for(map<uint32_t, NodeList>::iterator map_it = update_map.begin(); map_it != update_map.end(); ++map_it) {
		uint32_t index = map_it->first;
		NodeList &update_list = map_it->second;
		//cout << update_list.size() << " ";
		while(!update_list.empty()) {
			package.set_realfullpath(update_list.front());
			update_list.pop_front();
			if(index == selfIndex) {
				string *str;
                        	str = new string(package.SerializeAsString());
                        	pthread_mutex_lock(&notq_lock);
                        	notifyq.push(str);
				pthread_cond_signal(&notqueue_notempty);
                        	pthread_mutex_unlock(&notq_lock);
				//int ret = update(package);
			}
			//else if (index != toid){
			else {
				string update_str = package.SerializeAsString();
				pthread_mutex_lock(&msg_lock);
				int ret = svrclient.svrtosvr(update_str, update_str.size(), index);
				pthread_mutex_unlock(&msg_lock);
			}
		}
	} //cout << endl;
}


