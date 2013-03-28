
#include "matrix_server.h"

Worker::Worker() {
	
}

Worker::~Worker() {

}

TaskQueue wqueue;
TaskQueue rqueue;
TaskQueue mqueue;
CompQueue cqueue;
static pthread_mutex_t w_lock = PTHREAD_MUTEX_INITIALIZER;              // Lock for the "wait queue"
static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;                // Lock for the "ready queue"
static pthread_mutex_t c_lock = PTHREAD_MUTEX_INITIALIZER;                // Lock for the "complete queue"
static pthread_mutex_t m_lock = PTHREAD_MUTEX_INITIALIZER;              // Lock for the "migrate queue"
static pthread_mutex_t mutex_idle = PTHREAD_MUTEX_INITIALIZER;          // Lock for the "num_idle_cores"
static pthread_mutex_t mutex_finish = PTHREAD_MUTEX_INITIALIZER;        // Lock for the "finish file"

static pthread_mutex_t zht_lock = PTHREAD_MUTEX_INITIALIZER;        // Lock for the "zht"

//queue<string*> insertq;
queue<string*> waitq;
queue<string*> insertq_new;
queue<int*> migrateq;
bitvec migratev;
queue<string*> notifyq;

//pthread_mutex_t iq_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t waitq_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t iq_new_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mq_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t notq_lock = PTHREAD_MUTEX_INITIALIZER;

struct package_thread_args {
	queue<string*> *source;
	TaskQueue *dest;
	pthread_mutex_t *slock;
	pthread_mutex_t *dlock;
	Worker *worker;
};

static pthread_mutex_t msg_lock = PTHREAD_MUTEX_INITIALIZER;

/*
TaskQueue* ready_queue;		// Queue of tasks which are ready to run
TaskQueue* migrate_queue;       // Queue of tasks which are going to migrate
TaskQueue* wait_queue;		// Queue of tasks which are waiting for the finish of dependent tasks
TaskQueue* running_queue;	// Queue of tasks which are being executed
*/

ofstream fin_fp;
ofstream log_fp;
long msg_count[10];

int ON = 1;
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
	//svrzht.initialize(Env_var::cfgFile, Env_var::membershipFile, Env_var::TCP);
	//svrmig.initialize(Env_var::cfgFile, Env_var::membershipFile, Env_var::TCP);

	if(set_ip(ip)) {
		printf("Could not get the IP address of this machine!\n");
		exit(1);
	}
	
	for(int i = 0; i < 10; i++) {
		msg_count[i] = 0;
	}
	
	poll_interval = start_poll;
	poll_threshold = start_thresh;
	num_nodes = svrclient.memberList.size();
	num_cores = 4;
	num_idle_cores = num_cores;
	neigh_mode = 'd';
	//worker.num_neigh = (int)(sqrt(worker.num_nodes));
	num_neigh = (int)(log(num_nodes)/log(2));
	neigh_index = new int[num_neigh];
	selfIndex = getSelfIndex(ip, atoi(parameters[1]), svrclient.memberList);	// replace "localhost" with proper hostname, host is the IP in C++ string
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
	//cout << "Worker ip = " << ip << " selfIndex = " << selfIndex << " going to wait for " << rand_wait << " seconds" << endl;
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
	/*pthread_t *ready_queue_thread = new pthread_t();//(pthread_t*)malloc(sizeof(pthread_t));
	pthread_create(ready_queue_thread, &attr, check_ready_queue, NULL);*/
	try {
	pthread_t *ready_queue_thread = new pthread_t[num_cores];
	for(int i = 0; i < num_cores; i++) {
		err = pthread_create(&ready_queue_thread[i], &attr, check_ready_queue, (void*) this);
		if(err){
                	printf("work_steal_init: pthread_create: ready_queue_thread: %s\n", strerror(errno));
                        exit(1);
                }
	}

	pthread_t *wait_queue_thread = new pthread_t();
	err = pthread_create(wait_queue_thread, &attr, check_wait_queue, (void*) this);
	if(err){
                printf("work_steal_init: pthread_create: wait_queue_thread: %s\n", strerror(errno));
                exit(1);
        }

	pthread_t *complete_queue_thread = new pthread_t();
        err = pthread_create(complete_queue_thread, &attr, check_complete_queue, (void*) this);
        if(err){
                printf("work_steal_init: pthread_create: complete_queue_thread: %s\n", strerror(errno));
                exit(1);
        }
	
	package_thread_args rq_args, wq_args;
	rq_args.source = &insertq_new;	wq_args.source = &waitq;
	rq_args.dest = &rqueue;		wq_args.dest = &wqueue;
	rq_args.slock = &iq_new_lock;	wq_args.slock = &waitq_lock;
	rq_args.dlock = &lock;		wq_args.dlock = &w_lock;	
	rq_args.worker = this;		wq_args.worker = this;
	pthread_t *waitq_thread = new pthread_t();
	err = pthread_create(waitq_thread, &attr, HB_insertQ_new, (void*) &wq_args);
	if(err){
                printf("work_steal_init: pthread_create: waitq_thread: %s\n", strerror(errno));
                exit(1);
        }

	pthread_t *readyq_thread = new pthread_t();
        err = pthread_create(readyq_thread, &attr, HB_insertQ_new, (void*) &rq_args);
        if(err){
                printf("work_steal_init: pthread_create: ready_thread: %s\n", strerror(errno));
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
	//min_lines++;	
        string filename(file_worker_start);
        //string cmd("wc -l ");	
        //cmd = cmd + filename + " | awk {\'print $1\'}";
	
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

	delete ready_queue_thread;
	delete wait_queue_thread;
	delete complete_queue_thread;
	delete work_steal_thread;
	delete readyq_thread;
	delete waitq_thread;
	delete migrateq_thread;
	delete notq_thread;
	}
	catch (std::bad_alloc& exc) {
		cout << "work_steal_init: failed to allocate memory while creating threads" << endl;
		exit(1);
	}
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
		while(notifyq.size() > 0) {
			pthread_mutex_lock(&notq_lock);
			if(notifyq.size() > 0) {
				try {
				st = notifyq.front();
				notifyq.pop();
				}
				catch (exception& e) {
					cout << "void* notifyq: " << e.what() << endl;
					exit(1);
				}
			}
			else {
                                pthread_mutex_unlock(&notq_lock);
                                continue;
                        }
			pthread_mutex_unlock(&notq_lock);
			package.ParseFromString(*st);
			delete st;
			worker->update(package);
		}
	}
}

// Insert tasks into queue without repeated fields
//int32_t Worker::HB_insertQ_new(NoVoHT *map, Package &package) {
void* HB_insertQ_new(void* args) {

	package_thread_args targs = *((package_thread_args*)args);
	queue<string*> *source = targs.source;
	TaskQueue *dest = targs.dest;
	pthread_mutex_t *slock = targs.slock;
	pthread_mutex_t *dlock = targs.dlock;
	Worker *worker = targs.worker;

	string *st;
	Package package;

	while(ON) {
		while(source->size() > 0) {
			pthread_mutex_lock(slock);
			if(source->size() > 0) {
				try {
				st = source->front();
				source->pop(); //cout << "recd something" << endl;
				}
				catch (exception& e) {
					cout << "void* HB_insertQ_new: " << e.what() << endl;
					exit(1);
				}
			}
			else {
                                pthread_mutex_unlock(slock);
                                continue;
                        }
			pthread_mutex_unlock(slock);
			package.ParseFromString(*st);
			delete st;

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
                			qi->task_id = tokenize_string.at(i).at(0); //cout << "insertq: qi->task_id = " << qi->task_id << endl;
				}
				catch (exception& e) {
                                        cout << "void* HB_insertQ_new: (tokenize_string.at(i).at(0)) " << " " << e.what() << endl;
                                	exit(1);
                                }
				/*stringstream num_moves_ss;
				try {
			                num_moves_ss << tokenize_string.at(i).at(1);
				}
				catch (exception& e) {
                                        cout << "void* HB_insertQ_new: (tokenize_string.at(i).at(0)) " << " " << e.what() << endl;
                                        exit(1);
                                }
                		num_moves_ss >> qi->num_moves;*/

                		if(LOGGING) {
                                	task_fp << " taskid = " << qi->task_id;
                                	//task_fp << " num moves = " << qi->num_moves;
                		}
				
				pthread_mutex_lock(dlock);
				try {
		                        dest->push_back(qi);
                		}
		                catch (std::bad_alloc& exc) {
                		        cout << "HB_InsertQ_new: cannot allocate memory while adding element to ready queue" << endl;
		                        pthread_exit(NULL);
                		}
		                pthread_mutex_unlock(dlock);
		        }
			if(LOGGING) {
				log_fp << "Num tasks received = " << task_recd_count << " Queue length = " << dest->size() << endl;
			}
		}
	}
}

/*
// Insert tasks into queue
//int32_t Worker::HB_insertQ(NoVoHT *map, Package &package) {
void* HB_insertQ(void* args) {

	Worker *worker = (Worker*)args;

	string *st;
	Package package;

	while(ON) {
		while(insertq.size() > 0) {
			pthread_mutex_lock(&iq_lock);
                	if(insertq.size() > 0) {
				try {
				st = insertq.front();
				insertq.pop();
				}
				catch (exception& e) {
                                        cout << "void* HB_insertQ: " << e.what() << endl;
                                        exit(1);
                                }
			}
			else {
				pthread_mutex_unlock(&iq_lock);
				continue;
			}
			pthread_mutex_unlock(&iq_lock);
                	package.ParseFromString(*st);
			delete st;

			//cout << "worker id = " << worker->selfIndex << " numtasks recd = " << package.taskid_size() << endl;
			for (int j = 0; j < package.taskid_size(); j++) {
				TaskQueue_Item *qi = new TaskQueue_Item();
				qi->task_id = package.taskid(j);					//cout << " 2" << endl;
				qi->client_id = package.clientid(j);				//cout << " 3" << endl;
				qi->task_description = package.description(j);		//cout << " 4" << endl;

				if(package.submission_time(j) == 0) { // this task has just been submitted.
					timespec sub_time;
					clock_gettime(CLOCK_REALTIME, &sub_time);
					qi->submission_time = (uint64_t)sub_time.tv_sec * 1000000000 + (uint64_t)sub_time.tv_nsec;

					qi->num_moves = package.num_moves(j);

					if(LOGGING) {
                		        	task_fp << " taskid = " << package.taskid(j);
                        			task_fp << " clientid = " << package.clientid(j);
		                        	task_fp << " desc = " << package.description(j);
                		        	task_fp << " num moves = " << package.num_moves(j);
                        			task_fp << " sub time = " << package.submission_time(j); task_fp << endl;
                			}
				}

				else {	// this task has been stolen from another worker. so do not modify submission time. just increment the number of moves
					qi->submission_time = package.submission_time(j);
					qi->num_moves = package.num_moves(j) + 1;
				}
				//cout << "Task id = " << qi.task_id << endl;
				pthread_mutex_lock(&lock);
				//ready_queue->add_element(&qi);	// add task to ready queue
				try {
		                        rqueue.push_back(qi);
                		}
		                catch (std::bad_alloc& exc) {
                		        cout << "HB_InsertQ: cannot allocate memory while adding element to ready queue" << endl;
		                        pthread_exit(NULL);
                		}
				//cout << "Worker:" << worker->selfIndex << " QueueLength: = " <<  rqueue.size() << endl;
				pthread_mutex_unlock(&lock);
		}
	}
}
*/

/*
//int32_t Worker::HB_localinsertQ(NoVoHT *map, Package &r_package) {
void* HB_localinsertQ(void* args) {


	Worker *worker = (Worker*)args;

	string *st;
        Package r_package;
	try {
	st = reinterpret_cast<string*>(args);
        r_package.ParseFromString(*st);
	}
	catch (exception& e) {
                cout << "void* HB_localinsertQ: " << e.what() << endl;
        	exit(1);
        }
	int numSleep = r_package.mode();
	int num_tasks = r_package.num();
	string clientid = r_package.realfullpath();

	char task[10];
        memset(task, '\0', 10);
        sprintf(task, "%d", numSleep);

	timespec start_tasks, end_tasks;
	int num_packages = 0;
        static int id = 1;

	TaskQueue_Item *qi;
        
        int total_submitted1 = 0;
        //clock_gettime(CLOCK_REALTIME, &start_tasks);
	for(int j = 0; j < num_tasks; j++) {
		qi = new TaskQueue_Item();
		qi->task_id = id++;
		qi->client_id = clientid;
		qi->task_description = task;
		timespec sub_time;
		clock_gettime(CLOCK_REALTIME, &sub_time);
		qi->submission_time = (uint64_t)sub_time.tv_sec * 1000000000 + (uint64_t)sub_time.tv_nsec;
		qi->num_moves = 0;
		pthread_mutex_lock(&lock);
		//ready_queue->add_element(&qi);
		try {
			rqueue.push_back(qi);
		}
		catch (std::bad_alloc& exc) {
                        cout << "HB_localInsertQ: cannot allocate memory while adding element to ready queue" << endl;
                 	pthread_exit(NULL);
                }
		pthread_mutex_unlock(&lock);
	}
	//clock_gettime(CLOCK_REALTIME, &end_tasks);

        //timespec diff = timediff(start_tasks, end_tasks);
        //cout << "TIME TAKEN: " << diff.tv_sec << "  SECONDS  " << diff.tv_nsec << "  NANOSECONDS" << endl;

	//if (ready_queue->get_length() == 1024000) {
	if(rqueue.size() == 1024000) {
        	//cout << "TaskQueue_Item = " << sizeof(TaskQueue_Item) << " TaskQueue = " << sizeof(TaskQueue) << endl;
		cout << "TaskQueue_Item = " << sizeof(TaskQueue_Item) << " TaskQueue = " << sizeof(rqueue) << endl;
        }
	//cout << "Worker:" << worker->selfIndex << " QueueLength: = " <<  ready_queue->get_length() << endl;
	//return ready_queue->get_length();
	//return rqueue.size();
	//delete qa;
	delete st;
}
*/

map<uint32_t, NodeList> Worker::get_map(TaskQueue &mqueue) {
	map<uint32_t, NodeList> update_map;
	/*Package package;
	package.set_operation(operation);
	if(operation == 25) {
		package.set_currnode(toid);
	}*/
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
        /*Package package;
        package.set_operation(operation);
        if(operation == 25) {
                package.set_currnode(toid);
        }*/
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
//int Worker::migrateTasks(int num_tasks, ZHTClient &clientRet, int index){
void* migrateTasks(void *args) {

	Worker *worker = (Worker*)args;
	int index;
    while(ON) {
        //while(migrateq.size() > 0) {
	while(migratev.any()) {
			pthread_mutex_lock(&mq_lock);
			if(migratev.any()) {
				//int *index = (int*)args;                
                		//index = migrateq.front();
				index = migratev.pop();
                		//migrateq.pop();
				//cout << "1 worker = " << worker->selfIndex << " to index = " << index << " size = " << rqueue.size() << endl;
			}		        
			else {
				//cout << "migratev count = " << migratev.count() << endl;
				pthread_mutex_unlock(&mq_lock);
				continue;
			}
			if(index < 0 || index >= worker->num_nodes) {
				//cout << "bad index: worker = " << worker->selfIndex << " to index = " << index << endl;
                                pthread_mutex_unlock(&mq_lock);
                        	continue;
                        }
			pthread_mutex_unlock(&mq_lock);
			//cout << "2 worker = " << worker->selfIndex << " to index = " << index << " size = " << rqueue.size() << endl;
			pthread_mutex_lock (&m_lock); pthread_mutex_lock (&lock);
			int32_t num_tasks = rqueue.size()/2;
			if(num_tasks < 1) {
				pthread_mutex_unlock (&lock); pthread_mutex_unlock (&m_lock);
				continue;;
			}
			try {	//cout << "going to send " << num_tasks << " tasks" << endl;
				mqueue.assign(rqueue.end()-num_tasks, rqueue.end());
				rqueue.erase(rqueue.end()-num_tasks, rqueue.end());
				//cout << "rqueue size = " << rqueue.size() << " mqueue size = " << mqueue.size() << endl;
			}
			catch (...) {
				cout << "migrateTasks: cannot allocate memory while copying tasks to migrate queue" << endl;
				pthread_exit(NULL);
			}
			pthread_mutex_unlock (&lock);

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
		                //TaskQueue_item* qi = migrate_queue->remove_element();
                			if(mqueue.size() < 1) {
		                	        if(j > 0) {
                			        	total_submitted = total_submitted + j;
							package.set_realfullpath(alltasks);
			                	        string str = package.SerializeAsString();
							pthread_mutex_lock(&msg_lock);
        	        		        	int32_t ret = worker->svrclient.svrtosvr(str, str.length(), index);
							pthread_mutex_unlock(&msg_lock);
	        	                	}
        	        	        	//pthread_mutex_unlock (&m_lock);
	                	        	//return total_submitted;
		                        	//pthread_exit(NULL);
						total_submitted = num_tasks;
						break;
        	            		}
                			try {
						alltasks.append(mqueue.front()->task_id); alltasks.append("\'\""); // Task ID
						/*stringstream num_moves_ss;
			                        num_moves_ss << (mqueue.front()->num_moves + 1);
						alltasks.append(num_moves_ss.str());  alltasks.append("\'\""); // Number of moves*/
		
                			        if(LOGGING) {
				            		migrate_fp << " taskid = " << mqueue.front()->task_id;
			                    		//migrate_fp << " num moves = " << (mqueue.front()->num_moves + 1);
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
				string str = package.SerializeAsString(); //cout << "r1: " << total_submitted << " tasks" << endl;
				pthread_mutex_lock(&msg_lock);
				int32_t ret = worker->svrclient.svrtosvr(str, str.length(), index); //cout << "r1 sent" << endl;
				pthread_mutex_unlock(&msg_lock);
			}
			pthread_mutex_unlock (&m_lock);
			//cout << "matrix_server: No. of tasks sent = " << total_submitted << endl;
		}
	}
}

//request the worker with given index to send tasks
int32_t Worker::recv_tasks() {
//cout << " 3";

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
		//cout << "Worker::recv_tasks num_task = " << num_task << endl;
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
int32_t Worker::get_max_load()
{	//cout << " in max load " << endl;
	//max_load_struct* new_max_load;
	int i;
	int32_t max_load = -1000000, load;
	
	/*new_max_load = (max_load_struct*)malloc(sizeof(max_load_struct));
	if(new_max_load == NULL){
        	cout << "Worker::get_max_load: " << strerror(errno) << endl;
                exit(1);
        }*/
	//new_max_load->node_address = (char*)malloc(sizeof(char) * 30);
	//memset(new_max_load->node_address, '\0', 30);
	for(i = 0; i < num_neigh; i++)
	{	//cout << " max load i = " << i << " index = " << neigh_index[i] << endl;
		// Get Load information
		pthread_mutex_lock(&msg_lock);
		load = svrclient.svrtosvr(loadstr, loadstr.length(), neigh_index[i]);
		pthread_mutex_unlock(&msg_lock);
		//cout << "worker = " << selfIndex << " Load = " << load << endl;
		if(load > max_load)
		{
			max_load = load;
			max_loaded_node = neigh_index[i];
		}
	}
	//new_max_load->max_load = max_load;
	//new_max_load->index = max_load_node;
	//strcpy(new_max_load->node_address, neigh_ips[max_load_node]);
	//return new_max_load;
	//cout << "Max load = " << new_max_load->max_load << " index = " << new_max_load->index << endl;
	return max_load;
}

/*
 * Randomly choose neighbors dynamically
 */
void Worker::choose_neigh()
{	
	//time_t t;
	//srand((unsigned)time(&t));
	srand(time(NULL));
	int i, ran;
	int x;
	char *flag = new char[num_nodes];
	//cout << "num nodes = " << num_nodes << " num neigh = " << num_neigh << endl;
	/*for(i = 0; i < num_nodes; i++)
	{
		flag[i] = '0';
	}*/
	memset(flag, '0', num_nodes);
	//cout << "flag array set" << endl;
	for(i = 0; i < num_neigh; i++)
	{
		//cout << " i = " << i << endl;
		ran = rand() % num_nodes;		
		x = hostComp(svrclient.memberList.at(ran), svrclient.memberList.at(selfIndex));
		//cout << "ran a " << ran << " " << selfIndex << " " << x << endl;
		//while(flag[ran] == '1' || !strcmp(all_ips[ran], ip))
		while(flag[ran] == '1' || !x)
		{
			ran = rand() % num_nodes;
			x = hostComp(svrclient.memberList.at(ran), svrclient.memberList.at(selfIndex));
			//cout << "ran = " << ran << " x = " << x << endl;
		}
		flag[ran] = '1';
		//strcpy(neigh_ips[i], all_ips[ran]);
		//cout << "i = " << i << " ran = " << ran << endl;
		neigh_index[i] = ran;
		//cout << "ran b " << ran << " " << selfIndex << " " << x << endl;
	}
	//cout << "neigh index set" << endl;
	delete flag;
}

/*
 * Steal tasks from the neighbors
 */
int32_t Worker::steal_task()
{
	/*vector<struct HostEntity> nodeList = svrclient.memberList;
	int *neigh_index;
	neigh_index = (int*)malloc(sizeof(int) * num_neigh);
	if(neigh_index == NULL){
		cout << "Worker::steal_task: " << strerror(errno) << endl;
		exit(1);
	}
	char flag[num_nodes];
	int i;

	Package loadPackage, tasksPackage;	
	string loadmessage("Load Information!");
	loadPackage.set_virtualpath(loadmessage);
	loadPackage.set_operation(13);
	string loadstr = loadPackage.SerializeAsString();

	stringstream selfIndexstream;
	selfIndexstream << worker.selfIndex;
	string taskmessage(selfIndexstream.str());
	tasksPackage.set_virtualpath(taskmessage);
	tasksPackage.set_operation(14);
	string taskstr = tasksPackage.SerializeAsString();*/

	/*for(i = 0; i < num_neigh; i++)
	{
		neigh_ips[i] = (char*)malloc(sizeof(char) * 30);
		memset(neigh_ips[i], '\0', 30);
	}*/
	//cout << "Entered steal_task" << endl;
	/*if(LOGGING) {
                log_fp << "entered steal_task()" << endl;
        }*/
	choose_neigh();
	//cout << "Done choose_neigh" << endl;
	int32_t max_load = get_max_load();
	/*if(LOGGING) {
                log_fp << "Max loaded node = " << max_loaded_node << endl;
        }*/
	//cout << "Done get_max_load max_load = " << max_load << " index = " << max_loaded_node << endl;
	//int index;
	//max_load_struct* new_max_load;
	//new_max_load = get_max_load(neigh_index, loadstr);
	//cout << index << endl;
	// While all neighbors have no more available tasks,
	// double the "poll_interval" to steal again
	while(max_load <= 0 && work_steal_signal)
	{
		//cout << "workerid = " << selfIndex << " load = " << max_load << " failed attempts = " << failed_attempts << endl;
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
			//cout << worker.selfIndex << " stopping worksteal" << endl;
			return 0;
		}
		/*if(worker.poll_interval == 1024000) {
			return 2;
		}*/
		//cout << "worker.poll_interval = " << worker.poll_interval << endl;
		//choose_neigh(neigh_ips, all_ips, flag);
		//choose_neigh(neigh_index, nodeList, flag);
		//new_max_load  = get_max_load(neigh_index, loadstr);
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
	//cout << "workerid = " << selfIndex << " load = " << max_load << endl;
	//index = new_max_load->index;
	//cout << "Final index  = " << index << endl;
	//return recv_tasks(index, taskstr);
	return recv_tasks();
}

// thread to steal tasks it ready queue length becomes zero
void* worksteal(void* args){
	//cout << "entered worksteal thread" << endl;
	Worker *worker = (Worker*)args;

	/*int min_lines = worker->svrclient.memberList.size();
	//min_lines++;	
        string filename(file_worker_start);
        //string cmd("wc -l ");	
        //cmd = cmd + filename + " | awk {\'print $1\'}";
	
	string cmd("ls -l "); 	cmd.append(shared);	cmd.append("startinfo*");	cmd.append(" | wc -l");
        string result = executeShell(cmd);
	//cout << "server: minlines = " << min_lines << " cmd = " << cmd << " result = " << result << endl;
        while(atoi(result.c_str()) < min_lines) {
		sleep(2);
		 //cout << "server: " << worker.selfIndex << " minlines = " << min_lines << " cmd = " << cmd << " result = " << result << endl;
                result = executeShell(cmd);
        }
	cout << "server: " << worker->selfIndex << " minlines = " << min_lines << " cmd = " << cmd << " result = " << result << endl;*/
	
	int num = worker->svrclient.memberList.size() - 1;
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
	//cout << "worksteal started: server: " << worker->selfIndex << " minlines = " << 1 << " cmd = " << cmd1 << " result = " << result1 << endl;
	
	while(work_steal_signal) {
		//while(ready_queue->get_length() > 0) { }
		while(rqueue.size() > 0) { }
		
		// If there are no waiting ready tasks, do work stealing
		//if (worker.num_nodes > 1 && ready_queue->get_length() < 1)
		if (worker->num_nodes > 1 && rqueue.size() < 1)
		{
			int32_t success = worker->steal_task();			
			// Do work stealing until succeed
			while(success == 0)
			{
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
			//cout << "Received " << success << " tasks" << endl;
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
	//pthread_mutex_lock(&w_lock);
	pthread_mutex_lock(&lock);
	rqueue.push_back(*qi);
	//wqueue.erase(*qi);
	pthread_mutex_unlock(&lock);
	//pthread_mutex_unlock(&w_lock);
}

bool check(TaskQueue_Item *qi) {
	return qi==NULL;
}


void* check_wait_queue(void* args) {
	Worker *worker = (Worker*)args;
	TaskQueue_Item *qi;
        while(ON) {
                while(wqueue.size() > 0) {
			//for(TaskQueue::iterator it = wqueue.begin(); it != wqueue.end(); ++it) {
			int size = wqueue.size();
			for(int i = 0; i < size; i++) {
				//qi = *it;
				qi = wqueue[i];
				if(qi != NULL) {
					int status = worker->check_if_task_is_ready(qi->task_id); //cout << "task = " << qi->task_id << " status = " << status << endl;
					if(status == 0) {
						//cout << "task = " << qi->task_id << " status = " << status << endl;
						int ret = worker->move_task_to_ready_queue(&qi);
						pthread_mutex_lock(&w_lock);
						wqueue[i] = NULL;
						pthread_mutex_unlock(&w_lock);
					}
					/*if(status < 0) {
						cout << "negative numwait" << endl;
					}*/
				}
			}
			pthread_mutex_lock(&w_lock);
			TaskQueue::iterator last = remove_if(wqueue.begin(), wqueue.end(), check);
			wqueue.erase(last, wqueue.end());
			pthread_mutex_unlock(&w_lock);
			sleep(1);
		}
	}
}

static int work_exec_flag = 0;
// thread to monitor ready queue and execute tasks based on num of cores availability
void* check_ready_queue(void* args) {

	Worker *worker = (Worker*)args;

	TaskQueue_Item *qi;
	while(ON) {
		while(rqueue.size() > 0) {
				pthread_mutex_lock(&lock);
					if(rqueue.size() > 0) {						
						qi = rqueue.front();
						rqueue.pop_front();
					}
					else {
						pthread_mutex_unlock(&lock);
						continue;
					}
				
                                pthread_mutex_unlock(&lock);

                                pthread_mutex_lock(&mutex_idle);
                                worker->num_idle_cores--;
                                pthread_mutex_unlock(&mutex_idle);
						
				if(!work_exec_flag) {
					work_exec_flag = 1;
					worker_start << worker->ip << ":" << worker->selfIndex << " Got jobs..Started excuting" << endl;
				}

				//cout << "task to lookup = " << qi->task_id << endl;
				string value = worker->zht_lookup(qi->task_id);
				Package recv_pkg;
			        recv_pkg.ParseFromString(value); //cout << "check_ready_queue: task " << qi->task_id << " node history = " << recv_pkg.nodehistory() << endl;
				int num_vector_count, per_vector_count;
	                        vector< vector<string> > tokenize_string = tokenize(recv_pkg.realfullpath(), '\"', '\'', num_vector_count, per_vector_count);
				//cout << "worker " << worker->selfIndex<< " pertask processing done" << endl;
				/*cout << "task = " << qi->task_id << " notify list: ";
				for(int l = 0; l < tokenize_string.at(1).size(); l++) {
					cout << tokenize_string.at(1).at(l) << " ";
				} cout << endl;*/
				
				stringstream duration_ss;
				try {
					duration_ss <<  tokenize_string.at(0).at(1);
				}
				catch (exception& e) {
					cout << "void* check_ready_queue: num_vector_count = " << num_vector_count << " per_vector_count = " << per_vector_count << endl;
                                        cout << "void* check_ready_queue: (tokenize_string.at(0).at(1)) " << " " << e.what() << endl;
					cout << "void* check_ready_queue: value = " << value << endl;
                                        exit(1);
                                }
				long duration;
				//duration = 1000000;
				duration_ss >> duration;

				string client_id;
				try {
					client_id = tokenize_string.at(0).at(2);
				}
				catch (exception& e) {
                                        cout << "void* check_ready_queue: num_vector_count = " << num_vector_count << " per_vector_count = " << per_vector_count << endl;
                                        cout << "void* check_ready_queue: (tokenize_string.at(0).at(2)) " << " " << e.what() << endl;
                                        exit(1);
                                }
                 
				uint64_t sub_time;
				try {
					stringstream sub_time_ss;
					sub_time_ss << tokenize_string.at(0).at(3);
					sub_time_ss >> sub_time;
				}
				catch (exception& e) {
                                        cout << "void* check_ready_queue: num_vector_count = " << num_vector_count << " per_vector_count = " << per_vector_count << endl;
                                        cout << "void* check_ready_queue: (tokenize_string.at(0).at(3)) " << " " << e.what() << endl;
                                        exit(1);
                                }
				
				timespec task_start_time, task_end_time;
				clock_gettime(CLOCK_REALTIME, &task_start_time);
				//uint32_t exit_code = sleep(duration);
				uint32_t exit_code = usleep(duration);
				clock_gettime(CLOCK_REALTIME, &task_end_time);
				
				// push completed task into complete queue
				pthread_mutex_lock(&c_lock);
				cqueue.push_back(make_pair(qi->task_id, tokenize_string.at(1)));
				pthread_mutex_unlock(&c_lock);
						
				// append completed task
				uint64_t st = (uint64_t)task_start_time.tv_sec * 1000000000 + (uint64_t)task_start_time.tv_nsec;
				uint64_t et = (uint64_t)task_end_time.tv_sec * 1000000000 + (uint64_t)task_end_time.tv_nsec;
				timespec diff = timediff(task_start_time, task_end_time);
						
				pthread_mutex_lock(&mutex_idle);
				worker->num_idle_cores++; task_comp_count++;
				pthread_mutex_unlock(&mutex_idle);
						
				if(LOGGING) {
					string fin_str;
					stringstream out;
					out << qi->task_id << "+" << client_id << " exitcode " << " node history = " << recv_pkg.nodehistory() << exit_code << " Interval " << diff.tv_sec << " S  " << diff.tv_nsec << " NS" << " server " << worker->ip;
					fin_str = out.str();
					pthread_mutex_lock(&mutex_finish);
					fin_fp << fin_str << endl;
					pthread_mutex_unlock(&mutex_finish);
				}						
				delete qi;
		}
	}
}

int Worker::notify(ComPair &compair) {
	map<uint32_t, NodeList> update_map = worker->get_map(compair.second);
	int update_ret = worker->zht_update(update_map, "numwait", selfIndex);
	nots += compair.second.size(); log_fp << "nots = " << nots << endl;
	return update_ret;
	/*cout << "task = " << compair.first << " notify list: ";
	for(int l = 0; l < compair.second.size(); l++) {
		cout << compair.second.at(l) << " ";
	} cout << endl;*/
}

void* check_complete_queue(void* args) {
	Worker *worker = (Worker*)args;

        ComPair compair;
        while(ON) {
                while(cqueue.size() > 0) {
                                pthread_mutex_lock(&c_lock);
                                        if(cqueue.size() > 0) {
                                                compair = cqueue.front();
                                                cqueue.pop_front();
                                        }
                                        else {
                                                pthread_mutex_unlock(&c_lock);
                                                continue;
                                        }
                                pthread_mutex_unlock(&c_lock);
				worker->notify(compair);
		}
	}
}

int32_t Worker::get_load_info() {
	return (rqueue.size() - num_idle_cores);
}

int32_t Worker::get_monitoring_info() {
	if (LOGGING) {
		log_fp << "rqueue = " << rqueue.size() << " mqueue = " << mqueue.size() << " wqueue = " << wqueue.size() << endl;
	}
	return (((rqueue.size() + mqueue.size() + wqueue.size()) * 10) + num_idle_cores);
}

int32_t Worker::get_numtasks_to_steal() {
	return ((rqueue.size() - num_idle_cores)/2);
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
			cout << "lookup find nothing. key = " << key << endl;
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

	if(index != selfIndex) {
                pthread_mutex_lock(&msg_lock);
                int ret = svrclient.insert(str);
                pthread_mutex_unlock(&msg_lock);
		return ret;
        }
	else {
		string key = package.virtualpath();
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
			if(LOGGING) {
				if(old_numwait-1 == 0) {
					log_fp << "task = " << taskid << " is ready" << endl;
				}
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


