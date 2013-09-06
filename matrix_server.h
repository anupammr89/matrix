#ifndef MATRIX_SERVER_H_
#define MATRIX_SERVER_H_

#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <cstring>
#include <sstream>
#include <math.h>
#include <errno.h>
#include <algorithm>
#include "cpp_zhtclient.h"
#include "matrix_client.h"
#include "matrix_util.h"
#include <queue>
#include <deque>
#include <vector>
#include <map>

#define SIXTY_KILOBYTES 61440
#define STRING_THRESHOLD SIXTY_KILOBYTES

#define INITIAL_WAIT_TIME 1000
#define WAIT_THRESHOLD 1000000
#define MID_THRESH 500
#define MID_WAIT_TIME 100000
#define MID_THRESHOLD 10000000
#define FAIL_THRESHOLD 100

#define NUM_COMPUTE_SLOTS 4

typedef deque<TaskQueue_Item*> TaskQueue;
typedef list<TaskQueue_Item*> WaitQueue;
typedef deque<string> NodeList;
typedef pair<string, vector<string> > ComPair;
typedef deque<ComPair> CompQueue;

//typedef list<TaskQueue_Item*> TaskQueue;
//typedef list<string> NodeList;

using namespace std;

class Worker {
public:

	Worker();
	Worker(char *parameters[], NoVoHT *novoht);
	virtual ~Worker();

	string ip;

	int num_nodes;		// Number of computing nodes of Matrix
	int num_cores;		// Number of cores of a node
	int num_idle_cores;	// Number of idle cores of a node
	char neigh_mode;	// neighbors selection mechanism: 'static' or 'dynamic random'
	int num_neigh;		// Number of neighbors of a node
	int *neigh_index;	// Array to hold neighbors index for which to poll load
	int max_loaded_node;	// Neighbor index from which to steal task
	int selfIndex;		// Self Index in the membership list
	long poll_interval;	// The poll interval value for work stealing
	long poll_threshold;// Threshold beyond which polling interval should not double
	ZHTClient svrclient;	// for server to server communication
	
	int execute(TaskQueue_Item *qi);

	int recv_tasks();
	int32_t get_max_load();
	void choose_neigh();
	int steal_task();
	int32_t get_load_info();
	int32_t get_monitoring_info();
	int32_t get_numtasks_to_steal();

	int32_t get_ret_status(string client_id);

	int check_if_task_is_ready(string key);
	int move_task_to_ready_queue(TaskQueue_Item **qi);
	int move_task_to_ready_queue(string key, int index);
	int move(string key);

	int notify(ComPair &compair);
	int notify(string key);
	int zht_ins_mul(Package &package);

	uint32_t get_task_desc(string key);
	string zht_lookup(string key);
	int zht_insert(string str);
	int zht_remove(string key);
	int zht_append(string str);
	int zht_update(map<uint32_t, NodeList> &update_map, string field, uint32_t toid);

	int update(Package &package);
	int update_nodehistory(uint32_t currnode, string alltasks);
	int update_numwait(string alltasks);

	map<uint32_t, NodeList> get_map(TaskQueue &queue);
	map<uint32_t, NodeList> get_map(vector<string> &mqueue);
};

extern Worker *worker;
extern int ON;

extern ofstream fin_fp;
extern ofstream log_fp;
#define NUM_MSG 20
extern long msg_count[NUM_MSG];

extern long task_comp_count;

extern queue<string*> zht_ins;
extern queue<string*> waitq;
extern queue<string*> insertq;
extern queue<int*> migrateq;
extern bitvec migratev;

extern queue<string*> notifyq;

extern pthread_mutex_t zht_ins_lock;
extern pthread_mutex_t waitq_lock;
extern pthread_mutex_t iq_lock;
extern pthread_mutex_t mq_lock;
extern pthread_mutex_t notq_lock;

extern pthread_cond_t zhtinsqueue_notempty;
extern pthread_cond_t insertqueue_notempty;
extern pthread_cond_t notqueue_notempty;

//void work_steal_init(char *parameters[], NoVoHT *pmap);
void* worksteal(void* args);
void* check_wait_queue(void* args);
void* execute_thread(void* args);
void* check_complete_queue(void* args);

void* zht_ins_queue(void* args);
void* QueueInsert(void* args);
void* migrateTasks(void* args);
void* notQueue(void* args);

vector< vector<string> > tokenize(string input, char delim1, char delim2, int &num_vector, int &per_vector_count);

extern pthread_attr_t attr;
#endif 
