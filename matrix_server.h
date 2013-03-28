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
#include "cpp_zhtclient.h"
#include "matrix_client.h"
#include "matrix_util.h"
#include <queue>
#include <deque>
#include <vector>
#include <map>

#define SIXTY_KILOBYTES 61440
#define STRING_THRESHOLD SIXTY_KILOBYTES

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
	
	int recv_tasks();
	int32_t get_max_load();
	void choose_neigh();
	int steal_task();
	int32_t get_load_info();
	int32_t get_monitoring_info();
	int32_t get_numtasks_to_steal();
	string zht_lookup(string key);
	int zht_insert(string str);
	int zht_remove(string key);
	int zht_append(string str);
	int zht_update(map<uint32_t, deque<string> > &update_map, string field, uint32_t toid);

	int update(Package &package);
	int update_nodehistory(uint32_t currnode, string alltasks);
	int update_numwait(string alltasks);

	map<uint32_t, deque<string> > get_map(deque<TaskQueue_Item*> &queue);
};

extern Worker *worker;
extern int ON;

extern ofstream fin_fp;
extern ofstream log_fp;
extern long msg_count[10];

extern long task_comp_count;

//extern queue<string*> insertq;
extern queue<string*> insertq_new;
extern queue<int*> migrateq;
extern bitvec migratev;

//extern pthread_mutex_t iq_lock;
extern pthread_mutex_t iq_new_lock;
extern pthread_mutex_t mq_lock;

//void work_steal_init(char *parameters[], NoVoHT *pmap);
void* worksteal(void* args);
void* check_ready_queue(void* args);

void* HB_insertQ(void* args);
void* HB_insertQ_new(void* args);
void* HB_localinsertQ(void* args);
void* migrateTasks(void* args);

vector< vector<string> > tokenize(string input, char delim1, char delim2, int &num_vector, int &per_vector_count);

extern pthread_attr_t attr;
#endif 
