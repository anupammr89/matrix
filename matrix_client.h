#ifndef MATRIX_CLIENT_H_
#define MATRIX_CLIENT_H_

#include <iostream>
#include <map>
#include <vector>
#include <string>
#include <sstream>
#include <time.h>
#include <stdlib.h>
#include <math.h>
#include "matrix_util.h"

typedef map<int, vector<int> > AdjList; 	// vertex ---> list of vertices
typedef map<int, int> InDegree; 		// vertex ---> Indegree of the vertex
typedef map<int, string> AdjListString; 	// vertex ---> list of vertices delimited by \' with a final \"
typedef pair<int, string> TaskDAG_Value;	// mapping info of each vertex in DAG
typedef map<int, TaskDAG_Value> TaskDAG;	// vertex ---> indegree of vertex, adjlist in string format as above

void get_adjlist(int num_tasks, AdjList &adj_list, int DAG_choice);
void print_AdjList(AdjList &adj_list);
int get_DAG(AdjList &adj_list, TaskDAG &dag, string clientid);
void print_DAG(TaskDAG &dag);
TaskDAG generate_DAG(int &num_tasks, int &num_nodes, string clientid, int DAG_choice);

class MATRIXClient {

public:

	MATRIXClient();
	virtual ~MATRIXClient();

	string client_id;
	uint32_t selfindex;
	uint32_t total_tasks;
	
	vector<string> task_str_list; 

	int init(int num_tasks, int numSleep, ZHTClient &clientRet, int log, int index);	
	int initializeTasks(int num_tasks, int numSleep, int max_tasks_per_package, ZHTClient &clientRet, int DAG_choice);
	pthread_t monitor(int num_tasks, ZHTClient &clientRet);
};

#endif
