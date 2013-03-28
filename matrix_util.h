#ifndef MATRIX_UTIL_H_
#define MATRIX_UTIL_H_

#include <stropts.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <linux/netdevice.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <unistd.h>

#include <string>
#include <cstring>
#include <sstream>
#include <fstream>
#include <iostream>
#include <stdio.h>

#include <pthread.h>

#include <ctime>

#include "cpp_zhtclient.h"
#include <map>


class Mutex
{
private:
	pthread_mutex_t mutex;
public:
	Mutex();
	virtual ~Mutex();
	int Lock();
	int Unlock();
};

class TaskQueue_Item
{
public:
	/*uint32_t task_id;	
	string client_id;
	string task_description;
	uint32_t num_moves;
	uint64_t submission_time;
	TaskQueue_Item *next;*/

	string task_id;
	//uint32_t num_moves;

	TaskQueue_Item();
	virtual ~TaskQueue_Item();
};


class TaskQueue
{
public:
	TaskQueue_Item *head;
	TaskQueue_Item *tail;
	long TaskQueue_length;
	Mutex mutex;
	
	TaskQueue();
	virtual ~TaskQueue();

	long get_length();
	TaskQueue_Item* get_exec_desc();
	void add_element(TaskQueue_Item* qi);
	TaskQueue_Item* remove_element();
	long remove_n_elements(long num_tasks, TaskQueue* migrateq);
};

class bitvec {
public:
        vector<bool> bitvector;
        map<uint32_t, uint32_t> hmap;

        bitvec();
        bitvec(int size);
        virtual ~bitvec();
        bool any();
        bool none();
        uint32_t count();
        uint32_t size();
        bool at(int i);
        void set(int i);
        void set_all();
        void reset(int i);
        void reset_all();
        void dec(int i);
        void dec_all();
        bool test(int i);
        void flip();
        void flip(int i);
        void print();

        uint32_t pop();
        uint32_t first_set();
};

class Env_var {

public:

	Env_var();
	virtual ~Env_var();
	
	static char* LISTEN_PORT;
	static string membershipFile;
	static string cfgFile;
	static char* isTCP;
	static char* userName;
	static bool TCP;
	static int num_tasks;
	static void set_env_var(char *parameters[]);
};

extern int LOGGING;
extern int max_tasks_per_package;

extern string prefix;
extern string shared;

void set_dir(string p, string s);
int get_max_tasks_per_package();
timespec timediff(timespec start, timespec end);
int getSelfIndex(string hostName, vector<struct HostEntity> memberList);
int getSelfIndex(string hostName, int port, vector<struct HostEntity> memberList);
int get_local_ip(char* out_ip);
int set_ip(string &ip);
int hostComp(struct HostEntity a, struct HostEntity b);

#endif
