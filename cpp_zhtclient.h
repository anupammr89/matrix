#ifndef CPP_ZHTCLIENT_H_
#define CPP_ZHTCLIENT_H_

#include "zht_util.h"
#include "lru_cache.h"

class ZHTClient {
public:
	ZHTClient();
	virtual ~ZHTClient();

	bool TCP;
	int REPLICATION_TYPE; //serverside or client side. -1:error
	int NUM_REPLICAS; //-1:error
	int WORK_STEAL;
	int protocolType; //1:1TCP; 2:UDP; 3.... Reserved.  -1:error
	vector<struct HostEntity> memberList;

	int initialize(string configFilePath, string memberListFilePath, bool tcp);
	struct HostEntity str2Host(string str);
	struct HostEntity str2Host(string str, int &index);
	int str2Sock(string str);
	int str2SockLRU(string str, bool tcp);
	int insert(string str); //following functions should only know string, address where to can be calculated.
	int insert(string str, int sock); // only for test
	int lookup(string str, string &returnStr);
	int lookup(string str, string &returnStr, int sock); // only for test
	int append(string str); //operation num = 4
	int remove(string str);
	int tearDownTCP(); //only for TCP

	// Matrix client functions
	struct HostEntity index2Host(int index);
	int index2SockLRU(int index, bool tcp);
	int32_t send(string str, int size); 		   // general send
	int32_t svrtosvr(string str, int size, int index); // send to specific server
	int task_lookup(string str, int size, int index, string &returnStr); // get task description

private:
	static int UDP_SOCKET;
	static int CACHE_SIZE;
	static LRUCache<string, int> CONNECTION_CACHE;
};

void print(Package &package);
int copystring(char *c_str, string str);

#endif
