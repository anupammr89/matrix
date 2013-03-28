#include <string>
#include <cstring>
#include <iostream>
#include <sstream>
#include <fstream>

#include <algorithm>
#include <signal.h>
#include <sys/time.h>
#include <stdlib.h>
#include <stdio.h>
#include <list>
#include <vector>
#include <netdb.h>
#include <pthread.h>
#include <error.h>

#include "cpp_zhtclient.h"

#include "matrix_client.h"

using namespace std;

struct thread_data {
	vector<struct HostEntity> hostList;
	list<string> packageList;
};
vector<struct HostEntity> hostList;
//list<string> myPackagelist;
bool TCP;
int UDP_SOCKET = -1;
int CACHE_SIZE = 1024;

void sig_pipe(int signum) {
	printf("SIGPIPE Caught!\n");
	signal(SIGPIPE, sig_pipe);
}

//======================================ZHT configuration function===================================================================



int initconfig(ZHTClient &clientRet, string cfgFile, string memberList) {
	if (clientRet.initialize(cfgFile, memberList, TCP) != 0) {
		cout << "Crap! ZHTClient initialization failed, program exits." << endl;
		return -1;
	}

	return 0;
}



//====================================================================================================================================


int benchmarkInsert(string cfgFile, string memberList, vector<string> &pkgList,
		ZHTClient &clientRet, int numTest, int lenString) {

//	if (clientRet.initialize(cfgFile, memberList) != 0) { //zhouxb
	if (clientRet.initialize(cfgFile, memberList, TCP) != 0) {
		cout << "Crap! ZHTClient initialization failed, program exits." << endl;
		return -1;
	}

	for (int i = 0; i < numTest; i++) {
		Package package, package_ret;
		package.set_virtualpath(randomString(lenString)); //as key
		package.set_isdir(true);
		package.set_replicano(5); //orginal--Note: never let it be nagative!!!
		package.set_operation(3); // 3 for insert, 1 for look up, 2 for remove
		package.set_realfullpath(
				"Some-Real-longer-longer-and-longer-Paths--------");
		package.add_listitem("item-----2");
		package.add_listitem("item-----3");
		package.add_listitem("item-----4");
		package.add_listitem("item-----5");
		package.add_listitem("item-----6");
		string str = package.SerializeAsString();
//		cout << "package size = " << str.size() << endl;
//		cout<<"Client.cpp:benchmarkInsert: "<<endl;
//		cout<<"string: "<<str<<endl;
//		cout<<"Insert str: "<<str.c_str()<<endl;
//		cout<<"data(): "<< str.data()<<endl;
		pkgList.push_back(str);
	}
	double start = 0;
	double end = 0;
	start = getTime_msec();
	int errCount = 0;
	vector<string>::iterator it;
	int c = 0;
//	cout << "-----2" << endl;
	for (it = pkgList.begin(); it != pkgList.end(); it++) {
		//	cerr <<"insert count "<< c << endl;
		c++;
		string str_ins = *it;
//cout << "-----1" << endl;
		int ret = clientRet.insert(str_ins);
//cout << "-----2" << endl;
		if (ret < 0) {
			errCount++;
		}
	}
//close(sock);
	end = getTime_msec();
	cout << "Inserted " << numTest - errCount << " packages out of " << numTest
			<< ", cost " << end - start << " ms" << endl;
	return 0;
}

//pretty much same as benchmarkInsert.
int benchmarkAppend(ZHTClient &client, int numTest, int lenString) {
//	cout << "start to append"<<endl;
//      if (clientRet.initialize(cfgFile, memberList) != 0) { //zhouxb
	vector<string> pkgList_append;
	for (int i = 0; i < numTest; i++) {
		Package package, package_ret;
		package.set_virtualpath(randomString(lenString).append("-append")); //as key
		package.set_isdir(true);
		package.set_replicano(5); //orginal--Note: never let it be nagative!!!
		package.set_operation(4); // 3 for insert, 1 for look up, 2 for remove
		package.set_realfullpath(
				"Some-Real-longer-longer-and-longer-Paths--------");
		package.add_listitem("item-----2");
		package.add_listitem("item-----3");
		package.add_listitem("item-----4");
		package.add_listitem("item-----5");
		package.add_listitem("item-----6");
		string str = package.SerializeAsString();
//              cout << "package size = " << str.size() << endl;
//              cout<<"Client.cpp:benchmarkInsert: "<<endl;
//              cout<<"string: "<<str<<endl;
//              cout<<"Insert str: "<<str.c_str()<<endl;
//              cout<<"data(): "<< str.data()<<endl;
		pkgList_append.push_back(str);
	}
	double start = 0;
	double end = 0;
	start = getTime_msec();
	int errCount = 0;
	vector<string>::iterator it;
	int c = 0;
//cout << "-----0" << endl;
	for (it = pkgList_append.begin(); it != pkgList_append.end(); it++) {
		//      cerr <<"insert count "<< c << endl;
		c++;
		string str_ins = *it;
//cout << "-----1:about to append" << endl;
//	sleep(1);
		int ret = client.append(str_ins);
		if (ret != 0)
			cout << "client: append ret = " << ret << endl;
//cout << "-----2" << endl;
		if (ret < 0) {
			errCount++;
		}
	}
//close(sock);
	end = getTime_msec();
	cout << "Appended " << numTest - errCount << " packages out of " << numTest
			<< ", cost " << end - start << " ms" << endl;
	return 0;
}

int benmarkTimeAnalize(string cfgFile, string memberList,
		vector<string> &pkgList, ZHTClient &clientRet, int numTest,
		int lenString, string Recordpath) {
	ZHTClient client;

//	if (client.initialize(cfgFile, memberList) != 0) {
	if (client.initialize(cfgFile, memberList, TCP) != 0) {
		cout << "Crap! ZHTClient initialization failed, program exits." << endl;
		return -1;
	}

	double timeRecord[numTest]; //={0};
//	bzero(timeRecord, sizeof(timeRecord));
	clientRet = client; //reserve this client object for other benchmark(lookup/remove) to use.

	//vector<string> pkgList;
	int i = 0;
	for (i = 0; i < numTest; i++) {
		Package package, package_ret;
		package.set_virtualpath(randomString(lenString)); //as key
		package.set_isdir(true);
		package.set_replicano(5); //orginal--Note: never let it be nagative!!!
		package.set_operation(3); // 3 for insert, 1 for look up, 2 for remove
		package.set_realfullpath(
				"Some-Real-longer-longer-and-longer-Paths--------");
		package.add_listitem("item-----1");
		package.add_listitem("item-----2");
		package.add_listitem("item-----3");
		package.add_listitem("item-----4");
		package.add_listitem("item-----5");
		string str = package.SerializeAsString();
//		cout << "package size = " << str.size() << endl;
//		cout<<"Client.cpp:benchmarkInsert: "<<endl;
//		cout<<"string: "<<str<<endl;
//		cout<<"Insert str: "<<str.c_str()<<endl;
//		cout<<"data(): "<< str.data()<<endl;

		pkgList.push_back(str);
	}

	double start = 0;
	double end = 0;
	double istart = 0;
	double iend = 0;
	int errCount = 0;
	vector<string>::iterator it;
	int c = 0;

	ofstream record;
	record.open(Recordpath.c_str());

	start = getTime_msec();
	for (it = pkgList.begin(); it != pkgList.end(); it++) {
//		cout<<c<<endl;
		c++;
		double interval = 0;
		istart = getTime_usec();
		int op_ret = client.insert((*it));
		iend = getTime_usec();

		if (op_ret < 0) {
			errCount++;
			interval = -1;
		} else
			interval = iend - istart;
		record << interval << endl;
		timeRecord[c] = interval;

	}
	end = getTime_msec();
	record.close();

	cout << "Inserted " << numTest - errCount << " packages out of " << numTest
			<< ", cost " << end - start << " ms" << endl;

	return 0;
}

float benchmarkLookup(vector<string> strList, ZHTClient &client) {
	vector<string>::iterator it;

	double start = 0;
	double end = 0;
	start = getTime_msec();
	int errCount = 0;
//cout << "Client: benchmarkLookup: start lookup \n";
	int c = 0;
	for (it = strList.begin(); it != strList.end(); it++) {
		string result;
//		cout << c << endl;
		c++;
//		cout<<"lookup: "<<c<<endl;
//sleep(1);
//
//		cout << "Client: What I want to find: \n";
//		cout <<"Lookup: "<< (*it).c_str() << endl;
		if (client.lookup((*it), result) < 0) {

			errCount++;
		} else if (result.empty()) { //empty string
			errCount++;
		}
//		cout << "Client: What I get: ";
//		cout << result.c_str() << endl;
	}

	end = getTime_msec();

	cout << "Lookup " << strList.size() - errCount << " packages out of "
			<< strList.size() << ", cost " << end - start << " ms" << endl;
	return 0;
}

float benchmarkRemove(vector<string> strList, ZHTClient &client) {
	vector<string>::iterator it;

	for (it = strList.begin(); it != strList.end(); it++) {
		Package package;
		package.ParseFromString((*it));
		package.set_operation(2); // 3 for insert, 1 for look up, 2 for remove
		package.set_replicano(5); //5: original, 3 not original

		strList.erase(it);
		string newStr = package.SerializeAsString();
		strList.push_back(newStr);
	}

	double start = 0;
	double end = 0;
	start = getTime_msec();
	int errCount = 0;
	int c = 0;
	for (it = strList.begin(); it != strList.end(); it++) {
		string result;
		c++;
//		cout <<"Remove count "<< c << endl;

//		cout <<"Remove: "<< (*it).c_str() << endl;
		if (client.remove((*it)) < 0) {
			errCount++;
		}

	}

	end = getTime_msec();

	cout << "Remove " << strList.size() - errCount << " packages out of "
			<< strList.size() << ", cost " << end - start << " ms" << endl;
	return 0;
}
//This is an example.

int benchmarkALL(int numTest, int strLen) { //103+length
//	int para = strLen - 128;
	return 0;
}

int main(int argc, char* argv[]) {

	if(argc != 12) {
        	cout << "Usage: " << argv[0] << "\tper_client_task\tneighborlist_file\tzht_config_file\tprotocol[TCP/UDP]\tsleep_time\tsubmissionmode[0/1/2]\tmax_tasks_per_package\tindex\tnum_tasks\tprefix\tshared" << endl;
                exit(1);
        }
	srand(getpid() + getTime_usec());

	ZHTClient testClient;
	MATRIXClient mc;
	
	int per_client_task = atoi(argv[1]);
	string memberList(argv[2]);
	string cfgFile(argv[3]);
	char* isTCP = argv[4];
	if (strcmp("TCP", isTCP) == 0) {
		TCP = true;
	} else {
		TCP = false;
	}
	int sleep_time = atoi(argv[5]);
	int mode = atoi(argv[6]);
	int max_tasks_per_package = atoi(argv[7]);
	int index = atoi(argv[8]);
	int num_tasks = atoi(argv[9]);
	int logging = 1;
	initconfig(testClient, cfgFile, memberList);
	int num_workers = testClient.memberList.size();

	set_dir(argv[10], argv[11]);
	//initialze the client
	mc.init(num_tasks, sleep_time, testClient, logging, index);

	//initialze the tasks
	//mc.initializeTasks(per_client_task, sleep_time, mode, max_tasks_per_package, testClient);
//cout << " 1 ";
	int min_lines = num_workers;
	//min_lines++;
	string filename(shared);
        filename = filename + "startinfo*";
        string cmd("ls -l ");
        cmd = cmd + filename + " | wc -l";
//cout << cmd << endl;
        string result = executeShell(cmd);
	//cout << "bench: minlines = " << min_lines << " cmd = " << cmd << " result = " << result << endl;
	/*string filename(shared);
	filename = filename + "start_info";
        string cmd("wc -l ");
        cmd = cmd + filename + " | awk {\'print $1\'}";
        string result = executeShell(cmd); */
	//cout << "bench: minlines = " << min_lines << " cmd = " << cmd << " result = " << result << endl;
        while(atoi(result.c_str()) < min_lines) {
	       sleep(2); 
		//cout << "bench: minlines = " << min_lines << " cmd = " << cmd << " result = " << result << endl;
               result = executeShell(cmd);
        }
	
	//cout << "bench: minlines = " << min_lines << " cmd = " << cmd << " result = " << result << endl;
	//mc.submitTasks(mode, testClient);
	pthread_t monitor_thread;
	if(index == 1) {
		monitor_thread = mc.monitor(num_tasks, testClient);
	}
	mc.initializeTasks(per_client_task, sleep_time, mode, max_tasks_per_package, testClient);
	//mc.submitTasks(mode, testClient);
	if(index == 1){
		pthread_join(monitor_thread, NULL);
	}

	return 0;

}

