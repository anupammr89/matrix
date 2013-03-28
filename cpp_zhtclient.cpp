#include "cpp_zhtclient.h"
#include <stdint.h>
#include <sstream>

//static pthread_mutex_t msg_lock = PTHREAD_MUTEX_INITIALIZER;

int ZHTClient::UDP_SOCKET = -1;
int ZHTClient::CACHE_SIZE = 1024;
LRUCache<string, int> ZHTClient::CONNECTION_CACHE = LRUCache<string, int>(
		ZHTClient::CACHE_SIZE);

ZHTClient::ZHTClient() { // default all invalid value, so the client must be initialized to set the variables.
	//Since constructor can't return anything, we must have an initialization function that can return possible error message.
	this->NUM_REPLICAS = -1;
	this->REPLICATION_TYPE = -1;
	this->protocolType = -1;
}

ZHTClient::~ZHTClient() {
}

int ZHTClient::initialize(string configFilePath, string memberListFilePath,
		bool tcp) {

	this->TCP = tcp;
	//read cfg file
	this->memberList = getMembership(memberListFilePath);

	FILE *fp;
	char line[100], *key, *svalue;
	int ivalue;

	fp = fopen(configFilePath.c_str(), "r");
	if (fp == NULL) {
		cout << "Error opening the config file." << endl;
		return -1;
	}
	while (fgets(line, 100, fp) != NULL) {
		key = strtok(line, "=");
		svalue = strtok(NULL, "=");
		ivalue = atoi(svalue);

		if ((strcmp(key, "REPLICATION_TYPE")) == 0) {
			this->REPLICATION_TYPE = ivalue;
			//cout<<"REPLICATION_TYPE = "<< REPLICATION_TYPE <<endl;
		} //other config options follow this way(if).

		else if ((strcmp(key, "NUM_REPLICAS")) == 0) {
			this->NUM_REPLICAS = ivalue + 1; //note: +1 is must
			//cout<<"NUM_REPLICAS = "<< NUM_REPLICAS <<endl;
		} 

		else if ((strcmp(key, "WORK_STEAL")) == 0) {
			this->WORK_STEAL = ivalue;
			//cout<<"WORK_STEAL = "<< WORK_STEAL <<endl;			
		} else {
			cout << "Config file is not correct." << endl;
			return -2;
		}

	}

	return 0;

}


//============================Anupam==========================================================================================
//transfer a key to a host index where it should go
struct HostEntity ZHTClient::index2Host(int index) {
	
	struct HostEntity host = this->memberList.at(index);
	//cout << " Index = " << index << " host = " << host.port << endl;
	return host;
}

int ZHTClient::index2SockLRU(int index, bool tcp) {
        struct HostEntity dest = this->index2Host(index);

	stringstream ss;
	ss << dest.host;
	ss << ":";
	ss << dest.port;
	string key = ss.str();

	int sock = 0;
	if (tcp == true) {
		sock = CONNECTION_CACHE.fetch(key, tcp);
		if (sock <= 0) {
//			cout << "host not found in cache, making connection..." << endl;
			sock = makeClientSocket(dest.host.c_str(), dest.port, tcp);
//			cout << "created sock = " << sock << endl;
			if (sock <= 0) {
				cerr << "Client insert:making connection failed." << endl;
				return -1;
			} else {
				int tobeRemoved = -1;
				CONNECTION_CACHE.insert(key, sock, tobeRemoved);
				if (tobeRemoved != -1) {
//					cout << "sock " << tobeRemoved	<< ", will be removed, which shouldn't be 0."<< endl;
					close(tobeRemoved);
				}
			}
		} //end if sock<0
	} else { //UDP

		if (UDP_SOCKET <= 0) {
			sock = makeClientSocket(dest.host.c_str(), dest.port, TCP);
			UDP_SOCKET = sock;
		} else
			sock = UDP_SOCKET;
	}

	return sock;
}

//=====================================================================================================================================

//transfer a key to a host index where it should go
struct HostEntity ZHTClient::str2Host(string str) {
	Package pkg;
	pkg.ParseFromString(str);
	int index = myhash(pkg.virtualpath().c_str(), this->memberList.size());
	struct HostEntity host = this->memberList.at(index);

	return host;
}

struct HostEntity ZHTClient::str2Host(string str, int &index) {
	Package pkg;
	pkg.ParseFromString(str);
	int index_inner = myhash(pkg.virtualpath().c_str(),
			this->memberList.size());
	struct HostEntity host = this->memberList.at(index_inner);
	index = index_inner;
	return host;
}

//This store all connections
/*
 int ZHTClient::str2Sock(string str) { //give socket and update the vector of network entity
 int sock = 0;
 int index = -1;
 struct HostEntity dest = this->str2Host(str, index);
 cout<<"str2Sock: dest.sock = "<<dest.sock<<endl;
 if (dest.sock < 0) {
 sock = makeClientSocket(dest.host.c_str(), dest.port, 1);
 reuseSock(sock);
 dest.sock = sock;
 this->memberList.erase(this->memberList.begin() + index);
 this->memberList.insert(this->memberList.begin() + index, dest);

 }

 cout<<"str2Sock: after update: sock = "<<this->str2Host(str).sock<<endl;
 return dest.sock;
 }
 */
int ZHTClient::str2Sock(string str) { //give socket and update the vector of network entity
	int sock = 0;
	int index = -1;
	struct HostEntity dest = this->str2Host(str, index);
	if (TCP == true) {
		//	int sock = 0;
//		int index = -1;
//		struct HostEntity dest = this->str2Host(str, index);
//		cout<<"str2Sock: dest.sock = "<<dest.sock<<endl;
		if (dest.sock < 0) {
			sock = makeClientSocket(dest.host.c_str(), dest.port, TCP);
			reuseSock(sock);
			dest.sock = sock;
			this->memberList.erase(this->memberList.begin() + index);
			this->memberList.insert(this->memberList.begin() + index, dest);
		}
//		cout<<"str2Sock: after update: sock = "<<this->str2Host(str).sock<<endl;
		return dest.sock;
	} else { //UDP
		if (UDP_SOCKET < 0) {
			UDP_SOCKET = makeClientSocket(dest.host.c_str(), dest.port, TCP);
		}
		return UDP_SOCKET;

	}

}

//This store limited connections in a LRU cache, one item is one host VS one sock
int ZHTClient::str2SockLRU(string str, bool tcp) {
	struct HostEntity dest = this->str2Host(str);

	stringstream ss;
	ss << dest.host;
	ss << ":";
	ss << dest.port;
	string key = ss.str();

	int sock = 0;
	if (tcp == true) {
		sock = CONNECTION_CACHE.fetch(key, tcp);
		if (sock <= 0) {
//			cout << "host not found in cache, making connection..." << endl;
			sock = makeClientSocket(dest.host.c_str(), dest.port, tcp);
//			cout << "created sock = " << sock << endl;
			if (sock <= 0) {
				cerr << "Client insert:making connection failed." << endl;
				return -1;
			} else {
				int tobeRemoved = -1;
				CONNECTION_CACHE.insert(key, sock, tobeRemoved);
				if (tobeRemoved != -1) {
//					cout << "sock " << tobeRemoved	<< ", will be removed, which shouldn't be 0."<< endl;
					close(tobeRemoved);
				}
			}
		} //end if sock<0
	} else { //UDP

		if (UDP_SOCKET <= 0) {

			sock = makeClientSocket(dest.host.c_str(), dest.port, TCP);
			UDP_SOCKET = sock;
		} else {

			sock = UDP_SOCKET;
		}
	}

	return sock;
}

int ZHTClient::tearDownTCP() {
	if (TCP == true) {
		int size = this->memberList.size();
		for (int i = 0; i < size; i++) {
			struct HostEntity dest = this->memberList.at(i);
			int sock = dest.sock;
			if (sock > 0) {
				close(sock);
			}
		}
	}
	return 0;
}

//send a plain string to destination, receive return state.
int ZHTClient::insert(string str) {

	/*	int sock = -1;

	 struct HostEntity dest = this->str2Host(str);

	 //	cout<<"Client: will send to "<<dest.host<<endl;
	 //int ret = simpleSend(str, dest, sock);

	 sock = makeClientSocket(dest.host.c_str(), dest.port, 1);
	 //	cout<<"client sock = "<< sock<<endl;
	 reuseSock(sock);
	 generalSendTCP(sock, str.c_str());
	 */
	Package package;
	package.ParseFromString(str);

	char *c_str;
	int size = str.length();
        c_str = (char*)malloc((size + 5 + 1) * sizeof(char));
        if(c_str == NULL){
                cout << "ZHTClient::svrtosvr: " << strerror(errno) << endl;
                exit(1);
        }
        int len = copystring(c_str, str);

	if (package.virtualpath().empty()) //empty key not allowed.
		return -1;
	/*if (package.realfullpath().empty()) //coup, to fix ridiculous bug of protobuf!
		package.set_realfullpath(" ");*/

	package.set_operation(3); //1 for look up, 2 for remove, 3 for insert
	package.set_replicano(5); //5: original, 3 not original
	str = package.SerializeAsString();

	int sock = this->str2SockLRU(str, TCP);
	reuseSock(sock);
//	cout<<"sock = "<<sock<<endl;
//	int sentSize = generalSendTCP(sock, str.c_str());
	struct HostEntity dest = this->str2Host(str);
	sockaddr_in recvAddr;

	//cout << "cpp_zhtclient: zht_insert: " << str << endl;

	//pthread_mutex_lock(&msg_lock);
	int sentSize = generalSendTo(dest.host.data(), dest.port, sock, c_str, len, TCP);
	//pthread_mutex_unlock(&msg_lock);
	//int sentSize = generalSendTo(dest.host.data(), dest.port, sock, str.c_str(), str.size(), TCP);
	cout << "c++ str = " << str << endl;
	cout <<"Client inseret sent: " << sentSize << " c str = " << c_str << endl;
	int32_t* ret_buf = (int32_t*) malloc(sizeof(int32_t));

//	generalReveiveTCP(sock, (void*) ret_buf, sizeof(int32_t), 0);
	//pthread_mutex_lock(&msg_lock);
	generalReceive(sock, (void*) ret_buf, 4, recvAddr, 0, TCP);
	//pthread_mutex_unlock(&msg_lock);
	int ret = *(int32_t*) ret_buf;
	if (ret < 0) {
//		cerr << "zht_util.h: Failed to insert." << endl;
	}
//cout <<"Returned status: "<< *(int32_t*) ret<<endl;
//	d3_closeConnection(sock);
	free(ret_buf);

	//return ret_1;
	cout<<"insert got: "<< ret <<endl;
	return ret;
}

int ZHTClient::lookup(string str, string &returnStr) {

	Package package;
	package.ParseFromString(str);

	if (package.virtualpath().empty()) //empty key not allowed.
		return -1;
	/*if (package.realfullpath().empty()) //coup, to fix ridiculous bug of protobuf!
		package.set_realfullpath(" ");*/

	package.set_operation(1); // 1 for look up, 2 for remove, 3 for insert
	package.set_replicano(3); //5: original, 3 not original

	str = package.SerializeAsString();

	struct HostEntity dest = this->str2Host(str);
//	cout << "client::lookup is called, now send request..." << endl;

        char *c_str;
        int size = str.length();
        c_str = (char*)malloc((size + 5 + 1) * sizeof(char));
        if(c_str == NULL){
                cout << "ZHTClient::svrtosvr: " << strerror(errno) << endl;
                exit(1);
        }
        int len = copystring(c_str, str);

	Package pack;
	pack.ParseFromString(str);
//	cout<<"ZHTClient::lookup: operation = "<<pack.operation()<<endl;

//	int ret = simpleSend(str, dest, sock);
//	sock = makeClientSocket(dest.host.c_str(), dest.port, 1);
//	cout<<"client sock = "<< sock<<endl;

	int sock = this->str2SockLRU(str, TCP);
	reuseSock(sock);
//	cout<<"sock = "<<sock<<endl;
	sockaddr_in recvAddr;	//cout << "lookup str = " << str << endl;
	//pthread_mutex_lock(&msg_lock);
	int sentSize = generalSendTo(dest.host.data(), dest.port, sock, c_str, len, TCP);
	//pthread_mutex_unlock(&msg_lock);
	//int sentSize = generalSendTo(dest.host.data(), dest.port, sock, str.c_str(), str.size(), TCP);
//	int ret = generalSendTCP(sock, str.c_str());

//	cout << "ZHTClient::lookup: simpleSend return = " << ret << endl;
	char buff[Env::MAX_MSG_SIZE]; //Env::MAX_MSG_SIZE
	memset(buff, 0, sizeof(buff));
	int rcv_size = -1;
	int status = -1;
	string sRecv;
	string sStatus;

	//if (sentSize == str.length()) { //this only work for TCP. UDP need to make a new one so accept returns from server.
	if (sentSize == len) {
//		cout << "before protocol judge" << endl;
		//pthread_mutex_lock(&msg_lock);
		rcv_size = generalReceive(sock, (void*) buff, sizeof(buff), recvAddr, 0,
				TCP);
		//pthread_mutex_unlock(&msg_lock);

		if (rcv_size < 0) {
			cout << "Lookup receive error." << endl;
		} else {
			//cout << "ZHTClient::lookup: buff = " << buff << " size = " << rcv_size << endl;
			sRecv.assign(buff); //cout << "ZHTClient::lookup: for key " << package.virtualpath() << endl;//<< " recd = "  << sRecv << endl;
			try {
			returnStr = sRecv.substr(3); //the left is real thing need to be deserilized.
			}
			catch (exception& e) {
                                cout << "ZHTClient::lookup: (substr(3)) " << " " << e.what() << endl;
                        	exit(1);
                        }
			try {
			sStatus = sRecv.substr(0, 3); //the first three chars means status code, like 001, 002, -98, -99 and so on.
			}
			catch (exception& e) {
                                cout << "ZHTClient::lookup: (substr(0, 3)) " << " " << e.what() << endl;
                                exit(1);
                        }
		}

//		cout << "after protocol judge" << endl;
	}
//	d3_closeConnection(sock);

	if (!sStatus.empty())
		status = atoi(sStatus.c_str());

	return status;
}

int ZHTClient::append(string str) {

	Package package;
	package.ParseFromString(str);

	char *c_str;
        int size = str.length();
        c_str = (char*)malloc((size + 5 + 1) * sizeof(char));
        if(c_str == NULL){
                cout << "ZHTClient::svrtosvr: " << strerror(errno) << endl;
                exit(1);
        }
        int len = copystring(c_str, str);

	if (package.virtualpath().empty()) //empty key not allowed.
		return -1;
	/*if (package.realfullpath().empty()) //coup, to fix ridiculous bug of protobuf!
		package.set_realfullpath(" ");*/

	package.set_operation(4); //1 for look up, 2 for remove, 3 for insert, 4 for append
	package.set_replicano(5); //5: original, 3 not original
	str = package.SerializeAsString();

	int sock = this->str2SockLRU(str, TCP);
	reuseSock(sock);
//      cout<<"sock = "<<sock<<endl;
//      int sentSize = generalSendTCP(sock, str.c_str());
	struct HostEntity dest = this->str2Host(str);
	sockaddr_in recvAddr;
	//pthread_mutex_lock(&msg_lock);
	int sentSize = generalSendTo(dest.host.data(), dest.port, sock, c_str, len, TCP);
	//pthread_mutex_unlock(&msg_lock);
	//int sentSize = generalSendTo(dest.host.data(), dest.port, sock, str.c_str(), str.size(), TCP);
//      cout <<"Client inseret sent: "<<sentSize<<endl;
	int32_t* ret_buf = (int32_t*) malloc(sizeof(int32_t));
	
	//pthread_mutex_lock(&msg_lock);
//      generalReveiveTCP(sock, (void*) ret_buf, sizeof(int32_t), 0);
	generalReceive(sock, (void*) ret_buf, 4, recvAddr, 0, TCP);
	//pthread_mutex_unlock(&msg_lock);
	int ret = *(int32_t*) ret_buf;
	if (ret < 0) {
//              cerr << "zht_util.h: Failed to insert." << endl;
	}
//cout <<"Returned status: "<< *(int32_t*) ret<<endl;
//      d3_closeConnection(sock);
	free(ret_buf);

	//return ret_1;
//      cout<<"insert got: "<< *ret <<endl;
	return ret;
}

int ZHTClient::remove(string str) {

	Package package;
	package.ParseFromString(str);

	char *c_str;
        int size = str.length();
        c_str = (char*)malloc((size + 5 + 1) * sizeof(char));
        if(c_str == NULL){
                cout << "ZHTClient::svrtosvr: " << strerror(errno) << endl;
                exit(1);
        }
        int len = copystring(c_str, str);

	if (package.virtualpath().empty()) //empty key not allowed.
		return -1;
	/*if (package.realfullpath().empty()) //coup, to fix ridiculous bug of protobuf!
		package.set_realfullpath(" ");*/

	package.set_operation(2); //1 for look up, 2 for remove, 3 for insert
	package.set_replicano(3); //5: original, 3 not original
	str = package.SerializeAsString();

	int sock = this->str2SockLRU(str, TCP);
	reuseSock(sock);
//	cout<<"sock = "<<sock<<endl;
	struct HostEntity dest = this->str2Host(str);
	sockaddr_in recvAddr;
	//pthread_mutex_lock(&msg_lock);
	int sentSize = generalSendTo(dest.host.data(), dest.port, sock, c_str, len, TCP);
	//pthread_mutex_unlock(&msg_lock);
	//int sentSize = generalSendTo(dest.host.data(), dest.port, sock, str.c_str(), str.size(), TCP);
//		cout<<"remove sentSize "<< sentSize <<endl;
	int32_t* ret_buf = (int32_t*) malloc(sizeof(int32_t));

	//	generalReveiveTCP(sock, (void*) ret_buf, sizeof(int32_t), 0);
	//pthread_mutex_lock(&msg_lock);
	generalReceive(sock, (void*) ret_buf, sizeof(int32_t), recvAddr, 0, TCP);
	//pthread_mutex_unlock(&msg_lock);

//	generalSendTCP(sock, str.c_str());

//	int32_t* ret = (int32_t*) malloc(sizeof(int32_t));

//	generalReveiveTCP(sock, (void*) ret, sizeof(int32_t), 0);
	int ret_1 = *(int32_t*) ret_buf;
//	cout<<"remove got: "<< ret_1 <<endl;
//cout <<"Returned status: "<< *(int32_t*) ret<<endl;
//	d3_closeConnection(sock);
	free(ret_buf);

	return ret_1;
}

//=================================================Anupam=============================================================

int copystring(char *c_str, string str) {

	int i, j;
	int len = str.length();

	for(i = 0; i < 5; i++) {
		c_str[i] = '\0';
	}

	stringstream ss;
	ss << len;
	string len_str(ss.str());

	for(i = 0; i < len_str.length(); i++) {
		c_str[i] = len_str[i];
	}
	i = 5;
	/*for(j = 0 ; j < len; j++) {
		c_str[i++] = str[j];
	}*/
	memcpy(&c_str[i], str.c_str(), str.length()+1);
	//c_str[i++] = '\0';
	return (i+len);
}

int32_t ZHTClient::insertqueue(string str, int size) {

	Package package;
	package.ParseFromString(str);

	char *c_str;
	c_str = (char*)malloc((size + 5 + 1) * sizeof(char));
	if(c_str == NULL){
                cout << "ZHTClient::svrtosvr: " << strerror(errno) << endl;
                exit(1);
        }
	int len = copystring(c_str, str);

	if (package.virtualpath().empty()) //empty key not allowed.
		return -1;
	//cout << "C++ string len = " << str.length() << endl; cout << "C++ string: \n" << str << endl; 
	/*cout << "C string: " << endl;
	for(int i = 0; i < len; i++) cout << c_str[i]; cout << endl;
	return 1;*/

	int sock = this->str2SockLRU(str, TCP);
	reuseSock(sock);
	struct HostEntity dest = this->str2Host(str);
	sockaddr_in recvAddr;

	//pthread_mutex_lock(&msg_lock);
	//int sentSize = generalSendTo(dest.host.data(), dest.port, sock, str.c_str(), str.size(), TCP);
	int sentSize = generalSendTo(dest.host.data(), dest.port, sock, c_str, len, TCP);
	//pthread_mutex_unlock(&msg_lock);

	//cout << " Sent size = " << sentSize << endl;
	int32_t* ret_buf = (int32_t*) malloc(sizeof(int32_t));
	//pthread_mutex_lock(&msg_lock);
	generalReceive(sock, (void*) ret_buf, sizeof(int32_t), recvAddr, 0, TCP);
	//pthread_mutex_unlock(&msg_lock);
	int ret_1 = *(int32_t*) ret_buf;
	free(ret_buf);
	return ret_1;
}

// general server to server send (for load information and task stealing)
int32_t ZHTClient::svrtosvr(string str, int size, int index) {

	Package package;
	package.ParseFromString(str);

	if (package.virtualpath().empty()) 	//empty key not allowed.
		return -1;
	str = package.SerializeAsString();

	char *c_str;
	c_str = (char*)malloc((size + 5 + 1) * sizeof(char));
	if(c_str == NULL){
		cout << "ZHTClient::svrtosvr: " << strerror(errno) << endl;
		exit(1);
	} 
	int len = copystring(c_str, str);

	if (package.virtualpath().empty()) //empty key not allowed.
		return -1;
	//cout << "C++ string len = " << str.length() << endl; cout << "C++ string: \n" << str << endl; 
	/*cout << "C string: " << endl;
	for(int i = 0; i < len; i++) cout << c_str[i]; cout << endl;
	return 1;*/

	int sock = this->index2SockLRU(index, TCP);
	reuseSock(sock);

	struct HostEntity dest = this->index2Host(index);
	sockaddr_in recvAddr; //cout << "going to acquire lock" << endl;
	//pthread_mutex_lock(&msg_lock); //cout << "lock acquired" << endl;
	//int sentSize = generalSendTo(dest.host.data(), dest.port, sock, str.c_str(), str.size(), TCP);
	int sentSize = generalSendTo(dest.host.data(), dest.port, sock, c_str, len, TCP); //cout << "svrsvr sent" << endl;
	//pthread_mutex_unlock(&msg_lock);
	int32_t* ret_buf = (int32_t*) malloc(sizeof(int32_t));
	//pthread_mutex_lock(&msg_lock);
	generalReceive(sock, (void*) ret_buf, sizeof(int32_t), recvAddr, 0, TCP); //cout << "svrsvr recv" << endl;
	//pthread_mutex_unlock(&msg_lock); //cout << "lock released" << endl;
	int32_t ret_1 = *(int32_t*) ret_buf;
	free(ret_buf);

	//if(package.operation() == 14) {
	//	cout << "ZHTClient::svrtosvr num_task = " << ret_1 << endl;
	//}

	return ret_1;
}

//===================================================================================================================

