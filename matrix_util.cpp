
#include "matrix_util.h"

using namespace std;

int LOGGING;
int max_tasks_per_package;

char* Env_var::LISTEN_PORT;
string Env_var::membershipFile;
string Env_var::cfgFile;
char* Env_var::isTCP;
char* Env_var::userName;
bool Env_var::TCP;
int Env_var::num_tasks;

string prefix;
string shared;

void set_dir(string p, string s){
        prefix.append(p);
        shared.append(s);
}

Env_var::Env_var() {
}

Env_var::~Env_var() {
}

TaskQueue_Item::TaskQueue_Item() {
}

TaskQueue_Item::~TaskQueue_Item() {
}

void Env_var::set_env_var(char *parameters[]) {
  	LISTEN_PORT = parameters[1];
	membershipFile = parameters[2];
	cfgFile = parameters[3];
	isTCP = parameters[4];
	userName = parameters[5];
	LOGGING = atoi(parameters[6]);
	max_tasks_per_package = 1000;
	num_tasks = atoi(parameters[7]);

	if (!strcmp("TCP", isTCP)) {
		TCP = true;
//cout<<"TCP"<<endl;
	} else {
		TCP = false;
//cout<<"UDP"<<endl;
	}
}

int get_max_tasks_per_package() {
	return max_tasks_per_package;
}

/*
 * Find difference between two timespec struct and return it
 */
timespec timediff(timespec start, timespec end) {

	timespec diff;
	uint64_t ts, te;
	ts = (uint64_t)start.tv_sec * 1000000000 + (uint64_t)start.tv_nsec;
	te = (uint64_t)end.tv_sec * 1000000000 + (uint64_t)end.tv_nsec;
	diff.tv_sec = (te - ts)/1000000000;
	diff.tv_nsec = (te - ts) % 1000000000;

	return diff;
}

/*
 * Given a membership list, return the index of a given hostname/ip in that list
 */
int getSelfIndex(string hostName, vector<struct HostEntity> memberList) {
	int listSize = memberList.size();
	HostEntity host;
	int i = 0;
	for (i = 0; i < listSize; i++) {
		host = memberList.at(i);
		if (!(host.host.compare(hostName))) {
			break;
		}
	}
	if (i == listSize) {
		return -1;
	}

	return i;
}

/*
 * get self index from membership file based in hostname and port no
 */
int getSelfIndex(string hostName, int port, vector<struct HostEntity> memberList) {
		int listSize = memberList.size();
		HostEntity host;
		int i = 0;
		for (i = 0; i < listSize; i++) {
			host = memberList.at(i);
			//cout <<"i = " << i <<" host = " << host.host << " and port = " << host.port << endl;
			//int ret = host.host.compare(hostName); cout << "return = " << ret << endl;
			if (!(host.host.compare(hostName)) && port == host.port) {
				break;
			}
		}
		//cout<<"my index: "<<i<<endl;
		if (i == listSize) {
			return -1;
		}

		return i;
}

/*
 * Get local machine's IP address
 */
int get_local_ip(char* out_ip) {
	int i = 0;
	int sockfd;
	struct ifconf ifconf_local;
	char buf[512];
	struct ifreq *ifreq_local;
	char* ip;
	ifconf_local.ifc_len = 512;
	ifconf_local.ifc_buf = buf;
	if((sockfd = socket(AF_INET, SOCK_DGRAM, 0)) < 0)
	{
		return -1;
	}
	ioctl(sockfd, SIOCGIFCONF, &ifconf_local);
	close(sockfd);
	ifreq_local = (struct ifreq*)buf;
	for(i=(ifconf_local.ifc_len / sizeof(struct ifreq));i > 0;i--)
	{
		ip = inet_ntoa(((struct sockaddr_in*)&(ifreq_local->ifr_addr))->sin_addr);
		if(strcmp(ip, "127.0.0.1") == 0)
		{
			ifreq_local++;
			continue;
		}
		strcpy(out_ip, ip);
		return 0;
	}
	return -1;
}

int set_ip(string &ip) {
	//char ip_cstr[30];
	//int ret = get_local_ip(ip_cstr);
	//ip.assign(ip_cstr);
	//return ret;
	
	//string cmdip = "hostname";
	string cmdip = "/dev/shm/torusIP";
	string host;
	host = executeShell(cmdip);
	host = host.substr(0, host.size()-1);
	ip.assign(host);
	//ip[host.size()] = 0;
	//memcpy(ip, host.c_str(), host.size());
	return 0;
}

int hostComp(struct HostEntity a, struct HostEntity b) {

	if(a.host == b.host && a.port == b.port)
		return 0;
	else 
		return 1;
}

bitvec::bitvec() {
}

bitvec::bitvec(int size) {
        bitvector.resize(size);
        //hmap.reserve(size);
        reset_all();
}

bitvec::~bitvec() {
}

bool bitvec::any() {

        uint32_t bitvector_size = bitvector.size();
        for(uint32_t i = 0; i < bitvector_size; i++) {
                if(bitvector[i] == 1) {
                        return true;
                }
        }
        return false;
}

bool bitvec::none() {
        uint32_t i;
        uint32_t bitvector_size = bitvector.size();
        for(i = 0; i < bitvector_size; i++) {
                if(bitvector[i] == 1) {
                        return false;
                }
        }

        if(i == bitvector_size) {
                return true;
        }
}

uint32_t bitvec::count() {

        uint32_t bitvector_size = bitvector.size();
        uint32_t sum = 0;
        for(uint32_t i = 0; i < bitvector_size; i++) {
                if(bitvector[i] == 1) {
                        sum++;
                }
        }
        return sum;
}

uint32_t bitvec::size() {
        return bitvector.size();
}

bool bitvec::at(int i) {
        return bitvector.at(i);
}

void bitvec::set(int i) {
        bitvector.at(i) = 1;
        hmap[i] = hmap[i] + 1;
}

void bitvec::reset(int i) {
        bitvector.at(i) = 0;
        hmap[i] = 0;
}

void bitvec::dec(int i) {
        if(hmap[i] == 0) {
                return;
        }
        hmap[i] = hmap[i] - 1;
        if(hmap[i] == 0) {
                bitvector.at(i) = 0;
        }
}

void bitvec::set_all() {
        uint32_t bitvector_size = bitvector.size();
        for(uint32_t i = 0; i < bitvector_size; i++) {
                set(i);
        }
}

void bitvec::reset_all() {
        uint32_t bitvector_size = bitvector.size();
        for(uint32_t i = 0; i < bitvector_size; i++) {
                reset(i);
        }
}

void bitvec::dec_all() {
        uint32_t bitvector_size = bitvector.size();
        for(uint32_t i = 0; i < bitvector_size; i++) {
                dec(i);
        }
}

bool bitvec::test(int i) {
        return bitvector.at(i);
}

void bitvec::flip() {
        uint32_t bitvector_size = bitvector.size();
        for(uint32_t i = 0; i < bitvector_size; i++) {
                if(bitvector.at(i)) {
                        bitvector.at(i) = false;
                }
                else {
                        bitvector.at(i) = true;
                }
        }
}

void bitvec::flip(int i) {
        if(bitvector.at(i)) {
                bitvector.at(i) = false;
        }
        else {
                bitvector.at(i) = true;
        }
}

void bitvec::print() {
        uint32_t bitvector_size = bitvector.size();
        for(uint32_t i = 0; i < bitvector_size; i++) {
                cout << bitvector.at(i);
        }
        cout << endl;
}

uint32_t bitvec::first_set() {
        uint32_t bitvector_size = bitvector.size();
        for(uint32_t i = 0; i < bitvector_size; i++) {
                if(bitvector.at(i)) {
                        return i;
                }
        }
        return bitvector_size;
}

uint32_t bitvec::pop() {
        uint32_t large_index = 0, large_count = 0;
        uint32_t bitvector_size = bitvector.size();
        for(uint32_t i = 0; i < bitvector_size; i++) {
                if(hmap[i] > large_count) {
                        large_count = hmap[i];
                        large_index = i;
                }
        }
        if(large_count == 0) {
                return bitvector_size;
        }
        dec(large_index);
        return large_index;
}

