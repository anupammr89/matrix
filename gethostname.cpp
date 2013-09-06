#include <iostream>
#include <string>
#include <cstring>
#include <algorithm>
#include <fstream>

using namespace std;

//execute shell scripts and return results as a string
string executeShell(string str) {
	char* cmd = (char*) str.c_str();
	FILE* pipe = popen(cmd, "r");
	if (!pipe)
		return "ERROR";
	char buffer[128];
	string result = "";
	while (!feof(pipe)) {
		if (fgets(buffer, 128, pipe) != NULL)
			result += buffer;
	}
	pclose(pipe);
	return result;
}

int main() {

	string cmd("hostname");
	ofstream fp;
	fp.open("file");
	fp << executeShell(cmd);
}
