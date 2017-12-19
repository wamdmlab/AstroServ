//
// Created by Chen Yang on 4/14/17.
//

#ifndef CROSS_SRC_NAMEDPIPE_H_
#define CROSS_SRC_NAMEDPIPE_H_

#include <unistd.h>
#include <stdlib.h>
#include <fcntl.h>
#include <limits.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <stdio.h>
#include <string>
#include <errno.h>

using namespace std;

class NamedPipe {
public:

	static const int buffer_size = 1024;
	string fifo_name;
	// write fd is useless
//	int pipe_fd_wr;
	int pipe_fd_rd;
	ssize_t res;
	const int read_mode = O_RDONLY;
//	const int write_mode = O_WRONLY;
//	const int nonblock_mode = O_NONBLOCK;
//	int bytes_sent;
	char buffer[buffer_size];
    bool isnull = true; // the read block has be processed, or the read block is inited into null;
    char *cmdPoint = NULL;  // the head pointer of the current command string
	NamedPipe(string fifo_name);
	void openfifo();
	~NamedPipe();
	char* getCommand();
	
};


#endif /* CROSS_SRC_NAMEDPIPE_H_ */

