//
// Created by Chen Yang on 4/14/17.
//

#include <cstring>
#include <err.h>
#include "NamedPipe.h"
NamedPipe::NamedPipe(std::string fifo_name) {
		//buffer = new char[buffer_size];
//		bytes_sent = 0;
		this->fifo_name = fifo_name;
		// first delete the fifo file
		//remove(fifo_name.c_str());
	if (access(fifo_name.c_str(), F_OK) == -1) {
		res = mkfifo(fifo_name.c_str(), 0777);
	try {
			if (res != 0) {
				std::string errinfo="Could not create named pipe file: ";
				errinfo.append(fifo_name);
				recordErr(errinfo, __FILE__,__LINE__);
			}
		} catch (runtime_error err) {
				std::cerr<<err.what();
				exit(EXIT_FAILURE);
	}
	}
	}

	NamedPipe::~NamedPipe() {
	//	close(pipe_fd_wr);
		close(pipe_fd_rd);
	}

void NamedPipe::openfifo() {
	try{
		if ((pipe_fd_rd = open(fifo_name.c_str(), read_mode)) == -1)
		{
			std::string errinfo="Could not open ";
			errinfo.append(fifo_name);
			recordErr(errinfo, __FILE__,__LINE__);
		}} catch ( runtime_error err) {
		std::cerr<<err.what();
		exit(EXIT_FAILURE);
	}
}

char* NamedPipe::getCommand() {
    //printf("call getCommand\n");
    try {
        if (pipe_fd_rd == -1) {

            std::string errinfo = "when rading data, could not open ";
            errinfo.append(fifo_name);
            recordErr(errinfo, __FILE__, __LINE__);
        }
    } catch (runtime_error err) {
        std::cerr << err.what();
        exit(EXIT_FAILURE);
    }
    // the read function read the block. E.g., 11 22 33\n11 22 33. This case includes two commands,
    // so we need to split them and send each command to the main function. The "else" branch is used for splitting
    // the command block.
    while(1) {
        if (isnull) {
            memset(buffer, '\0', buffer_size);
            do {
                res = read(pipe_fd_rd, buffer, buffer_size);
				if(res<=0)
					// assuming no sleep, "while" will always run read funtion
					// cuasing to be 100% CPU-utli to waste the performance
					//delay 10ms
					usleep(10000);
				else
					break;
            } while (1);
            cmdPoint = strtok(buffer, "\n");
            isnull = false;
            break;
        } else {
            cmdPoint = strtok(NULL, "\n");
            if (cmdPoint == NULL)
                isnull = true;
            else
                break;
        }
    }
		return cmdPoint;
	}
