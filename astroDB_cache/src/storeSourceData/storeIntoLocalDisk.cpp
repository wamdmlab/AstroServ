//
// Created by Chen Yang on 7/2/17.
//

#include <fstream>
#include <sstream>
#include <cmhead.h>
#include <iostream>
#include "storeSourceData.h"

#define STORE_DIR "sourceDataTmp"


storeIntoLocalDisk::storeIntoLocalDisk(int threadNum) {
    store_thpool=thpool_init(threadNum);
    pthread_mutex_init(&thpool_mutex,NULL);
}

void storeIntoLocalDisk::store(void *arg) {

    struct timeval start, end;
    gettimeofday(&start, NULL);

    StarFileFits *obj =  (StarFileFits *)arg;
    std::string fileName;
    const char *p=strrchr(obj->fileName,'/');
    if(p==NULL)
        fileName = obj->fileName;
    else
        fileName = p+1;

    if(access(STORE_DIR,6)==-1) {
        mkdir(STORE_DIR,0777);
    }

    std::ostringstream fileDir;
    fileDir<<STORE_DIR<<"/"<<fileName;
//    std::string a = fileDir.str();
    std::ofstream ofile;
    ofile.open(fileDir.str());

    CMStar *next = obj->starList;

    while (next) {
        ofile << next->raw_info<<std::endl;
        next=next->next;
    }
    ofile.close();
    delete obj;
    gettimeofday(&end, NULL);
    float time_use=(end.tv_sec-start.tv_sec)*1000000+(end.tv_usec-start.tv_usec);

    std::cout<< "using "<< time_use/1000000<<"sec to store " <<fileDir.str()<<std::endl;
}

void storeIntoLocalDisk::addStoreThread(StarFileFits *obj) {

    pthread_mutex_lock(&thpool_mutex);

    thpool_add_work(store_thpool, store, obj);

    pthread_mutex_unlock(&thpool_mutex);
}

storeIntoLocalDisk::~storeIntoLocalDisk() {
    thpool_destroy(store_thpool);
    pthread_mutex_destroy(&thpool_mutex);
}
