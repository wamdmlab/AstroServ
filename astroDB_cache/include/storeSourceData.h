//
// Created by Chen Yang on 5/11/17.
//

#ifndef ASTRODB_CACHE_STORESOURCEDATA_H
#define ASTRODB_CACHE_STORESOURCEDATA_H

#include <sys/types.h>
#include "thpool.h"
#include "pthread.h"
#include "StarFileFits.h"
#include <unistd.h>
#include <sys/stat.h>


#define DEFAULT_STORE_THREAD_NUM 5
class storeIntoLocalDisk {

public:
    storeIntoLocalDisk(int threadNum);

    void addStoreThread(StarFileFits *obj);

    ~storeIntoLocalDisk();
private:
    threadpool store_thpool;
    pthread_mutex_t thpool_mutex;
    static void store(void *arg);


};

#endif //ASTRODB_CACHE_STORESOURCEDATA_H
