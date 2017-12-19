//
// Created by hadoop on 3/23/17.
//

/**
 * send matched results to redis using multiple threads
 */
//
// Created by Chen Yang on 9/22/17.
//
#include <iostream>
#include <err.h>
#include <cmhead.h>
#include "sendToRedis.h"
#include "acl_cpp/lib_acl.hpp"
int sendPerLine::control = 0;
extern char redisHost[];
void sendPerLine::RedisInit(const char *redisHost, acl::redis_client_cluster *cluster) {
    // init socket module for windows
    acl::acl_cpp_init();
    //const char* redis_addr = "127.0.0.1:6379";
    int conn_timeout = 10, rw_timeout = 10;
    size_t max_conns = MAX_CONNS;

    // the redis client connection
    //conn = new acl::redis_client(redisHost, conn_timeout, rw_timeout);
    // declare redis cluster ojbect

    cluster->set_retry_inter(2);  // set the retry time for the connect pool when the connect is failed.
    cluster->set_redirect_max(10); // set the maximum redirect time for a MOVE/ASK command
    cluster->set_redirect_sleep(1000); // if the current redirect time is more than 2,
    // the thread will sleep 2 sec before the next redirect starts.

    cluster->init(NULL, redisHost, max_conns, conn_timeout, rw_timeout);

    //cluster->set(redisHost, conn_timeout, rw_timeout, max_conns);
}

void* sendPerLine::singleThreadWithCache( void * arg) {
    // get key and value
    const parameters *pms = (parameters*) arg;
    size_t lo = pms->lo;
    size_t hi = pms->hi;
    std::vector<std::vector<std::string> > *sendList = pms->sendList;

    acl::redis cmd;
    cmd.set_cluster(pms->cluster, MAX_CONNS);
    for( size_t i = lo; i != hi; i++) {
        if( sendList->at(i).size() == 0) {
            // if i is null
            continue;
        }
        size_t keyIndex = sendList->at(i)[0].find_first_of(' ');
        // std::cerr << sendList->at(i)[0];
        std::string key = sendList->at(i)[0].substr(0, keyIndex);

        const auto biter = 0;
        const auto eiter= sendList->at(i).size();
        std::vector<std::pair<const char*, double> > sendlst;
        sendlst.reserve(eiter);

        for(size_t ii=biter;ii!=eiter;++ii) {
            auto &sendi=sendList->at(i).at(ii);
            size_t beg=sendi.find_last_of(" ");
            double time = atof(sendi.substr(beg,sendi.size()-beg).c_str());
            sendlst.push_back(std::make_pair(sendi.c_str(),time));
        }

        //std::string value = sendString.substr(keyIndex+1);
        try {
            int failedTime = 0;
            while (cmd.zadd(key.c_str(), sendlst) < 0) {
                ++failedTime;
                if (failedTime >5 && failedTime!=SEND_RETRY_TIME) {
                    // when redis nodes is down, the insert is failed
                    //if the redis nodes is restarted, the insert will still be failed.
                    //we reset cluster to update the hash slot information, and the insert will be success.
                    pms->cluster->set(redisHost, MAX_CONNS, 10, 10);
                    cmd.set_cluster(pms->cluster, MAX_CONNS);
                    sleep(SEND_RETRY_SLEEP_TIME);
                }else if(failedTime == SEND_RETRY_TIME) {
                    std::ostringstream oString;
                    oString<<"after retrying "<< SEND_RETRY_TIME<<" times,"
                           <<" insert failed: star "<<key<<"and the error code is "<<cmd.result_error()
                           <<"from redis";
                    std::string errinfo=oString.str();
                    recordErr(errinfo, __FILE__,__LINE__);
                }
            }
        } catch (std::runtime_error err) {
            std::cerr<< err.what();
            exit(EXIT_FAILURE);
        }

        //only delete the list which has been stored in redis. We cannot use the clear(),
        // because the match() may insert the new list in starDataCache in the other thread,
        // when the storing operation is delayed for the brokendown nodes.
        //we do not use iterater but use (biter,eiter) because anther thread may call push_back
        // and space is not enough, and the vector call realloc to cause the iterater to be invalid!!!
        sendList->at(i).erase(sendList->at(i).begin()+biter,sendList->at(i).begin()+eiter);
        //cmd.rpush(key.c_str(), value.c_str(), NULL);
        cmd.clear();

    }

    pthread_exit(0);

}


void sendPerLine::sendResWithCache(acl::redis_client_cluster *cluster,
                                   StarFileFits *refStars, const int usedThreads,
                                   const std::string mode,randAbstar *const abstar) {
    pthread_attr_t attrs[usedThreads];
    pthread_t ids[usedThreads];
    parameters pms[usedThreads];
    size_t steps = refStars->starDataCache.size() / usedThreads;
    size_t start = control * steps + 1;
    control = (control + 1) % usedThreads;
    size_t end = start + steps;
    steps = ( end - start) / usedThreads;

    int i = 0;

    for( i = 0; i != usedThreads - 1; i++) { //
        pms[i].mode = mode;
        pms[i].cluster = cluster;
        pms[i].sendList = &refStars->starDataCache;
        pms[i].lo = start + i * steps;
        pms[i].hi = pms[i].lo + steps;

        pthread_attr_init(&attrs[i]);

        // create first thread
        pthread_create(&ids[i], &attrs[i], singleThreadWithCache, &pms[i]);

    }

    pms[i].mode = mode;
    pms[i].cluster = cluster;
    pms[i].sendList = &refStars->starDataCache;
    pms[i].lo = start + i * steps;
    pms[i].hi = end;
    pthread_attr_init(&attrs[i]);
    // create last thread
    pthread_create(&ids[i], &attrs[i], singleThreadWithCache, &pms[i]);

    // wait for all threads stopping
    for( i = 0; i < usedThreads; i++) {
        pthread_join(ids[i], NULL);
        pthread_attr_destroy(&attrs[i]);
    }

    if(abstar!=NULL && abstar->getAbStarNum()) {
        sendPerBlock sendAb;
        abstar->bulidIntervalIdx();
        sendAb.sendResWithAbStar(cluster,usedThreads,abstar);
    }

}

void sendPerLine::sendResWithNoCache(acl::redis_client_cluster *cluster, StarFileFits *objStars,
                                     const int usedThreads, const std::string mode,
                                     randAbstar *const abstar) {

    pthread_attr_t attrs[usedThreads];
    pthread_t ids[usedThreads];
    parameters pms[usedThreads];
    size_t start =(mode==LINECACHE)?1:0;
    size_t end=(mode==LINECACHE)?(objStars->starDataCache.size()):(objStars->matchedStar.size());
    size_t steps =  end / usedThreads;

    int i=0;
    for ( i = 0; i != usedThreads -1; i++) { //
        pms[i].timestamp=objStars->starList->time;
        pms[i].mode = mode;
        pms[i].cluster = cluster;
        pms[i].lo = start + i * steps;
        pms[i].hi = pms[i].lo + steps;

        pthread_attr_init(&attrs[i]);

        // create first thread
        if(mode == LINECACHE) {
            //use sendResWithNoCache to send the rest
            // when the system will exit normally.
            pms[i].sendList = &objStars->starDataCache;
            pthread_create(&ids[i], &attrs[i], singleThreadWithCache, &pms[i]);
        }
        else {
            pms[i].proToMatchedStar = &objStars->matchedStar;
            pthread_create(&ids[i], &attrs[i], singleThreadWithNoCache, &pms[i]);
        }
    }

    pms[i].timestamp=objStars->starList->time;
    pms[i].mode = mode;
    pms[i].cluster = cluster;
    pms[i].lo = start + i * steps;
    pms[i].hi = end;
    pthread_attr_init(&attrs[i]);
    // create first thread
    if(mode == LINECACHE) {
        //use sendResWithNoCache to send the rest
        // when the system will exit normally.
        pms[i].sendList = &objStars->starDataCache;
        pthread_create(&ids[i], &attrs[i], singleThreadWithCache, &pms[i]);
    }
    else {
        pms[i].proToMatchedStar = &objStars->matchedStar;
        pthread_create(&ids[i], &attrs[i], singleThreadWithNoCache, &pms[i]);
    }

    for (i = 0; i < usedThreads; i++) {
        pthread_join(ids[i], NULL);
        pthread_attr_destroy(&attrs[i]);
    }

    if(abstar!=NULL && abstar->getAbStarNum()) {
        sendPerBlock sendAb;
        abstar->bulidIntervalIdx();
        sendAb.sendResWithAbStar(cluster,usedThreads,abstar);
    }

}

void* sendPerLine::singleThreadWithNoCache( void * arg) {

    const parameters *pms = (parameters*) arg;
    size_t lo = pms->lo;
    size_t hi = pms->hi;
    const std::vector<std::string> *sendList = pms->proToMatchedStar;

    acl::redis cmd;
    cmd.set_cluster(pms->cluster, MAX_CONNS);
    for(size_t i = lo; i!=hi;i++) {
        size_t keyIndex = sendList->at(i).find_first_of(' ');
        // std::cerr << sendList->at(i)[0];
        std::string key = sendList->at(i).substr(0, keyIndex);
        std::vector<std::pair<const char *,double>> sendlst;
        sendlst.push_back(std::make_pair(sendList->at(i).c_str(),pms->timestamp));

        try {
            int failedTime = 0;
            //   std::cerr<<sendList->at(i).c_str();
            while (cmd.zadd(key.c_str(), sendlst) < 0) {
                ++failedTime;
                if (failedTime >5 && failedTime!=SEND_RETRY_TIME) {
                    // when redis nodes is down, the insert is failed
                    //if the redis nodes is restarted, the insert will still be failed.
                    //we reset cluster to update the hash slot information, and the insert will be success.
                    pms->cluster->set(redisHost, MAX_CONNS, 10, 10);
                    cmd.set_cluster(pms->cluster, MAX_CONNS);
                    sleep(SEND_RETRY_SLEEP_TIME);
                }else if(failedTime == SEND_RETRY_TIME) {
                    std::ostringstream oString;
                    oString<<"after retrying "<< SEND_RETRY_TIME<<" times,"
                           <<" insert failed: star "<<key<<"and the error code is "<<cmd.result_error()
                           <<"from redis";
                    std::string errinfo=oString.str();
                    recordErr(errinfo, __FILE__,__LINE__);
                }
            }
        } catch (std::runtime_error err) {
            std::cerr<< err.what();
            exit(EXIT_FAILURE);
        }
        cmd.clear();

    }
    pthread_exit(0);
}
