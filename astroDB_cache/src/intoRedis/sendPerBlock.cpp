//
// Created by Chen Yang on 4/9/17.
//

#include <iostream>
#include <err.h>
#include "sendToRedis.h"
#include "acl_cpp/lib_acl.hpp"
#include <sstream>

extern char redisHost[];
void sendPerBlock::sendResWithPerTable(acl::redis_client_cluster *cluster,
                                       const std::string &starTable,const int usedThreads,
                                       const double timestamp,
                                       randAbstar * const abstar) {
    acl::redis cmd;
    cmd.set_cluster(cluster, MAX_CONNS);
    size_t keystart = starTable.find_first_of(' ')+1;
    size_t keyend = starTable.find_first_of(' ', keystart)-keystart;
    // std::cerr << sendList->at(i)[0];
    std::string key = starTable.substr(keystart, keyend);
    std::vector<std::pair<const char *, double >> sendlst;
     sendlst.push_back(std::make_pair(starTable.c_str(),timestamp));
    try {
        int failedTime = 0;
        while (cmd.zadd(key.c_str(), sendlst) < 0) {
            ++failedTime;
            if (failedTime >5 && failedTime!=SEND_RETRY_TIME) {
                // when redis nodes is down, the insert is failed
                //if the redis nodes is restarted, the insert will still be failed.
                //we reset cluster to update the hash slot information, and the insert will be success.
                cluster->set(redisHost, MAX_CONNS, 10, 10);
                cmd.set_cluster(cluster, MAX_CONNS);
                sleep(SEND_RETRY_SLEEP_TIME);
            }else if(failedTime == SEND_RETRY_TIME) {
                std::ostringstream oString;
                oString<<"after retrying "<< SEND_RETRY_TIME<<" times,"
                       <<" insert failed: CCD "<<key<<"and the error code is "<<cmd.result_error()
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

    if(abstar!=NULL && abstar->getAbStarNum()) {
        abstar->bulidIntervalIdx();
        sendResWithAbStar(cluster,usedThreads,abstar);
    }
}

void sendPerBlock::sendResWithPerBlock(acl::redis_client_cluster *cluster,
                                       const std::vector<std::string> &starBlock,
                                       const std::vector<size_t > &matchedStar_abNum,
                                       const std::vector<size_t > &matchedStar_newAbNum,
                                       const int usedThreads, const std::string mode, int genCount,
                                       const double timestamp,
                                       randAbstar * const abstar) {


   // abstar->bulidIntervalIdx();


    const size_t starBlockNum = starBlock.size();
    const size_t thNum = starBlockNum<usedThreads?starBlockNum:usedThreads;
    pthread_attr_t attrs[thNum];
    pthread_t ids[thNum];
    parameters pms[thNum];
    size_t start=0;
    size_t end=starBlockNum;
    size_t steps =  end / thNum;
    int i;
    for( i = 0; i != thNum -1; i++) { //
        pms[i].timestamp = timestamp;
        pms[i].mode = mode;
        pms[i].cluster = cluster;
        pms[i].genCount = genCount;
        pms[i].proToMatchedStar = &starBlock;
        pms[i].proToAbNum = &matchedStar_abNum;
        pms[i].proToNewAbNum = &matchedStar_newAbNum;
        pms[i].lo = start + i * steps;
        pms[i].hi = pms[i].lo + steps;

        pthread_attr_init(&attrs[i]);

        // create first thread
        //
            pthread_create(&ids[i], &attrs[i], singleThreadWithPerBlock, &pms[i]);
    }

    pms[i].genCount = genCount;
    pms[i].timestamp = timestamp;
    pms[i].mode = mode;
    pms[i].cluster = cluster;
    pms[i].proToMatchedStar = &starBlock;
    pms[i].proToAbNum = &matchedStar_abNum;
    pms[i].proToNewAbNum = &matchedStar_newAbNum;
    pms[i].lo = start + i * steps;
    pms[i].hi = end;
    pthread_attr_init(&attrs[i]);
    // create last thread
        pthread_create(&ids[i], &attrs[i], singleThreadWithPerBlock, &pms[i]);

    // wait for all threads stopping
    for( i = 0; i < thNum; i++) {
        pthread_join(ids[i], NULL);
        pthread_attr_destroy(&attrs[i]);
    }


    if(abstar!=NULL && abstar->getAbStarNum()) {
        abstar->bulidIntervalIdx();
        sendResWithAbStar(cluster,usedThreads,abstar);
    }
}

void* sendPerBlock::singleThreadWithPerBlock( void * arg) {
    const parameters *pms = (parameters*) arg;
    size_t lo = pms->lo;
    size_t hi = pms->hi;
    const std::vector<std::string> *sendList = pms->proToMatchedStar;

    acl::redis cmd;
//    acl::redis abcountCmd;

    cmd.set_cluster(pms->cluster, MAX_CONNS);
//    abcountCmd.set_cluster(pms->cluster, MAX_CONNS);

    for(size_t i = lo; i!=hi;i++) {
        if(sendList->at(i).empty())
            continue;

        size_t keystart = sendList->at(i).find_first_of('_')+1;
        size_t keyend = sendList->at(i).find_first_of('_',keystart)+1;
        keyend = sendList->at(i).find_first_of('_',keyend);
        // std::cerr << sendList->at(i)[0];
        std::string keyTail = sendList->at(i).substr(keystart, keyend-keystart);
        std::string blockKey = "block_" +keyTail;

        //**********************filter some partition to send**********************
//        size_t blockNoStart = blockKey.find_last_of('_')+1;
//        size_t blockNoEnd = blockKey.length();
//        int blockNo;
//        sscanf(blockKey.substr(blockNoStart,blockNoEnd-blockNoStart).c_str(),"%d",&blockNo);
//        if((blockNo+1)%4 != 1)
//            continue;
        //**********************filter some partition to send**********************
        std::vector<std::pair<const char *, double >> sendlst;
        sendlst.push_back(std::make_pair(sendList->at(i).c_str(),pms->timestamp));

        try {
            int failedTime = 0;
            //   std::cerr<<sendList->at(i).c_str();
            while (cmd.zadd(blockKey.c_str(), sendlst) < 0) {
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
                           <<" insert failed: star "<<blockKey<<"and the error code is "<<cmd.result_error()
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

        // generate the abnormal star count view
        //add the pms->genCount into abCount for avoiding this current abCount and the previous abCount are the same
        // and the current abCount covers the previous abCount
        std::string abCount = std::to_string(pms->proToAbNum->at(i)) +" "
                +std::to_string(pms->proToNewAbNum->at(i)) +" "
                +std::to_string(pms->genCount);
        std::vector<std::pair<const char *, double >> abConutSendlst;
        abConutSendlst.push_back(std::make_pair(abCount.c_str(),pms->timestamp));
//        std::string aa= abCount.str();
        std::string abCountKey = "abcount_"+keyTail;

        try {
            int failedTime = 0;
            //   std::cerr<<sendList->at(i).c_str();
            while (cmd.zadd(abCountKey.c_str(), abConutSendlst) < 0) {
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
                           <<" insert failed: star "<<abCountKey<<"and the error code is "<<cmd.result_error()
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
    // generate the abnormal star count view
    }

    pthread_exit(0);

}

void sendPerBlock::sendResWithAbStar(acl::redis_client_cluster *cluster,
                                     const int usedThreads,
                                     randAbstar * const abstar) {
    const auto &abStar = abstar->getabBuffer();
    const auto &idx = abstar->getIntervalIdx();
    const size_t abStarNum = abStar.size();
    const size_t idxNum = idx.size();
    size_t max = idxNum>abStarNum?idxNum:abStarNum;
    const size_t thNum = max<usedThreads?max:usedThreads;
    pthread_attr_t attrs[thNum];
    pthread_t ids[thNum];
    parameters pms[thNum];
    size_t start=0;
    size_t end=abStarNum;
    size_t steps; //=  aBend / thNum;
    size_t cirNum = thNum;
    int i;
   abStarNum<=thNum?(steps = 1,cirNum=abStarNum):steps = end/thNum;
    for( i = 0; i != cirNum -1; i++) { //
        pms[i].timestamp = abstar->getTime();
        pms[i].proToMatchedStar = &abStar;
        pms[i].lo = start + i * steps;
        pms[i].hi = pms[i].lo + steps;
    }

    pms[i].timestamp = abstar->getTime();
    pms[i].proToMatchedStar = &abStar;
    pms[i].lo = start + i * steps;
    pms[i].hi = end;

    start=0;
    end=idxNum;
    idxNum<=thNum?(steps = 1,cirNum=idxNum):steps = end/thNum;
    for( i = 0; i != cirNum -1; i++) {
        pms[i].proToIdx = &idx;
        pms[i].ilo = start + i * steps;
        pms[i].ihi = pms[i].ilo + steps;
    }
    pms[i].proToIdx = &idx;
    pms[i].ilo = start + i * steps;
    pms[i].ihi = end;

    for(i=0;i!=thNum;++i) {
        pms[i].cluster = cluster;
        pthread_attr_init(&attrs[i]);
        pthread_create(&ids[i], &attrs[i], singleThreadWithAbStar, &pms[i]);
    }


    // wait for all threads stopping
    for( i = 0; i < thNum; ++i) {
        pthread_join(ids[i], NULL);
        pthread_attr_destroy(&attrs[i]);
    }

}

void* sendPerBlock::singleThreadWithAbStar(void *arg) {
    const parameters *pms = (parameters*) arg;
    size_t lo = pms->lo;
    size_t hi = pms->hi;
    size_t ilo = pms->ilo;
    size_t ihi = pms->ihi;


    const std::vector<std::string> *sendList = pms->proToMatchedStar;
    const auto *idx = pms->proToIdx;
    acl::redis cmd;
    cmd.set_cluster(pms->cluster, MAX_CONNS);

    for(size_t i = lo; i!=hi;i++) {
        size_t keyend = sendList->at(i).find_first_of(' ');
        std::string key("ab_");
        key.append(sendList->at(i).substr(0, keyend));

        //**********************filter some partition to send**********************
//        size_t starNoStart = key.find_first_of('_',7)+1;
//        size_t starkNoEnd = key.find_first_of('_',starNoStart);
//        int blockNo;
//        sscanf(key.substr(starNoStart,starkNoEnd-starNoStart).c_str(),"%d",&blockNo);
//        if((blockNo+1)%4 != 1)
//            continue;
        //**********************filter some partition to send**********************

        //add the abnorm begin time to the tail of star name
        // for distinguishing the same star has mutil-abnorm cases
        size_t abBeginTime = sendList->at(i).find_last_of(' ')+1;
        key.append("_").append(sendList->at(i).substr(abBeginTime));

        //update the star name in the star information string
        std::string strWithoutStarID = sendList->at(i).substr(keyend+1);
        std::string newStarStr = key+" "+strWithoutStarID;

        //delete the abnorm begin time
        // from the tail of the star information string
        size_t beginTimeIdx = newStarStr.find_last_of(' ');
        char *str = const_cast<char *>(newStarStr.c_str());
        str[beginTimeIdx] = '\0';

        std::vector<std::pair<const char *, double >> sendlst;
        sendlst.push_back(std::make_pair(str,pms->timestamp));

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
                           <<" insert failed: abnormal star "<<key<<"and the error code is "<<cmd.result_error()
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

    for(size_t i = ilo; i!=ihi;i++) {

        std::string key  = idx->at(i).first;
        //**********************filter some partition to send**********************
//        size_t starIdxNoStart = key.find_first_of('_',11)+1;
//        size_t starkIdxNoEnd = key.find_first_of('_',starIdxNoStart);
//        int blockNo;
//        sscanf(key.substr(starIdxNoStart,starkIdxNoEnd-starIdxNoStart).c_str(),"%d",&blockNo);
//        if((blockNo+1)%4 != 1)
//            continue;
        //**********************filter some partition to send**********************

        try {
            int failedTime = 0;
            //   std::cerr<<sendList->at(i).c_str();
            while (cmd.zadd(key.c_str(), idx->at(i).second) < 0) {
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
                           <<" insert failed: intenval index "<<key<<"and the error code is "<<cmd.result_error()
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