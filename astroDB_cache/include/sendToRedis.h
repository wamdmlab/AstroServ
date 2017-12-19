//
// Created by Chen Yang on 3/23/17.
//

#ifndef ASTRODB_CACHE_SENDTOREDIS_H
#define ASTRODB_CACHE_SENDTOREDIS_H

#include "StarFileFits.h"
#include <unistd.h>
#include "randAbStar.h"
#define MAX_CONNS 1000
#define SEND_RETRY_TIME 20    //
#define SEND_RETRY_SLEEP_TIME 5 //sec

#define LINECACHE "perLineWithCache"
#define LINENOCACHE "perLineWithNoCache"
#define SENDTABLE "starTableAsaString"
#define SENDBLOCK "perBlockAsaString"

typedef struct parameters {
    acl::redis_client_cluster *cluster;
    std::vector<std::vector<std::string> > *sendList=NULL; //use in perLineWithCache
    std::string mode;
    const std::vector<std::string> *proToMatchedStar=NULL;   //use in perLineWithNoCache and perBlockAsaString
    const std::vector<std::pair<std::string,std::vector<std::pair<acl::string,double>>>> *proToIdx=NULL; // use in the intenval index
    const std::vector<size_t > *proToAbNum=NULL;
    const std::vector<size_t > *proToNewAbNum=NULL;
    size_t lo=0;
    size_t hi=0;
    size_t ilo=0;
    size_t ihi=0;
    double timestamp;
    int genCount;
} parameters;

class sendPerLine
{
public:
    static void RedisInit(const char *redisHost, acl::redis_client_cluster *cluster);
    void sendResWithCache(acl::redis_client_cluster *cluster, StarFileFits *refStars,
                          const int usedThreads,const std::string mode,
                          randAbstar *const abstar=NULL);
    void sendResWithNoCache(acl::redis_client_cluster *cluster, StarFileFits *objStars,
                            const int usedThreads,const std::string mode,
                            randAbstar *const abstar=NULL);

private:
    static void* singleThreadWithCache( void * arg);
    static void* singleThreadWithNoCache( void * arg);
    static int control;  //use in sendResWithCache
    // mutex controling threads
};

class sendPerBlock
{
public:
    void sendResWithPerTable(acl::redis_client_cluster *cluster, const std::string &starTable,
                             const int usedThreads,
                             const double timestamp,
                             randAbstar * const abstar=NULL);
    void sendResWithPerBlock(acl::redis_client_cluster *cluster,
                                           const std::vector<std::string> &starBlock,
                                           const std::vector<size_t > &matchedStar_abNum,
                                           const std::vector<size_t > &matchedStar_newAbNum,
                                           const int usedThreads, const std::string mode, int genCount,
                                           const double timestamp,
                                           randAbstar * const abstar=NULL);
    void sendResWithAbStar(acl::redis_client_cluster *cluster,const int usedThreads,
                           randAbstar * const abstar=NULL);

private:
    static void* singleThreadWithPerBlock( void * arg);
    static void* singleThreadWithAbStar(void *arg);
};
#endif //ASTRODB_CACHE_SENDTOREDIS_H

