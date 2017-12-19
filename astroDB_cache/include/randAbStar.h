//
// Created by Chen Yang on 4/17/17.
//

#ifndef ASTRODB_CACHE_RANDABSTAR_H
#define ASTRODB_CACHE_RANDABSTAR_H

#include <map>
#include <vector>
#include <random>
#include <set>
#include "StarFileFits.h"
#include "thpool.h"
#include "Partition.h"
#define MAXTIMEINTERVAL 2400


class randAbstar{

public:
    static void initRandAbstar(Partition *refStar);
    //@param bernoulliParam is the probability when the event occurs once.
    //if bernoulliParam [0,1] is larger, the abnormal pharse will occur more frequently.
    //@param geometric_distribution param[0,1]. If it is larger, the abnormal stars will be less.
    // we use this->geometricParam=1-geometricParam;
    //if this->geometricParam is larger, the abnormal stars will be more.
    randAbstar(double bernoulliParam, double geometricParam);
    // this star block has the abnormal stars or not,
    // set 1 to exlain the program is in the abnorm phase.
    bool isAb();
    //if the program is in the obnorm phase, set the number of abnormal stars
    // and the abnormal interval time
    void setAbMag();
    // write the abnormal mag to the original star data (call it once and only set one star),
    // using it inside match function
    //@param objStar points the original star data
    //@return 0 is normal star; 1 is the newest generated abnormal star;
    // 2 is the abnormal star, which is generated before this time
    int popAbMag(CMStar *objStar, char *starinfo);
    // @return the abnormal star data
    std::vector<std::string> & getabBuffer();
    //build the interval index
    void bulidIntervalIdx();

    const double &getTime();
    //
    std::vector<std::pair<std::string,std::vector<std::pair<acl::string,double>>>> &getIntervalIdx();

    //free the memory which is pointed by the static pointers
    // do not write these codes in delrandAbstar() to the destructor function,
    // because delete keyword will call
    // the destructor function when we delete a object.
    // However, the pointed memory data still will be used in other objects
    static void delrandAbstar();

    // return the status which represents whether the program generates the news abnormal stars
    bool getAb();
    //@return abnormal Star number in this time.
    size_t getAbStarNum();
    //@return abnormal star number until now.
    size_t getAbStarTotalNum();

private:
    size_t getAbNum();
    double bernoulliParam, geometricParam;
    std::vector<float>* getAbTime();
    static std::vector<std::string> starArr;
    static std::map<std::string,std::vector<float>*> abMag;
    std::random_device rd;
    bool ab;
    std::default_random_engine mt;
    static pthread_mutex_t abMag_mutex;
    static pthread_mutex_t idxBegin_mutex;
    static pthread_mutex_t abMagThpool_mutex;
    static threadpool abMag_thpool;
    double timeDouble;
    static void genMagThread(void *param);
    //the newext status of the whole interval index,
    // and every item reconds the abnormal star inverval time is end or not.
    // the second item "l" represents that the abnormal status is going on,
    // and "r" represents that the abnormal status is stopped.
    static std::map<std::string,std::pair<std::string,std::string>> idxNewestStatus;
    // appending and updating part of inteval index in the current time
    std::vector<std::pair<std::string,std::vector<std::pair<acl::string,double>>>> intenvalIdx;
    std::vector<std::string> abBuffer;
    //abnormal star name in the current time
    static std::set<std::string> *abNameBuffer;

    //the newest generated abnormal star
    std::set<std::string> newGenAbStar;
};

typedef struct mvparam {
    size_t lo;
    size_t hi;
    std::vector<std::vector<float >*> *magVec;
    randAbstar *proAbstar;
} magVecParam;

#endif //ASTRODB_CACHE_RANDABSTAR_H
