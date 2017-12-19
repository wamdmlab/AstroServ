//
// Created by Chen Yang on 4/17/17.
//

#include "randAbStar.h"
#include <cmhead.h>
#include <functional>
#include <algorithm>
#include <string.h>
#include <iostream>

#define MAX_CLEAN_TIME 100
#define ABMAG_THREAD_NUM 8
std::vector<std::string> randAbstar::starArr;
std::map<std::string,std::vector<float>*> randAbstar::abMag;
 pthread_mutex_t randAbstar::abMag_mutex;
 pthread_mutex_t randAbstar::idxBegin_mutex;
pthread_mutex_t randAbstar::abMagThpool_mutex;
threadpool randAbstar::abMag_thpool;
std::map<std::string,std::pair<std::string,std::string>> randAbstar::idxNewestStatus;
std::set<std::string> *randAbstar::abNameBuffer=NULL;


randAbstar::randAbstar(double bernoulliParam, double geometricParam) {
   this->bernoulliParam=bernoulliParam;
    this->geometricParam=1-geometricParam;
    ab= false;
    timeDouble = -1;
}
void randAbstar::initRandAbstar(Partition *refStar) {
    int totalZone = refStar->getTotalZone();
    const CMZone *za = refStar->getZoneArr();
    for(int i = 0; i!=totalZone; ++i) {
        if(za[i].starNum != 0) {
            CMStar *p = za[i].star;
            while(p) {
                starArr.push_back(std::string(p->redis_key));
                p=p->next;
            }
        }
    }
    pthread_mutex_init (&abMag_mutex, NULL);
    pthread_mutex_init (&idxBegin_mutex,NULL);
    pthread_mutex_init (&abMagThpool_mutex,NULL);
    abMag_thpool=thpool_init(ABMAG_THREAD_NUM);
}

void randAbstar::delrandAbstar() {
    pthread_mutex_destroy(&abMag_mutex);
    pthread_mutex_destroy(&idxBegin_mutex);
    pthread_mutex_destroy(&abMagThpool_mutex);
    thpool_destroy(abMag_thpool);
    for(auto iter = abMag.begin(); iter!=abMag.end();++iter)
        if(iter->second!=NULL)
        delete iter->second;
    delete abNameBuffer;
}

bool randAbstar::isAb(){
    mt.seed(rd());
    std::bernoulli_distribution bd(bernoulliParam);
    ab=bd(mt);
    return ab;
}

bool randAbstar::getAb(){
    return ab;
}

size_t randAbstar::getAbNum() {
    mt.seed(rd());
    std::geometric_distribution<int> pd(geometricParam);
    auto dice= std::bind(pd,mt);
    //dice() may return 0;
    size_t abNum = dice()+1;
    return abNum<=starArr.size()?abNum:starArr.size();
}

std::vector<float>* randAbstar::getAbTime() {
    mt.seed(rd());
    std::geometric_distribution<int> pd(0.01);
    auto dice= std::bind(pd,mt);
    size_t timeInterval = (dice()+1)%MAXTIMEINTERVAL; //avoids dice() = 0
    std::vector<float> *abTime = new std::vector<float>;
    abTime->reserve(timeInterval);

    mt.seed(rd());
    std::uniform_int_distribution<int> genMag(1,1000);

    for(size_t i = 1; i!=timeInterval+1; i++)  // i=1 avoids timeInterval = 0
        abTime->push_back(genMag(mt)*0.00193432);
    return abTime;
}


void randAbstar::genMagThread(void *param) {
    auto pms = static_cast<magVecParam*>(param);
    for(size_t i=pms->lo;i!=pms->hi;++i)
        pms->magVec->at(i)= pms->proAbstar->getAbTime();
}


void randAbstar::setAbMag() {

//    struct timeval tstart[10], tend[10];
//    double time[10]={0};

    size_t abNum = getAbNum(); //starArr.size();
    magVecParam param[abNum];
    size_t threadNum = abNum<=ABMAG_THREAD_NUM?abNum:ABMAG_THREAD_NUM;
    std::vector<std::vector<float>*> magVector;
    magVector.resize(abNum);
    size_t start =0;
    size_t end = abNum;
    size_t steps = end/threadNum;
    size_t i;

pthread_mutex_lock(&abMagThpool_mutex);
//    gettimeofday(&tstart[5], NULL);
    for( i = 0;i!=threadNum-1; ++i) {
        param[i].lo = start+i * steps;
        param[i].hi = param[i].lo + steps;
        param[i].magVec = &magVector;
        param[i].proAbstar= this;
        thpool_add_work(abMag_thpool, genMagThread, &param[i]);
    }

    param[i].lo = start + i * steps;
    param[i].hi = end;
    param[i].magVec = &magVector;
    param[i].proAbstar= this;
    thpool_add_work(abMag_thpool, genMagThread, &param[i]);

    thpool_wait(abMag_thpool);
pthread_mutex_unlock(&abMagThpool_mutex);

//    gettimeofday(&tend[5], NULL);
//    time[5]+=((tend[5].tv_sec-tstart[5].tv_sec)*1000000+(tend[5].tv_usec-tstart[5].tv_usec))/1000000.00;
    std::vector<std::string> starArrCopy(starArr);
    mt.seed(rd());

    size_t restNum = starArrCopy.size()-1;
    for(size_t i = 0; i!=abNum; ++i) {
//    gettimeofday(&tstart[1], NULL);
        std::uniform_int_distribution<int> ud(0,restNum);
        size_t num = ud(mt);
//    gettimeofday(&tend[1], NULL);
//    time[1]+=((tend[1].tv_sec-tstart[1].tv_sec)*1000000+(tend[1].tv_usec-tstart[1].tv_usec))/1000000.00;
        std::vector<float>* abTime = magVector.at(i);

//     gettimeofday(&tstart[2], NULL);
        auto iter = abMag.find(starArrCopy.at(num));
//     gettimeofday(&tend[2], NULL);
//     time[2]+=((tend[2].tv_sec-tstart[2].tv_sec)*1000000+(tend[2].tv_usec-tstart[2].tv_usec))/1000000.00;
        pthread_mutex_lock(&abMag_mutex);
       if(iter==abMag.end()) {
//           gettimeofday(&tstart[3], NULL);
           std::string &tmp = starArrCopy.at(num);
           abMag.insert(make_pair(tmp, abTime));
           newGenAbStar.insert(tmp);
//           gettimeofday(&tend[3], NULL);
//           time[3]+=((tend[3].tv_sec-tstart[3].tv_sec)*1000000+(tend[3].tv_usec-tstart[3].tv_usec))/1000000.00;
       }
        else {
           delete iter->second; // the last abnormal mag expires
           iter->second = abTime;
       }
        pthread_mutex_unlock(&abMag_mutex);

//        gettimeofday(&tstart[4], NULL);
        if(num != restNum)
            starArrCopy.at(num)=starArrCopy.at(restNum);
        --restNum;
//        gettimeofday(&tend[4], NULL);
//        time[4]+=((tend[4].tv_sec-tstart[4].tv_sec)*1000000+(tend[4].tv_usec-tstart[4].tv_usec))/1000000.00;
    }

}

int randAbstar::popAbMag(CMStar *objStar,char *starinfo) {
    float mag;
    std::string starID;
     int returnFlag = 0;
    pthread_mutex_lock(&abMag_mutex);
   auto isExist =  abMag.find(std::string(objStar->redis_key));
    if(isExist==abMag.end()) {
     pthread_mutex_unlock(&abMag_mutex);
        returnFlag=0;
        return returnFlag;
    }
    else {
          if(newGenAbStar.find(std::string(objStar->redis_key)) != newGenAbStar.end())
              returnFlag=1;
        else
              returnFlag=2;
        starID = isExist->first;
        mag = isExist->second->back();
            isExist->second->pop_back();
         if(isExist->second->empty())
            abMag.erase(isExist);
    }
    pthread_mutex_unlock(&abMag_mutex);
      char * head=starinfo, *end;
      for(int i=1;i!=7;++i)
          head=strstr(head," ")+1; // find mag
      end=strstr(head," ");

      std::string abmagStr = std::to_string(mag);
        auto abIter = abmagStr.begin();
        for(;head!=end;++head) {    // replace mag into the abnormal mag
            if(abIter!=abmagStr.end())
                *head=*abIter++;
            else
                *head='0';
        }
            objStar->mag = mag;
            abBuffer.push_back(std::string(starinfo));
    return returnFlag;
    }

size_t randAbstar::getAbStarNum() {
    return abBuffer.size();
}

std::vector<std::string> & randAbstar::getabBuffer() {
    return abBuffer;
}

const double &randAbstar::getTime() {
    return timeDouble;
}

void randAbstar::bulidIntervalIdx() {
    auto abIter = abBuffer.begin();
    size_t aDend;
    ///////// in the same star block, the timestamp is the same.
    const size_t timeBegin=(*abIter).find_last_of(" ")+1;
    const std::string time=(*abIter).substr(timeBegin, (*abIter).size()-timeBegin);
    timeDouble =atof(time.c_str());
    /////// in the sname star block, the timestamp is the same.
    std::string starId;
    pthread_mutex_lock(&idxBegin_mutex);
    std::set<std::string> *lastAbNameBuffer = abNameBuffer;
    abNameBuffer = new std::set<std::string>;
    for(abIter=abBuffer.begin();abIter!=abBuffer.end();++abIter) {
        aDend = (*abIter).find_first_of(" ");
        starId="ab_";
        starId.append((*abIter).substr(0,aDend));
        abNameBuffer->insert(starId);
            auto idxBeginIter = idxNewestStatus.find(starId);

            if(idxBeginIter==idxNewestStatus.end()) {
                //bulid inteval index
                //if the newest interval index status has not this star
                //add this star in intenvalIdx
                std::vector<std::pair<acl::string,double>> bufVec;
//                bufVec.push_back(std::make_pair(acl::string((starId+"_"+time+"_l").c_str()),timeDouble));
                bufVec.push_back(std::make_pair(acl::string((starId+"_"+time+"_r").c_str()),timeDouble));
                intenvalIdx.push_back(make_pair("idx_"+starId+"_"+time,bufVec));
                idxNewestStatus.insert(make_pair(starId, make_pair("l",time)));

                //append the begin time of abnorm to the tail of star information string
                (*abIter).append(" ").append(time);
                //if the newest interval index status has this star
            } else {
                //if the abnormal status is going on, add this star
                if((*idxBeginIter).second.first == "l"){
                    std::vector<std::pair<acl::string,double>> bufVec;
                    bufVec.push_back(std::make_pair(acl::string((starId+"_"+(*idxBeginIter).second.second+"_r").c_str()),timeDouble));
                    intenvalIdx.push_back(make_pair("idx_"+starId+"_"+(*idxBeginIter).second.second,bufVec));

                    //append the begin time of abnorm to the tail of star information string
                    (*abIter).append(" ").append((*idxBeginIter).second.second);
                } else {
                    //if the abnormal status is stopped and the star is abnormal again,
                    // add the star as the new abnormal begin
                    std::vector<std::pair<acl::string, double>> bufVec;
//                    bufVec.push_back(std::make_pair(acl::string((starId+"_"+time+"_l").c_str()),timeDouble));
                    bufVec.push_back(std::make_pair(acl::string((starId+"_"+time+"_r").c_str()),timeDouble));
                    intenvalIdx.push_back(make_pair("idx_" + starId + "_" + time,bufVec));
                    //we update this star status into the re-abnormal beginning
                    idxNewestStatus.at(starId)=std::make_pair("l",time);

                    //append the begin time of abnorm to the tail of star information string
                    (*abIter).append(" ").append(time);
                }
            }
        }
            if(lastAbNameBuffer != NULL) {
                //justify the last abnormal phase to be stopped or not
                /* the previous time :A
                 * star1 ...
                 * star2 ....
                 *
                 * the current time: B
                 * star2 .....
                 * star3 .
                 *
                 * A-B = star1, idxNewestStatus.at(star1)="r"
                 *
                 * the next time:C
                 * star1 .
                 * star2 .....
                 * B-C=star3,dxNewestStatus.at(star3)="r"
                 * and intenvalIdx.push_back(make_pair(starId, std::pair("l",time)));
                  */
                std::vector<std::string> abStarDifference;
                std::set_difference(lastAbNameBuffer->begin(),lastAbNameBuffer->end(),
                        abNameBuffer->begin(),abNameBuffer->end(),
                                    inserter(abStarDifference,abStarDifference.begin()));
                for(auto it = abStarDifference.begin();it!=abStarDifference.end();++it)
                    idxNewestStatus.at(*it)=std::make_pair("r",time);
                delete lastAbNameBuffer;
            }

    pthread_mutex_unlock(&idxBegin_mutex);
}

std::vector<std::pair<std::string,std::vector<std::pair<acl::string,double>>>> &randAbstar::getIntervalIdx() {
    return intenvalIdx;
}

size_t randAbstar::getAbStarTotalNum() {
    return idxNewestStatus.size();
}