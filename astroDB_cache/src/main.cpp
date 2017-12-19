/*
 ######################################################################
 ## Time-stamp:    2017/11/29
 ## Filename:      $Name:  $
 ## Version:       $Revision: 1.2 $
 ## Author:        Chen Yang <yangchenwo@126.com>
 ## Purpose:       Aserv's insertion component
 ## CVSAuthor:     $Author:  $
 ## Note:
 #-
 ## $Id: main.cpp,v 1.2 2017/11/29 08:11:15 $
 #======================================================
 */

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <sys/time.h>
#include <iostream>
#include <err.h>
#include "function.h"
#include "StarFileFits.h"
#include "CrossMatchSphere.h"
#include "CrossMatch.h"
#include "NamedPipe.h"
#include "sendToRedis.h"
#include <string.h>
#include "randAbStar.h"
#include "err.h"
#include "acl_cpp/lib_acl.hpp"
#include <math.h>
#include <cstring>
#include <iomanip>
#include "storeSourceData.h"

#define TestCrossMatch1

void showHelp();
void setDefaultValue();
int getStrValue(char *src, char *name, char *value);
int parsePara(int argc, char** argv);
void mainSphere(char *refFile, char *objFile, char *outFile);
void mainPlane(char *refFile, char *objFile, char *outFile);
void mainSphereTest(char *refFile, char *objFile, char *outFile);
void mainPlaneTest(char *refFile, char *objFile, char *outFile);

int matchOT; //is match ot in database, may be need acceleration
int showResult; //0输出所有结果，1输出匹配的结果，2输出不匹配的结果
int printResult; //0将匹配结果不输出到终端，1输出到终端
int showProcessInfo; //0不输出处理过程信息，1输出
int fitsHDU = 3; //fitsHDU=3: read fits file from the 3rd hdu
int useCross; //use cross method
double minZoneLength; //the min length of the zone's side
double searchRadius;
float areaBox; //判断两颗星是一颗星的最大误差，默认为20角秒

int wcsext = 2;
float magErrThreshold = 0.05; //used by getMagDiff
float minZoneLen = areaBox * 10;
float searchRds = areaBox;

int dbConfigInCommandLine;
int areaWidth;
int areaHeight;
float planeErrorRedius;
int fluxRatioSDTimes; //factor of flux filter

int gridX; //对星表进行分区计算fluxratio，gridX为分区X方向上的个数。
int gridY; //对星表进行分区计算fluxratio，gridY为分区Y方向上的个数。

//query server cluster uses the cpu number
//never use in 2017.5.11
int cpu;
int method;
char *cmdDbInfo;
char *refFile;
char *objFile;
char *outFile;
char *configFile;
//StoreDataPostgres *dataStore;
StarFileFits *refStarFile = NULL, *objStarFile = NULL;
Partition *zones;

acl::redis_client_cluster *cluster= new acl::redis_client_cluster();
//acl::redis_client *conn;
char redisHost[50]={0};
int crossTime = 0;
int threadNum = 5;
string writeRedisMode = LINENOCACHE;
int blockNum =10;
storeIntoLocalDisk storeDisk(DEFAULT_STORE_THREAD_NUM);
// the polar coordinates of offsets for xy coordinate system
//currently, it is no use at 27/9 2017
int xi = 0;
int yi = 0;
struct cmd {
    char *command;
    int count;
};

void setDefaultValue() {

	method = PLANE_METHOD;
	cpu = 32;
	gridX = 1;
	gridY = 1;
	areaWidth = 3016;
	areaHeight = 3016;
	matchOT = 0;
	areaBox = 1.5;
	minZoneLength = areaBox;
	searchRadius = 50;
	fitsHDU = 3;
}



void * crossThread( void * command) {

struct cmd *comm = static_cast<struct cmd *>(command);

    char *cmdstr = comm->command;
    int genCount = comm->count;
    char  *buffer;
    double randParam[2]={0};
    //randParam[0] is the bernoulliParam
    //randParam[1] is the geometricParam
    for(int randParamNum = 0;randParamNum !=2; ++randParamNum) {
        buffer = strrchr(cmdstr,' ')+1;
         randParam[1-randParamNum]=atof(buffer);
        *(buffer-1) = '\0';
    }
    char *isspace=strrchr(cmdstr,' ');
    const char *filePath= isspace + 1;
    *isspace = '\0';
    string commandString(cmdstr);
    isspace = strchr(cmdstr,' ');
    *isspace='\0';
    const char *keyString = cmdstr;


    std::cout<<"cross target star file: "<<filePath<<std::endl;
	if (method == PLANE_METHOD) {
        try{
		if (areaWidth == 0 || areaHeight == 0) {
            std::string errinfo="in plane coordinate mode, must assign \"-width\" and \"-height\"\n";
            recordErr(errinfo, __FILE__,__LINE__);
        }
    } catch (runtime_error err) {
        std::cerr<<err.what();
        exit(EXIT_FAILURE);
    }
		//#ifndef TestCrossMatch
		//mainPlane(refFile, objFile, outFile);

		//printf("starting plane cross match...\n");


        //randParam[0] is bernoulliParam.
        // control the abnorm is occuring or not. bernoulliParam [0,1] is larger,
        // the abnorm will occur more easily.
        //randParam[1] is geometricParam.
        // control the number of abnormal stars if the abnorm is occuring.
        // if geometricParam is larger in abnorm phase, the abnormal stars will be more.
        randAbstar *abstar = new randAbstar(randParam[0],randParam[1]);

          if(abstar->isAb())
             abstar->setAbMag();  // generate the new abnormal stars

        struct timeval start, end;
		gettimeofday(&start, NULL);

		//dataStore->store(refStarFile, 1);

		StarFileFits *objStarFile = new StarFileFits(filePath, areaBox, fitsHDU, wcsext,
				fluxRatioSDTimes, magErrThreshold, gridX, gridY, xi ,yi);
        // read the object star table.
        objStarFile->readStar(false,writeRedisMode);
		//objStarFile->readProerty();
		objStarFile->setFieldWidth(areaWidth);
		objStarFile->setFieldHeight(areaHeight);

		CrossMatch *cm = new CrossMatch();
		cm->setFieldHeight(areaHeight);
		cm->setFieldWidth(areaWidth);
		//目前minZoneLength和searchRadius没有考虑
		// core code!!
        std::string starTable; // use in SENDTABLE mode

		if(writeRedisMode==LINECACHE)
                cm->match(refStarFile, objStarFile, zones, areaBox,abstar);
        else if(writeRedisMode==LINENOCACHE) {
			objStarFile->matchedStarArrReverse(MAX_CACHE_LINE);
			cm->match(objStarFile, zones, areaBox, abstar);
		}
        else if(writeRedisMode==SENDTABLE)
            cm->match(objStarFile,starTable,zones, areaBox,abstar);
        else if(writeRedisMode==SENDBLOCK) {
            objStarFile->matchedStarArrResize(blockNum); //use in SENDBLOCK mode
			cm->match(objStarFile,objStarFile->matchedStar,zones, areaBox,abstar);
		}

//        auto &intervalIdx = abstar->getIntervalIdx();
//        auto &abStarData = abstar->getabBuffer();
		objStarFile->getMagDiff();
		objStarFile->fluxNorm();
		objStarFile->tagFluxLargeVariation();
		objStarFile->judgeInAreaPlane();
		//dataStore->store(objStarFile, 0);
		//cm->printMatchedRst(outFile, objStarFile, areaBox);


		//gettimeofday(&end, NULL);

        if(writeRedisMode==LINECACHE){
            sendPerLine *spl = new sendPerLine();
            spl->sendResWithCache(cluster, refStarFile, threadNum, writeRedisMode,abstar);
            delete spl;
        }
        else if(writeRedisMode==LINENOCACHE){
            sendPerLine *spl = new sendPerLine();
            spl->sendResWithNoCache(cluster, objStarFile, threadNum, writeRedisMode,abstar);
            delete spl;
        }
        else if(writeRedisMode==SENDTABLE) {
			sendPerBlock *spb = new sendPerBlock();
			spb->sendResWithPerTable(cluster, starTable,threadNum,
                                     objStarFile->starList->time,abstar);
			delete spb;
		}
        else if(writeRedisMode==SENDBLOCK) {
            sendPerBlock *spb = new sendPerBlock();
            spb->sendResWithPerBlock(cluster,objStarFile->matchedStar,
                                     objStarFile->matchedStar_abNum,
                                     objStarFile->matchedStar_newAbNum,
                                     threadNum, writeRedisMode, genCount,
                                     objStarFile->starList->time,abstar);
            delete spb;
        }

		gettimeofday(&end, NULL);
        objStarFile->abStar=abstar->getAbStarNum();
        size_t aa=0, aaa=0;
//        for(auto i : objStarFile->matchedStar_abNum) aa+=i;
//        for(auto i : objStarFile->matchedStar_newAbNum) aaa+=i;
		float time_use=(end.tv_sec-start.tv_sec)*1000000+(end.tv_usec-start.tv_usec);//微秒
		ostringstream stringStream;
		stringStream << " "<< time_use/1000000 << " "
				<< objStarFile->matchedCount << " " << objStarFile->OTStarCount <<
				" " << objStarFile->abStar;

		commandString = commandString.append(stringStream.str());

        storeDisk.addStoreThread(objStarFile);
		// send results to redis, such as the command, the send time, and the matched star number,
        // the original star number and the abnormal star number.

		acl::redis cmd;
		cmd.set_cluster(cluster, MAX_CONNS);
		try {
			int failedTime = SEND_RETRY_TIME;
			while (cmd.rpush(keyString, commandString.c_str(), NULL) < 0&&failedTime) {
				sleep(SEND_RETRY_SLEEP_TIME);
				--failedTime;
			}
			if (!failedTime)
			{
				std::string errinfo="The results cannot insert redis.";
				recordErr(errinfo, __FILE__,__LINE__);
			}
		} catch(runtime_error err){
               std::cerr<<err.what();
			   exit(EXIT_FAILURE);
			}
		cmd.clear();

        //delete refStarFile;
        delete cm;

        //delete objStarFile in classes of storeSourceData.h
        //delete objStarFile;
        delete abstar;
        delete comm->command;
        delete command;
	}

    std::cout<<commandString<<std::endl;
    if(writeRedisMode==LINECACHE){
        std::cout << "part of star data are insert success in current inteval time with perLineWithCache mode!"
                  <<std::endl;
    }
    else if(writeRedisMode==LINENOCACHE){
        std::cout << "star data are insert success with perLineWithNoCache mode!"
                  <<std::endl;
    }
    else if(writeRedisMode==SENDTABLE)
		std::cout << "star data as a single string are insert success with starTableAsaString mode!"
				  <<std::endl;
    else if(writeRedisMode==SENDBLOCK)
        std::cout << "star data as mutil-block are insert success with starBlockAsaString mode!"
                  <<std::endl;;


    pthread_exit(0);
}


void *sendSpaceIdx(void * arg) {


    const parameters *pms = (parameters *) arg;
    size_t lo = pms->lo;
    size_t hi = pms->hi;
    const std::vector<std::string> *sendList = pms->proToMatchedStar;

    acl::redis cmd;
    cmd.set_cluster(pms->cluster, MAX_CONNS);
    for (size_t i = lo; i != hi; i++) {
        if (sendList->at(i).empty())
            continue;
        std::ostringstream ostring;
        ostring << i;
        std::string key = pms->mode + "_" + ostring.str();

        //**********************filter some partition to send**********************
//        size_t blockNoStart = key.find_last_of('_')+1;
//        size_t blockNoEnd = key.length();
//        int blockNo;
//        sscanf(key.substr(blockNoStart,blockNoEnd-blockNoStart).c_str(),"%d",&blockNo);
//        if((blockNo+1)%4 != 1)
//            continue;
        //**********************filter some partition to send**********************

        try {
            int failedTime = 0;
            while (cmd.set(key.c_str(), refStarFile->matchedStar.at(i).c_str()) < 0) {
                ++failedTime;
                if (failedTime > 5 && failedTime != SEND_RETRY_TIME) {
                    // when redis nodes is down, the insert is failed
                    //if the redis nodes is restarted, the insert will still be failed.
                    //we reset cluster to update the hash slot information, and the insert will be success.
                    cluster->set(redisHost, MAX_CONNS, 10, 10);
                    cmd.set_cluster(cluster, MAX_CONNS);
                    sleep(SEND_RETRY_SLEEP_TIME);
                } else if (failedTime == SEND_RETRY_TIME) {
                    std::ostringstream oString;
                    oString << "after retrying " << SEND_RETRY_TIME << " times,"
                            << " inserting space index failed: CCD " << key << "and the error code is "
                            << cmd.result_error()
                            << "from redis";
                    std::string errinfo = oString.str();
                    recordErr(errinfo, __FILE__, __LINE__);
                }
            }
        } catch (std::runtime_error err) {
            std::cerr << err.what();
            exit(EXIT_FAILURE);
        }
        cmd.clear();

    }
    pthread_exit(0);
}

// write space partitions of the reference star table into Redis
// with tuple (starid, space paritition id) when writeRedisMode is starBlockAsaString
void initSpaceIndex() {

    CrossMatch *cm = new CrossMatch();
    cm->setFieldHeight(areaHeight);
    cm->setFieldWidth(areaWidth);

    refStarFile->matchedStarArrResize(blockNum);
    cm->match(refStarFile,refStarFile->matchedStar,zones, areaBox);
    char key[50] = "spaceIdx_";
	ostringstream tmp;
	tmp<<refStarFile->starListCopy->ccdNum;
    char ccdinfo_key[50] = "ccd_";
    strcat(ccdinfo_key,tmp.str().c_str());
	strcat(key,tmp.str().c_str());

    ostringstream ccdinfo_val;
    ccdinfo_val<<std::setprecision(9)<<refStarFile->ra0<<" "<<refStarFile->dec0
               <<" "<<zones->getMinX()<<" "<<zones->getMinY()<<" "
               <<zones->getMaxX()<<" "<<zones->getMaxY();


    //std::cerr<<ccdinfo_val.str().c_str();
    acl::redis cmd;
//    string aa = ccdinfo_val.str().c_str();
    cmd.set_cluster(cluster, MAX_CONNS);
    try {
        int failedTime = 0;
        while (cmd.set(ccdinfo_key, ccdinfo_val.str().c_str()) < 0) {
            ++failedTime;
            if (failedTime > 5 && failedTime != SEND_RETRY_TIME) {
                // when redis nodes is down, the insert is failed
                //if the redis nodes is restarted, the insert will still be failed.
                //we reset cluster to update the hash slot information, and the insert will be success.
                cluster->set(redisHost, MAX_CONNS, 10, 10);
                cmd.set_cluster(cluster, MAX_CONNS);
                sleep(SEND_RETRY_SLEEP_TIME);
            } else if (failedTime == SEND_RETRY_TIME) {
                std::ostringstream oString;
                oString << "after retrying " << SEND_RETRY_TIME << " times,"
                        << " inserting ccd information failed: CCD " << ccdinfo_key << "and the error code is "
                        << cmd.result_error()
                        << "from redis";
                std::string errinfo = oString.str();
                recordErr(errinfo, __FILE__, __LINE__);
            }
        }
    } catch (std::runtime_error err) {
        std::cerr << err.what();
        exit(EXIT_FAILURE);
    }
    cmd.clear();


    const size_t starBlockNum = blockNum;
    const size_t thNum = starBlockNum<threadNum?starBlockNum:threadNum;
    pthread_attr_t attrs[thNum];
    pthread_t ids[thNum];
    parameters pms[thNum];
    size_t start=0;
    size_t end=starBlockNum;
    size_t steps =  end / thNum;
    int i;
    for( i = 0; i != thNum -1; i++) { //
        pms[i].mode = std::string(key);
        pms[i].cluster = cluster;
        pms[i].proToMatchedStar = &refStarFile->matchedStar;
        pms[i].lo = start + i * steps;
        pms[i].hi = pms[i].lo + steps;

        pthread_attr_init(&attrs[i]);

        // create first thread
        //
        pthread_create(&ids[i], &attrs[i], sendSpaceIdx, &pms[i]);
    }

    pms[i].mode = std::string(key);
    pms[i].cluster = cluster;
    pms[i].proToMatchedStar = &refStarFile->matchedStar;
    pms[i].lo = start + i * steps;
    pms[i].hi = end;
    pthread_attr_init(&attrs[i]);
    // create last thread
    pthread_create(&ids[i], &attrs[i], sendSpaceIdx, &pms[i]);

    // wait for all threads stopping
    for( i = 0; i < thNum; i++) {
        pthread_join(ids[i], NULL);
        pthread_attr_destroy(&attrs[i]);
    }

    delete cm;
    refStarFile->delStarListCopy();
    refStarFile->matchedStar.clear();
}

/**
 * 目前minZoneLength和searchRadius没有考虑
 */


int main(int argc, char** argv) {

	if (argc == 1) {
		showHelp();
		return 0;
	}

	setDefaultValue();

	cmdDbInfo = (char*) malloc(LINE);
	memset(cmdDbInfo, 0, LINE);
	refFile = (char*) malloc(LINE);
	memset(refFile, 0, LINE);
	objFile = (char*) malloc(LINE);
	memset(objFile, 0, LINE);
	outFile = (char*) malloc(LINE);
	memset(outFile, 0, LINE);
	configFile = (char*) malloc(LINE);
	memset(configFile, 0, LINE);

	//dataStore = new StoreDataPostgres();

	if (parsePara(argc, argv) == 0) {
		return 0;
	}
	NamedPipe pipe("/tmp/Squirrel_pipe_test");
	// init the cluster pointer using the redis_client_cluster()
    sendPerLine::RedisInit(redisHost,cluster);

	// refStar stay in memory
	refStarFile = new StarFileFits(refFile, areaBox, fitsHDU, wcsext,
			fluxRatioSDTimes, magErrThreshold, gridX, gridY, xi, yi);


	refStarFile->readStar(true,writeRedisMode);    //read template table
	//refStarFile->readStar(false);
	//refStarFile->readProerty();

    zones = new Partition(areaBox, minZoneLen, searchRds);
	zones->partitonStarField(refStarFile);

    if(writeRedisMode == SENDBLOCK)
         initSpaceIndex();

    // copy refrence star name to randAbstar
    randAbstar::initRandAbstar(zones);

    //printf("ref stars init time is: %ld us\n",
	//		(end.tv_sec-start.tv_sec)*1000000+(end.tv_usec-start.tv_usec));

	//scanf("%s", command);

	//int i = 0;
	//while (strcmp(command, "exit") != 0) {
	pipe.openfifo();
	char *command = new char[pipe.buffer_size];
            strcpy(command, pipe.getCommand());
    std::vector<pthread_t> pidVector;
    // the genarateing number to add into the abnormal star count view
    int genCount = 0;
    struct cmd *comm = new struct cmd;
	while (strcmp(command, "Squirrel_exit") != 0) {
        ++genCount;
        comm->command=command;
        comm->count=genCount;
		// call another process thread every time receiving a signal
        pthread_attr_t crossProcessAttrs;
		pthread_t crossProcessId;
		pthread_attr_init(&crossProcessAttrs);
		// create first thread
        pthread_create(&crossProcessId, &crossProcessAttrs, crossThread, comm);
		//pthread_join(crossProcessId, NULL);
        //delete the command pointer in crossThread function;
        command = new char[pipe.buffer_size];
        comm = new struct cmd;
        strcpy(command, pipe.getCommand());
        pthread_attr_destroy(&crossProcessAttrs);
        pidVector.push_back(crossProcessId);
	}
    for(std::vector<pthread_t>::iterator iter = pidVector.begin();iter!=pidVector.end();iter++) {
          int ret = pthread_join(*iter, NULL);
          if(ret!=0) {
              cout << "pthread_join error when wating crossThread: error_code=" << ret << endl;
          }
      }

    if(writeRedisMode == LINECACHE){
        sendPerLine *sendRest=new sendPerLine();
        sendRest->sendResWithNoCache(cluster,refStarFile,threadNum,writeRedisMode);
        delete sendRest;
    }
      delete command;
      delete cluster;
      delete refStarFile;
      delete zones;
      randAbstar::delrandAbstar();
	//dataStore->matchOTFlag = matchOT;

	//if (dbConfigInCommandLine == 0) {
	//  dataStore->readDbInfo(configFile);
	//} else {
	//  getDBInfo(cmdDbInfo, dataStore);
	//}
	free(cmdDbInfo);
	free(refFile);
	free(objFile);
	free(outFile);
	free(configFile);
	//cluster->stop_monitor(true);
	//delete dataStore;

	return 0;
}

void mainPlane(char *refFile, char *objFile, char *outFile) {

	printf("starting plane cross match...\n");

	long start, end;
	start = clock();

	int wcsext = 2;
	float magErrThreshold = 0.05; //used by getMagDiff

	StarFileFits *refStarFile, *objStarFile;
	refStarFile = new StarFileFits(refFile, areaBox, fitsHDU, wcsext,
			fluxRatioSDTimes, magErrThreshold, gridX, gridY,xi,yi);
	refStarFile->readStar(true,writeRedisMode);
	refStarFile->readProerty();
	//dataStore->store(refStarFile, 1);

	objStarFile = new StarFileFits(objFile, areaBox, fitsHDU, wcsext,
			fluxRatioSDTimes, magErrThreshold, gridX, gridY,xi,yi);
	objStarFile->readStar(false,writeRedisMode);
	objStarFile->readProerty();
	objStarFile->setFieldWidth(areaWidth);
	objStarFile->setFieldHeight(areaHeight);

	CrossMatch *cm = new CrossMatch();
	cm->setFieldHeight(areaHeight);
	cm->setFieldWidth(areaWidth);
	//目前minZoneLength和searchRadius没有考虑
	// core code!!
	cm->match(refStarFile, objStarFile, areaBox);
	objStarFile->getMagDiff();
	objStarFile->fluxNorm();
	objStarFile->tagFluxLargeVariation();
	objStarFile->judgeInAreaPlane();
	//dataStore->store(objStarFile, 0);
	cm->printMatchedRst(outFile, objStarFile, areaBox);

	delete cm;
	delete objStarFile;
	delete refStarFile;

	end = clock();
	printf("total time is: %fs\n", (end - start) * 1.0 / ONESECOND);
}

/**
 * 天球坐标匹配
 * @param refFile 模板星表文件
 * @param objFile 目标星表文件
 * @param outFile
 */
void mainSphere(char *refFile, char *objFile, char *outFile) {

	printf("starting sphere cross match...\n");

	long start, end;
	start = clock();

	int wcsext = 2;
	float magErrThreshold = 0.05; //used by getMagDiffdata

	StarFileFits *refStarFile, *objStarFile;
	refStarFile = new StarFileFits(refFile, areaBox, fitsHDU, wcsext,
			fluxRatioSDTimes, magErrThreshold, gridX, gridY,xi,yi);
	refStarFile->readStar(true,writeRedisMode);
	refStarFile->readProerty();
	//dataStore->store(refStarFile, 1);

	objStarFile = new StarFileFits(objFile, areaBox, fitsHDU, wcsext,
			fluxRatioSDTimes, magErrThreshold, gridX, gridY,xi,yi);
	objStarFile->readStar(false,writeRedisMode);
	objStarFile->readProerty();

	CrossMatchSphere *cms = new CrossMatchSphere();
	//目前minZoneLength和searchRadius没有考虑
	cms->match(refStarFile, objStarFile, areaBox);
	objStarFile->getMagDiff();
	objStarFile->fluxNorm();
	objStarFile->tagFluxLargeVariation();
	objStarFile->wcsJudge(wcsext);
	//dataStore->store(objStarFile, 0);

	delete cms;
	delete objStarFile;
	delete refStarFile;

	end = clock();
	printf("total time is: %fs\n", (end - start) * 1.0 / ONESECOND);
}

/**
 * 如果返回0,则表示解析参数出错
 * @param argc
 * @param argv
 * @return 0 or 1
 */
int parsePara(int argc, char** argv) {

	int i = 0;
	for (i = 1; i < argc; i++) {
		if (strcmp(argv[i], "-method") == 0) {
			if (i + 1 >= argc || strlen(argv[i + 1]) == 0) {
				printf("-method must follow a stirng\n");
				return 0;
			}
			if (strcmp(argv[i + 1], "sphere") == 0) {
				method = SPHERE_METHOD;
			} else {
				method = PLANE_METHOD;
			}
			i++;
		} else if (strcmp(argv[i], "-redisHost") == 0) {
			if (i + 1 >= argc || strlen(argv[i + 1]) == 0) {
				printf("-redisHost must follow a stirng\n");
				return 0;
			}
			strcpy(redisHost, argv[i + 1]);
            if(!strstr(redisHost,":"))
            {
                printf("ERROR: redisHost must be IP:PORT.\n");
                return 0;
            }
			i++;

		} else if (strcmp(argv[i], "-threadNumber") == 0) {
            if (i + 1 >= argc || strlen(argv[i + 1]) == 0) {
                printf("-redisHost must follow an integer\n");
                return 0;
            }
            threadNum = atoi(argv[i + 1]);
            i++;
        } else if (strcmp(argv[i], "-xi") == 0) {
                if (i + 1 >= argc || strlen(argv[i + 1]) == 0) {
                    printf("-xi must follow an integer\n");
                    return 0;
                }
                xi = atoi(argv[i + 1]);
                i++;

		} else if (strcmp(argv[i], "-yi") == 0) {
            if (i + 1 >= argc || strlen(argv[i + 1]) == 0) {
                printf("-xi must follow an integer\n");
                return 0;
            }
            yi = atoi(argv[i + 1]);
            i++;

        }
        else if (strcmp(argv[i], "-writeRedisMode") == 0){
            if (i + 1 >= argc || strlen(argv[i + 1]) == 0) {
                printf("-writeRedisMode must follow a string\n");
                return 0;
            }
            writeRedisMode=argv[i + 1];
              i++;
        } else if (strcmp(argv[i], "-blockNum") == 0){
			if (i + 1 >= argc || strlen(argv[i + 1]) == 0) {
				printf("-blockNum must follow an integer in perBlockAsaString mode\n");
				return 0;
			}
			blockNum = atoi(argv[i + 1]);
			i++;
		}
//        else if (strcmp(argv[i], "-bernoulliParam") == 0){
//            if (i + 1 >= argc || strlen(argv[i + 1]) == 0) {
//                printf("-bernoulliParam must follow a floating number\n");
//                return 0;
//            }
//            bernoulliParam = atof(argv[i + 1]);
//            i++;
//        }
//        else if (strcmp(argv[i], "-geometricParam") == 0){
//            if (i + 1 >= argc || strlen(argv[i + 1]) == 0) {
//                printf("-geometricParam must follow a floating number\n");
//                return 0;
//            }
//            geometricParam = atof(argv[i + 1]);
//            i++;
//        }
        else if (strcmp(argv[i], "-times") == 0) {
			if (i + 1 >= argc || strlen(argv[i + 1]) == 0) {
				printf("-times must follow a integer\n");
				return 0;
			}
			crossTime = atoi(argv[i + 1]);
			i++;

		} else if (strcmp(argv[i], "-errorRadius") == 0) {
			if (i + 1 >= argc || strlen(argv[i + 1]) == 0) {
				printf("-errorRadius must follow a number\n");
				return 0;
			}
			areaBox = atof(argv[i + 1]);
			minZoneLength = areaBox;
			searchRadius = areaBox;
			i++;
		} else if (strcmp(argv[i], "-searchRadius") == 0) {
			if (i + 1 >= argc || strlen(argv[i + 1]) == 0) {
				printf("-searchRadius must follow a number\n");
				return 0;
			}
			searchRadius = atof(argv[i + 1]);
			i++;
		} else if (strcmp(argv[i], "-minZoneLength") == 0) {
			if (i + 1 >= argc || strlen(argv[i + 1]) == 0) {
				printf("-minZoneLength must follow a number\n");
				return 0;
			}
			minZoneLength = atof(argv[i + 1]);
			i++;
		} else if (strcmp(argv[i], "-width") == 0) {
            if (i + 1 >= argc || strlen(argv[i + 1]) == 0) {
                printf("-width must follow a number\n");
                return 0;
            }
            areaWidth = atoi(argv[i + 1]);
            i++;
        }
            else if (strcmp(argv[i], "-cpu") == 0) {
                if (i + 1 >= argc || strlen(argv[i + 1]) == 0) {
                    printf("-cpu must follow a number\n");
                    return 0;
                }
                cpu = atoi(argv[i + 1]);
                i++;
		} else if (strcmp(argv[i], "-height") == 0) {
			if (i + 1 >= argc || strlen(argv[i + 1]) == 0) {
				printf("-height must follow a number\n");
				return 0;
			}
			areaHeight = atoi(argv[i + 1]);
			i++;
		} else if (strcmp(argv[i], "-fitsHDU") == 0) {
			if (i + 1 >= argc || strlen(argv[i + 1]) == 0) {
				printf("-fitsHDU must follow a numbe\n");
				return 0;
			}
			fitsHDU = atoi(argv[i + 1]);
			i++;
		} else if (strcmp(argv[i], "-fluxSDTimes") == 0) {
			if (i + 1 >= argc || strlen(argv[i + 1]) == 0) {
				printf("-fluxSDTimes must follow a number\n");
				return 0;
			}
			fluxRatioSDTimes = atoi(argv[i + 1]);
			i++;
		} else if (strcmp(argv[i], "-cross") == 0) {
			useCross = 1;
		} else if (strcmp(argv[i], "-ref") == 0) {
			if (i + 1 >= argc || strlen(argv[i + 1]) == 0) {
				printf("-ref must follow reference file name\n");
				return 0;
			}
			strcpy(refFile, argv[i + 1]);
			i++;
		} else if (strcmp(argv[i], "-sample") == 0) {
			if (i + 1 >= argc || strlen(argv[i + 1]) == 0) {
				printf("-sample must follow sample file name\n");
				return 0;
			}
			strcpy(objFile, argv[i + 1]);
			i++;
		} else if (strcmp(argv[i], "-output") == 0) {
			if (i + 1 >= argc || strlen(argv[i + 1]) == 0) {
				printf("-o must follow output file name\n");
				return 0;
			}
			strcpy(outFile, argv[i + 1]);
			i++;
		} else if (strcmp(argv[i], "-mode") == 0) {
			if (i + 1 >= argc || strlen(argv[i + 1]) == 0) {
				printf("-mode must follow cpu or gpu\n");
				return 0;
			}
			if (strcmp(argv[i + 1], "gpu") == 0) {
				cpu = 0;
			}
			i++;
		} else if (strcmp(argv[i], "-dbConfigFile") == 0) {
			if (i + 1 >= argc || strlen(argv[i + 1]) == 0) {
				printf("-mode must follow file name\n");
				return 0;
			}
			strcpy(configFile, argv[i + 1]);
			dbConfigInCommandLine = 0;
			i++;
		} else if (strcmp(argv[i], "-dbInfo") == 0) {
			if (i + 1 >= argc || strlen(argv[i + 1]) == 0) {
				printf("-mode must follow \"name1=value1,name2=value2...\"\n");
				return 0;
			}
			strcpy(cmdDbInfo, argv[i + 1]);
			dbConfigInCommandLine = 1;
			i++;
		} else if (strcmp(argv[i], "-show") == 0) {
			if (i + 1 >= argc || strlen(argv[i + 1]) == 0) {
				printf("-show must follow all, matched or unmatched\n");
				return 0;
			}
			if (strcmp(argv[i + 1], "all") == 0) {
				showResult = 0;
			} else if (strcmp(argv[i + 1], "matched") == 0) {
				showResult = 1;
			} else if (strcmp(argv[i + 1], "unmatched") == 0) {
				showResult = 2;
			} else {
				printf("-show must follow all, matched or unmatched\n");
			}
			i++;
		} else if (strcmp(argv[i], "-terminal") == 0) {
			printResult = 1;
		} else if (strcmp(argv[i], "-matchot") == 0) {
			matchOT = 1;
			printf("matchOT=%d\n", matchOT);
		} else if (strcmp(argv[i], "-processInfo") == 0) {
			showProcessInfo = 1;
		} else if (strcmp(argv[i], "-g") == 0
				|| strcmp(argv[i], "-grid") == 0) {
			if (i + 1 >= argc || strlen(argv[i + 1]) == 0) {
				printf("-g or -grid must follow number,number\n");
				return 0;
			}
			if (2 != sscanf(argv[i + 1], "%d,%d", &gridX, &gridY)) {
				printf("-g or -grid must follow number*number\n");
				return 0;
			}
			i++;
		} else if (strcmp(argv[i], "-h") == 0
				|| strcmp(argv[i], "-help") == 0) {
			showHelp();
			return 0;
		} else if (strcmp(argv[i], "-v") == 0
				|| strcmp(argv[i], "-version") == 0) {
			printf("%s\n", VERSION);
			return 0;
		} else {
			printf("%s is unknow parameter\n", argv[i]);
			showHelp();
			return 0;
		}
	}
	return 1;
}

void showHelp() {
	printf("crossmatch v1.2 (2012 Nov 15)\n");
	printf(
			"usage: crossmatch [-method <plane> ] [-errorRadius <20>] [-width <3096>] [...]\n");
	printf(
			"usage: crossmatch [-method <sphere>] [-errorRadius <0.00556>] [...]\n");
	printf(
			"-method <sphere|plane>:     cross match method, using sphere coordinate or plane corrdinate\n");
	printf(
			"-width <number>:            on plane corrdinate partition, the max value of X axis\n");
	printf(
			"-height <number>:           on plane corrdinate partition, the max value of Y axis\n");
	printf(
			"-errorRadius <number>:      the max error between two point, unit is degree, defalut 0.005556\n");
	printf(
			"-searchRadius <number>:     the search area's radius, unit is degree, defalut equals errorRadius\n");
	printf(
			"-minZoneLength <number>:    the min length of the zone's side, unit is degree, defalut equals errorRadius\n");
	//printf("                            default, the total zone number equals the reference catalog star numbers\n");
	printf(
			"-fitsHDU <number>:          read fits file from the fitsHDU-th data area\n");
	printf("-ref <path>:                reference table path\n");
	printf("-sample <path>:             sample table path\n");
	printf("-output <path>:             output table path\n");
	//printf("\t-mode\n");
	//printf("\t\tgpu: executed by gpu\n");
	//printf("\t\tcpu: executed by cpu\n");
	printf(
			"-dbConfigFile <fileName>:   file contain database config information.\n");
	printf(
			"                            Notice: -dbInfo is prior to -dbConfigFile\n");
	printf(
			"-dbInfo \"<name1=value1,name2=value2...>\": this option include the database configure information\n");
	printf(
			"                            host=localhost, IP Address or host name\n");
	printf(
			"                            port=5432, the port of PostgreSQL use\n");
	printf("                            dbname=svomdb, database name\n");
	printf("                            user=wanmeng, database user name\n");
	printf("                            password= ,database user password\n");
	printf("                            catfile_table=catfile_id \n");
	printf("                            match_talbe=crossmatch_id \n");
	printf("                            ot_table=ot_id \n");
	printf("                            ot_flux_table=ot_flux_id \n");
	printf("-show <all|matched|unmatched>:\n");
	printf(
			"                            all:show all stars in sample table including matched and unmatched\n");
	printf(
			"                            matched:show matched stars in sample table\n");
	printf(
			"                            unmatched:show unmatched stars in sample table\n");
	printf("-terminal:                  print result to terminal\n");
	printf("-matchot:                  print result to terminal\n");
	printf(
			"-cross:                     compare zone method with cross method, find the zone method omitted stars, and output to file\n");
	printf("-processInfo:               print process information\n");
	printf(
			"-fluxSDTimes <number>:      the times of flux SD, use to filter matched star with mag\n");
	printf(
			"-g <Xnumber,Ynumber>:       the partition number in X and Y direction, used to calculate fluxratio, default is 1,1\n");
	printf("-h or -help:                show help\n");
	printf("-v or -version:             show version number\n");
	printf("-redisHost:             set Redis host:port\n");
    printf("-writeRedisMode:             perLineWithCache: read from cache data and write redis with per line\n"
           "                             perLineWithNoCache: directly write data to redis without cache memory\n"
           "                             starTableAsaString: a star table as an entirety to write to redis\n"
           "                             perBlockAsaString: split the star table into several blocks and an block as an entirety\n");
	printf("-blockNum:                   split the star table into blockNUm blocks and write into redis in perBlockAsaString mode\n"
				                         "the default is 10\n");
    printf("-cpu:                        the query server cluster uses the cpu core number, which is used in the parition number of "
                                         "the space index (reference table)\n");
    printf("-xi:                          the polar coordinates of offsets for x-axis in xy coordinate system");
    printf("-yi:                          the polar coordinates of offsets for y-axis in xy coordinate system");
//    printf("-bernoulliParam:             control the abnorm is occuring or not. bernoulliParam [0,1] is larger,"
//                                         "the abnorm will occur more easily. The default is 0.5\n");
//    printf("-geometricParam:             control the number of abnormal stars if the abnorm is occuring. if geometricParam is larger"
//                                         " in abnorm phase, the abnormal stars will be more. The default is 0.5\n");
	printf("-times:             set Redis run times number\n");
	printf("example: \n");
	printf(
			"\t1: crossmatch -method sphere -g 2,2 -errorRadius 0.006(20 arcsec) -searchRadius 0.018 -fitsHDU 2 -ref reference.cat -sample sample.cat -output output.cat -processInfo\n");
	printf(
			"\t2: crossmatch -method plane  -g 2,2 -errorRadius 10 -searchRadius 30 -width 3096 -height 3096 -fitsHDU 2 -ref reference.cat -sample sample.cat -output output.cat -processInfo\n");
	//printf("Notes: default area box is 0.005556 degree, output all result, not print to terminal, not print process information, not compare the result with cross method\n");
}

int getStrValue(char *src, char *name, char *value) {

	char *str1 = NULL;
	char *str2 = NULL;
	char *start = NULL;
	str1 = strstr(src, name);
	if (str1 == 0)
		return 0;
	start = str1 + strlen(name) + 1;
	str2 = strchr(str1, ',');
	if (str2 == 0)
		str2 = src + strlen(src);
	int len = str2 - start;
	if (len != 0) {
		strncpy(value, start, len); //there is a =, so need add 1
		value[len] = '\0';
	} else {
		strcpy(value, "");
	}
	return 1;
}

/**
 * 比较分区方式和不分区方式匹配结果的区别
 * @param refFile
 * @param objFile
 * @param outFile
 */
void mainSphereTest(char *refFile, char *objFile, char *outFile) {

	int wcsext = 2;
	int magErrThreshold = 0.05; //used by getMagDiff

	StarFileFits *refStarFile, *objStarFile, *refnStarFile, *objnStarFile;
	refStarFile = new StarFileFits(refFile, areaBox, fitsHDU, wcsext,
			fluxRatioSDTimes, magErrThreshold, gridX, gridY,xi,yi);
	objStarFile = new StarFileFits(objFile, areaBox, fitsHDU, wcsext,
			fluxRatioSDTimes, magErrThreshold, gridX, gridY,xi,yi);
	refnStarFile = new StarFileFits(refFile, areaBox, fitsHDU, wcsext,
			fluxRatioSDTimes, magErrThreshold, gridX, gridY,xi,yi);
	objnStarFile = new StarFileFits(objFile, areaBox, fitsHDU, wcsext,
			fluxRatioSDTimes, magErrThreshold, gridX, gridY,xi,yi);
	refStarFile->readStar(true,writeRedisMode);
	objStarFile->readStar(false,writeRedisMode);
	refnStarFile->readStar(true,writeRedisMode);
	objnStarFile->readStar(false,writeRedisMode);

	CrossMatchSphere *cms = new CrossMatchSphere();
	printf("sphere match\n");
	cms->match(refStarFile, objStarFile, areaBox);
	printf("sphere match NoPartition\n");
	cms->matchNoPartition(refnStarFile, objnStarFile, areaBox);
	printf("sphere compare\n");
	cms->compareResult(objStarFile, objnStarFile, "out_sphere.cat", areaBox);

	delete objStarFile;
	delete refStarFile;
	delete objnStarFile;
	delete refnStarFile;
}

void mainPlaneTest(char *refFile, char *objFile, char *outFile) {

	int wcsext = 2;
	int magErrThreshold = 0.05; //used by getMagDiff

	StarFileFits *refStarFile, *objStarFile, *refnStarFile, *objnStarFile;
	refStarFile = new StarFileFits(refFile, areaBox, fitsHDU, wcsext,
			fluxRatioSDTimes, magErrThreshold, gridX, gridY,xi,yi);
	objStarFile = new StarFileFits(objFile, areaBox, fitsHDU, wcsext,
			fluxRatioSDTimes, magErrThreshold, gridX, gridY,xi,yi);
	refnStarFile = new StarFileFits(refFile, areaBox, fitsHDU, wcsext,
			fluxRatioSDTimes, magErrThreshold, gridX, gridY,xi,yi);
	objnStarFile = new StarFileFits(objFile, areaBox, fitsHDU, wcsext,
			fluxRatioSDTimes, magErrThreshold, gridX, gridY,xi,yi);
	refStarFile->readStar(true,writeRedisMode);
	objStarFile->readStar(false,writeRedisMode);
	refnStarFile->readStar(true,writeRedisMode);
	objnStarFile->readStar(false,writeRedisMode);

	CrossMatch *cms = new CrossMatch();
	printf("plane match\n");
	cms->match(refStarFile, objStarFile, areaBox);
	printf("plane match NoPartition\n");
	cms->matchNoPartition(refnStarFile, objnStarFile, areaBox);
	printf("plane compare\n");
	cms->compareResult(objStarFile, objnStarFile, "out_plane.cat", areaBox);

	delete objStarFile;
	delete refStarFile;
	delete objnStarFile;
	delete refnStarFile;

}
