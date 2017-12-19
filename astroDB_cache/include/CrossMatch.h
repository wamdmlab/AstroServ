//
// Created by Chen Yang on 10/7/16.
//

#ifndef CROSSMATCH_H
#define	CROSSMATCH_H

#include "StarFile.h"
#include "Partition.h"
#include "PartitionSphere.h"
#include "randAbStar.h"


class CrossMatch {
public:

    static const int TimesOfErrorRadius = 10; //length of search radius

    CrossMatch();
    CrossMatch(const CrossMatch& orig);
    virtual ~CrossMatch();
    void match(char *refName, char *objName, float errorBox);
    void match(StarFile *refStarFile, StarFile *objStarFile, float errorBox);
    void match(StarFile *objStarFile, std::string &starTable,
                           Partition * zones, float errorBox, randAbstar * const abstar=NULL);
    void match(StarFile *objStarFile,std::vector<std::string> &starBlock,
                           Partition * zones, float errorBox, randAbstar * const abstar=NULL);
    void match(StarFile *obj, Partition * zones, float errorBox,randAbstar * const abstar=NULL);
    void match(StarFile *ref, StarFile *obj,Partition * zones, float errorBox,
               randAbstar * const abstar=NULL);
    void compareResult(char *refName, char *objName, char *outName, float errorBox);
    void compareResult(StarFile *objStarFile,StarFile *objStarFileNoPtn, const char *outfName, float errorBox);
    void matchNoPartition(char *refName, char *objName, float errorBox);
    void matchNoPartition(StarFile *ref, StarFile *obj, float errorBox);
    //int printMatchedRstCount();
    void printMatchedRst(char *outfName, float errorBox);
    void printMatchedRst(char *outfName, StarFile *starList, float errorBox);
    void printOTStar(char *outfName, float errorBox);
    //int printOTStarCount();
    void printAllStarList(char *outfName, StarFile *starList, float errorBox);
    void freeAllMemory();
    void testCrossMatch();
    void partitionAndNoPartitionCompare();
    void setFieldHeight(float fieldHeight);
    void setFieldWidth(float fieldWidth);

protected:

    StarFile *refStarFile;
    StarFile *objStarFile;
    StarFile *refStarFileNoPtn;
    StarFile *objStarFileNoPtn;

protected:
    float errRadius; //两颗星匹配的最小距离
    Partition *zones;
    float fieldWidth; //星表视场的宽度，如果使用时不设置，则程序会自动计算，maxX-minX
    float fieldHeight; //星表视场的高度，如果使用时不设置，则程序会自动计算，maxY-minY
};


#endif	/* CROSSMATCH_H */

