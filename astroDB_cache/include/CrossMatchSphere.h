//
// Created by Chen Yang on 3/30/17.
//

#ifndef CROSSMATCHSPHERE_H
#define	CROSSMATCHSPHERE_H

#include "Partition.h"
#include "PartitionSphere.h"

class CrossMatchSphere {
public:

    static const int TimesOfErrorRadius = 10; //length of search radius

    CrossMatchSphere();
    CrossMatchSphere(const CrossMatchSphere& orig);
    virtual ~CrossMatchSphere();

    void match(char *refName, char *objName, float errorBox);
    void match(StarFile *ref, StarFile *obj, float errorBox);
    void compareResult(char *refName, char *objName, char *outName, float errorBox);
    void compareResult(StarFile *objStarFile,StarFile *objStarFileNoPtn, const char *outfName, float errorBox);
    void matchNoPartition(char *refName, char *objName, float errorBox);
    void matchNoPartition(StarFile *refStarFileNoPtn, StarFile *objStarFileNoPtn, float errorBox);
    void printMatchedRst(char *outfName, float errorBox);
    void printMatchedRst(char *outfName, CMStar *starList, float errorBox);
    void printOTStar(char *outfName, float errorBox);
    void printOTStar2(char *outfName, float errorBox);
    void printAllStarList(char *outfName, CMStar *starList, float errorBox);
    void freeStarList(CMStar *starList);
    void freeAllMemory();

protected:

    StarFile *refStarFile;
    StarFile *objStarFile;
    StarFile *refStarFileNoPtn;
    StarFile *objStarFileNoPtn;

protected:
    float errRadius; //两颗星匹配的最小距离
    PartitionSphere *zones;
};

#endif	/* CROSSMATCHSPHERE_H */
