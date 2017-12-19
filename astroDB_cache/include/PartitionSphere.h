//
// Created by Chen Yang on 10/12/16.
//
#ifndef PARTITIONSPHERE_H
#define	PARTITIONSPHERE_H

#include "cmhead.h"
#include "StarFile.h"

class PartitionSphere {
protected:

    double errRadius;
    double areaBox; //判断两颗星是一颗星的最大误差，默认为20角秒
    double minZoneLength; //3 times of areaBox
    double searchRadius; //search radius, great circle

    //for plane coordiante's partition
    int areaWidth;
    int areaHeight;
    float zoneInterval;
    int planeZoneX;
    int planeZoneY;
    int showProcessInfo;

    long raMini;
    long decMini;
    long raMaxi;
    long decMaxi;

    float raMinf;
    float decMinf;
    float raMaxf;
    float decMaxf;

    int absDecMin; //in north or south, the max is different,
    int absDecMax;

    int decNode; //number of subarea in dec
    int raNode; //number of subarea in ra
    int zoneLength; //arc second, length of subarea's width or height
    double factor; //=3600/zoneLength

    float *raRadiusIndex;

    int totalZone; //分区的总个数
    int totalStar; //星的总数
    CMZone *zoneArray; //分区数组

public:
    PartitionSphere();
    PartitionSphere(const PartitionSphere& orig);
    PartitionSphere(float errBox, float minZoneLen, float searchRds);
    virtual ~PartitionSphere();

    void partitonStarField(StarFile *starFile);
    void getMatchStar(CMStar *point);
    void getMatchStar1(CMStar *point);
    void getMatchStar2(CMStar *point);
    void freeZoneArray();

protected:
    void initRaRadiusIndex();
    void initAreaNode(CMStar *point);
    void addDataToTree(CMStar *head);
    double searchSimilarPoint(CMStar *branch, CMStar *point, CMStar **goalPoint);
    long *getPointSearchBranch(CMStar *point, long *number);
    long getPointBranch(CMStar *point);
    void getAreaBoundary(CMStar *head);
    void getZoneLength();
    void addPointToBranchSort(CMStar *point, CMZone *branch);
    void addPointToBranchNotSort(CMStar *point, CMZone *branch);
    bool hasSimilarPoint(CMStar *point);
    void freeStarList(CMStar *starList);

};

#endif	/* PARTITION_H */

