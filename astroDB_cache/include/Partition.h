//
// Created by Chen Yang on 10/12/16.
//

#ifndef PARTITION_H
#define	PARTITION_H

#include "cmhead.h"
#include "StarFile.h"

class Partition {
protected:
    float maxx;
    float maxy;
    float minx;
    float miny;
    float fieldWidth; //星表视场的宽度，maxX-minX, 当前不允许手动设置，后续计算依赖maxX-minX
    float fieldHeight; //星表视场的高度，maxY-minY,当前不允许手动设置，后续计算依赖maxY-minY

    float errRadius; //两颗星匹配的最小距离
    float searchRadius; //搜索匹配分区时的矩形搜索区域（边长为2*searchRadius）
    float minZoneLength; //最小分区长度 = 3*errRadius
    float zoneInterval; //实际分区长度
    float zoneIntervalRecp; //实际分区长度

    int zoneXnum; //分区在X方向上的个数
    int zoneYnum; //分区在Y方向上的个数
    int totalZone; //分区的总个数
    long totalStar; //星的总数
    CMZone *zoneArray; //分区数组

public:
    Partition();
    Partition(const Partition& orig);
    Partition(float errBox, float minZoneLen, float searchRds);
    virtual ~Partition();
    int getTotalZone();
    void partitonStarField(StarFile *starFile);
    std::pair<long, char *> getMatchStar(CMStar *objStar,bool isBlock=false);
    void printZoneDetail(char *fName);
    void freeZoneArray();
    CMZone *getZoneArr() const;
    int getZoneXnum();
    int getZoneYnum();
    void setSearchRadius(float searchRadius);
    float getSearchRadius() const;
    void setErrRadius(float errRadius);
    float getErrRadius() const;
    void setMinZoneLength(float minZoneLength);
    float getMinZoneLength() const;
    void setFieldHeight(float fieldHeight);
    void setFieldWidth(float fieldWidth);
    float getFieldHeight() const;
    float getFieldWidth() const;
    float getMinY() const;
    float getMinX() const;
    float getZoneInterval() const;
    float getMaxX() const;
    float getMaxY() const;

protected:
    CMStar *searchSimilarStar(long zoneIdx, CMStar *star);
    long *getStarSearchZone(CMStar *star, long &sZoneNum);
    long getZoneIndex(CMStar * star);
    void getMinMaxXY(CMStar *starList);
    void addStarToZone(CMStar *star, long zoneIdx);
    void freeStarList(CMStar *starList);
};

#endif	/* PARTITION_H */

