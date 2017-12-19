//
// Created by Chen Yang on 12/18/16.
//

#ifndef STARFILEREF_H
#define	STARFILEREF_H

#include "StarFile.h"
#include <vector>
#include <string>
#include "acl_cpp/lib_acl.hpp"
#define   MAX_CACHE_LINE   180001
class AreaBox {
public:
  int top;
  int left;
  int right;
  int down;
};

class FluxPartition {
public:
  int curIdx1;
  int curIdx2;
  int number1;
  int number2;
  double *fluxRatios1;
  double *fluxRatios2;
  double magDiff;
  double fluxRatioAverage;
  double fluxRatioMedian;
  double standardDeviation;
  double timesOfSD;
};

class StarFileFits : public StarFile{
protected:
  float fieldWidth; //星表视场的宽度
  float fieldHeight; //星表视场的高度

public:
  
  int fileExist;
  
  int showProcessInfo;

  const char * fileName;
  float areaBox;
  int fitsHDU;
  int wcsext;
  double airmass;
  double jd;

  double magDiff;
  double fluxRatioAverage;
  double fluxRatioMedian;
  double standardDeviation;
  int fluxRatioSDTimes; //abs(ratioRatio - flusRatioAverage) > flusRatioSD * standardDeviation;

  float magErrThreshold; //对magerr小于magErrThreshold的星计算magDiff

  int gridX;
  int gridY;
 // the polar coordinates of offsets for xy coordinate system
  int xi;
  int yi;
  FluxPartition *fluxPtn;
  //alluxio::jAlluxioFileSystem alluxioClient;
  //acl::redis_client_cluster *conn = NULL;
  // save temp star data

  StarFileFits();
  StarFileFits(const char * fileName);
  StarFileFits( const char* fileName, float areaBox, int fitsHDU, int wcsext,
            int fluxRatioSDTimes, float magErrThreshold, int gridX, int gridY,int xi, int yi);
//  StarFileFits( const char* fileName, float areaBox, int fitsHDU, int wcsext,
//              int fluxRatioSDTimes, float magErrThreshold, int gridX, int gridY,
//			  acl::redis_client_cluster *conn);
  StarFileFits(const StarFileFits& orig);
  virtual ~StarFileFits();

  void readStar(bool isRef,const std::string writeMode);
  void readStar(const char * fileName, bool isRef,const std::string writeMode);
  void readProerty();
  void setMagErrThreshold(float magErrThreshold);
  void setFitsHDU(int fitsHDU);
  void setAreaBox(float areaBox);
  void setFluxRatioSDTimes(int fluxRatioSDTimes);
  void setFileName(char* fileName);
  void getMagDiff();
  void fluxNorm();
  void tagFluxLargeVariation();
  void wcsJudge(int wcsext);
  void judgeInAreaPlane();
  void setFieldHeight(float fieldHeight);
  void setFieldWidth(float fieldWidth);
  void delStarListCopy();
private:
  double getFieldFromWCSFloat(const char *fileName, int wcsext, const char *field);
  int isInAreaBox(int x, int y, AreaBox ab);
  void printerror(int status);
  void setStandardDeviation();
  void setFluxRatioMedian();
  void setFluxRatioAverage();
  void setMagDiff();
  void freeFluxPtn();
  void setra0Anddec0Forsimdata();
};

#endif	/* STARFILEREF_H */

