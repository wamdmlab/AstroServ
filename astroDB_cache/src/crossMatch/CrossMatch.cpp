//
// Created by Chen Yang on 10/7/16.
//

#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <vector>
#include <math.h>
#include <time.h>
#include <err.h>

#include "StarFileFits.h"
#include "CrossMatch.h"
#include<iomanip>
#include "transform_tp_s.h"
#include <math.h>
#include <fstream>
extern int blockNum;

CrossMatch::CrossMatch() {

  refStarFile = NULL;
  objStarFile = NULL;
  refStarFileNoPtn = NULL;
  objStarFileNoPtn = NULL;
  zones = NULL;
  fieldWidth = 0;
  fieldHeight = 0;
}

CrossMatch::CrossMatch(const CrossMatch& orig) {
  fieldWidth = 0;
  fieldHeight = 0;
}

CrossMatch::~CrossMatch() {
  freeAllMemory();
}

void CrossMatch::match(StarFile *refStarFile, StarFile *objStarFile, float errorBox) {

    //refStarFile->starList = NULL;

    CMStar *nextStar = objStarFile->starList;
    while (nextStar) {
        // core code!!
       zones->getMatchStar(nextStar);
        nextStar = nextStar->next;
    }
#ifdef PRINT_CM_DETAIL
    printf("partition match done!\n");
#endif
}

void CrossMatch::match(char *refName, char *objName, float errorBox) {

    refStarFile = new StarFile();
    refStarFile->readStar(refName);
    objStarFile = new StarFile();
    objStarFile->readStar(objName);

    match(refStarFile, objStarFile, errorBox);
}

//send star table as a string
void CrossMatch::match(StarFile *objStarFile,std::string &starTable,
                       Partition * zones, float errorBox, randAbstar *const abstar) {

  //refStarFile->starList = NULL;
//  const bool isAb = (abstar==NULL)? false:(abstar->isAb());
//  if(isAb)
//    abstar->setAbMag();  // generate the new abnormal stars

  CMStar *nextStar = objStarFile->starList;
  while (nextStar) {
    // core code!!
    std::pair<long, char *> matchResult=zones->getMatchStar(nextStar);
    if (matchResult.first == -1) {
      objStarFile->OTStarCount++;
    } else {
      objStarFile->matchedCount++;
      // if the nextStar is abnormal star, add abnormal mag into the nextStar, or else return and do nothing
      abstar->popAbMag(nextStar, matchResult.second);
      starTable.append(matchResult.second).append("\n");
    }

    nextStar = nextStar->next;
  }

#ifdef PRINT_CM_DETAIL
  printf("partition match done!\n");
#endif
}

//send star table as mutil blocks
void CrossMatch::match(StarFile *objStarFile,std::vector<std::string> &starBlock,
                       Partition * zones, float errorBox,  randAbstar * const abstar) {

//    const bool isAb = (abstar==NULL)? false:(abstar->isAb());
//    if(isAb)
//        abstar->setAbMag();  // generate the new abnormal stars
  int totalZoneNum = zones->getTotalZone();
  try{
    if(totalZoneNum<blockNum)
    {
      std::string errinfo="the blockNum must be more than the zone num by patitioning";
      recordErr(errinfo, __FILE__,__LINE__);
    }
  } catch (std::runtime_error err) {
    std::cerr<<err.what();
    exit(EXIT_FAILURE);
  }
  int col=zones->getZoneXnum();    //the zone space colume.
                                  // the real boundary is slightly more than getZoneXnum(),
                                  // getZoneXnum() is the floor boundary
  int line=zones->getZoneYnum();  // the zone space line
  int col_block; //the x-axis block number
  int line_block; // the y-axis block number
  int col_inte;  // the x-axis block interval
  int line_inte;  // the y-axis block interval
  int co;
  int current_line_block_idx;
  int current_block_num;
  int rest_block_num;
  int index;
    try {
        if (blockNum <= 1) {
            std::string errinfo = "the blockNum must be more than 1";
            recordErr(errinfo, __FILE__, __LINE__);
        }
    }catch (std::runtime_error err) {
        std::cerr<<err.what();
        exit(EXIT_FAILURE);
    }

  for(co= static_cast<int>(floor(sqrt(blockNum)));co!=1;--co)
        if(blockNum%co==0) break;
  try{
    if(co==1){
      std::string errinfo="the blockNum cannot be a prime number";
      recordErr(errinfo, __FILE__,__LINE__);
    }
  } catch (std::runtime_error err) {
    std::cerr<<err.what();
    exit(EXIT_FAILURE);
  }

  //the maximum block num is in the longest line to make sure each block is nearly square.
  int maxblock = blockNum/co;
     if(maxblock<co){
       int a;
       a=maxblock;
       maxblock=co;
       co=a;
     }
  if(col>line){
    col_block=maxblock;
    line_block=co;
  }else{
    col_block=co;
    line_block=maxblock;
  }
//the maximum block num is in the longest line to make sure each block is nearly square.
    int last_block_line_idx=line-1;
    int last_block_col_idx=col-1;
    bool case1_line_flag= false;
    bool case1_col_flag= false;
  if(line%line_block!=0) {
      // E.g., case 1: line_block = 5 line=12 line_inte=3; the block form: 3 3 3 2 1,
      // the condition is line%line_block!=0&&line%(line_block-1)==0, the last block is always 1;
      // case 2: line_block = 5 line=7 line_inte=1; the block form: 1 1 1 1 3
      // the condition is line%line_block!=0&&line%(line_block-1)!=0
      line_inte = line / (line_block - 1);
      // proint the start position of the last line block like the block form: 1 1 1 1 3
      if(line%(line_block-1)!=0)
          last_block_line_idx = line_inte*(line_block-1);
      else
          case1_line_flag=true;
  } else
      // E.g., case 3: line_block = 5 line=25 line_inte=5; the block form:   5 5 5 5 5
      line_inte = line / line_block;

    if(col%col_block!=0){
    col_inte = col/(col_block-1);
    if(col%(col_block-1)!=0)
        last_block_col_idx = col_inte*(col_block-1);
        else
        case1_col_flag = true;
    }
  else
    col_inte = col/col_block;
  //refStarFile->starList = NULL;
    CMStar *nextStar;
    if(objStarFile->isRef == false)
        nextStar= objStarFile->starList;
    else {
        //the each zone interval
        const float zoneInterval = zones->getZoneInterval();

        /*   |     ____
         * y |    |    |
         *   |    |    |
     *  minY |....|____|
         *   |____:______
         *      minX   x
         *
         * */
        //the start x at x-axis which has the first star
        const float zoneMinX = zones->getMinX();
        //the start Y at y-axis which has the first star
        const float zoneMinY = zones->getMinY();
        const float zoneMaxX = zones->getMaxX();
        const float zoneMaxY = zones->getMaxY();
        double x;
        double y;
//        double ra;
//        double dec;
        int top_right_coordinate = -1;
        int lower_left_coordinate = -1;
//        int lower_right_coordinate = -1;
//        int top_left_coordinate = -1;
//        double ra0_radian = M_PI/180 * objStarFile->ra0;
//        double dec0_radian = M_PI/180 * objStarFile->dec0;
      //  std::ofstream of;
     //   of.open("radec");
        for(int i=0;i<=line_block;++i){
            if(i<line_block) {
                y = i * line_inte * zoneInterval + zoneMinY;
                if (i == line_block - 1 && case1_line_flag == true)
                    y = (i - 1) * line_inte * zoneInterval + (line_inte - 1) * zoneInterval + zoneMinY;
            }
            else
                y=zoneMaxY;
            for(int j=0;j<=col_block;++j) {
                if(j<col_block) {
                    x = j * col_inte * zoneInterval + zoneMinX;
                    if (j == col_block - 1 && case1_col_flag == true)
                        x = (j - 1) * col_inte * zoneInterval + (col_inte - 1) * zoneInterval + zoneMinX;
                }
                else
                    x = zoneMaxX;

//                //eliminate the lower right corner and top left corner
//                if((i==0 && j == col_block) || (i==line_block && j == 0))
//                    continue;

                //transform pixel xy into ra and dec according to
                // the centre ra0 and dec0 in one CCD picture
                //input radian, and return radian
//                x=x/zoneMaxX;
//                y=y/zoneMaxY;
//                slTP2S(x/zoneMaxX,y/zoneMaxY,ra0_radian,dec0_radian,ra,dec);

                // store the lower left and the top right coordinates of each block
                lower_left_coordinate = i * col_block +j;
//                lower_right_coordinate = lower_left_coordinate-1;
//                top_left_coordinate = lower_left_coordinate - col_block;
                top_right_coordinate = lower_left_coordinate - (col_block + 1);

                // lower_left_coordinate cannot exist in the col_block colume and the line_block line
                if(lower_left_coordinate<blockNum && j != col_block) {
                    std::ostringstream tmp;
                    //tmp <<std::setiosflags(std::ios::fixed);
                    tmp << std::setprecision(9)<<x <<" " << y;
                    starBlock.at(lower_left_coordinate).append(tmp.str()).append("\n");
                }

//                if(lower_right_coordinate < blockNum && j!=0) {
//                    std::ostringstream tmp;
//                    //tmp <<std::setiosflags(std::ios::fixed);
//                    tmp << std::setprecision(9)<<ra <<" " << dec;
//                    starBlock.at(lower_right_coordinate).append(tmp.str()).append("\n");
//                }
//                if(top_left_coordinate>=0 && j != col_block) {
//                    std::ostringstream tmp;
//                    //tmp <<std::setiosflags(std::ios::fixed);
//                    tmp << std::setprecision(9)<<ra <<" " << dec;
//                    starBlock.at(top_left_coordinate).append(tmp.str()).append("\n");
//                }
                // top_right_coordinate cannot exist in the 0 line and the 0 colume
                if(top_right_coordinate>=0 && j != 0) {
                    std::ostringstream tmp;
                    //tmp <<std::setiosflags(std::ios::fixed);
                    tmp << std::setprecision(9)<<x <<" " << y;
                    starBlock.at(top_right_coordinate).append(tmp.str()).append("\n");
                }
                 // of<<180/M_PI * ra <<" " <<180/M_PI * dec<<"\n";
//                of<<x/zoneMaxX <<" " <<y/zoneMaxY<<"\n";
            }
        }
       // of.close();
        nextStar = objStarFile->starListCopy;
    }

    // ******using in test when most stars skew in one block******
//    int index_text_iter = 0;
    // ******using in test when most stars skew in one block******

  while (nextStar) {
    // core code!!
    std::pair<long, char *> matchResult = zones->getMatchStar(nextStar, true);
      int abType;
    if (matchResult.first == -1 && objStarFile->isRef == false) {

      objStarFile->OTStarCount++;
    } else {
        if(objStarFile->isRef == false) {
            objStarFile->matchedCount++;

            // if the nextStar is abnormal star, add abnormal mag into the nextStar, or else return and do nothing
             abType = abstar->popAbMag(nextStar, matchResult.second);
        }
      int tmp_line = matchResult.first / col;
      //process the complete rectangle
      // for the above three cases, in case 1 the last line is the last line block
      // in case 3 the last line is in the last line block
      // int case 2 line(>= last_block_line_idx) is in the last line block.
      if (tmp_line >= last_block_line_idx)
        current_line_block_idx = line_block - 1;
      else
        current_line_block_idx = tmp_line / line_inte;
      current_block_num = current_line_block_idx * col_block;
      //process the rest
      int tmp_col = matchResult.first % col;
      if (tmp_col >= last_block_col_idx)
        rest_block_num = col_block - 1;
      else
        rest_block_num = tmp_col / col_inte;
      index = current_block_num + rest_block_num;
//    if(index>=blockNum) {
//    std::cerr << matchResult.first<<std::endl
//                <<index << std::endl
//              <<line_block << std::endl
//              <<line_inte << std::endl
//              <<col_block << std::endl
//              <<col_inte << std::endl;
//    }

        // ******using in test when most stars skew in one block******
//        index = index_text_iter++;
//        if(index>=blockNum)
//            index = 0;
        // ******using in test when most stars skew in one block******
        if(objStarFile->isRef == false) {
            if(abType != 0) {
                ++objStarFile->matchedStar_abNum.at(index);
                if (abType == 1)
                    ++objStarFile->matchedStar_newAbNum.at(index);
            }
            std::ostringstream oString;
            oString<<nextStar->mag;
            starBlock.at(index).append(nextStar->redis_key)
                    .append(" ").append(oString.str()+"\n");
        }
        else {
            std::ostringstream oString;
            oString<<index;
            char *s = strchr(matchResult.second, ' ');
            *s='\0';
            char *p = strrchr(matchResult.second, '_');
            *p='\0';
            char starid[50];
            strcpy(starid,matchResult.second);
            strcat(starid,"_");
            strcat(starid,oString.str().c_str());
            strcat(starid,"_");
            strcat(starid,p+1);
            //resume
            *p='_';
            strcpy(nextStar->match->redis_key,starid);
            starBlock.at(index).append(starid).append(" ").append(s+1).append("\n");
            *s=' ';
        }
    }
    nextStar = nextStar->next;
  }

    int a;

#ifdef PRINT_CM_DETAIL
  printf("partition match done!\n");
#endif
}

////refStarFile without cache(perLineWithNoCache)
void CrossMatch::match(StarFile *objStarFile,  Partition * zones,
                       float errorBox,randAbstar *const abstar) {

  //refStarFile->starList = NULL;
//  const bool isAb = (abstar==NULL)? false:(abstar->isAb());
//  if(isAb)
//    abstar->setAbMag();  // generate the new abnormal stars

  CMStar *nextStar = objStarFile->starList;
  while (nextStar) {
      // core code!!
      std::pair<long, char *> matchResult=zones->getMatchStar(nextStar);
    if(matchResult.first == -1) {

      objStarFile->OTStarCount ++;
    } else {
      objStarFile->matchedCount ++;
      // if the nextStar is abnormal star, add abnormal mag into the nextStar, or else return and do nothing
      abstar->popAbMag(nextStar, matchResult.second);
      objStarFile->matchedStar.push_back(std::string(matchResult.second));
    }

    nextStar = nextStar->next;
  }

#ifdef PRINT_CM_DETAIL
  printf("partition match done!\n");
#endif
}
//refStarFile with cache(perLineWithCache)
void CrossMatch::match(StarFile *refStarFile, StarFile *objStarFile,
                       Partition * zones, float errorBox,randAbstar *const abstar) {

  //refStarFile->starList = NULL;
//  const bool isAb = (abstar==NULL)? false:(abstar->isAb());
//  if(isAb)
//    abstar->setAbMag();  // generate the new abnormal stars

  CMStar *nextStar = objStarFile->starList;
  while (nextStar) {
      // core code!!
	  std::pair<long, char *> matchResult = zones->getMatchStar(nextStar);
    if(matchResult.first == -1) {

    	objStarFile->OTStarCount ++;
    } else {
      objStarFile->matchedCount ++;
      // if the nextStar is abnormal star, add abnormal mag into the nextStar, or else return and do nothing
      abstar->popAbMag(nextStar, matchResult.second);
      //matchResult.first is ref star id, and starts from 1 to n
      //if ref star id is more than starDataCache.size(), we expand the starDataCache size to matchResult.first+1
      //to avoid refStarFile->starDataCache[matchResult.first] is out of range.
      if(matchResult.first>refStarFile->starDataCache.size())
        refStarFile->starDataCache.resize(matchResult.first+1);

    	refStarFile->starDataCache[matchResult.first].push_back(std::string(matchResult.second));

    }
//    if( nextStar->mag >= 50) objStarFile->abStar ++;
    nextStar = nextStar->next;
  }


#ifdef PRINT_CM_DETAIL
  printf("partition match done!\n");
#endif
}


//refStarFile without cache


/**
 * circulate each star on 'refList', find the nearest on as the match star of objStar
 * the matched star is stored on obj->match, 
 * the distance between two stars is stored on obj->error
 * @param ref
 * @param obj
 */
void CrossMatch::matchNoPartition(char *refName, char *objName, float errorBox) {

  refStarFileNoPtn = new StarFile();
  refStarFileNoPtn->readStar(refName);
  objStarFileNoPtn = new StarFile();
  objStarFileNoPtn->readStar(objName);

  matchNoPartition(refStarFileNoPtn, objStarFileNoPtn, errorBox);
}

/**
 * the matched star is stored on obj->match, 
 * the distance between two stars is stored on obj->error
 * @param ref
 * @param obj
 */
void CrossMatch::matchNoPartition(StarFile *refStarFileNoPtn, StarFile *objStarFileNoPtn, float errorBox) {

  CMStar *tObj = objStarFileNoPtn->starList;

  while (tObj) {
    CMStar *tRef = refStarFileNoPtn->starList;
    float tError = getLineDistance(tRef, tObj);
    tObj->match = tRef;
    tObj->error = tError;
    tRef = tRef->next;
    while (tRef) {
      tError = getLineDistance(tRef, tObj);
      if (tError < tObj->error) {
        tObj->match = tRef;
        tObj->error = tError;
      }
      tRef = tRef->next;
    }
    tObj = tObj->next;
  }

#ifdef PRINT_CM_DETAIL
  printf("no partition match done!\n");
#endif
}

void CrossMatch::freeAllMemory() {

  if (NULL != refStarFile)
    delete refStarFile;
  if (NULL != objStarFile)
    delete objStarFile;
  if (NULL != refStarFileNoPtn)
    delete refStarFileNoPtn;
  if (NULL != objStarFileNoPtn)
    delete objStarFileNoPtn;
  if (NULL != zones) {
    delete zones;
  }
}

void CrossMatch::compareResult(char *refName, char *objName, char *outfName, float errorBox) {

  match(refName, objName, errorBox);
  matchNoPartition(refName, objName, errorBox);
  compareResult(objStarFile, objStarFileNoPtn, outfName, errorBox);
}

void CrossMatch::compareResult(StarFile *objStarFile, StarFile *objStarFileNoPtn, const char *outfName, float errorBox) {

  if (NULL == objStarFile || NULL == objStarFileNoPtn) {
    printf("StarFile is null\n");
    return;
  }

  FILE *fp = fopen(outfName, "w");

  CMStar *tStar1 = objStarFile->starList;
  CMStar *tStar2 = objStarFileNoPtn->starList;
  int i = 0, j = 0, k = 0, m = 0, n = 0, g = 0;
  while (NULL != tStar1 && NULL != tStar2) {
    if (NULL != tStar1->match && NULL != tStar2->match) {
      i++;
      float errDiff = fabs(tStar1->error - tStar2->error);
      if (errDiff < CompareFloat)
        n++;
    } else if (NULL != tStar1->match && NULL == tStar2->match) {
      j++;
    } else if (NULL == tStar1->match && NULL != tStar2->match) {//ommit and OT
      k++;
      if (tStar2->error < errorBox)
        g++;
    } else {
      m++;
    }
    tStar1 = tStar1->next;
    tStar2 = tStar2->next;
  }
  fprintf(fp, "total star %d\n", i + j + k + m);
  fprintf(fp, "matched %d , two method same %d\n", i, n);
  fprintf(fp, "partition matched but nopartition notmatched %d\n", j);
  fprintf(fp, "nopartition matched but partition notmatched %d, small than errorBox %d\n", k, g);
  fprintf(fp, "two method are not matched %d\n", m);

  fprintf(fp, "\nX1,Y1,X1m,Y1m,err1 is the partition related info\n");
  fprintf(fp, "X2,Y2,X2m,Y2m,err2 is the nopartition related info\n");
  fprintf(fp, "X1,Y1,X2,Y2 is orig X and Y position of stars\n");
  fprintf(fp, "X1m,Y1m,X2m,Y2m is matched X and Y position of stars\n");
  fprintf(fp, "pos1,pos2 is the two method's match distance\n");
  fprintf(fp, "the following list is leaked star of partition method, total %d\n", g);
  fprintf(fp, "X1\tY1\tX2\tY2\tX1m\tY1m\tX2m\tY2m\tpos1\tpos2\n");
  tStar1 = objStarFile->starList;
  tStar2 = objStarFileNoPtn->starList;
  while (NULL != tStar1 && NULL != tStar2) {
    if (NULL == tStar1->match && NULL != tStar2->match && tStar2->error < errorBox) { //ommit and OT
      fprintf(fp, "%12f %12f %12f %12f %12f %12f %12f %12f %12f %12f\n",
              tStar1->pixx, tStar1->pixy, tStar2->pixx, tStar2->pixy,
              0.0, 0.0, tStar2->match->pixx, tStar2->match->pixy,
              tStar1->error, tStar2->error);
    }
    tStar1 = tStar1->next;
    tStar2 = tStar2->next;
  }

  fprintf(fp, "the following list is OT\n");
  fprintf(fp, "X1\tY1\tX2\tY2\tX1m\tY1m\tX2m\tY2m\tpos1\tpos2, total %d\n", k - g);
  tStar1 = objStarFile->starList;
  tStar2 = objStarFileNoPtn->starList;
  while (NULL != tStar1 && NULL != tStar2) {
    if (NULL == tStar1->match && NULL != tStar2->match && tStar2->error > errorBox) { //ommit and OT
      fprintf(fp, "%12f %12f %12f %12f %12f %12f %12f %12f %12f %12f\n",
              tStar1->pixx, tStar1->pixy, tStar2->pixx, tStar2->pixy,
              0.0, 0.0, tStar2->match->pixx, tStar2->match->pixy,
              tStar1->error, tStar2->error);
    }
    tStar1 = tStar1->next;
    tStar2 = tStar2->next;
  }

  fclose(fp);
}

void CrossMatch::printMatchedRst(char *outfName, float errorBox) {

  FILE *fp = fopen(outfName, "w");
  fprintf(fp, "Id\tX\tY\tmId\tmX\tmY\tdistance\n");

  long count = 0;
  CMStar *tStar = objStarFile->starList;
  while (NULL != tStar) {
    if (NULL != tStar->match && tStar->error < errorBox) {
      fprintf(fp, "%8d %12f %12f %8d %12f %12f %12f\n",
              tStar->starId, tStar->pixx, tStar->pixy, tStar->match->starId,
              tStar->match->pixx, tStar->match->pixy, tStar->error);
      count++;
    }
    tStar = tStar->next;
  }
  fclose(fp);

#ifdef PRINT_CM_DETAIL
  printf("matched stars %d\n", count);
#endif
}

void CrossMatch::printMatchedRst(char *outfName, StarFile *starFile, float errorBox) {

  FILE *fp = fopen(outfName, "w");
  fprintf(fp, "Id\tX\tY\tmId\tmX\tmY\tdistance\n");

  long count = 0;
  CMStar *tStar = starFile->starList;
  while (NULL != tStar) {
    if (NULL != tStar->match && tStar->error < errorBox) {
        // print match results to alluxio file
      fprintf(fp, "%8d %12f %12f %8d %12f %12f %12f\n",
              tStar->starId, tStar->pixx, tStar->pixy, tStar->match->starId,
              tStar->match->pixx, tStar->match->pixy, tStar->error);
      count++;
    }
    tStar = tStar->next;
  }
  fclose(fp);

#ifdef PRINT_CM_DETAIL
  printf("matched stars %d\n", count);
#endif
}

void CrossMatch::printAllStarList(char *outfName, StarFile *starFile, float errorBox) {

  FILE *fp = fopen(outfName, "w");
  fprintf(fp, "Id\tX\tY\tmId\tmX\tmY\tdistance\n");

  long count = 0;
  CMStar *tStar = starFile->starList;
  while (NULL != tStar) {
    if (NULL != tStar->match) {
      fprintf(fp, "%8d %12f %12f %8d %12f %12f %12f\n",
              tStar->starId, tStar->pixx, tStar->pixy, tStar->match->starId,
              tStar->match->pixx, tStar->match->pixy, tStar->error);
    } else {
      fprintf(fp, "%8d %12f %12f %8d %12f %12f %12f\n",
              tStar->starId, tStar->pixx, tStar->pixy, 0, 0.0, 0.0, tStar->error);
    }
    count++;
    tStar = tStar->next;
  }
  fclose(fp);

#ifdef PRINT_CM_DETAIL
  printf("matched stars %d\n", count);
#endif
}

void CrossMatch::printOTStar(char *outfName, float errorBox) {

  FILE *fp = fopen(outfName, "w");
  fprintf(fp, "Id\tX\tY\n");

  long count = 0;
  CMStar *tStar = objStarFile->starList;
  while (NULL != tStar) {
    if (NULL == tStar->match) {
      fprintf(fp, "%8d %12f %12f\n",
              tStar->starId, tStar->pixx, tStar->pixy);
      count++;
    }
    tStar = tStar->next;
  }
  fclose(fp);

#ifdef PRINT_CM_DETAIL
  printf("OT stars %d\n", count);
#endif
}

void CrossMatch::testCrossMatch() {

  char refName[30] = "data/referance.cat";
  char objName[30] = "data/object.cat";
  char matchedName[30] = "data/matched.cat";
  char otName[30] = "data/ot.cat";
  float errorBox = 0.7;

  CrossMatch *cm = new CrossMatch();
  cm->match(refName, objName, errorBox);
  cm->printMatchedRst(matchedName, errorBox);
  cm->printOTStar(otName, errorBox);
  cm->freeAllMemory();

}

void CrossMatch::partitionAndNoPartitionCompare() {

  char refName[30] = "data/referance.cat";
  char objName[30] = "data/object.cat";
  char cmpOutName[30] = "data/cmpOut.cat";
  float errorBox = 0.7;

  CrossMatch *cm = new CrossMatch();
  cm->compareResult(refName, objName, cmpOutName, errorBox);
  cm->freeAllMemory();

}

void CrossMatch::setFieldHeight(float fieldHeight) {
  this->fieldHeight = fieldHeight;
}

void CrossMatch::setFieldWidth(float fieldWidth) {
  this->fieldWidth = fieldWidth;
}
