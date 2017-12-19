//
// Created by Chen Yang on 3/30/17.
//

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include "CrossMatchSphere.h"
#include "StarFileFits.h"

CrossMatchSphere::CrossMatchSphere() {
    refStarFile = NULL;
    objStarFile = NULL;
    refStarFileNoPtn = NULL;
    objStarFileNoPtn = NULL;
    zones = NULL;
}

CrossMatchSphere::CrossMatchSphere(const CrossMatchSphere& orig) {
}

CrossMatchSphere::~CrossMatchSphere() {
  freeAllMemory();
}

/**
 * 使用时应该注意
 * 这个方法会自动调用[默认]的StarFile类的readStar方法，无法调用到StarFile子类的readStar方法
 * @param refName
 * @param objName
 * @param errorBox
 */
void CrossMatchSphere::match(char *refName, char *objName, float errorBox) {

    refStarFile = new StarFile(refName);
    objStarFile = new StarFile(objName);

    refStarFile->readStar();
    objStarFile->readStar();
    
    match(refStarFile, objStarFile, errorBox);
}

void CrossMatchSphere::match(StarFile *refStarFile, StarFile *objStarFile, float errorBox) {

    float minZoneLen = errorBox * TimesOfErrorRadius;
    float searchRds = errorBox;

    zones = new PartitionSphere(errorBox, minZoneLen, searchRds);
    zones->partitonStarField(refStarFile);
    refStarFile->starList = NULL;

    int i = 0;
    CMStar *nextStar = objStarFile->starList;
    while (nextStar) {
        zones->getMatchStar1(nextStar);
        nextStar = nextStar->next;
        i++;
    }
    //  printf("i=%d\n", i);

#ifdef PRINT_CM_DETAIL
    printf("partition match done!\n");
#endif
}

/**
 * circulate each star on 'refList', find the nearest on as the match star of objStar
 * the matched star is stored on obj->match, 
 * the distance between two stars is stored on obj->error
 * @param ref
 * @param obj
 */
void CrossMatchSphere::matchNoPartition(char *refName, char *objName, float errorBox) {

    refStarFileNoPtn = new StarFile(refName);
    objStarFileNoPtn = new StarFile(objName);
    
    refStarFileNoPtn->readStar();
    objStarFileNoPtn->readStar();

    matchNoPartition(refStarFileNoPtn, objStarFileNoPtn, errorBox);

}

/**
 * the matched star is stored on obj->match, 
 * the distance between two stars is stored on obj->error
 * @param ref
 * @param obj
 */
void CrossMatchSphere::matchNoPartition(StarFile *refStarFileNoPtn, StarFile *objStarFileNoPtn, float errorBox) {

    CMStar *tObj = objStarFileNoPtn->starList;

    while (tObj) {
        CMStar *tRef = refStarFileNoPtn->starList;
        float tError = getGreatCircleDistance(tRef, tObj);
        tObj->match = tRef;
        tObj->error = tError;
        tRef = tRef->next;
        while (tRef) {
            tError = getGreatCircleDistance(tRef, tObj);
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

void CrossMatchSphere::freeStarList(CMStar *starList) {

    if (NULL != starList) {
        CMStar *tStar = starList->next;
        while (tStar) {
            starList->next = tStar->next;
            free(tStar->line);
            free(tStar);
            tStar = starList->next;
        }
        free(starList);
    }
}

void CrossMatchSphere::freeAllMemory() {

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

void CrossMatchSphere::compareResult(char *refName, char *objName, char *outfName, float errorBox) {

    match(refName, objName, errorBox);
    matchNoPartition(refName, objName, errorBox);
    compareResult(objStarFile, objStarFileNoPtn, outfName, errorBox);
}

void CrossMatchSphere::compareResult(StarFile *objStarFile, StarFile *objStarFileNoPtn, const char *outfName, float errorBox) {
    
    if(NULL == objStarFile || NULL == objStarFileNoPtn){
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
    fprintf(fp, "X1\tY1\tX2\tY2\tX1m\tY1m\tX2m\tY2m\terr1\terr2\n");
    tStar1 = objStarFile->starList;
    tStar2 = objStarFileNoPtn->starList;
    while (NULL != tStar1 && NULL != tStar2) {
        //if (NULL == tStar1->match && NULL != tStar2->match && tStar2->error < errorBox) { //ommit and OT
        if (tStar1->error > errorBox && tStar2->error < errorBox) { //ommit and OT
            fprintf(fp, "%12f %12f %12f %12f %12f %12f %12f %12f %12f %12f\n",
                    tStar1->alpha, tStar1->delta, tStar2->alpha, tStar2->delta,
                    0.0, 0.0, tStar2->match->alpha, tStar2->match->delta,
                    tStar1->error, tStar2->error);
        }
        tStar1 = tStar1->next;
        tStar2 = tStar2->next;
    }

    fprintf(fp, "the following list is OT, total %d\n", k-g);
    fprintf(fp, "X1\tY1\tX2\tY2\tX1m\tY1m\tX2m\tY2m\terr1\terr2\n");
    tStar1 = objStarFile->starList;
    tStar2 = objStarFileNoPtn->starList;
    while (NULL != tStar1 && NULL != tStar2) {
        //if (NULL == tStar1->match && NULL != tStar2->match && tStar2->error > errorBox) { //ommit and OT
        if (tStar1->error > errorBox && tStar2->error > errorBox) { //ommit and OT
            fprintf(fp, "%12f %12f %12f %12f %12f %12f %12f %12f %12f %12f\n",
                    tStar1->alpha, tStar1->delta, tStar2->alpha, tStar2->delta,
                    0.0, 0.0, tStar2->match->alpha, tStar2->match->delta,
                    tStar1->error, tStar2->error);
        }
        tStar1 = tStar1->next;
        tStar2 = tStar2->next;
    }

    fclose(fp);
    freeAllMemory();
}

void CrossMatchSphere::printMatchedRst(char *outfName, float errorBox) {

    FILE *fp = fopen(outfName, "w");
    fprintf(fp, "Id\tX\tY\tmId\tmX\tmY\tdistance\n");

    long count = 0;
    CMStar *tStar = objStarFile->starList;
    while (NULL != tStar) {
        if (NULL != tStar->match && tStar->error < errorBox) {
            fprintf(fp, "%8d %12f %12f %8d %12f %12f %12f\n",
                    tStar->starId, tStar->alpha, tStar->delta, tStar->match->starId,
                    tStar->match->alpha, tStar->match->delta, tStar->error);
            count++;
        }
        tStar = tStar->next;
    }
    fclose(fp);

#ifdef PRINT_CM_DETAIL
    printf("matched stars %d\n", count);
#endif
}

void CrossMatchSphere::printMatchedRst(char *outfName, CMStar *starList, float errorBox) {

    FILE *fp = fopen(outfName, "w");
    fprintf(fp, "Id\tX\tY\tmId\tmX\tmY\tdistance\n");

    long count = 0;
    CMStar *tStar = starList;
    while (NULL != tStar) {
        if (NULL != tStar->match && tStar->error < errorBox) {
            fprintf(fp, "%8d %12f %12f %8d %12f %12f %12f\n",
                    tStar->starId, tStar->alpha, tStar->delta, tStar->match->starId,
                    tStar->match->alpha, tStar->match->delta, tStar->error);
            count++;
        }
        tStar = tStar->next;
    }
    fclose(fp);

#ifdef PRINT_CM_DETAIL
    printf("matched stars %d\n", count);
#endif
}

void CrossMatchSphere::printAllStarList(char *outfName, CMStar *starList, float errorBox) {

    FILE *fp = fopen(outfName, "w");
    fprintf(fp, "Id\tX\tY\tmId\tmX\tmY\tdistance\n");

    long count = 0;
    CMStar *tStar = starList;
    while (NULL != tStar) {
        if (NULL != tStar->match) {
            fprintf(fp, "%8d %12f %12f %8d %12f %12f %12f\n",
                    tStar->starId, tStar->alpha, tStar->delta, tStar->match->starId,
                    tStar->match->alpha, tStar->match->delta, tStar->error);
        } else {
            fprintf(fp, "%8d %12f %12f %8d %12f %12f %12f\n",
                    tStar->starId, tStar->alpha, tStar->delta, 0, 0.0, 0.0, tStar->error);
        }
        count++;
        tStar = tStar->next;
    }
    fclose(fp);

#ifdef PRINT_CM_DETAIL
    printf("matched stars %d\n", count);
#endif
}

void CrossMatchSphere::printOTStar(char *outfName, float errorBox) {

    FILE *fp = fopen(outfName, "w");
    fprintf(fp, "Id\tra\tdec\tmId\tmra\tmdec\tdistance\n");

    long count = 0;
    CMStar *tStar = objStarFile->starList;
    while (NULL != tStar) {
        if (NULL == tStar->match || tStar->error > errorBox) {
            fprintf(fp, "%8d %12f %12f ",
                    tStar->starId, tStar->alpha, tStar->delta);
            if (NULL != tStar->match) {
                fprintf(fp, "%8d %12f %12f %12f",
                        tStar->match->starId, tStar->match->alpha, tStar->match->delta, tStar->error);
            }
            fprintf(fp, "\n");
            count++;
        }
        tStar = tStar->next;
    }
    fclose(fp);

#ifdef PRINT_CM_DETAIL
    printf("OT stars %d\n", count);
#endif
}

void CrossMatchSphere::printOTStar2(char *outfName, float errorBox) {

    FILE *fp = fopen(outfName, "w");

    long count = 0;
    CMStar *tStar = objStarFile->starList;
    int t0 = 0, t1 = 0, t2 = 0;
    while (NULL != tStar) {
        if (tStar->matchNum < 1) {
            printf("%5d %s", tStar->id, tStar->line);
            t0++;
        } else if (tStar->matchNum == 1) {
            t1++;
        } else {
            t2++;
        }
        if (NULL == tStar->match && tStar->matchNum <= 1) {
            fprintf(fp, "%s", tStar->line);
            count++;
        }
        tStar = tStar->next;
    }
    fclose(fp);

#ifdef PRINT_CM_DETAIL
    //  printf("t0=%d\n", t0);
    //  printf("t1=%d\n", t1);
    //  printf("t2=%d\n", t2);
#endif
    printf("OT stars %ld\n", count);
}
