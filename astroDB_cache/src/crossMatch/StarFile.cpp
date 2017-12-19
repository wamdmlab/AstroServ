//
// Created by Chen Yang on 10/12/16.
//

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include "StarFile.h"
#include "cmhead.h"

StarFile::StarFile() {
  this->fileName = NULL;
  this->starList = NULL;
  this->starListCopy=NULL;
  this->starNum = 0;
  this->OTStarCount = 0;
  this->abStar = 0;
  this->matchedCount = 0;
  this->isRef= false;
}

StarFile::StarFile(const char* fileName) {
  this->fileName = fileName;
  this->starList = NULL;
  this->starListCopy=NULL;
  this->starNum = 0;
  this->OTStarCount = 0;
  this->abStar = 0;
  this->matchedCount = 0;
  this->isRef= false;
}

StarFile::StarFile(const StarFile& orig) {

	this->matchedCount = 0;
	this->OTStarCount = 0;
	this->abStar = 0;
  this->isRef= false;
  this->starNum = orig.starNum;
  this->fileName = orig.fileName;
  this->starListCopy=NULL;
  CMStar *tStar = NULL;
  CMStar *origlst = orig.starList;

  starList = (CMStar *) malloc(sizeof (CMStar));
  memcpy(starList, origlst, sizeof (CMStar));
  starList->line = (char*) malloc(strlen(origlst->line) + 1);
  strcpy(starList->line, origlst->line);
  origlst = origlst->next;
  tStar = starList;

  while (NULL != origlst) {
    tStar->next = (CMStar *) malloc(sizeof (CMStar));
    memcpy(tStar->next, origlst, sizeof (CMStar));
    tStar->next->line = (char*) malloc(strlen(origlst->line) + 1);
    strcpy(tStar->next->line, origlst->line);
    origlst = origlst->next;
    tStar = tStar->next;
  }
}

StarFile::~StarFile() {
  if (NULL != starList) {
    CMStar *tStar = starList->next;
    while (tStar) {
      starList->next = tStar->next;
      if (NULL != tStar->line)free(tStar->line);
      free(tStar);
      tStar = starList->next;
    }
    if (NULL != starList->line)free(starList->line);
    free(starList);
  }
}

void StarFile::matchedStarArrResize(size_t size) {
  matchedStar.resize(size);
  matchedStar_abNum.resize(size);
  matchedStar_newAbNum.resize(size);
}

void StarFile::matchedStarArrReverse(size_t size) {
    matchedStar.reserve(size);
    matchedStar_abNum.reserve(size);
    matchedStar_newAbNum.reserve(size);
}

void StarFile::readStar() {
  readStar(fileName);
}

void StarFile::setra0Anddec0() {

}

void StarFile::readStar(const char * fileName) {

  if (NULL == fileName) {
    printf("file name is NULL!\n");
    return;
  }

  FILE *fp = fopen(fileName, "r");

  if (NULL == fp) {
    printf("cannot open file %s!\n", fileName);
    return;
  }

  starNum = 0;
  CMStar *tStar = NULL;
  CMStar *nextStar = NULL;
  char line[MaxStringLength];

  while (fgets(line, MaxStringLength, fp) != NULL) {
    nextStar = (CMStar *) malloc(sizeof (CMStar));
    if (3 == sscanf(line, "%f%f%f", &nextStar->pixx, &nextStar->pixy, &nextStar->mag)) {
      nextStar->id = starNum;
      nextStar->next = NULL;
      nextStar->line = (char*) malloc(strlen(line) + 1);
      strcpy(nextStar->line, line);
      if (NULL == starList) {
        starList = nextStar;
        tStar = nextStar;
      } else {
        tStar->next = nextStar;
        tStar = nextStar;
      }
      starNum++;
    } else {
      if (NULL != nextStar) {
        free(nextStar);
      }
    }
  }

#ifdef PRINT_CM_DETAIL
  printf("%s read %d stars\n", fileName, starNum);
#endif
}

void StarFile::writeStar(char * outFile) {
}
