//
// Created by Chen Yang on 10/12/16.
//
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include "cmhead.h"

void getTodayDateTime(char *dateTimeStr){
    time_t rawtime;
    struct tm * timeinfo;
    time( &rawtime );
    timeinfo= localtime ( &rawtime ); 
    //"%Y%m%d%I%M%S"
    strftime (dateTimeStr,80,"%Y%m%d",timeinfo); 
}

char * strReplace(char *src, const char *oldstr, const char *newstr) {
    char *needle;
    char *tmp;
    if (strlen(oldstr) == strlen(newstr) && strcmp(oldstr, newstr) == 0) {
        return 0;
    }

    while ((needle = strstr(src, oldstr))) {// && (needle - src <= len)
        tmp = (char*) malloc(strlen(src) + (strlen(newstr) - strlen(oldstr)) + 1);
        strncpy(tmp, src, needle - src);
        tmp[needle - src] = '\0';
        strcat(tmp, newstr);
        strcat(tmp, needle + strlen(oldstr));
        src = strdup(tmp);
        free(tmp);
    }
    return src;
}

double angToRad(double angle) {
    return angle * ANG_TO_RAD;
}

/**
 * get the great circle distance of two point with same DEC, and RA difference is 
 * @param dec
 * @param errorRadius
 * @return 
 */
float getAngleFromGreatCircle(double dec, double dist) {
    double rst = acos((cos(dist * ANG_TO_RAD) - pow(sin(dec * ANG_TO_RAD), 2)) /
            pow(cos(dec * ANG_TO_RAD), 2));
    return rst*RAD_TO_ANG;
}

float getLineDistance(CMStar *p1, CMStar *p2) {
    float xdiff = p1->pixx - p2->pixx;
    float ydiff = p1->pixy - p2->pixy;
    float dist = sqrt(xdiff * xdiff + ydiff * ydiff);
    return dist;
}

double getGreatCircleDistance(CMStar *p1, CMStar *p2) {
    double rst = 0.0;
    if (fabs(p1->alpha - p2->alpha) > CompareFloat || fabs(p1->delta - p2->delta) > CompareFloat) {
        rst = RAD_TO_ANG * acos(sin(ANG_TO_RAD * (p1->delta)) * sin(ANG_TO_RAD * (p2->delta)) +
                cos(ANG_TO_RAD * (p1->delta)) * cos(ANG_TO_RAD * (p2->delta)) * cos(ANG_TO_RAD * (fabs(p1->alpha - p2->alpha))));
    }
    return rst;
}

/**
 * judge a string is a blank string, only contain ' ', '\t','\n','\r',eg
 * @param str
 * @param len
 * @return if it is a blank string return true, others return false
 */
bool isEmpty(char *str) {

    if (NULL != str && '\0' != str[0]) {
        int len = strlen(str);
        for (int i = 0; i < len && '\0' != str[i]; i++) {
            if (str[i] != ' ' && str[i] != '\t' && str[i] != '\r' &&
                    str[i] != '\n' && str[i] != '\x0b') {
                return false;
            }
        }
    }
    return true;
}

/**
 * judge a string whether contain a number char ('0'~"9")
 * @param str
 * @param len
 * @return 
 */
bool hasNumber(char *str) {

    if (NULL != str && '\0' != str[0]) {
        int len = strlen(str);
        for (int i = 0; i < len && '\0' != str[i]; i++) {
            if (str[i] >= '0' && str[i] <= '9') {
                return true;
            }
        }
    }
    return false;
}

template<typename T>
inline T getMax(const T& a, const T& b) {
    return a > b ? a : b;
}

long countFileLines(char *fName) {

    FILE *fp = fopen(fName, "r");

    if (fp == NULL) {
        return 0;
    }

    long lineNum = 0;
    char line[MaxStringLength];
    while (fgets(line, MaxStringLength, fp) != NULL) {
        lineNum++;
    }
    fclose(fp);
    return lineNum;
}

void ltrim(char *s) {
    char *p;
    p = s;
    while (*p == ' ' || *p == '\t') {
        p++;
    }
    strcpy(s, p);
}

void rtrim(char *s) {
    int i;

    i = strlen(s) - 1;
    while ((s[i] == ' ' || s[i] == '\t' || s[i] == '\n' || s[i] == '\r' || s[i] == '\x0b') && i >= 0) {
        i--;
    };
    s[i + 1] = '\0';
}

void trim(char *s) {
    ltrim(s);
    rtrim(s);
}

void trimAll(char *s) {
    char *pTmp = s;

    while (*s != '\0') {
        if (*s != ' ' && *s != '\t') {
            *pTmp++ = *s;
        }
        s++;
    }
    *pTmp = '\0';
}

void quickSort(int min, int max, double a[]) {
    double key = a[min];
    int i = min;
    int j = max;
    //float temp;
    if (min >= max)
        return;
    while (i < j) {

        while ((i < j) && (key <= a[j])) {
            j--;
        }
        if (key > a[j]) {
            a[i] = a[j];
            a[j] = key;
            i++;
        }

        while ((i < j) && (key >= a[i])) {
            i++;
        }
        if (key < a[i]) {
            a[j] = a[i];
            a[i] = key;
            j--;
        }

    }
    quickSort(min, i - 1, a);
    quickSort(i + 1, max, a);
}

double getMedian(double array[], int len) {

    double median = 0.0;
    if (len % 2 == 0) {
        median = (array[len / 2 - 1] + array[len / 2]) / 2;
    } else {
        median = array[len / 2];
    }
    return median;
}

double getAverage(double array[], int len) {

    if (array == NULL) return 0;

    double total = 0.0;
    int i = 0;
    for (i = 0; i < len; i++)
        total += array[i];

    return total / len;
}

double getStandardDeviation(double array[], int len, double average) {
    int i;
    double total = 0.0;
    double tmp;
    for (i = 0; i < len; i++) {
        tmp = array[i] - average;
        total += tmp * tmp;
    }
    double msd = sqrt(total / len);
    return msd;
}
