//
// Created by Chen Yang on 10/12/16.
//


#ifndef STARFILE_H
#define	STARFILE_H

#include <string>
#include <vector>
class CMStar;

class StarFile {
public:
    const char * fileName;
    CMStar *starList;
    //used in SENDBLOCK mode
    CMStar *starListCopy;
    long starNum;
    int matchedCount;
    int OTStarCount;
    //abnormal star number in ccd
    size_t abStar;
    bool isRef;
    std::vector<std::vector<std::string> > starDataCache;
    //////////////// matached star information
    //matched stars in each block
    std::vector<std::string> matchedStar;
    //abnormal star number in each block
    std::vector<size_t> matchedStar_abNum;
    std::vector<size_t> matchedStar_newAbNum;
    //the centre ra dec for each ccd
    double ra0, dec0;
    //////////////
    //std::vector<std::string> redisStrings;

public:
    StarFile();
    StarFile(const char * fileName);
    StarFile(const StarFile& orig);
    virtual ~StarFile();

    virtual void readStar();
    virtual void setra0Anddec0();
    virtual void readStar(const char * fileName);
    void writeStar(char * outFile);
    void matchedStarArrResize(size_t size);
    void matchedStarArrReverse(size_t size);
private:
};

#endif	/* STARFILE_H */

