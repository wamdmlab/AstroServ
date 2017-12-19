//
// Created by Chen Yang on 4/4/17.
//

#ifndef ASTRODB_CACHE_ERR_H
#define ASTRODB_CACHE_ERR_H
#include <sstream>
#include <iostream>
#include <string>
#include <stdexcept>

inline void recordErr(std::string errinfo, std::string orginFile, size_t line)
{

    std::ostringstream oString;
    oString <<errinfo<<std::endl<<"File: "<< orginFile <<" Line: "<<line<<std::endl;
    std::string errStr = oString.str();
    throw std::runtime_error(errStr);
}



#endif //ASTRODB_CACHE_ERR_H
