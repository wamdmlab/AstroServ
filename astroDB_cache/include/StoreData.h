//
// Created by Chen Yang on 7/2/17.
//

#ifndef STOREDATA_H
#define	STOREDATA_H

#include "StarFile.h"

class StoreData {
protected:

public:
    virtual void store(StarFile *starFile) = 0;
};

#endif	/* STOREDATA_H */

