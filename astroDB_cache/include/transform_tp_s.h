#ifndef TRANSFORM_TP_S_H
#define	TRANSFORM_TP_S_H

#include <math.h>
void slTP2S(double XI, double ETA, double RAZ, double DECZ, double &RA, double &DEC);
void slS2TP(double RA, double DEC, double RAZ, double DECZ, double &XI, double &ETA, int &J);
#endif

