//
// Created by Chen Yang on 9/22/17.
//

/*
 *  Projection of spherical coordinates onto tangent plane:
 *  "gnomonic" projection - "standard coordinates"
 *  (single precision)
 *
 *  Given:
 *     RA,DEC      real  spherical coordinates of point to be projected
 *     RAZ,DECZ    real  spherical coordinates of tangent point
 *
 *  Returned:
 *     XI,ETA      real  rectangular coordinates on tangent plane (0<=XI ETA <=1)
 *     J           int   status:   0 = OK, star on tangent plane
 *                                 1 = error, star too far from axis
 *                                 2 = error, antistar on tangent plane
 *                                 3 = error, antistar too far from axis
 */

#include "transform_tp_s.h"
void slS2TP(double RA, double DEC, double RAZ, double DECZ, double &XI, double &ETA, int &J) {

    float TINY = 1E-6;

    double SDECZ = sin(DECZ);
    double SDEC = sin(DEC);
    double CDECZ = cos(DECZ);
    double CDEC = cos(DEC);
    double RADIF = RA - RAZ;
    double SRADIF = sin(RADIF);
    double CRADIF = cos(RADIF);

    //Reciprocal of star vector length to tangent plane
    double DENOM = SDEC * SDECZ + CDEC * CDECZ*CRADIF;

    //Handle vectors too far from axis
    if (DENOM > TINY) {
        J = 0;
    } else if (DENOM >= 0.0) {
        J = 1;
        DENOM = TINY;
    } else if (DENOM>-TINY) {
        J = 2;
        DENOM = -TINY;
    } else {
        J = 3;
    }
    //Compute tangent plane coordinates (even in dubious cases)
    XI = CDEC * SRADIF / DENOM;
    ETA = (SDEC * CDECZ - CDEC * SDECZ * CRADIF) / DENOM;

}

double slRA2P(double angle) {

    double a2p = 6.283185307179586476925287;

    double tangle = fmod(angle, a2p);
    if (tangle < 0.0) {
        tangle += a2p;
    }
    return tangle;
}

/**
 *  Transform tangent plane coordinates into spherical
 *  (single precision)
 *
 *  Given:
 *     XI,ETA      real  tangent plane rectangular coordinates (0<=XI ETA <=1)
 *     RAZ,DECZ    real  spherical coordinates of tangent point
 *
 *  Returned:
 *     RA,DEC      real  spherical coordinates (0-2pi,+/-pi/2)
 *
 *  Called:        slRA2P
 */
void slTP2S(double XI, double ETA, double RAZ, double DECZ, double &RA, double &DEC) {

    double SDECZ = sin(DECZ);
    double CDECZ = cos(DECZ);

    double DENOM = CDECZ - ETA*SDECZ;
    double tatan2 = atan2(XI, DENOM);

    RA = slRA2P(tatan2 + RAZ);
    DEC = atan2(SDECZ + ETA*CDECZ, sqrt(XI * XI + DENOM * DENOM));
}