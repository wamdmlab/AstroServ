//
// Created by Chen Yang on 10/30/16.
//

#ifndef FUNCTION_H
#define	FUNCTION_H


#define VERSION "cross match 1.4"
#define TREE_NODE_LENGTH 64800		//the totle number of tree node (360*180)
#define AREA_WIDTH 360
#define AREA_HEIGHT 180

#define BLOCK_BASE 32

//#define PI 3.141592653
#define ERROR_GREAT_CIRCLE 0.005555555556			//(20.0/3600.0)=0.005555555556
#define	SUBAREA	0.05555555556			//(60.0/3600.0)=0.016666666667 this value must big enough, to insure all data all find.

#define LINE 1024
#define ONESECOND CLOCKS_PER_SEC

#define ANG_TO_RAD 0.017453293
#define RAD_TO_ANG 57.295779513

#define INDEX_SIZE 1<<20

#define SPHERE_METHOD 1 
#define PLANE_METHOD 2

#endif	/* FUNCTION_H */

