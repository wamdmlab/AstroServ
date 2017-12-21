#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "array.h"

void itoa (int n,char s[])
{
int i,j,sign,k;
char tmp;

if((sign=n)<0)//record positive or negative
        n=-n;//let n be positive
          i=0;
do{
        s[i++]=n%10+'0';//fetch next digit
}while ((n/=10)>0);//delete this digit

if(sign<0)
        s[i++]='-';
s[i]='\0';

for(j=i-1,k=0;j>k;j--,k++)
       {
       tmp = s[k];
        s[k] = s[j];
         s[j] = tmp;
        }
}


int main(int argc, char* argv[])
{
    char const* const fileName = argv[1]; /* should check that argc > 1 */
    FILE* file;
    char line[256];
    IntArray a1,a22;
    ShortArray a2;
    DoubleArray a3,a4,a5,a6,a7,a8,a9,a10,a11,a12,a13,a14,a15,a16,a17,a18,a19,a20,a21;
    int v1,v22;
    short v2;
    double v3,v4,v5,v6,v7,v8,v9,v10,v11,v12,v13,v14,v15,v16,v17,v18,v19,v20,v21;
    int i;
    FILE *fp;
    char fname[128];
    
    file = fopen(fileName, "r"); /* should check the result */
    if(file == NULL)
    {
        puts("Couldn't open file.");
        exit(1);
    }
    
    /*initially 100000 numbers.*/
    initIntArray(&a1, ARRAY_INITIAL_SIZE);
    initIntArray(&a22, ARRAY_INITIAL_SIZE);
    initShortArray(&a2, ARRAY_INITIAL_SIZE);
    initDoubleArray(&a3, ARRAY_INITIAL_SIZE);
    initDoubleArray(&a4, ARRAY_INITIAL_SIZE);
    initDoubleArray(&a5, ARRAY_INITIAL_SIZE);
    initDoubleArray(&a6, ARRAY_INITIAL_SIZE);
    initDoubleArray(&a7, ARRAY_INITIAL_SIZE);
    initDoubleArray(&a8, ARRAY_INITIAL_SIZE);
    initDoubleArray(&a9, ARRAY_INITIAL_SIZE);
    initDoubleArray(&a10, ARRAY_INITIAL_SIZE);
    initDoubleArray(&a11, ARRAY_INITIAL_SIZE);
    initDoubleArray(&a12, ARRAY_INITIAL_SIZE);
    initDoubleArray(&a13, ARRAY_INITIAL_SIZE);
    initDoubleArray(&a14, ARRAY_INITIAL_SIZE);
    initDoubleArray(&a15, ARRAY_INITIAL_SIZE);
    initDoubleArray(&a16, ARRAY_INITIAL_SIZE);
    initDoubleArray(&a17, ARRAY_INITIAL_SIZE);
    initDoubleArray(&a18, ARRAY_INITIAL_SIZE);
    initDoubleArray(&a19, ARRAY_INITIAL_SIZE);
    initDoubleArray(&a20, ARRAY_INITIAL_SIZE);
    initDoubleArray(&a21, ARRAY_INITIAL_SIZE);


    while (fgets(line, sizeof(line), file)) {
        /* note that fgets don't strip the terminating \n, checking its
 *            presence would allow to handle lines longer that sizeof(line) */
        sscanf(line,"%d %hi %lf %lf %lf %lf %lf %lf %lf %lf %lf %lf %lf %lf %lf %lf %lf %lf %lf %lf %lf %d",&v1,&v2,&v3,&v4,&v5,&v6,&v7,&v8,&v9,&v10,&v11,&v12,&v13,&v14,&v15,&v16,&v17,&v18,&v19,&v20,&v21,&v22);
        insertIntArray(&a1,v1);
        insertShortArray(&a2,v2);
        insertDoubleArray(&a3,v3);
        insertDoubleArray(&a4,v4);
        insertDoubleArray(&a5,v5);
        insertDoubleArray(&a6,v6);
        insertDoubleArray(&a7,v7);
        insertDoubleArray(&a8,v8);
        insertDoubleArray(&a9,v9);
        insertDoubleArray(&a10,v10);
        insertDoubleArray(&a11,v11);
        insertDoubleArray(&a12,v12);
        insertDoubleArray(&a13,v13);
        insertDoubleArray(&a14,v14);
        insertDoubleArray(&a15,v15);
        insertDoubleArray(&a16,v16);
        insertDoubleArray(&a17,v17);
        insertDoubleArray(&a18,v18);
        insertDoubleArray(&a19,v19);
        insertDoubleArray(&a20,v20);
        insertDoubleArray(&a21,v21);
        insertIntArray(&a22,v22);
    }
    //finish loading to dynamic array.

    for(i=1;i<23;i++){
    memset(fname, 0, 128);
    //fname should be catano-column.
    sprintf(fname,"%s-%d",fileName,i);
    fp=fopen(fname,"wb");
    switch(i) {
    case 1:
    fwrite(a1.array, sizeof(int), a1.used, fp);
    break;
    case 2:
    fwrite(a2.array, sizeof(short),a2.used, fp);
    break;
    case 3:
    fwrite(a3.array, sizeof(double),a3.used, fp);
    break;
    case 4:
    fwrite(a4.array, sizeof(double),a4.used, fp);
    break;
    case 5:
    fwrite(a5.array, sizeof(double),a5.used, fp);
    break;
    case 6:
    fwrite(a5.array, sizeof(double),a6.used, fp);
    break;
    case 7:
    fwrite(a7.array, sizeof(double),a7.used, fp);
    break;
    case 8:
    fwrite(a8.array, sizeof(double),a8.used, fp);
    break;
    case 9:
    fwrite(a9.array, sizeof(double),a9.used, fp);
    break;
    case 10:
    fwrite(a10.array, sizeof(double),a10.used, fp);
    break;
    case 11:
    fwrite(a11.array, sizeof(double),a11.used, fp);
    break;
    case 12:
    fwrite(a12.array, sizeof(double),a12.used, fp);
    break;
    case 13:
    fwrite(a13.array, sizeof(double),a13.used, fp);
    break;
    case 14:
    fwrite(a14.array, sizeof(double),a14.used, fp);
    break;
    case 15:
    fwrite(a15.array, sizeof(double),a15.used, fp);
    break;
    case 16:
    fwrite(a16.array, sizeof(double),a16.used, fp);
    break;
    case 17:
    fwrite(a17.array, sizeof(double),a17.used, fp);
    break;
    case 18:
    fwrite(a18.array, sizeof(double),a18.used, fp);
    break;
    case 19:
    fwrite(a19.array, sizeof(double),a19.used, fp);
    break;
    case 20:
    fwrite(a20.array, sizeof(double),a20.used, fp);
    break;
    case 21:
    fwrite(a21.array, sizeof(double),a21.used, fp);
    break;
    case 22:
    fwrite(a22.array, sizeof(int),a22.used, fp);
    break;
    default:
    printf("wrong column number.\n");
    break;
    }
    fclose(fp);
}
 printf("succesfully convert catalog to binary files and write to disk: %s.\n",fname);
    
freeIntArray(&a1);
freeIntArray(&a22);
freeShortArray(&a2);
freeDoubleArray(&a3);
freeDoubleArray(&a4);
freeDoubleArray(&a5);
freeDoubleArray(&a6);
freeDoubleArray(&a7);
freeDoubleArray(&a8);
freeDoubleArray(&a9);
freeDoubleArray(&a10);
freeDoubleArray(&a11);
freeDoubleArray(&a12);
freeDoubleArray(&a13);
freeDoubleArray(&a14);
freeDoubleArray(&a15);
freeDoubleArray(&a16);
freeDoubleArray(&a17);
freeDoubleArray(&a18);
freeDoubleArray(&a19);
freeDoubleArray(&a20);
freeDoubleArray(&a21);
    /* may check feof here to make a difference between eof and io failure -- network
 *        timeout for instance */
    return 0;
}
