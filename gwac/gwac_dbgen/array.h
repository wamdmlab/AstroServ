//array.h


#define ARRAY_INITIAL_SIZE 100000

typedef struct {
  int *array;
  size_t used;
  size_t size;
} IntArray;

void initIntArray(IntArray *a, size_t initialSize);
void insertIntArray(IntArray *a, int element);
void freeIntArray(IntArray *a);

typedef struct {
  double *array;
  size_t used;
  size_t size;
} DoubleArray;

void initDoubleArray(DoubleArray *a, size_t initialSize);
void insertDoubleArray(DoubleArray *a, double element);
void freeDoubleArray(DoubleArray *a);

typedef struct {
  short *array;
  size_t used;
  size_t size;
} ShortArray;

void initShortArray(ShortArray *a, size_t initialSize);
void insertShortArray(ShortArray *a, short element);
void freeShortArray(ShortArray *a);


/*one row from file line is a struct unit, hard code the number of fields to 22. if add or delete, need modify*/
/*no alignment: sizeof(structure) == sizeof(first_member) + ... + sizeof(last_member).*/
typedef struct __attribute__((packed))
{
  char fnum[2]; //the number of fields in this tuple, actually unsigned short.
  char f1_lens[4]; //the bytes in the first field, actually unsigned int.
  char f1[4];   //data content of the first field, actually int.
  char f2_lens[4]; //the bytes in the second field, actually unsigned int.
  char f2[2];   //data content of the second field, actually short.
  char f3_lens[4];
  char f3[8];
  char f4_lens[4];
  char f4[8];
  char f5_lens[4];
  char f5[8];
  char f6_lens[4];
  char f6[8];
  char f7_lens[4];
  char f7[8];
  char f8_lens[4];
  char f8[8];
  char f9_lens[4];
  char f9[8];
  char f10_lens[4];
  char f10[8];
  char f11_lens[4];
  char f11[8];
  char f12_lens[4];
  char f12[8];
  char f13_lens[4];
  char f13[8];
  char f14_lens[4];
  char f14[8];
  char f15_lens[4];
  char f15[8];
  char f16_lens[4];
  char f16[8];
  char f17_lens[4];
  char f17[8];
  char f18_lens[4];
  char f18[8];
  char f19_lens[4];
  char f19[8];
  char f20_lens[4];
  char f20[8];
  char f21_lens[4];
  char f21[8];
  char f22_lens[4];
  char f22[4];
} Tuple;


typedef struct __attribute__ ((__packed__))
{
  Tuple *array;
  size_t used;
  size_t size;
} TupleArray;

void initTupleArray(TupleArray *a, size_t initialSize);
void insertTupleArray(TupleArray *a, Tuple element);
void freeTupleArray(TupleArray *a);
