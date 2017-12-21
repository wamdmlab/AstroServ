#include <stdlib.h>
#include <stdio.h>
#include "array.h"

void initIntArray(IntArray *a, size_t initialSize) {
  a->array = (int *)malloc(initialSize * sizeof(int));
  a->used = 0;
  a->size = initialSize;
}

void insertIntArray(IntArray *a, int element) {
  if (a->used == a->size) {
    a->size *= 2;
    a->array = (int *)realloc(a->array, a->size * sizeof(int));
  }
  a->array[a->used++] = element;
}

void freeIntArray(IntArray *a) {
  free(a->array);
  a->array = NULL;
  a->used = a->size = 0;
}

void initShortArray(ShortArray *a, size_t initialSize) {
  a->array = (short *)malloc(initialSize * sizeof(short));
  a->used = 0;
  a->size = initialSize;
}

void insertShortArray(ShortArray *a, short element) {
  if (a->used == a->size) {
    a->size *= 2;
    a->array = (short *)realloc(a->array, a->size * sizeof(short));
  }
  a->array[a->used++] = element;
}

void freeShortArray(ShortArray *a) {
  free(a->array);
  a->array = NULL;
  a->used = a->size = 0;
}


void initDoubleArray(DoubleArray *a, size_t initialSize) {
  a->array = (double *)malloc(initialSize * sizeof(double));
  a->used = 0;
  a->size = initialSize;
}

void insertDoubleArray(DoubleArray*a, double element) {
  if (a->used == a->size) {
    a->size *= 2;
    a->array = (double *)realloc(a->array, a->size * sizeof(double));
  }
  a->array[a->used++] = element;
}

void freeDoubleArray(DoubleArray *a) {
  free(a->array);
  a->array = NULL;
  a->used = a->size = 0;
}

void initTupleArray(TupleArray *a, size_t initialSize) {
  a->array = (Tuple *)malloc(initialSize * sizeof(Tuple));
  a->used = 0;
  a->size = initialSize;
}
void insertTupleArray(TupleArray *a, Tuple element){
  if (a->used == a->size) {
    a->size *= 2;
    a->array = (Tuple *)realloc(a->array, a->size * sizeof(Tuple));
  }
  a->array[a->used++] = element;
}
void freeTupleArray(TupleArray *a){
  free(a->array);
  a->array = NULL;
  a->used = a->size = 0;
}
