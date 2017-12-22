#!/usr/bin/env python
#  script name: multicsv2bin.py
#  Usage : multicsv2bin.py 
#  purpose: convert csv catalogs into MonetDB compliant binary format.
#           
#  Author:  Wan Meng <wanmeng@bao.ac.cn>
#
#------------------------------------
import numpy as np
import os, sys
from itertools import count
import multiprocessing as mp
from timer import Timer

def gen_filenames(catastart, catadir="/data/sim-240cata-7G/", prefix="RA240_DEC10_sqd300-",suffix=".cat"):
    """
    return a generator of a list of auto increment filenames
    cataname like "RA240_DEC10_sqd300-0001.cat"
    from the start_th number of catalog file.
    """
    for i in count(catastart):
        yield "%s%s%04d%s" %(catadir, prefix, i, suffix)
        
#bindir = "/data/binarycatalogs-12in1"
#bindir = "/data/binarycatalogs-24in1"
#bindir = "/data/binarycatalogs-48in1"
catadir = "/data/sim-240cata-7G/"
prefix = "RA240_DEC10_sqd300-"
def convert(catastart, bindir, catadir, prefix, suffix, n=None):
    """
    convert one or n catalogues from csv format to binary. Could designate another destionation dir to store results of binary catalogs.
    input filenames are given by several parts, so could jigsaw a batch of input ascii files.
        format:{prefix}{catastart}{suffix}. prefix must be absolute path!
        like: {RA240_DEC10_sqd300-}{1}{.cat}
    output binary files name: prefix-index.suffix-littleindex. The index field is padded with leading zeros.
        like: RA240_DEC10_sqd300-0001.cat-1
    """
    if n is not None:  #single process version, convert a batch of files at a time.
        g=gen_filenames(catastart,catadir,prefix,suffix)
        for x in range(n):
            catname=g.next()
            outputname = "%s-*" %catname
            content = '''/usr/bin/time -f %e '''
            #csv2bincmd = os.path.join(sys.path[0], 'csv2bin ')
            csv2bincmd = os.path.join(os.getcwd(), 'csv2bin ')
            #cmd = content + csv2bincmd + catname + "; mv %s  %s"  %(outputname, bindir)
            #cmd1 = content + csv2bincmd + catname
            #print  cmd1
            #os.system(cmd1)
            cmd2 = "cp %s  %s"  %(outputname, bindir)
            #cmd2 = "cp /home/wamdm/wm/gwac/combinedcsv-200in1-270M/RA240_DEC10_sqd225-ccd17-0001.cat-{1..22}  /home/wamdm/wm/gwac/binarycatalogs-200in1"
            print cmd2
            os.system(cmd2)
    else: #multiprocessing version, convert one file at a time.
        catname = "%s%s%04d%s" %(catadir, prefix, catastart, suffix)
        outputname = "%s-*" %catname
        content = '''/usr/bin/time -f %e '''
        #csv2bincmd = os.path.join(sys.path[0], 'csv2bin ')
        csv2bincmd = os.path.join(os.getcwd(), 'csv2bin ')
        cmd = content + csv2bincmd + catname + "; mv %s %s"  %(outputname, bindir)
        cmd1 = content + csv2bincmd + catname
        os.system(cmd1)

        cmd2 = "mv %s  %s"  %(outputname, bindir)
        #cmd2 = "cp /home/wamdm/wm/gwac/combinedcsv-200in1-270M/RA240_DEC10_sqd225-ccd17-0001.cat-1  /home/wamdm/wm/gwac/binarycatalogs-200in1"
        print cmd2
        os.system(cmd2)

def convert_wrap(args):
    return convert(*args)
#one day has 86400 catalogs, create to 86400.
#run this script in the /data filesystem
#convert(42066,prefix="/data/sim-catalogs/RA240_DEC10_sqd300-",catastart=10001)

#def multiconvert(processes, n1cata=14658, n2cata=54999):
def multiconvert(n1cata, n2cata, bindir, catadir, prefix, suffix):
    """
    benefit from multiporcessing by splitting task of genarating thousands of catalogs
    into individual catalog to be processed simultaneously.
    """
    # Create a pool class and run the jobs-the number of jobs is equal to the number of catalogs needed to generate.
    pool = mp.Pool(processes=mp.cpu_count()-1)
    args = [(catano, bindir,catadir,prefix,suffix) for catano in range(n1cata, n2cata+1)]
    #results = pool.map(convert, range(n1cata, n2cata+1))
    results = pool.map(convert_wrap, args)

    # Synchronize the main process with the job processes to ensure proper cleanup.
    pool.close()
    pool.join()

#usage1: in shell
#nohup python multicsv2bin.py > logmult-20141017-cs63M-48in1 &
#start = 1
#end = 209
#binarydir = "/data/binarycatalogs-48in1"
#if __name__ == '__main__':
#    with Timer() as t:
#        if __name__ == "__main__":
#            #launch 40 cores to convert binary catalogs from 1 to 86400.
#            multiconvert(start, end)
#    print "multicsv2bin: convert catalogs from %d to %d elapsed time: %.3f s." %(start, end, t.secs)
#usage2: in ipython
#  from multicsv2bin import *
#  convert(1)
#convert(1, "/home/wamdm/wm/gwac/binarycatalogs-200in1", "/home/wamdm/wm/gwac/combinedcsv-200in1-270M/", "RA240_DEC10_sqd225-ccd17-", ".cat", 1)
