import os, multiprocessing,numpy
from timer import Timer

def genCombingname(catano, sourcedir, prefix, suffix):
   combingname = sourcedir+prefix+"%04d" %catano+suffix+' '
   return combingname

def genCombingnames(catano, ratio, sourcedir, prefix, suffix): 
    start = catano
    end = catano+ratio-1
    combingnames = ''
    for catano in range(start,end+1):
        combingnames += genCombingname(catano, sourcedir, prefix, suffix)
    #print combingnames 
    #like: /data/sim-catalogs/RA240_DEC10_sqd300-0001.cat /data/sim-catalogs/RA240_DEC10_sqd300-0002.cat /data/sim-catalogs/RA240_DEC10_sqd300-0003.cat 
    return combingnames

def genDestname(round, ratio, destdir, prefix, suffix):
    destname = destdir+prefix+"%04d" %round+suffix
    return destname

def combine(startno, endno, ratio, destdir='/data/sim-240cata-7G/',sourcedir='/data/sim-catalogs/', prefix='RA240_DEC10_sqd300-', suffix='.cat'):
    """
    combine ratio catalogs into one larger catalog.
    """
    round = numpy.ceil(float(startno)/ratio)
    for catano in range(startno,endno,ratio): #start=1,end=86400,step=240
        combinednames = genCombingnames(catano,ratio,sourcedir,prefix,suffix)
        destname = genDestname(round, ratio, destdir, prefix, suffix)
        cmd = "cat %s > %s" %(combinednames, destname)
        os.system(cmd)
        round += 1
        print "finish combine %d files into %s" %(ratio, destname)

def combine_once(catano, combinednames, destname, ratio):
        cmd = "cat %s > %s" %(combinednames, destname)
        os.system(cmd)
        print "finish combine %d files into %s" %(ratio, destname)

def multicombine(startno, endno, ratio, destdir='/data/sim-240cata-7G/',sourcedir='/data/sim-catalogs/', prefix='RA240_DEC10_sqd300-', suffix='.cat'):
    pool = multiprocessing.Pool(multiprocessing.cpu_count()-1)
    
    round = numpy.ceil(float(startno)/ratio)
    for x in range(startno,endno,ratio):
        combinednames = genCombingnames(x,ratio,sourcedir,prefix,suffix)
        destname = genDestname(round, ratio, destdir, prefix, suffix)
        pool.apply_async(combine_once, (x,), dict(combinednames=combinednames, destname=destname, ratio=ratio))
        round += 1
    pool.close()
    pool.join()
 

#################
#Usage: first clean up /data/sim-240cata-7G/ directory.
#nohup python combineCata.py >logcomb-20141017-48in1 &
#startno=1
#endno = 10008
ratio = 48
destdir = "/data/sim-240cata-7G/"
if __name__ == '__main__':
    with Timer() as t:
        #combine(1, 86400, ratio=7200, destdir="/data/sim-240cata-7G/")
        combine(1, 10008, ratio=ratio, destdir="/data/sim-240cata-7G/")
        #combine(1, 1000, ratio=100, destdir="/data/sim-240cata-7G/")
        #combine(1, 10000, ratio=1000, destdir="/data/sim-240cata-7G/")
    print "combine %d catalogs, %d:1, elasped time: %.3f s" %(endno-startno+1, ratio, t.secs)
