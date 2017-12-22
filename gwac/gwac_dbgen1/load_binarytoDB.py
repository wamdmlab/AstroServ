import numpy as np
import os
from timer import Timer
import multiprocessing as mp
 
def genBinfilenames(catastart=1, prefix="RA240_DEC10_sqd300-", suffix=".cat", bindir="/data/binarycatalogs/"):
    """
        #binary filenames format:dir/prefix-index.suffix-littleindex
        #like: /home/mengw/simulator/ucac4/RA240_DEC10_sqd300-0001.cat-1
    """
    binfilenames = ""
    for i in range(1,23):
        binfilenames += "'%s%s%04d%s-%d'," %(bindir,prefix,catastart,suffix,i)
    binfilenames = binfilenames[0:-1]
    return binfilenames


#usage: getSqlCmd(genBinfilenames(1))
def getSqlCmd(tblno, infilenames):
    cmd = "COPY BINARY INTO targets%s FROM (%s)\"" %(tblno, binfilenames)
    print cmd.strip('"\'')
    return cmd

#usage: getBashMclientCmd(genBinfilenames(1))
def getBashMclientCmd(dbname, tblno, binfilenames):
    cmd = "mclient %s -s \"COPY BINARY INTO targets%s FROM (%s)\"" %(dbname, tblno, binfilenames)
    print cmd.strip('"\'')
    return cmd

def load(log, tblno, startno, num, prefix="RA240_DEC10_sqd300-",suffix=".cat",bindir="/data/binarycatalogs", dbname='gwacdb'):
    """
    load the binary data into single table target#tblno and measure loading time, record into a log.
    """
    logfile=open(log, 'w+')
    with Timer() as T:
        for catano in range(startno, startno+num):
            binfilenames = genBinfilenames(catano, prefix, suffix, bindir)
            cmd = "echo 'mcilent %s -s COPY INTO targetss%d'; mclient %s -s \"COPY BINARY INTO targetss%d FROM (%s)\" " %(dbname, tblno, dbname, tblno, binfilenames)
            with Timer() as t:
                os.system(cmd)
            print >>logfile,"=> loading catano %d elapsed time: %.3f s\n" %(catano, t.secs)
    print >>logfile,"Total loading elapsed time: %.3f s\n" %(T.secs)

    cmd = """echo 'mclient %s -s INSERT INTO targets%d..FROM targetss%d'; mclient %s -i -s "insert into targets%d(imageid, zone, ra, \\"dec\\", mag, pixel_x, pixel_y, ra_err, dec_err, x, y, z, flux, flux_err, normmag, flag, background, threshold, mag_err, ellipticity, class_star, orig_catid) select * from targetss%d;" """ %(dbname, tblno, tblno, dbname, tblno, tblno)
    with Timer() as t1:
        os.system(cmd)
    print >>logfile,"=> INSERT INTO target%d ...FROM targetss%d time: %.3f\n" %(tblno, tblno, t1.secs)
    logfile.close()
         

def loadOneTable(tableno, prefix="RA240_DEC10_sqd300-",suffix=".cat",bindir="/data/binarycatalogs"):
    """
    load the binary data into MonetDB and measure loading time, record into a log.
    """
    catastart = ids[tableno-1,1]
    cataend = ids[tableno-1,2]
    for catano in range(catastart, cataend+1):
        binfilenames = genBinfilenames(catano, prefix, suffix, bindir)
        cmd = "mclient -d gwacdb -f tab -s \"trace COPY BINARY INTO loadtarget%d FROM (%s)\" 1>>logcopy%d 2>&1" %(tableno, binfilenames, tableno)
        with Timer() as t:
            os.system(cmd)
        print "=> loading catano %d into table %d elapsed time: %.3f s" %(catano, tableno, t.secs)

    
def multiLoad(catastart, catacount, prefix="RA240_DEC10_sqd300-",suffix=".cat",bindir="/data/binarycatalogs"):
    """
    load multiple tables simultaneously using multiload to 
    """
    segcount = catacount/tablecount
    for i in range(tablecount):
        ids[i] = [i+1, segcount*i+1, segcount*(i+1)]

    pool = mp.Pool(processes=tablecount)
    results = pool.map(loadOneTable, ids[:,0])
    # Synchronize the main process with the job processes to ensure proper cleanup.
    pool.close()
    pool.join()

