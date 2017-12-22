import os, sys, socket
from combineCata import combine, multicombine
from multicsv2bin import convert, multiconvert
from load_binarytoDB import load
from timer import Timer
import numpy as np
from time import strftime
import stargen_notemplt_mp as stargen

year = strftime("%Y")  
mon  = strftime("%m")  
day  = strftime("%d")  
hour = strftime("%H")
min  = strftime("%M") 
sec  = strftime("%S") 
def todaystr():  
    '''  
    get date string  
    date format="YYYYMMDDHHMMSS"  
    '''  
    return year+mon+day+hour+min+sec  

#ratiosize = {96:'130M', 192:'260M', 384:'520M', 768:'1G', 1536:'2G', 300:'400M'}
ratiosize = {200:'270M'}
#ratiosize = {1:'1.35M'}

def call_combine(today,ratio,startno,endno, cbddir, srcdir, prefix,suffix):
    log1file=os.path.join(sys.path[0], 'logcomb-%s-%din1' %(today, ratio))
    log1 = open(log1file, 'w+')
    cmd = "mkdir -p %s" %cbddir
    os.system(cmd)
    #combine ratio catalogs into a larger one.
    with Timer() as t1:
        combine(startno, endno, ratio, destdir=cbddir, sourcedir=srcdir, prefix=prefix, suffix=suffix)
    print >>log1, "combine %d catalogs, %d:1, elasped time: %.3f s" %(endno-startno+1, ratio, t1.secs)
    log1.close()

def call_multicombine(today,ratio,startno,endno, cbddir, srcdir, prefix,suffix):
    log1file=os.path.join(sys.path[0], 'logcomb-%s-%din1' %(today, ratio))
    log1 = open(log1file, 'w+')
    cmd = "mkdir -p %s" %cbddir
    os.system(cmd)
    #combine ratio catalogs into a larger one.
    with Timer() as t1:
        multicombine(startno, endno, ratio, destdir=cbddir, sourcedir=srcdir, prefix=prefix, suffix=suffix)
    print >>log1, "combine %d catalogs, %d:1, elasped time: %.3f s" %(endno-startno+1, ratio, t1.secs)
    log1.close()

def call_multiconvert(today, ratio, startno, endno, binarydir, cbddir, prefix,suffix):
    #create a dirctory first for placing converted binary catalogs.
    start = int(np.ceil(float(startno)/ratio))
    end = int(np.ceil(float(endno)/ratio))
    #arguments for convert
    cmd = "mkdir -p  %s" %binarydir
    os.system(cmd)
    log2file=os.path.join(sys.path[0], 'logmult-%s-cs%s-%din1-%dto%d' %(today, ratiosize[ratio], ratio, endno, end))
    log2 = open(log2file, 'w+')
    with Timer() as t2:
        multiconvert(start, end, bindir=binarydir, catadir=cbddir, prefix=prefix, suffix=suffix)
    print >>log2,"convert catalogs from %d to %d elapsed time: %.3f s." %(start, end, t2.secs)
    log2.close()

def call_convert(today, ratio, startno, endno, binarydir, cbddir, prefix, suffix):
    #create a dirctory first for placing converted binary catalogs.
    start = startno
    end = int(np.ceil(float(endno)/ratio))
    #arguments for convert
    cmd = "mkdir -p  %s" %binarydir
    os.system(cmd)
    log2file=os.path.join(sys.path[0], 'logmult-%s-cs%s-%din1-%dto%d' %(today, ratiosize[ratio], ratio, endno, end))
    log2 = open(log2file, 'w+')
    with Timer() as t2:
        convert(start, bindir=binarydir, catadir=cbddir, prefix=prefix, suffix=suffix, n=end)
    print >>log2,"convert catalogs from %d to %d elapsed time: %.3f s." %(start, end, t2.secs)
    log2.close()

def call_load(today, ratio, tblno, startno, endno, binarydir, prefix, suffix, dbname):
    #only drop targets table when startno=1, ie loading from an empty, otherwise it means we dont need to load from scratch, no need to delete table.
    if startno == 1:
        os.system("echo 'drop table targetss%d and targets%s and targets_seq'; mclient %s -s 'drop table targetss%s'" %(tblno, tblno, dbname, tblno))
        os.system("mclient %s -s 'drop table targets%s CASCADE'" %(dbname, tblno))
        os.system("mclient %s -s 'drop sequence targets_seq'" %dbname);
        os.system("echo 'create table targetss and targets' ");
        os.system(" mclient %s -s 'create table targetss%s(imageid int, zone smallint, ra double, \"dec\" double, mag double, pixel_x double, pixel_y double, ra_err double, dec_err double, x double, y double, z double, flux double, flux_err double, normmag double, flag double, background double, threshold double, mag_err double, ellipticity double, class_star double, orig_catid int)' " %(dbname, tblno))
        os.system(" mclient %s -s 'CREATE SEQUENCE \"targets_seq\" as BIGINT'" %dbname)
        os.system(""" mclient %s -s 'create table targets%s(id bigint DEFAULT NEXT VALUE FOR \"targets_seq\", imageid int, zone smallint, ra double, \"dec\" double, mag double, pixel_x double, pixel_y double, ra_err double, dec_err double, x double, y double, z double, flux double, flux_err double, normmag double, flag double, background double, threshold double, mag_err double, ellipticity double, class_star double, orig_catid int, PRIMARY KEY (id) )' """ %(dbname, tblno))

    else: #when startno is no 1, means loading continues with existing data, so no need to delete targets1 table.
        os.system("echo 'drop table targetss%d '; mclient %s -s 'drop table targetss%s'" %(tblno, dbname, tblno))
        os.system("echo 'create table targetss'");
        os.system(" mclient %s -s 'create table targetss%s(imageid int, zone smallint, ra double, \"dec\" double, mag double, pixel_x double, pixel_y double, ra_err double, dec_err double, x double, y double, z double, flux double, flux_err double, normmag double, flag double, background double, threshold double, mag_err double, ellipticity double, class_star double, orig_catid int)' " %(dbname, tblno))
        
    
    #load binary files into MonetDB.
    log3 = os.path.join(sys.path[0], 'logload-%s-cs%s-%din1-%dto%d' %(today, ratiosize[ratio], ratio, startno, endno))
    if ratio != 1:
        #using one core to load one table.
        num = int(np.ceil(float(endno-startno+1)/ratio))
        #combinedstartno is the start combined-catalog number we need to find in binarycatalogs-xin1 folder, should only be interger multiple of ratio +1.
        combinedstartno = int(startno/ratio) + 1
    else:
        num = int(np.ceil(float(endno-startno+1)))
        combinedstartno = int(startno)
    with Timer() as t3:
        load(log3, tblno, startno=combinedstartno, num=num, prefix=prefix,suffix=suffix,bindir=binarydir, dbname=dbname)
    etime = t3.secs
    with open(log3, 'a') as logfile:
        logfile.write("finished loading %d %s catalogs into MonetDB, elapsed time: %.3f s." %(num, ratiosize[ratio], etime))

def pipeline(ifsimcat, ifcomb, ifconv, ifload, startno, endno, ratio, tblno, cbddir, srcdir, binarydir, prefix="RA240_DEC10_sqd225-",suffix=".cat", dbname='gwacdb'):
    #first check if startno is valid. startno should only be interger multiple of ratio +1.
    if startno!= 1 and startno % ratio != 1:
        sys.exit("error: startno should only be interger multiple of ratio +1, invalid startno: %d" %startno)
    
    today = todaystr()

    ratio=ratiosize.keys()[0]
    if ifsimcat==True:
        #stargenparams['n1cata']=startno
        #stargenparams['n2cata']=endno        
        stargen.star_generator(**stargenparams)
    if ifcomb==True:
        call_multicombine(today,ratio,startno,endno, cbddir, srcdir, prefix,suffix)
    if ifconv==True: 
        call_multiconvert(today,ratio,startno, endno, binarydir, cbddir, prefix, suffix)
    if ifload==True:
        call_load(today, ratio, tblno, startno, endno, binarydir, prefix, suffix, dbname)

#when simulator_pipeline.py is used independently, uncomment machine_tableno and stargenparams
machine_tableno = {
    'stones01.scilens.private' :   0,
    'stones02.scilens.private' :   1,
    'stones03.scilens.private' :   2,
    'stones04.scilens.private' :   3,
    'stones05.scilens.private' :   4,
    'stones06.scilens.private' :   5,
    'stones07.scilens.private' :   6,
    'stones08.scilens.private' :   7,
    'stones09.scilens.private' :   8,
    'stones10.scilens.private' :   9,
    'stones11.scilens.private' :   10,
    'stones12.scilens.private' :   11,
    'stones13.scilens.private' :   12,
    'stones14.scilens.private' :   13,
    'stones15.scilens.private' :   14,
    'stones16.scilens.private' :   15,
    'gwacdb'                   :   16,
    'wamdm80'                  :   17
}

stargenparams = {
    'n1cata'       :  1,
    'n2cata'       :  24,  #12000,
    'abnormpoint'  :  0,
    'newcatano'    :  1,
    'ccdno'        :  1,
    'destdir'      :  os.path.split( os.path.realpath( sys.argv[0] ) )[0]+"/../catalog.csv/",
    #'templatefile' :  '/data/gwac/RA240_DEC10_sqd180_233.2_246.8_3.4_16.6_175637.cat',
    'templatefile' :  os.path.split( os.path.realpath( sys.argv[0] ) )[0]+'/RA240_DEC10_sqd225.cat',
    'rac'          :  240,
    'decc'         :  10,
    'sqd'          :  225,
    'pixscal'      :  11.7,
    'xi'           :  0,
    'yi'           :  0,
    'sizes'        :  [4096,4096],
    'zoneheight'   :  10. #arcsec
}
#
#cmbrate = 1 #2400/200=12 and 34M*200=6.6G ,*15proc=100G, plus copy-on-write = 200G, can fit in total mem 256G.
#pipeline(ifsimcat=True, ifcomb=True, ifconv=True, ifload=False, startno=int(stargenparams['n1cata']), endno=int(stargenparams['n2cata']), ratio=cmbrate, tblno=machine_tableno[socket.gethostname()], cbddir="/scratch/meng/gwac/combinedcsv-%din1-%s/" %(cmbrate, ratiosize[cmbrate]), srcdir=stargenparams['destdir'], binarydir="/scratch/meng/gwac/binarycatalogs-%din1/" %cmbrate, prefix="RA%03d_DEC%d_sqd%d-ccd%s-" %(stargenparams['rac'], stargenparams['decc'], stargenparams['sqd'],stargenparams['ccdno']), suffix=".cat", dbname='gwacdb')
#usage:
#nohup python pipeline.py &
