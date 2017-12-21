#  script name: binary_simulator_notemplt_mp.py
#  Usage : from binary_simulator_notemplt_mp.py 
#  purpose: 
#          one day GWAC has 86400 catalogs.
#          this script generates simulated ASCII catalogs only, not generate template from UCAC4! 
#          could work in either single process or multiple process mode by parameter overloading.
#         
# *note* :
#     To generate template from UCAC4 using: binary_simulatorbinary.py
#  Author:  Wan Meng <wanmeng@bao.ac.cn>
#
#------------------------------------

import os, sys, shutil
from os.path import getsize
#import matplotlib.pyplot as plt
import datetime
import scipy as S
import numpy as np
import pywcs
import pyfits
import re
import pdb
#import monetdb.sql
import struct
import ctypes
import multiprocessing as mp, logging, functools
from itertools import count
import ntpath
import warnings
import random
warnings.simplefilter("ignore")

#from WU Chao
def readcat(fil):
   """
   return:
   ra,dec,imag,sigi,mag1,magB,magV,gmag,rmag
   """
   ff=open(fil)
   oo = []
   for f0 in ff:
       f0t = f0.strip()
       id  = f0t[0:10]
       ra  = f0t[11:21]
       dec = f0t[23:36]
       mag1 = f0t[36:43]
       magB = f0t[168:175]
       magV = f0t[175:182]
       gmag = f0t[182:189]
       rmag = f0t[189:196]
       imag = f0t[196:203]
       if len(imag.strip())==0:
           imag= -9999
       sigi= f0t[227:233]
       if len(sigi.strip()) ==0:
          sigi = -9999
       if len(magB.strip())==0:
          magB = -9999
       if len(magV.strip())==0:
          magV = -9999

       if len(gmag.strip())==0:
          gmag = -9999

       if len(rmag.strip())==0:
          rmag = -9999


       oo.append([ra,dec,imag,sigi,mag1,magB,magV,gmag,rmag])
   return S.array(oo,dtype='float')


#from WU Chao
def calSquareDegree_conver(sqdeg,dec0):
    """
    input sqdeg: unit in deg.
    dec0: dec center in decimal deg.
    assume decw = sqrt(sqdeg)
    return: raw

    ref:http://en.wikipedia.org/wiki/Solid_angle
    """
    sa = sqdeg/((180./S.pi)**2)
    decw = S.sqrt(sqdeg)
    raw = S.degrees(sa /(S.sin(S.radians(dec0+decw/2.) ) - S.sin(S.radians(dec0-decw/2.))))
    return raw


def getcats(rac,decc,decw,sqd=180,outfile=None):
    """
    get point sources from UCAC4 catalog.
    """
    raw = calSquareDegree_conver(sqd,decc)   
    #generate a ucac4.txt catalog.
    cmd = "u4test %s %s  %s  %s  u4b/" %(rac,decc,raw,decw)
    print "cmd = %s " %cmd
    os.system('rm -fr ucac4.txt')
    os.system(cmd)
    dd=readcat('ucac4.txt')
    #ra,dec,imag,sigi,mag1,magB,magV,gmag,rmag
    #only use ra,dec,mag1
    radecmag=S.array(dd).take([0,1,4], axis=1) # only take column number of 0,1,4

    #wcs need to change according to ra,dec, used to (ra, dec)-->(x, y)
    wcs= genwcs(rac, decc, pixscal, sizes)
   
    #use wcs to translate RA,DEC to x,y, according to new rac,decc.
    ra=radecmag[0]
    dec=radecmag[1]
    x,y = wcs.wcs_sky2pix(ra, dec, 0)  #(RA,DEC)-->(x,y)

    #add the x,y field as the 3th,4th column.
    dd=np.concatenate(radecmag, x, axis=1)
    dd=np.concatenate(dd, y, axis=1)
    
    if outfile:
        S.savetxt(outfile,radecmag)
    else:
        outfile="RA%s_DEC%s_sqd%s.txt" %(rac,decc,sqd)
        S.savetxt(outfile, dd)
    return radecmag


#from WU Chao
def genwcs(rac,decc,pixscal=11.7,sizes=[4096,4096]):
    """
    rac: center ra,
    decc: center dec,
    pixscal: pixel scale in arcsec/pixel
    sizes: CCD pixels size, e.g. 1k= [1024,1024], 4k= [4096,4096]
    """
    pixd= pixscal/3600.
    xc = sizes[0]/2. + 0.5
    yc = sizes[1]/2. + 0.5

    ddtm= S.zeros(sizes)

    wcs = pywcs.WCS()
    wcs.naxis1 = sizes[0]
    wcs.naxis2 = sizes[1]

    wcs.wcs.ctype = ['RA---TAN', 'DEC--TAN']
    wcs.wcs.crpix = S.array([xc,yc])
    wcs.wcs.crval = S.array([rac,decc])
    wcs.wcs.cd    = S.array([[pixd,0],[0,pixd]])
    wcs.wcs.cdelt = S.array([ 1.,  1.])
    hh = wcs.to_header()
    hh.update('IMAGEW',wcs.naxis1)
    hh.update('IMAGEH',wcs.naxis2)
    hdu = pyfits.PrimaryHDU(header=hh)
    return wcs


#produce one simulation catalog 
def addGaussNoisetoxy(dd, wcs_transtool,sigma=0.07):
    """
    This is first kind of x,y noise, add Noise distribution ~N(0, 0.1*pixel) of shape: (radecmagxy.shape[0],2) to samplefile[x,y column]
    change RA,DEC according to x,y. wcs is a local variable produced by firstimg.py
    r^2=x^2+y^2, sigma_x=0.07,sigma_y=0.07,so r^2=0.1,
    """
    #set the number of stars
    n=dd.shape[0]

    #noise = np.random.normal(0, sigma,(n,2)) # scale is standard deviation

    #add noise to xy
    #dd[:,3:5] += 0#noise 
    
    # mapping to CCD, filter to reserve only column x,y both >0 and <4096!
    dd =dd[(dd[:,3]>0) & (dd[:,4]>0) & (dd[:,3]<4096) & (dd[:,4]<4096)]

    #change RA,DEC of noise xy
    RA,DEC= wcs_transtool.wcs_pix2sky(dd[:,3], dd[:,4], 0)  #(x,y)->(RA,DEC)
    dd[:,0] = RA # dd[:,0:2]=RA,DEC doesn't work!
    dd[:,1] = DEC

    return dd


#add delta_ra, delta_dec to RA,DEC, then translate RA,DEC to x,y
def addMountMovetoRADEC(dd, rac=150, decc=40, peakval=10., pixscal=11.7, sizes=[4096,4096]):
    """
    Mount moving need to be add to x,y, this is second kind of x,y error, each image has the same err value.
    range from (-10.,10.) pixel.
    the ra,dec of stars within CCD camera is constant(star positions are constant), only its xy are changed.
    """
    #change delta in pixel to degree. 10 pix=0.0325 deg
    mov_deg = peakval*pixscal/3600.

    #delta_ra, delta_dec =np.random.uniform(-mov_deg, mov_deg, size=2)
    delta_ra = 0
    delta_dec = 0

    #add mount moving to ra,dec.
    ra=dd[:,0]
    dec=dd[:,1]
    #mount is moving, but stars position is constant,so star ra,dec aren't changed.
    #ra[:] = ra+delta_ra  #ra[:] is a view of ra!
    #dec[:] = dec+delta_dec

    #mount center is a new ra,dec.
    rac=rac+delta_ra
    decc=decc+delta_dec
    
    #wcs need to change according to new ra,dec.
    wcs= genwcs(rac, decc, pixscal, sizes)

    #use wcs to translate RA,DEC to x,y, according to new rac,decc.
    x,y = wcs.wcs_sky2pix(ra, dec, 0)  #(RA,DEC)-->(x,y)
    dd[:,3] = x # xy[:,3:4]=RA,DEC doesn't work!
    dd[:,4] = y

    # mapping to CCD, filter to reserve only column x,y both >0 and <4096!
    dd =dd[(dd[:,3]>0) & (dd[:,4]>0) & (dd[:,3]<sizes[0]) & (dd[:,4]<sizes[1])]

    return dd

def addGaussMagErrtoMag(dd):
    """
    add merr directly to mag column. for each mag, the magerr follows Gauss distribution, only difference is sigma.
    from xlp, the magerr~N(0,sigma)
for those objects with mag>14 the relation is:
m(x)=m0+m1*x+m2*x*x+m3*x*x*x
m0              = -73.7154
m1               = 15.7868
m2               = -1.13085
m3              = 0.0271142

where magerr sigma is m(x), x is mag

for those obj. with mag <14, it will be 
n(x)=n0+n1*x+n2*x*x+n3*x*x*x
n0              = -1.2375
n1              = 0.362355
n2              = -0.035126
n3              = 0.00113084

    """
    mag=dd[:,2] #mag is the 3rd column.
    m0              = -73.7154
    m1               = 15.7868
    m2               = -1.13085
    m3              = 0.0271142
    #m(x)=m0+m1*x+m2*x*x+m3*x*x*x

    n0              = -1.2375
    n1              = 0.362355
    n2              = -0.035126
    n3              = 0.00113084
    #n(x)=n0+n1*x+n2*x*x+n3*x*x*x

    merr_sigma= np.where(mag>14, m0+m1*mag+m2*np.power(mag,2)+m3*np.power(mag,3), n0+n1*mag+n2*np.power(mag,2)+n3*np.power(mag,3))
    #plt.figure()
    #plt.ylim(0,1)
    #plt.plot(mag, merr_sigma,'r.')
    merr_sigma =np.abs(merr_sigma)  #for 6,7,8 mag, there are minus values.
    merr = [np.random.normal(0,sigma_i) for sigma_i in merr_sigma]
    merr = np.array(merr)

    #add a column merr to[ra,dec,mag,x,y] after mag as 4th column. zip(merr) must be a column vector when axis=1
    #become [ra,dec,mag,merr,x,y]
    #oo =np.concatenate((dd[:,0:3],zip(merr),dd[:,3:5]),axis=1)

    #add merr to mag.
    mag[:] = mag +merr
    
    return dd

def addExtinctiontoMag(dd, abnormpoint, peakval=0.5) :
    """
    add atmospheric extinction to mag, this is second kind of mag error, each image has the same err value.
    range from (-0.5, 0.5)
    """
    mag=dd[:,2]
    n=mag.shape[0]
    delta_mag2=np.random.uniform(-peakval,peakval, size=n)
    for i in range(abnormpoint):
        a=random.randint(0, n-1)
        mag[a]=mag[a]+50
    #directly add delta_mag2 to mag column of dd.
    dd[:,2] =mag + delta_mag2  #mag[:] is a view of mag.
    

    return dd 

def genOneSimuCatalog(catano, abnormpoint, newcatano, ccd, destdir, templatefile, rac, decc, xi, yi, sqd, wcs_transtool, origintime, pixscal=11.7, sizes=[4096,4096], zoneheig=10.):
    """
    do the simulation according to catano number.
    """
    dirname = ntpath.dirname(templatefile)  #like '/data/sim-catalogs'
    #basename = ntpath.basename(templatefile) #like 'RA240_DEC10_sqd300.cat'
    #samplefilepurename= re.findall(r'.+?(?=.cat)', basename)[0] #like 'RA240_DEC10_sqd300'

    #catalog number start from 1,not 0.
    #- zerofill  zfill() or %04d is ok.
    timestamp = catano
    #catano = (timestamp - origintime)/15 + 1
    catano=newcatano
    simcatafilename = "%sRA%03d_DEC%d_sqd%d-ccd%s-%04d.cat"  %(destdir, rac, decc, sqd, ccd, catano)

    #String that will be written at the beginning of the file.
    head=''
    #head="#   1 NUMBER  Running object number\n"
    #head += "#   2 ALPHA_J2000     Right ascension of barycenter (J2000)          [deg]\n"
    #head += "#   3 DELTA_J2000     Declination of barycenter (J2000)              [deg]\n"
    #head += "#   4 MAG                                                            [mag]\n"
    #head += "#   5 X_IMAGE         Object position along x                        [pixel]\n"
    #head += "#   6 Y_IMAGE         Object position along y                        [pixel]"
   
    #use the template catalog to generate simulation catalog     
    dd = np.loadtxt(templatefile,usecols=(1,2,3,4,5))
    
    #add x,y noise and to x,y column.
    dd=addGaussNoisetoxy(dd, wcs_transtool,sigma=0.0000001)#0.07
    #print(dd[:,3])
     
    oo=addMountMovetoRADEC(dd, rac=rac, decc=decc, peakval=1.709)

    #add mag gauss err and atmospheric extinction to mag column.
    uu=addGaussMagErrtoMag(oo)
    dd=addExtinctiontoMag(uu, abnormpoint, 0.5)

    #add the zone as the first column, using decl/20 arcsec.
    zone=np.floor(dd[:,1]/zoneheig*3600)
    dd=np.concatenate((zip(zone),dd), axis=1)

    #add the imageid field:catano as the zeroth column.
    n=dd.shape[0]
    imageid=np.ones(n,dtype=np.uint32)*catano
    dd=np.concatenate((zip(imageid),dd), axis=1)
    
    ccdid=np.ones(n,dtype=np.uint32)*ccd
    dd=np.concatenate((zip(ccdid),dd), axis=1)
    #add ra_err and dec_err as seventh and eighth column, using random values.
    radec_err=np.random.random((n,2))
    dd=np.concatenate((dd,radec_err),axis=1)

    #add x,y,z as the ninth,tenth,eleventh column, calculated by ra,dec.
    x=np.cos(np.radians(dd[:,2]))*np.cos(np.radians(dd[:,3]))
    y=np.sin(np.radians(dd[:,2]))*np.cos(np.radians(dd[:,3]))
    z=np.sin(np.radians(dd[:,3]))
    dd=np.concatenate((dd,zip(x),zip(y),zip(z)),axis=1)

    #compute flux = 10^(-0.4*magnitude)*zero_point_flux as the twelve column.
    #zero point flux(Jy) at V band is 3640 in Johnson system. output unit is mJy.
    Fo=3640
    flux=np.power(10, -0.4*dd[:,3])*Fo*1000
    dd=np.concatenate((dd,zip(flux)),axis=1)

    #add 13th-20th fields,all random.
    fillrandoms=np.random.random((n,8))
    dd=np.concatenate((dd,fillrandoms),axis=1)

    #add the orig_catid as 21th column.
    num=np.arange(1,n+1,dtype=np.uint32)
    
    dd=np.concatenate((dd,zip(num)),axis=1) 
    ####################time col
    date=np.arange(1,n+1, dtype=np.uint32)
    date[date>0]=timestamp
    dd=np.concatenate((dd,zip(date)),axis=1)
    ####################time col
    #add the shifting to xy 
    dd[:,6]=dd[:,6]+xi
    dd[:,7]=dd[:,7]+yi
    
    #save to simulation file.
    if head:
        np.savetxt(simcatafilename, dd, fmt=["%d","%10.4f","%10.4f","%10.4f","%10.4f","%10.4f"], header=head)
    else:
        #every field is seperated with a blank space, only need to assign the decimal place. x/y/z need more accuracy.
        np.savetxt(simcatafilename, dd, fmt=["%d","%d","%d","%.4f","%.4f","%.4f","%.4f","%.4f","%.4f","%.4f","%.16f","%.16f","%.16f","%.4f","%.4f","%.4f","%.4f","%.4f","%.4f","%.4f","%.4f","%.4f","%d","%d"])
    
    #print str(simcatafilename)
    filesize=getsize(simcatafilename)/1024.0/1024.0
    now=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print str(n)+' '+str(filesize)+' '+now       
    #transform the txt format to binary catalog, one column writes to one separate binary file.
    # if not set format, int type in file will be read as float type.
    """
    aa=np.loadtxt(simcatafilename,dtype={'names':('imageid','zone','ra','dec','mag','x_pix','y_pix','ra_err','dec_err','x','y','z','flux','flux_err','normmag','flag','background','threshold','mag_err','ellipticity','class_star','orig_catid'),'formats':(np.int32,np.float64,np.float64,np.float64,np.float64,np.float64,np.float64,np.float64,np.float64,np.float64,np.float64,np.float64,np.float64,np.float64,np.float64,np.float64,np.float64,np.float64,np.float64,np.float64,np.float64,np.int32)}) #read the format.
    col=22
    for i in range(0,col):
        fp=open("%s-%d" %(simcatafilename,i),'wb+')
        if type(aa[0][i]) is np.int32:
            bytes = ctypes.create_string_buffer(4*n)
            for j in range(0,n):
                struct.pack_into('i',bytes,j*4,aa[j][i])
        elif type(aa[0][i]) is np.float64:
            bytes = ctypes.create_string_buffer(8*n)
            for j in range(0,n):
                struct.pack_into('d',bytes,j*8,aa[j][i])
        fp.write(bytes)
        fp.close()
    """
    # return simulated catalog file name can be used to convert to binary format.
    return simcatafilename


def getOneNightCatalog(catano, abnormpoint, newcatano, ccd, destdir, templatefile, rac, decc, xi, yi, sqd, pixscal, sizes, zoneheig, origintime):
    """
    produce one night's catalog from UCAC4 sample catalog file just produced by firstimg.py, name like: RA200_DEC40_sqd250.txt
    generate a single catalog in multi processing mode.
    """
    wcs=genwcs(rac, decc,  pixscal, sizes)
    genOneSimuCatalog(catano, abnormpoint, newcatano, ccd, destdir, templatefile, rac, decc, xi, yi, sqd, wcs, origintime, pixscal, sizes, zoneheig)
    
def star_generator(**workparams):
    """
    benefit from multiporcessing by splitting task of genarating thousands of catalogs
    into individual catalog to be processed simultaneously.
    """
    if not os.path.exists(workparams['templatefile']):
        sys.exit('%s: template file does not exits.' %workparams['templatefile'])
    if os.path.exists(workparams['destdir']):
        shutil.rmtree(workparams['destdir'])
    if not os.path.exists(workparams['destdir']):
        os.makedirs(workparams['destdir'])

    # Create a pool class and run the jobs-the number of jobs is equal to the number of catalogs needed to generate.
    pool = mp.Pool(processes=mp.cpu_count()-1)
    pool.map(functools.partial(getOneNightCatalog, abnormpoint=workparams['abnormpoint'], newcatano=workparams['newcatano'], ccd=workparams['ccdno'], destdir=workparams['destdir'], templatefile=workparams['templatefile'], rac=workparams['rac'], decc=workparams['decc'], xi=workparams['xi'], yi=workparams['yi'], sqd=workparams['sqd'], pixscal=workparams['pixscal'], sizes=workparams['sizes'], zoneheig=workparams['zoneheight'], origintime=workparams['n1cata']), range(workparams['n1cata'], workparams['n2cata']+1,15)) 
    # Synchronize the main process with the job processes to ensure proper cleanup.
    pool.close()
    pool.join()

'''
workparams = {
    'n1cata'       :  1,
    'n2cata'       :  2400,
    'ccdno'        :  machine_tableno[],
    'destdir'      :  "../catalog.csv",
    'templatefile' :  'RA240_DEC10_sqd225.cat',
    'rac'          :  240,
    'decc'         :  10,
    'sqd'          :  225,
    'pixscal'      :  11.7,
    'sizes'        :  [4096,4096],
    'zoneheight'   :  10. #arcsec
} 
'''
# End main
if __name__ == "__main__":
    star_generator(**workparams)
