import os, sys, socket
from combineCata import combine, multicombine
from multicsv2bin import convert, multiconvert
from load_binarytoDB import load
from timer import Timer
import numpy as np
import scipy as S
from time import strftime
import stargen_notemplt_mp as stargen
import pywcs
import pyfits

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


def getcats(rac,decc,decw,sqd=180):
    """
    get point sources from UCAC4 catalog.
    """
    raw = calSquareDegree_conver(sqd,decc)
    #generate a ucac4.txt catalog.
    cmd = "/home/mengw/ucac4/access/u4test %s %s  %s  %s  /home/mengw/ucac4/u4b/" %(rac,decc,raw,decw)
    print "cmd = %s " %cmd
    os.system('rm -fr ucac4.txt')
    os.system(cmd)
    dd=readcat('ucac4.txt')
    #ra,dec,imag,sigi,mag1,magB,magV,gmag,rmag
    #only use ra,dec,mag1
    radecmag=S.array(dd).take([0,1,4], axis=1) # only take column number of 0,1,4

    extfile="RA%s_DEC%s_sqd%s.txt" %(rac,decc,sqd)
    S.savetxt(extfile, radecmag)
    return radecmag

def getxy(rac,decc,extfile,outfile, pixscal=11.7, sizes=[4096,4096],outwcsfile='tt1.wcs'):
    '''
    get xy from catalog file with RA DEC values, add x,y as last two columns into outfile.
    '''
    wcs= genwcs(rac,decc, pixscal,sizes)  #wcs file is generated at runtime everytime.
    radecmag = S.loadtxt(extfile)
    x,y=wcs.wcs_sky2pix(radecmag[:,0],radecmag[:,1],0)
    xy =S.array(zip(x,y))
    oo =S.concatenate((radecmag,xy),axis=1)

    #filter x,y out of [0, 4096][0, 4096]
    oo=oo[(oo[:,3]>0) & (oo[:,3]<4096) &(oo[:,4]>0) &(oo[:,4]<4096)]

    #add id column as zeroth column
    id=np.arange(1,len(oo)+1, dtype=np.uint32)
    oo=np.concatenate((zip(id),oo),axis=1)
    S.savetxt(outfile, oo, fmt=["%d","%10.4f","%10.4f","%10.4f","%10.4f","%10.4f"])
    return oo



def gettempfromUCAC4(rac,decc,sqd=225,pixscal=11.7,sizes=[4096,4096]):
    decw=np.floor(np.sqrt(sqd))
    getcats(rac,decc,decw,sqd)

    outfile="RA%s_DEC%s_sqd%s.cat" %(rac,decc,sqd)
    extfile="RA%s_DEC%s_sqd%s.txt" %(rac,decc,sqd)
    oo=getxy(rac,decc,extfile,outfile,pixscal,sizes)


gettempfromUCAC4(240, 10, 180)
