import os, sys, socket
from datetime import datetime
sys.path.append(os.path.split( os.path.realpath( sys.argv[0] ) )[0])
#sys.path.append('../gwac_pipeline')

from simulator_pipeline import pipeline, stargenparams, machine_tableno 
#import simulator_pipeline

####################time col
import datetime
t_str = '2017-10-12 00:00:00'
d = datetime.datetime.strptime(t_str, '%Y-%m-%d %H:%M:%S') 
now=datetime.datetime.now()
timedelta=now-d
timestamp=timedelta.days*24*3600 + timedelta.seconds
####################time col

ccdno=int(sys.argv[1])
rac=int(sys.argv[2])
decc=int(sys.argv[3])
# the initial x and y
xi=int(sys.argv[4])
yi=int(sys.argv[5])
abnormpoint=int(sys.argv[6])
fp=open(os.path.split( os.path.realpath( sys.argv[0] ) )[0]+'/init_imageid.txt','r')
c=fp.readline()
fp.close()
catano=int(c)+1
fp=open(os.path.split( os.path.realpath( sys.argv[0] ) )[0]+'/init_imageid.txt','w')
fp.write(str(catano))
fp.close()

#os.system("sudo sysctl -w vm.swappiness=70")

#2400 is the number of each day's catalogs of one CCD.
    #n1=i
    #n2=i+2399
for i in '1': 
    cmbrate = 200 #2400/200=12 and 34M*200=6.6G ,*15proc=100G, plus copy-on-write = 200G, can fit in total mem 256G.
    ratiosize = {200:'270M'}
    startno=1
    endno=1
    stargenparams['ccdno']=ccdno
    stargenparams['newcatano']=catano
    stargenparams['n1cata']=timestamp
    stargenparams['n2cata']=timestamp+(endno-1)*15
    stargenparams['rac']=rac
    stargenparams['decc']=decc
    stargenparams['xi']=xi
    stargenparams['yi']=yi
    stargenparams['abnormpoint']=abnormpoint
    tblno=1 #The back one is used to put data into monetdb, and does not has effect to generate star data 
            #tblno=machine_tableno[socket.gethostname()]
    srcdir=stargenparams['destdir']
    cbddir="../combinedcsv-%din1-%s/" %(cmbrate, ratiosize[cmbrate])
    binarydir="../binarycatalogs-%din1/" %cmbrate
    prefix="RA%03d_DEC%d_sqd%d-ccd%s-" %(stargenparams['rac'], stargenparams['decc'], stargenparams['sqd'],stargenparams['ccdno'])
    suffix=".cat"
    dbname='gwacdb'
#    print "\nInitial parameters:\nstartno="+str(startno)+"\nendno="+str(endno)+"\ncmbrate="+str(cmbrate)+"\nhostname:"+socket.gethostname()+"\nccdno(tblno)="+str(tblno)+"\nsrcdir="+str(srcdir)+"\ncbddir="+str(cbddir)+"\nbinarydir="+str(binarydir)+"\nprefix="+prefix+"\nsuffix="+suffix+"\nratiosize[%d]=" %cmbrate +ratiosize[cmbrate] +"\ndbname="+dbname+"\n"
    #1.Generate simulated catalog files. Uncomment the next line if this step is finished.
    #edit the prefix of cbddir, binarydir according to your machine.
    os.chdir('.')
    pipeline(ifsimcat=True, ifcomb=False, ifconv=False, ifload=False, startno=startno, endno=endno, ratio=cmbrate, tblno=tblno, cbddir=cbddir, srcdir=srcdir, binarydir=binarydir, prefix=prefix, suffix=suffix, dbname=dbname)
    #2.Combine multiple catalogs into one so as to speed up db loading. Uncomment the next line if this step is finished.
#   pipeline(ifsimcat=False, ifcomb=True, ifconv=False, ifload=False, startno=startno, endno=endno, ratio=cmbrate, tblno=tblno, cbddir=cbddir, srcdir=srcdir, binarydir=binarydir, prefix=prefix, suffix=suffix, dbname=dbname)
    #startTime = datetime.now()
    #3. Convert the combined files from CSV format to binary. Uncomment the next line if this step is finished.
   #pipeline(ifsimcat=False, ifcomb=False, ifconv=True, ifload=False, startno=int(stargenparams['n1cata']), endno=int(stargenparams['n2cata']), ratio=cmbrate, tblno=machine_tableno[socket.gethostname()], cbddir="/home/wamdm/wm/gwac/combinedcsv-%din1-%s/" %(cmbrate, ratiosize[cmbrate]), srcdir=stargenparams['destdir'], binarydir="/home/wamdm/wm/gwac/binarycatalogs-%din1/" %cmbrate, prefix="RA%03d_DEC%d_sqd%d-ccd%s-" %(stargenparams['rac'], stargenparams['decc'], stargenparams['sqd'],stargenparams['ccdno']), suffix=".cat", dbname='gwacdb')
   #pipeline(ifsimcat=False, ifcomb=False, ifconv=True, ifload=False, startno=startno, endno=endno, ratio=cmbrate, tblno=tblno, cbddir=cbddir, srcdir=srcdir, binarydir=binarydir, prefix=prefix, suffix=suffix, dbname=dbname)
    #4. Load the binary files into MonetDB.
    #pipeline(ifsimcat=False, ifcomb=False, ifconv=False, ifload=True, startno=int(stargenparams['n1cata']), endno=int(stargenparams['n2cata']), ratio=cmbrate, tblno=machine_tableno[socket.gethostname()], cbddir="/data/gwac/combinedcsv-%din1-%s/" %(cmbrate, ratiosize[cmbrate]), srcdir=stargenparams['destdir'], binarydir="/data/gwac/binarycatalogs-%din1/" %cmbrate, prefix="RA%03d_DEC%d_sqd%d-ccd%s-" %(stargenparams['rac'], stargenparams['decc'], stargenparams['sqd'],stargenparams['ccdno']), suffix=".cat", dbname='gwacdb')
    #print datetime.now() - startTime
    #os.chdir('/data/gwac/gwac_pipeline/')
    #cmd="./5_gwac_uniquecatalog.sh %d %d" %(n1,n2)
    #os.system(cmd)
sys.exit(0)
#os.system("sudo sysctl -w vm.swappiness=0")
