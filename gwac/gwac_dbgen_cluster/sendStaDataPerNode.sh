#!/bin/bash
 
 
 #
 # Licensed to the Apache Software Foundation (ASF) under one 
 # or more contributor license agreements. See the NOTICE file 
 # distributed with this work for additional information 
 # regarding copyright ownership. The ASF licenses this file 
 # to you under the Apache License, Version 2.0 (the 
 # "License"); you may not use this file except in compliance 
 # with the License. You may obtain a copy of the License at 
 # 
 # http://www.apache.org/licenses/LICENSE-2.0 
 # 
 # Unless required by applicable law or agreed to in writing, software 
 # distributed under the License is distributed on an "AS IS" BASIS, 
 # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 # See the License for the specific language governing permissions and 
 # limitations under the License. 
 #  
 # Authored by WAMDM: http://idke.ruc.edu.cn/ 
 #

 
#genFileNum=$1
masterhost=$1
ccdno=$2
bernoulliParam=$3
geometricParam=$4
redisIPBackup=$5
templateTable=$6
sourceFilePath=$7
threadNumber=$8
getTemplateFlag=$9
redisWriteMode=${10}
blockNum=${11}
ra0=${12}
dec0=${13}
xi=${14}
yi=${15}
#sleeptime=15
post=1984
stoppost=1986
tmp=""
hostname=$(hostname)
maxLogSize=10485760
maxTimeOut=2
#redisIP=`ps -ef | grep [r]edis-server | awk BEGIN{i=0}'{a[i++]=$9} END{srand();j=int(rand()*100%NR);print a[j]}'`
       
       echo "exit" | nc -q 0 $hostname $stoppost #Terminate the previous killSend.sh
       nohup ./killSend.sh $$ $masterhost & 
#for i in $(seq 1 $genFileNum)
#do
#t1=$((time -p (python ../gwac_dbgen/pipeline.py $ccdno >dtmp)) 2>&1 | head -1 |awk '{print $2}')

#eval $(python ../gwac_dbgen/pipeline.py $ccdno | awk -F '{a=$1} END{print "tmp="a}')

#backup the original star table
#nohup scp -r ../catalog.csv/* ../data_backup &

###############################+ test template data to put in redis without cross validation. 
#dt=`date "+%Y-%m-%d %H:%M:%S"`  #annotate pipelinedata=`python ../gwac_dbgen/pipeline.py $ccdno $abnum`and the catalog.csv/$sourceFile and template_table/template are the same.
#pipelinedata="0 0 $dt"
##############################+
resultNum=0
abStarNum=0
stopinfo="stop $hostname $resultNum $abStarNum"

if [ "$getTemplateFlag" == "gt" ]
then
 pipelinedata=`python ../gwac_dbgen/pipeline.py $ccdno $ra0 $dec0 $xi $yi 0`
      ./genTemplateTable.sh
	  ./send.py $masterhost $post "$stopinfo"
	  exit 0
fi

##########jedis client
#sourceFile=$sourceFilePath/$(ls ../$sourceFilePath)
#/home/wamdm/jdk1.7.0_79/bin/java -jar ../JedisTest.jar ../$sourceFile 5 1 >>1.txt 2>&1
#redisMonitorData="1 1 1"
##########jedis client

if [ "$getTemplateFlag" == "first" ]
then

redisIP=`ps -ef | grep [r]edis-server | awk 'FNR==1{print $9}'`

if [ "$redisIP" == "" ]
 then
    redisIP=$redisIPBackup
 fi
 eval $(echo $redisIP | awk -F ":" '{i=$1;p=$2} END{print "ip="i";port="p}')
 
       rm -rf /tmp/slave.pipe
	   mkfifo /tmp/slave.pipe   
  pgrep "astroDB_cache" | while read Spid
         do
            kill $Spid
         done
rm -rf /tmp/Squirrel_pipe_test # delete Squirrel pipe file to prevent unknown reason to block write to Squirrel_pipe_test at the first time (echo "$hostname $pipelinedata ../$sourceFile" > /tmp/Squirrel_pipe_test)
          #rm -rf ${hostname}_memUsage.txt   # delete the last the file of memory usage	
templateTable="$templateTable/$(ls ../$templateTable)"		  
paramStr="-times 1 -redisHost $redisIP -method plane -grid 4,4 -errorRadius 1.5 -searchRadius 50 -ref ../$templateTable -threadNumber $threadNumber -width 3016 -height 3016 -terminal -writeRedisMode $redisWriteMode -xi $xi -yi $yi"

if [ "$blockNum" != "-1" ]
   then
      paramStr="$paramStr -blockNum $blockNum"
   fi
nohup ../astroDB_cache/astroDB_cache $paramStr &
getTemplateFlag="next"
fi

while true
do
if [ "$getTemplateFlag" == "slave_exit" ]
then 
    exit 0
fi
 pipelinedata=`python ../gwac_dbgen/pipeline.py $ccdno $ra0 $dec0 $xi $yi 0`

sourceFile=$sourceFilePath/$(ls ../$sourceFilePath)

#if astroDB_cache is launch_failed, this shell will be blocked here
echo "$hostname $pipelinedata $HOME/gwac/$sourceFile $bernoulliParam $geometricParam" > /tmp/Squirrel_pipe_test
sumresult=""

for((timeout=1;timeout<=$maxTimeOut;timeout++))
do
   sleep 0.5
   result=`$HOME/redis-3.2.11/src/redis-cli -h $ip -p $port -c lpop $hostname`
   if [ "$result" == "" ]
     then
       continue
     fi
   echo "$result" | ./send.py $masterhost $post
   sumresult="$sumresult\n$result"
   eval $(echo "$result" | awk -v abs=$abStarNum '{abs+=$NF} END{print "abStarNum="abs}')
   resultNum=$(($resultNum+1))
  # if [ $resultNum -eq 1 ]  # break until receiving one result, but need a large maxTimeOut. E.g., maxTimeOut=10000000
  #   then 
  #     break
  #   fi
done

#./sampleMemUsage.sh # sample the usage amount of memory
stopinfo="stop $hostname $resultNum $abStarNum"
sumresult=`echo $sumresult | cut -c 3-`

if [ "$sumresult" == "" ]
then 
stopinfo="$stopinfo timeout"
else
#### log
     newLogName=`ls logs/ | grep "slave" | tail -1`
if [ "$newLogName" == "" ]
then
     newLogName="slave_${hostname}_1_`date "+%Y-%m-%d-%H:%M:%S"`"
     echo -e "PipelineLine Pipelinesize PipelineDate RedisTime(s) RedisStorageLine CrossCertifiedErrorLine\n0 0 0 0 0 0" > logs/$newLogName
fi
     logSize=`ls -lk logs/$newLogName 2>/dev/null | awk '{print $5}'`
     if [ $logSize -le $maxLogSize ]
     then
       echo -e $sumresult >>logs/$newLogName 
     else
     newLogNum=$(echo $newLogName | awk -F "_" '{print $3}')
     newLogNum=$(($newLogNum+1))
     newLogName="slave_${hostname}_${newLogNum}_`date "+%Y-%m-%d-%H:%M:%S"`"
     echo -e "PipelineLine Pipelinesize PipelineDate RedisTime(s) RedisStorageLine CrossCertifiedErrorLine\n$sumresult" > logs/$newLogName
     fi 
   ####log
fi

#echo $(hostname) >>dtmp
#t2=$((time -p (cat dtmp | nc -q 0 $masterhost $post)) 2>&1 | head -1 |awk '{print $2}')
#timeleft=`echo "$sleeptime-($t1+$t2)"| bc`
#sleep $timeleft
#done
./send.py $masterhost $post "$stopinfo"
#pkill killSend.sh >/dev/null 2>&1 # easily cause redis-server terminates unexpectedly
#lsof -Pnl +M -i4 | grep $stoppost | awk '{print $2}' | xargs kill >/dev/null 2>&1
#rm -rf dtmp

resultNum=0
abStarNum=0

eval $(cat < /tmp/slave.pipe | awk '{bp=$1;gp=$2;gt=$3;ra=$4;dec=$5;x=$6;y=$7} END{print "bernoulliParam="bp";geometricParam="gp";getTemplateFlag="gt";ra0="ra";dec0="dec";xi="x";yi="y}')

done
