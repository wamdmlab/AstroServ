#!/bin/bash

fpid=$1
masterhost=$2
stoppost=1986
hostname=$(hostname)
ps ax | grep [.]/sendStaDataPerNode.sh | awk '{print $1}' | while read pid
do
  if [ $fpid -ne $pid ]
    then
        kill $pid
    fi
done
#lsof -Pnl +M -i4 | grep $stoppost | awk '{print $2}' | xargs kill >/dev/null 2>&1
tmp=`nc -lp $stoppost`
#echo "$tmp" >>1.txt
if [ "$tmp" == "force" ]
then
spid=`ps -ax | awk '{ print $1 }' | grep $fpid`
 while [ "$fpid" == "$spid" ]
   do  
     kill -9 $fpid
     spid=`ps -ax | awk '{ print $1 }' | grep $fpid`
   done

      pgrep "astroDB_cache" | while read Spid
         do
            kill $Spid
         done    
    
 echo "$hostname has stopped!" | ./send.py $masterhost $stoppost
exit 0
elif [ "$tmp" == "kill" ]
then
    echo "0 0 slave_exit 0 0" > /tmp/slave.pipe
    echo "Squirrel_exit" > /tmp/Squirrel_pipe_test
exit 0
elif [ "$tmp" == "exit" ]
then 
exit 0
else
echo "$hostname's \"force\" is error, and the real message is $tmp." | ./send.py $masterhost $stoppost
fi
