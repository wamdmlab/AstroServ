#!/bin/bash

masterhost="wamdm100"
stoppostMC=1987
hostname=$(hostname)
lsof -Pnl +M -i4 | grep $stoppostMC | awk '{print $2}' | xargs kill >/dev/null 2>&1
if [ "$1" == "force" ]
then
nohup >/dev/null 2>&1 ./listenStop.py $hostname $stoppostMC 2>&1 &
#pid=`lsof -Pnl +M -i4 | grep $stopPost | awk '{print $2}'`
#if [ "$pid" == "" ]
#then
# echo "run \" nohup ./listenStop.sh & \" first."
# exit 0
#fi
sleep 0.2
echo "force $hostname" | nc -q 0 $masterhost 1984
:>listen_stop.txt
#./send.py $masterhost 1984 "stop! $hostname"
#nohup nc -lk $stopPost >>listen_stop.txt 2>&1 &
#./printStopInfo.sh 
#./listenStop.sh &
tmp=""
i=1
until [ "$tmp" == "finished" ]
do
tmp=`sed -n ${i}p listen_stop.txt`
#echo $i
#sleep 1
if [ "$tmp" == "" ]
then
 continue
fi
echo $tmp
i=$(($i+1))
done
#echo $tmp
pid=`lsof -Pnl +M -i4 | grep $stoppostMC | awk '{print $2}'`
while [ "$pid" != "" ]
do
#echo "!!!"
kill  $pid
wait $pid 2>/dev/null #suppress Terminated message
pid=`lsof -Pnl +M -i4 | grep $stoppostMC | awk '{print $2}'`
done 
exit 0
fi

if [ "$1" == "stop!" ]
then
echo "stop! $hostname" | nc -q 0 $masterhost 1984
echo "waiting..."
tmp=`nc -lp $stoppostMC`
echo $tmp
exit 0 
fi

echo "Usage: $0 [stop! | force ]"
echo "stop!                         -- Stop cluster next time in a normal way."
echo "force                         -- Forced to stop cluster now.."
