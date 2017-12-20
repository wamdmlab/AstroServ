#!/bin/bash

#paralleling the orders cannot write in sumLineAndDataSize.sh, becuase "wait" order will wait all child process of this father process. sumLineAndDataSize.sh has created some chlid processes to cause wait order to not exit
abhost=$1
order=$2
aborder=$3
username=$4
# in nodehostname, each line is hostname-ra0-dec0
for line in `cat nodehostname`
do
    ccdno=$(($ccdno+1))
    eval $(echo $line | awk -F "-" '{print "host="$1";ra0="$2";dec0="$3";xi="$4";yi="$5}')
	{
    sen=""
    if [ "$host" == "$abhost" ]
      then
        orderSen=$aborder  
      else
       orderSen=$order  
    fi
	for word in `echo $orderSen | awk -v co=$ccdno -v ra=$ra0 -v dec=$dec0 -v x=$xi -v y=$yi '{for(i=1;i<=NF;++i) if($i=="ccdno") print co; else if($i=="ra0") print ra; else if($i=="dec0") print dec; else if($i=="xi") print x; else if($i=="yi") print y; else print $i}'`
         do
            sen="$sen $word"
         done
		 
	  #echo "$username@$host \"$sen\""	 
	  ssh -f -n -t $username@$host "$sen"
    echo "Simulated data generator is running on $host." 
 }&   #& means mutil-process
done
wait

#
