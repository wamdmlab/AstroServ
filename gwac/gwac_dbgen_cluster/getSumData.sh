#!/bin/bash
masterhost="wamdm100"
hostname=$(hostname)
echo "print $hostname" | nc -q 0 $masterhost 1984
tmp=`nc -lp 1985`
echo $tmp > stalog.txt
#echo "17000 33.6789 2016-11-17 12:12:12" >stalog.txt
echo "Saving is finished."
