#/bin/bash

rm -rf logs/*
echo "clear $(hostname)'s logs"
username=$(whoami)
for line in `cat nodehostname | awk -F "-" '{print $1}'`
do
{
ssh $username@$line "rm -rf gwac/gwac_dbgen_cluster/logs/*"
echo "clear $line's logs"
} &

done
wait
