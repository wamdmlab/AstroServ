#!/bin/bash

HDFSPath="AstroDB_simulation_original_data"
dataBackupPath="data_backup"
username="wamdm"
echo -e "\nBackup last simulation data into HDFS... \n"

for host in `awk -F "-" '{print $1}' nodehostname`
do
{
 ssh $username@$host "source /etc/profile; cd gwac; hadoop fs -put $dataBackupPath/* /$HDFSPath; rm -rf $dataBackupPath/* " #"source" loads the environment value of a remote node.
 echo "$host has finished the data backup."
}&
done
wait
