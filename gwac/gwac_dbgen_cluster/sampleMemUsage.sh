#!/bin/bash

hostname=`hostname`
free -k | awk 'FNR==3{print $3}' >> ${hostname}_memUsage.txt
