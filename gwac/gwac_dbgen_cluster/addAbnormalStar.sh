#!/bin/bash

sumport=1984
masterhost="wamdm100"
abhost=$1
bernoulliParam=$2
geometricParam=$3

echo "abnormal $abhost $bernoulliParam $geometricParam" | nc -q 0 $masterhost 1984
echo "finished."
exit 0
