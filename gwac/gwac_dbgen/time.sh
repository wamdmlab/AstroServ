#!/bin/bash
TIME_1=`date +%s`
echo $TIME_1
python pipeline.py
TIME_2=`date +%s`
echo $TIME_2
