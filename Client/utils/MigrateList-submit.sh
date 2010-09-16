#!/bin/sh
IFS=,
list="6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30"
for num in $list 
do
     echo $num
     bash MigrateList-countTime.sh data/rawDatasetList"$num".txt > log14-4/rawDatasetList"$num".log 2>&1 &
done
