#!/bin/sh
echo "begin migrating"
# exec 13<datasetlist.txt
declare -i num
let num=0
while read LINE 
      do
      let num=$num+1
      echo "****************$num: migrating $LINE****************************"
      START=$(date +%s)
      python dbs2Todbs3DatasetMigrate.py http://vocms09.cern.ch:8880/INT2RG/servlet/DBSServlet http://cmssrv48.fnal.gov:8585/DBS $LINE
      END=$(date +%s)
      DIFF=$(( $END - $START ))
      echo "********************It took $DIFF seconds ***********************"
      echo "done"
      echo 
done < $1
