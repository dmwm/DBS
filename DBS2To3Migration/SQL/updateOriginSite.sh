#! /bin/sh
#
# This script is for copy DBS2 SENAME  into DBS3 Origin site namie.
# You need to run this script on lxplus so the oracle env is setup and minimize 
# the network connection. 
# Here is how you should run it:
#
#       nohup ./updateOriginSite.sh >updateOriginSite.log 2>&1 &
#
# Y. Guo Feb.5 , 2014
#
#

sqlplus acct@cms_dbs/pd @updateOriginSite.sql
exit
