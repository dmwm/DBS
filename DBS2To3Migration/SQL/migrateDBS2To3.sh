#! /bin/sh
#
# This script is for copying DBS2 data into DBS3 schema.
# You need to run this script on lxplus so the oracle env is setup and minimize 
# the network connection. 
# Here is how you should run it:
#
#       nohup ./migrateDBS2To3.sh >migrateDBS2To3.log 2>&1 &
#
# Y. Guo Apiel 11, 2013
#
#

sqlplus cms_dbs3_prod_owner@cms_dbs/dbs3_prod_pd @MIGRATION.sql
exit
