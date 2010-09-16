
export DBS3_ROOT=/uscms/home/yuyi/dbs3-test
export DBS3_SERVER_ROOT=$DBS3_ROOT/DBS/DBS3/Server/Python

#python
export ORACLE_HOME=$DBS3_ROOT/External/Oracle/instantclient_11_1
export LD_LIBRARY_PATH=$ORACLE_HOME
export PATH=$ORACLE_HOME:$PATH

#WMCore Infrastructure
export WTBASE=$DBS3_ROOT/External/WMCORE/src
export WMCORE_BIN=$DBS3_ROOT/External/WMCORE/bin
export PYTHONPATH=$DBS3_SERVER_ROOT/src:$WTBASE/python:$DBS3_ROOT/External/CherryPy-3.1.2:$DBS3_ROOT/External/Cheetah-2.4.0/build/lib.linux-x86_64-2.6:$DBS3_ROOT/External/CherryPy-3.1.2/build/lib:$DBS3_ROOT/External/SQLAlchemy-0.5.8/build/lib:$DBS3_ROOT/External/python-cjson-1.0.5/build/lib.linux-x86_64-2.6:$DBS3_ROOT/External/cx_Oracle-5.0.2/build/lib.linux-x86_64-2.6-11g:$PYTHONPATH

#Config file 
export WMAGENT_CONFIG=$DBS3_ROOT/DBS/DBS3/Server/Python/control/cms_dbs_writer.py

#start all components as daemons
#$WMCORE_BIN/wmcoreD --start

#stop all components
#$WMCORE_BIN/wmcoreD --stop

#start DBS web services as an none-deamobn
dbs3_start1(){
if [ -z "$1" ]
   then
       $WTBASE/python/WMCore/WebTools/Root.py -i $DBS3_ROOT/Config/cms_dbs_writer.py
   else
       $WTBASE/python/WMCore/WebTools/Root.py -i $DBS3_ROOT/Config/$1.py
fi
}


dbs3start(){
if [ -z "$1" ]   
   then
       dbs3_start1 1>/dev/null 2>&1 &
   else
       dbs3_start1 $1 1>/dev/null 2>&1 &
fi
}

