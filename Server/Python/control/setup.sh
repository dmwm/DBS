if [ -z "$DBS3_ROOT" ]; then
    export DBS3_ROOT=/uscms/home/yuyi/dbs3-test
    export WTBASE=$DBS3_ROOT/External/WMCORE/src
    export WMCORE_BIN=$DBS3_ROOT/External/WMCORE/bin
    export DBS3_SERVER_ROOT=$DBS3_ROOT/DBS/Server/Python
    export ORACLE_HOME=$DBS3_ROOT/External/Oracle/instantclient_11_1
    export PYTHON_HOME=$DBS3_ROOT/External/python/2.6.8-comp2
    export LD_LIBRARY_PATH=$ORACLE_HOME:$PYTHON_HOME/lib:$DBS3_ROOT/External/zeromq/2.1.7/lib\
:$DBS3_ROOT/External/curl/7.24.0-comp/lib
    export PATH=$ORACLE_HOME:$PYTHON_HOME/bin:$PATH
    export PYTHONPATH=$DBS3_ROOT/DBS/PycurlClient/src/python\
:$DBS3_ROOT/DBS/Server/Python/control\
:$DBS3_ROOT/DBS/Server/Python/src\
:$WTBASE/python\
:$DBS3_ROOT/External/py2-zmq/2.1.7-comp/lib/python2.6/site-packages\
:$DBS3_ROOT/External/zeromq/2.1.7/lib\
:$DBS3_ROOT/External/CherryPy-3.1.2\
:$DBS3_ROOT/External/Cheetah-2.4.0/build/lib.linux-x86_64-2.6\
:$DBS3_ROOT/External/CherryPy-3.1.2/build/lib\
:$DBS3_ROOT/External/SQLAlchemy-0.5.8/build/lib\
:$DBS3_ROOT/External/python-cjson-1.0.5/build/lib.linux-x86_64-2.6\
:$DBS3_ROOT/External/cx_Oracle-5.0.2/build/lib.linux-x86_64-2.6-11g

    export WMAGENT_CONFIG=$DBS3_ROOT/DBS/Server/Python/control/cms_dbs_writer.py    
    #export X509_CERT_DIR=/etc/grid-security/certificates
    export SOCKS5_PROXY=socks5://localhost:5678
else
    export DBS3_ROOT=$DBS3_SERVER_ROOT/../../
    export WTBASE=$WMCORE_ROOT/src
    export WMCORE_BIN=$WMCORE_ROOT/bin
    export WMAGENT_CONFIG=$DBS3_ROOT/Server/Python/control/cms_dbs_writer.py
fi

echo $DBS3_ROOT

#start all components as daemons
#$WMCORE_BIN/wmcoreD --start

#stop all components
#$WMCORE_BIN/wmcoreD --shutdown

#start DBS web services as an none-deamobn
dbs3_start1(){
if [ -z "$1" ]
   then
#When running server interactively, one needs to use a different config file.
       $WTBASE/python/WMCore/WebTools/Root.py -i $DBS3_ROOT/DBS/Server/Python/control/DBSConfig.py
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

