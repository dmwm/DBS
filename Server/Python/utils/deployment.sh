#!/bin/bash

#all these parameters are used to generate correct config and setup script files
dburl='oracle://user:passwd@db'
dbowner='schemaowner'
config='intlyy'
access='access_intlyy.log'
error='error_intlyy.log'

export DBS3_ROOT=$PWD
export dlogs=$DBS3_ROOT/logs_deployment
mkdir -p $dlogs

mkdir -p $DBS3_ROOT/EXTERNAL


echo "Installing DBS3 Server Dependencies"
echo "This can take upto 15 minutes..."
echo ""

install_python(){
cd $DBS3_ROOT/EXTERNAL
wget http://www.python.org/ftp/python/2.6.4/Python-2.6.4.tgz  
tar xzvf Python-2.6.4.tgz 
mkdir -p python 
cd $DBS3_ROOT/EXTERNAL/Python-2.6.4
./configure --prefix $DBS3_ROOT/EXTERNAL/python
make install
cd $DBS3_ROOT/EXTERNAL
export PATH=$DBS3_ROOT/EXTERNAL/python/bin:$PATH
}
echo "Installing python 2.6.4"
install_python 1>$dlogs/python.log 2>&1


install_cherrypy(){
cd $DBS3_ROOT/EXTERNAL
wget http://download.cherrypy.org/cherrypy/3.1.2/CherryPy-3.1.2.tar.gz
tar -xzvf CherryPy-3.1.2.tar.gz
cd CherryPy-3.1.2
python setup.py install
cd $DBS3_ROOT/EXTERNAL
}
echo "Installing Cheerypy 3.1.2"
install_cherrypy 1>$dlogs/cherrypy.log 2>&1


install_cheetah(){
cd $DBS3_ROOT/EXTERNAL
wget http://pypi.python.org/packages/source/C/Cheetah/Cheetah-2.4.0.tar.gz#md5=873f5440676355512f176fc4ac01011e
tar xzvf Cheetah-2.4.0.tar.gz
cd Cheetah-2.4.0
python setup.py install
cd $DBS3_ROOT/EXTERNAL
}
echo "Installing Cheetah 2.4.0"
install_cheetah 1>$dlogs/cheetah.log 2>&1


install_sqlalchemy(){
cd $DBS3_ROOT/EXTERNAL
wget http://prdownloads.sourceforge.net/sqlalchemy/SQLAlchemy-0.5.6.tar.gz?download
tar xzvf SQLAlchemy-0.5.6.tar.gz
cd SQLAlchemy-0.5.6
python setup.py install
cd $DBS3_ROOT/EXTERNAL
}
echo "Installing sqlalchemy 0.5.6"
install_sqlalchemy 1>$dlogs/sqlalchemy.log 2>&1

install_openid(){
cd $DBS3_ROOT/EXTERNAL
wget http://openidenabled.com/files/python-openid/packages/python-openid-2.2.4.tar.gz
tar xzvf python-openid-2.2.4.tar.gz
cd python-openid-2.2.4
python setup.py install
cd $DBS3_ROOT/EXTERNAL
}
echo "Installing openid 2.2.4"
install_openid > $dlogs/openid.log



install_cjson(){
cd $DBS3_ROOT/EXTERNAL
wget http://pypi.python.org/packages/source/p/python-cjson/python-cjson-1.0.5.tar.gz#md5=4d55b66ecdf0300313af9d030d9644a3
tar xzvf python-cjson-1.0.5.tar.gz
cd python-cjson-1.0.5
python setup.py install
cd $DBS3_ROOT/EXTERNAL
}
echo "Installing cjson 1.0.5"
install_cjson 1>$dlogs/cjson.log 2>&1


install_oracleclient(){
cd $DBS3_ROOT/EXTERNAL
wget http://lepp.cornell.edu/~ak427/files/basic-11.1.0.70-linux-x86_64.zip
wget http://lepp.cornell.edu/~ak427/files/jdbc-11.1.0.7.0-linux-x86_64.zip
wget http://lepp.cornell.edu/~ak427/files/sdk-11.1.0.7.0-linux-x86_64.zip
wget http://lepp.cornell.edu/~ak427/files/sqlplus-11.1.0.7.0-linux-x86_64.zip
mkdir Oracle
mv basic-11.1.0.70-linux-x86_64.zip jdbc-11.1.0.7.0-linux-x86_64.zip sdk-11.1.0.7.0-linux-x86_64.zip sqlplus-11.1.0.7.0-linux-x86_64.zip Oracle/.
cd Oracle
unzip basic-11.1.0.70-linux-x86_64.zip
unzip jdbc-11.1.0.7.0-linux-x86_64.zip
unzip sdk-11.1.0.7.0-linux-x86_64.zip
unzip sqlplus-11.1.0.7.0-linux-x86_64.zip
cd instantclient_11_1/
export ORACLE_HOME=$PWD
export LD_LIBRARY_PATH=$ORACLE_HOME
ln -s libclntsh.so.11.1 libclntsh.so
ln -s libocci.so.11.1 libocci.so
cd $DBS3_ROOT/EXTERNAL
}
echo "Installing Oracle client"
install_oracleclient 1>$dlogs/oracleclient.log 2>&1


install_cxoracle(){
cd $DBS3_ROOT/EXTERNAL
wget http://prdownloads.sourceforge.net/cx-oracle/cx_Oracle-5.0.2.tar.gz?download
tar xzvf cx_Oracle-5.0.2.tar.gz
cd cx_Oracle-5.0.2
python setup.py build
python setup.py install
cd $DBS3_ROOT/EXTERNAL
}
echo "Installing cx_Oracle"
install_cxoracle 1>$dlogs/cxoracle.log 2>&1

install_wmcore(){
cd $DBS3_ROOT/EXTERNAL
wget http://lepp.cornell.edu/~ak427/files/WMCORE.tar.gz
tar xzvf WMCORE.tar.gz
cd $DBS3_ROOT/EXTERNAL
}
echo "Installing WMCore Modules"
install_wmcore 1>$dlogs/wmcore.log 2>&1

install_dbs(){
cd $DBS3_ROOT
export CVSROOT=:pserver:anonymous@cmscvs.cern.ch:/cvs_server/repositories/CMSSW
export CVS_RSH=ssh
cvs -d `echo $CVSROOT | awk -F@ '{print $1":98passwd\@"$2}'` login
cvs co DBS/DBS3
}
echo "Installing DBS3 Modules"
install_dbs 1>$dlogs/dbs.log 2>&1


gen_setup(){
cd $DBS3_ROOT
cat > setup.sh  << EOA

export DBS3_ROOT=`pwd`
export DBS3_SERVER_ROOT=\$DBS3_ROOT/DBS/DBS3/Server/Python

#python
export ORACLE_HOME=\$DBS3_ROOT/EXTERNAL/Oracle/instantclient_11_1
export LD_LIBRARY_PATH=\$ORACLE_HOME
export PATH=\$DBS3_ROOT/EXTERNAL/python/bin:\$ORACLE_HOME:\$PATH

#WMCore Infrastructure
export WTBASE=\$DBS3_ROOT/EXTERNAL/WMCORE/src
export PYTHONPATH=\$DBS3_SERVER_ROOT/src:\$WTBASE/python:\$PYTHONPATH


dbs3_start1(){
if [ -z "\$1" ]
   then
       \$WTBASE/python/WMCore/WebTools/Root.py -i \$DBS3_ROOT/CONFIG/$config.py
   else
       \$WTBASE/python/WMCore/WebTools/Root.py -i \$DBS3_ROOT/CONFIG/\$1.py
fi
}


dbs3start(){
if [ -z "\$1" ]                           # Is parameter #1 zero length? or no parameter passed
   then
       dbs3_start1 1>/dev/null 2>&1 &
   else
       dbs3_start1 \$1 1>/dev/null 2>&1 &
fi
}

alias dbs3='python \$DBS3_SERVER_ROOT/tests/DBS3SimpleClient.py '

EOA

mkdir CONFIG
cat > CONFIG/$config.py << EOA
"""
DBS Server  configuration file
"""
import os, logging
from WMCore.Configuration import Configuration

config = Configuration()

config.component_('Webtools')
config.Webtools.port = 8585
config.Webtools.host = '::'
config.Webtools.access_log_file = os.environ['DBS3_ROOT'] +"/LOGS/$access"
config.Webtools.error_log_file = os.environ['DBS3_ROOT'] +"/LOGS/$error"
config.Webtools.log_screen = True
config.Webtools.application = 'DBSServer'

config.component_('DBSServer')
config.DBSServer.templates = os.environ['WTBASE'] + '/templates/WMCore/WebTools'
config.DBSServer.title = 'DBS Server'
config.DBSServer.description = 'CMS DBS Service'

config.DBSServer.section_('views')

active=config.DBSServer.views.section_('active')
active.section_('dbs3')
active.dbs3.object = 'WMCore.WebTools.RESTApi'
active.dbs3.section_('model')
active.dbs3.model.object = 'dbs.web.DBSReaderModel'
active.dbs3.section_('formatter')
active.dbs3.formatter.object = 'WMCore.WebTools.RESTFormatter'

active.dbs3.database = '$dburl'
active.dbs3.dbowner = '$dbowner'
EOA

mkdir LOGS
}
gen_setup 

echo "Deployment successfully finished."

echo "see setup.sh file to start the server..."

