#!/bin/bash

# CVS tags for various involved modules
wmcore_tag=DBS_3_S3_0
dbs3_tag=DBS_3_S3_0_pre2

#Configure
#all these parameters are used to generate default config and setup script files
dburl='oracle://account-name:pd@dbname'
dbowner='owner'
service='DBS'
instance='cms_dbs'
version='DBS_3_0_0'

externaldir='External'
configdir='Config'
serverlogsdir='Logs'

DBS3_ROOT=$PWD
dExternal=$DBS3_ROOT/$externaldir
dConfig=$DBS3_ROOT/$configdir
dServerLogs=$DBS3_ROOT/$serverlogsdir

dlogs=$DBS3_ROOT/logs_deployment
mkdir -p $dlogs

##### Start Deployment

mkdir -p $dExternal
mkdir -p $dConfig
mkdir -p $dServerLogs

echo "Installing DBS3 Server Dependencies"
echo "This can take upto 15 minutes..."
echo ""

##this python has problem with sqlalchemy
#install_python(){
#cd $dExternal
#wget http://www.python.org/ftp/python/2.6.4/Python-2.6.4.tgz
#tar xzvf Python-2.6.4.tgz
#mkdir -p python
#cd $dExternal/Python-2.6.4
#./configure --prefix $dExternal/python
#make install
#cd $dExternal
#export PATH=$dExternal/python/bin:$PATH
#}
#echo "Installing python 2.6.4"
#install_python 1>$dlogs/python.log 2>&1

install_cherrypy(){
cd $dExternal
wget http://download.cherrypy.org/cherrypy/3.1.2/CherryPy-3.1.2.tar.gz
tar -xzvf CherryPy-3.1.2.tar.gz
cd CherryPy-3.1.2
#python setup.py install
python setup.py build
cd $dExternal
}
echo "Building Cheerypy 3.1.2"
install_cherrypy 1>$dlogs/cherrypy.log 2>&1

install_cheetah(){
cd $dExternal
wget http://pypi.python.org/packages/source/C/Cheetah/Cheetah-2.4.0.tar.gz#md5=873f5440676355512f176fc4ac01011e
tar xzvf Cheetah-2.4.0.tar.gz
cd Cheetah-2.4.0
#python setup.py install
python setup.py build
cd $dExternal
}
echo "Building Cheetah 2.4.0"
install_cheetah 1>$dlogs/cheetah.log 2>&1

install_sqlalchemy(){
cd $dExternal
wget http://prdownloads.sourceforge.net/sqlalchemy/SQLAlchemy-0.5.8.tar.gz?download
tar xzvf SQLAlchemy-0.5.8.tar.gz
cd SQLAlchemy-0.5.8
#python setup.py install
python setup.py build
cd $dExternal
}
echo "building sqlalchemy 0.5.8"
install_sqlalchemy 1>$dlogs/sqlalchemy.log 2>&1

install_openid(){
cd $dExternal
wget http://openidenabled.com/files/python-openid/packages/python-openid-2.2.4.tar.gz
tar xzvf python-openid-2.2.4.tar.gz
cd python-openid-2.2.4
#python setup.py install
python setup.py build
cd $dExternal
}
echo "Building openid 2.2.4"
install_openid 1>$dlogs/openid.log 2>&1



install_cjson(){
cd $dExternal
wget http://pypi.python.org/packages/source/p/python-cjson/python-cjson-1.0.5.tar.gz#md5=4d55b66ecdf0300313af9d030d9644a3
tar xzvf python-cjson-1.0.5.tar.gz
cd python-cjson-1.0.5
#python setup.py install
python setup.py build
cd $dExternal
}
echo "Building cjson 1.0.5"
install_cjson 1>$dlogs/cjson.log 2>&1

install_oracleclient(){
cd $dExternal
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
mkdir network
mkdir network/admin
cd network/admin
wget http://cmsdoc.cern.ch/cms/cpt/Software/download/cms/SOURCES/cms/oracle-env/19.0/tnsnames.ora
cd $dExternal
}
echo "Installing Oracle client"
install_oracleclient 1>$dlogs/oracleclient.log 2>&1


install_cxoracle(){
cd $dExternal
wget http://prdownloads.sourceforge.net/cx-oracle/cx_Oracle-5.0.2.tar.gz?download
tar xzvf cx_Oracle-5.0.2.tar.gz
cd cx_Oracle-5.0.2
python setup.py build
python setup.py install
cd $dExternal
}
echo "Installing cx_Oracle"
install_cxoracle 1>$dlogs/cxoracle.log 2>&1

install_wmcore(){
cd $dExternal
#wget http://lepp.cornell.edu/~ak427/files/WMCORE.tar.gz
#tar xzvf WMCORE.tar.gz
export CVSROOT=:pserver:anonymous@cmscvs.cern.ch:/cvs_server/repositories/CMSSW
export CVS_RSH=ssh
cvs -d `echo $CVSROOT | awk -F@ '{print $1":98passwd\@"$2}'` login
cvs co WMCORE/src/python/WMCore/Database
cvs co WMCORE/src/python/WMCore/HTTPFrontEnd
cvs co WMCORE/src/python/WMCore/WebTools
cvs co WMCORE/src/python/WMCore/DataStructs
cvs co WMCORE/src/python/WMCore/WMLogging.py
cvs co WMCORE/src/python/WMCore/WMFactory.py
cvs co WMCORE/src/python/WMCore/WMException.py
cvs co WMCORE/src/python/WMCore/Configuration.py
cvs co WMCORE/src/python/WMCore/DAOFactory.py
cvs co WMCORE/src/python/WMCore/Lexicon.py
cvs co WMCORE/src/python/WMCore/WMExceptions.py
cvs co WMCORE/src/templates/WMCore/WebTools
cvs co WMCORE/src/templates/WMCore/__init__.py
cvs co -r $wmcore_tag WMCORE
cd $dExternal
}
echo "Installing WMCore Modules"
install_wmcore 1>$dlogs/wmcore.log 2>&1

#don't need this because we already checked out dbs to get this script.
#install_dbs(){
#cd $DBS3_ROOT
#export CVSROOT=:pserver:anonymous@cmscvs.cern.ch:/cvs_server/repositories/CMSSW
#export CVS_RSH=ssh
#cvs -d `echo $CVSROOT | awk -F@ '{print $1":98passwd\@"$2}'` login
#cvs co -r $dbs3_tag DBS/DBS3
#}
#echo "Installing DBS3 Modules"
#install_dbs 1>$dlogs/dbs.log 2>&1


gen_setup(){
cd $DBS3_ROOT
cat > setup.sh  << EOA

export DBS3_ROOT=`pwd`
export DBS3_SERVER_ROOT=\$DBS3_ROOT/DBS/DBS3/Server/Python

#python
export ORACLE_HOME=\$DBS3_ROOT/$externaldir/Oracle/instantclient_11_1
export LD_LIBRARY_PATH=\$ORACLE_HOME
export PATH=\$ORACLE_HOME:\$PATH

#WMCore Infrastructure
export WTBASE=\$DBS3_ROOT/$externaldir/WMCORE/src
export PYTHONPATH=\$DBS3_SERVER_ROOT/src:\$WTBASE/python:\$DBS3_ROOT/$externaldir/CherryPy-3.1.2\
:\$DBS3_ROOT/$externaldir/Cheetah-2.4.0/build/lib.linux-x86_64-2.6\
:\$DBS3_ROOT/$externaldir/CherryPy-3.1.2/build/lib\
:\$DBS3_ROOT/$externaldir/SQLAlchemy-0.5.8/build/lib\
:\$DBS3_ROOT/$externaldir/python-cjson-1.0.5/build/lib.linux-x86_64-2.6\
:\$DBS3_ROOT/$externaldir/cx_Oracle-5.0.2/build/lib.linux-x86_64-2.6-11g\
:\$PYTHONPATH


dbs3_start1(){
if [ -z "\$1" ]
   then
       \$WTBASE/python/WMCore/WebTools/Root.py -i \$DBS3_ROOT/$configdir/$instance.py
   else
       \$WTBASE/python/WMCore/WebTools/Root.py -i \$DBS3_ROOT/$configdir/\$1.py
fi
}


dbs3start(){
if [ -z "\$1" ]   
   then
       dbs3_start1 1>/dev/null 2>&1 &
   else
       dbs3_start1 \$1 1>/dev/null 2>&1 &
fi
}

EOA

cat > $dConfig/$instance.py << EOA
"""
DBS Server  configuration file
"""
import os, logging
from WMCore.Configuration import Configuration

config = Configuration()

config.component_('Webtools')
config.Webtools.port = 8585
config.Webtools.host = '::'
config.Webtools.access_log_file = os.environ['DBS3_ROOT'] +"/$serverlogsdir/$instance.log"
config.Webtools.error_log_file = os.environ['DBS3_ROOT'] +"/$serverlogsdir/$instance.log"
config.Webtools.log_screen = True
config.Webtools.application = '$instance'

config.component_('$instance')
config.$instance.templates = os.environ['WTBASE'] + '/templates/WMCore/WebTools'
config.$instance.title = 'DBS Server'
config.$instance.description = 'CMS DBS Service'
config.$instance.admin='cmsdbs'

config.$instance.section_('views')

active=config.$instance.views.section_('active')
active.section_('$service')
active.$service.object = 'WMCore.WebTools.RESTApi'
active.$service.section_('model')
active.$service.model.object = 'dbs.web.DBSReaderModel'
active.$service.section_('formatter')
active.$service.formatter.object = 'WMCore.WebTools.RESTFormatter'

active.$service.dbowner = '$dbowner'
active.$service.version = '$version'
active.$service.section_('database')
active.$service.database.connectUrl = '$dburl'
active.$service.database.engineParameters = {'pool_size': 15, 'max_overflow': 10, 'pool_timeout' : 200 }
EOA

cat > $dConfig/${instance}_writer.py << EOA
"""
DBS Server  configuration file
"""
import os, logging
from WMCore.Configuration import Configuration

config = Configuration()

config.component_('Webtools')
config.Webtools.port = 8585
config.Webtools.host = '::'
config.Webtools.access_log_file = os.environ['DBS3_ROOT'] +"/$serverlogsdir/${instance}_writer.log"
config.Webtools.error_log_file = os.environ['DBS3_ROOT'] +"/$serverlogsdir/${instance}_writer.log"
config.Webtools.log_screen = True
config.Webtools.application = '$instance'

config.component_('$instance')
config.$instance.templates = os.environ['WTBASE'] + '/templates/WMCore/WebTools'
config.$instance.title = 'DBS Server'
config.$instance.description = 'CMS DBS Service'
config.$instance.section_('views')
config.$instance.admin='cmsdbs'

active=config.$instance.views.section_('active')
active.section_('$service')
active.$service.object = 'WMCore.WebTools.RESTApi'
active.$service.section_('model')
active.$service.model.object = 'dbs.web.DBSWriterModel'
active.$service.section_('formatter')
active.$service.formatter.object = 'WMCore.WebTools.RESTFormatter'

active.$service.dbowner = '$dbowner'
active.$service.version = '$version'
active.$service.section_('database')
active.$service.database.connectUrl = '$dburl'
active.$service.database.engineParameters = {'pool_size': 15, 'max_overflow': 10, 'pool_timeout' : 200 }
EOA

}
gen_setup

echo "Deployment successfully finished."

echo "see setup.sh file to start the server..."
                                                                      
