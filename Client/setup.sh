#!/bin/bash
#export DBS3_ROOT=/uscms/home/yuyi/dbs3-test
export DBS3_ROOT=$PWD/../..
export PYTHON_HOME=$DBS3_ROOT/External/python/2.6.8-comp2
export PATH=$PYTHON_HOME/bin:$PATH
export DBS3_CLIENT=$DBS3_ROOT/DBS/Client
export DBS3_CLIENT_ROOT=$DBS3_ROOT/DBS/Client
export PYTHONPATH=$DBS3_CLIENT:$DBS3_CLIENT/src/python:$DBS3_ROOT/DBS/PycurlClient/src/python
export LD_LIBRARY_PATH=$PYTHON_HOME/lib:$DBS3_ROOT/External/curl/7.24.0-comp/lib
#need to be updated for your installation.
#for server running on localhost
#export DBS_READER_URL=http://cms-xen39.fnal.gov:8787/dbs/dev/global/DBSReader
#export DBS_WRITER_URL=http://cms-xen39.fnal.gov:8787/dbs/dev/global/DBSWriter
#export SOCKS5_PROXY=''

#for server running on remote host with front end
#export DBS_READER_URL=https://dbs3-dev01.cern.ch/dbs/int/global/DBSReader
#export DBS_WRITER_URL=https://dbs3-dev01.cern.ch/dbs/int/global/DBSWriter
#export SOCKS5_PROXY=socks5://localhost:5678
#alias dbs="python $PWD/cmdline/dbs.py"

export DBS_READER_URL=http://cms-xen39.fnal.gov:8787/dbs/dev/global/DBSReader
export DBS_WRITER_URL=http://cms-xen39.fnal.gov:8787/dbs/dev/global/DBSWriter
echo $DBS3_CLIENT
