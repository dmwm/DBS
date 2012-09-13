#!/bin/bash
export cjson=$DBS3_ROOT/External/python-cjson-1.0.5/build/lib.linux-x86_64-2.6/
export PYTHONPATH=$PWD:$PWD/src/python:$cjson:$PYTHONPATH
#need to be updated for your installation.
export LD_LIBRARY_PATH=$PWD/../../../external/python/2.6.4-comp/lib:$LD_LIBRARY_PATH
export DBS_READER_URL=http://cms-xen39.fnal.gov:8787/dbs/int/global/DBSReader
export DBS_WRITER_URL=http://cms-xen39.fnal.gov:8787/dbs/int/global/DBSWriter
alias dbs="python $PWD/cmdline/dbs.py"
