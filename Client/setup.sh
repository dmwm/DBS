#!/bin/bash
export cjson=$DBS3_ROOT/External/python-cjson-1.0.5/build/lib.linux-x86_64-2.6/
export PYTHONPATH=$PWD:$PWD/src/python:$cjson:$PYTHONPATH
#export DBS_READER_URL=http://vocms09.cern.ch:8585/DBS
#export DBS_WRITER_URL=http://vocms09.cern.ch:8585/DBS
#export DBS_WRITER_URL=http://cmssrv48.fnal.gov:8687/DBS
#need to be updated for your installation.
export LD_LIBRARY_PATH=$PWD/../../../external/python/2.6.4-comp/lib:$LD_LIBRARY_PATH
export DBS_WRITER_URL=http://cms-xen40.fnal.gov/cms_dbs/DBS
export DBS_READER_URL=http://cms-xen40.fnal.gov/cms_dbs/DBS
alias dbs="python $PWD/cmdline/dbs.py"
