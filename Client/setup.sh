#!/bin/bash
#export cjson=$DBS3_ROOT/External/python-cjson-1.0.5/build/lib.linux-x86_64-2.6/
export cjson=/uscms/home/anzar/devDBS3/external/python-cjson-1.0.5/build/lib.linux-i686-2.6/
export PYTHONPATH=$PWD/src/python:$cjson:$PYTHONPATH
export DBS_READER_URL=http://vocms09.cern.ch:8585/DBS
export DBS_WRITER_URL=http://vocms09.cern.ch:8585/DBS
alias dbs="python $PWD/cmdline/dbs.py"
