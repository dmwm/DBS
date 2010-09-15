#!/bin/bash

#setup initial test counter to construct unique insert data names
export DBS_TEST_COUNTER=3
export DBS_TEST_CONFIG_READER=$DBS3_ROOT/Config/cms_dbs.py
export DBS_TEST_CONFIG_WRITER=$DBS3_ROOT/Config/cms_dbs_writer.py

export PYTHONPATH=$DBS3_SERVER_ROOT/tests:$PYTHONPATH
export DBS_TEST_ROOT=$DBS3_SERVER_ROOT/tests/dbsserver_t

#parse the config files to set correct database and dbowner variables
parser=$DBS_TEST_ROOT/utils/ParseConfig.py
export DBS_TEST_DBURL_READER=`python $parser $DBS_TEST_CONFIG_READER | head -1`
export DBS_TEST_DBOWNER_READER=`python $parser $DBS_TEST_CONFIG_READER | tail -1`
export DBS_TEST_DBURL_WRITER=`python $parser $DBS_TEST_CONFIG_WRITER | head -1`
export DBS_TEST_DBOWNER_WRITER=`python $parser $DBS_TEST_CONFIG_WRITER | tail -1`

dbstest(){

if [ -z "$1" ]
    then
	echo "Possible argumens: dao, business, web, all, or individual test files"
    else
	export DBS_TEST_COUNTER=`expr $DBS_TEST_COUNTER + 1`
	echo "DBS TEST COUNTER IS: $DBS_TEST_COUNTER"

	case "$1" in 
	"dao")
	    python $DBS_TEST_ROOT/unittests/dao_t/Oracle_t/__init__.py
	;;
	"business")
	    python $DBS_TEST_ROOT/unittests/business_t/__init__.py
	;;
	"web")
	    python $DBS_TEST_ROOT/unittests/web_t/__init__.py
	;;
	"all")
	    python $DBS_TEST_ROOT/unittests/dao_t/Oracle_t/__init__.py
	    python $DBS_TEST_ROOT/unittests/business_t/__init__.py
	    python $DBS_TEST_ROOT/unittests/web_t/__init__.py
	;;
	*)
	    python $1
	esac
fi
}


#standalone command line tool
alias dbs='python $DBS3_SERVER_ROOT/tests/dbsserver_t/utils/DBSRestApi.py '
