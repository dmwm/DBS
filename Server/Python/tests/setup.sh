#!/bin/bash

[ -z "$DBS3_ROOT" ] && {
        echo "DBS3_ROOT not set"
        exit 1
}

#setup initial test counter to construct unique insert data names
export DBS_TEST_COUNTER=3
export DBS_TEST_CONFIG_READER=$DBS3_ROOT/Config/${1:-cms_dbs}.py
export DBS_TEST_CONFIG_WRITER=$DBS3_ROOT/Config/${1:-cms_dbs}_writer.py
#choose the service name from the configuration files.. In reader and writer it has to be the same
export DBS_TEST_SERVICE="DBS"

echo ${1}
[ -f $DBS_TEST_CONFIG_READER ] || {
        echo "Reader config $DBS_TEST_CONFIG_READER not found"
        exit 1
}
[ -f $DBS_TEST_CONFIG_WRITER ] || {
        echo "Writer config $DBS_TEST_CONFIG_WRITER not found"
        exit 1
}

export PYTHONPATH=$DBS3_SERVER_ROOT/tests:$PYTHONPATH
export DBS_TEST_ROOT=$DBS3_SERVER_ROOT/tests/dbsserver_t

#parse the config files to set correct database and dbowner variables
parser=$DBS_TEST_ROOT/utils/ParseConfig.py
export DBS_TEST_DBURL_READER=`python $parser $DBS_TEST_CONFIG_READER database`
export DBS_TEST_DBOWNER_READER=`python $parser $DBS_TEST_CONFIG_READER dbowner`
export DBS_TEST_DBURL_WRITER=`python $parser $DBS_TEST_CONFIG_WRITER database`
export DBS_TEST_DBOWNER_WRITER=`python $parser $DBS_TEST_CONFIG_WRITER dbowner`

dbstest(){

if [ -z "$1" ]
    then
	echo "Possible arguments: dao, business, web, all, or individual test files"
    else
	export DBS_TEST_COUNTER=$(( $DBS_TEST_COUNTER + 1 ))
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
	"migration")
	    python $DBS_TEST_ROOT/unittests/migration_t/__init__.py
	;;
	"all")
	    python $DBS_TEST_ROOT/unittests/dao_t/Oracle_t/__init__.py
	    python $DBS_TEST_ROOT/unittests/business_t/__init__.py
	    python $DBS_TEST_ROOT/unittests/web_t/__init__.py
	    python $DBS_TEST_ROOT/unittests/migration_t/__init__.py
	;;
	*)
	    python $1
	esac
fi
}


#standalone command line tool
alias dbst='python $DBS3_SERVER_ROOT/tests/dbsserver_t/utils/DBSRestApi.py '
