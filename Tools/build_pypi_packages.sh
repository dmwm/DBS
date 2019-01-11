#!/bin/bash
#
# Script used to build each application from the DBS repo and upload to pypi
#

pycurl_client=false
dbs_client=false

TOBUILD=$1

case $TOBUILD in
    dbs-client)
      echo "Building dbs-client package"
      dbs_client=true
      ;;
    pycurl-client)
      echo "Building pycurl-client package"
      pycurl_client=true
      ;;
    all)
      echo "Building pycurl-client package"
      echo "Building dbs-client package"
      pycurl_client=true
      dbs_client=true
      ;;
    *)
      echo "Please enter one of the following arguments."
      echo "  all              Build all DBS packages"
      echo "  dbs-client       Build dbs-client package"
      echo "  pycurl-client    Build pycurl-client package"
      exit 1
esac

/bin/rm setup.py

if $dbs_client
  then
  /bin/cp setup_dbs_client.py setup.py
  python setup.py sdist upload
fi

if $pycurl_client
  then
  /bin/cp setup_pycurl_client.py setup.py
  python setup.py sdist upload
fi


