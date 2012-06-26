#!/bin/bash

WORKINGDIR=/tmp/$USER/DBS3_Life_Cycle_Agent_Test
SCRAM_ARCH=slc5_amd64_gcc461
SWAREA=$WORKINGDIR/sw
REPO=comp.pre

GRIDENVSCRIPT=/afs/cern.ch/cms/LCG/LCG-2/UI/cms_ui_env.sh

DBS3CLIENTVERSION=3.0.18
DBS3CLIENT=cms+dbs3-client+$DBS3CLIENTVERSION
DBS3CLIENTDOC=cms+dbs3-client-webdoc+$DBS3CLIENTVERSION

DATAPROVIDERVERSION=1.0.2
DATAPROVIDER=cms+lifecycle-dataprovider+$DATAPROVIDERVERSION

LIFECYCLEAGENTVERSION=1.0.4
LIFECYCLEAGENT=cms+PHEDEX-lifecycle+$LIFECYCLEAGENTVERSION

PRIVATEDIR=$HOME/private

GITZIPBALL=https://nodeload.github.com/giffels/LifeCycleTests/zipball/mydev-branch

check_success() 
{
  if [ $# -ne 2 ]; then
    echo "check_success expects exact two parameters."
    exit 1
  fi

  local step=$1
  local exit_code=$2

  if [ $exit_code -ne 0 ]; then
    echo "$step was not successful"
    exit $exit_code
  fi
}

check_x509_proxy()
{
  if [ -e $GRIDENVSCRIPT ]; then
    source $GRIDENVSCRIPT
    voms-proxy-info --exists &> /dev/null

    check_success "Checking for valid x509 proxy" $?

  else
    echo "Grid environment cannot be set-up. $GRIDENVSCRIPT is missing."
    exit 2

  fi
}

copy_x509_proxy()
{
  local proxy_file=x509up_u$(id -u)
  if [ -f $PRIVATEDIR/$proxy_file ]; then
    cp -a $PRIVATEDIR/$proxy_file /tmp/$proxy_file
    check_success "Copying X509 Proxy failed" $?
  fi
}

cleanup_workingdir()
{
  ## Clean up pre-exists workdir
  local proxy_file=x509up_u$(id -u)

  if [ -d $WORKINGDIR ]; then
    echo "$WORKINGDIR already exists. Cleaning up ..."
    rm -rf $WORKINGDIR
    check_success "Cleaning up $WORKDIR" $?
    rm -rf /tmp/$proxy_file
    check_success "Cleaning up /tmp/$proxy_file" $?
  fi
}

prepare_bootstrap()
{
  ## prepare bootstrapping
  mkdir -p $SWAREA
  wget -O $SWAREA/bootstrap.sh http://cmsrep.cern.ch/cmssw/cms/bootstrap.sh
  check_success "Preparing bootstrapping" $?
}

bootstrapping()
{
  ## bootstrapping
  chmod +x $SWAREA/bootstrap.sh
  sh -x $SWAREA/bootstrap.sh setup -repository $REPO -path $SWAREA -arch $SCRAM_ARCH >& $SWAREA/bootstrap_$SCRAM_ARCH.log
  check_success "Bootstrapping" $?
}

install_software()
{
  cleanup_workingdir
  prepare_bootstrap
  bootstrapping

  ## software installation 
  source $SWAREA/$SCRAM_ARCH/external/apt/*/etc/profile.d/init.sh
  apt-get update -y
  apt-get install $DBS3CLIENT $DBS3CLIENTDOC $DATAPROVIDER $LIFECYCLEAGENT -y
  check_success "Install $DBS3CLIENT, $DBS3CLIENTDOC, $DATAPROVIDER and $LIFECYCLEAGENT" $?
}

setup_dbs_client()
{
  if [ ! -d $SWAREA ]; then
    echo "Software area not found in $SWAREA. Installing software ..."
    install_software
  fi

  ## setup dbs_client
  source $SWAREA/$SCRAM_ARCH/cms/dbs3-client/$DBS3CLIENTVERSION/etc/profile.d/init.sh
  export DBS_READER_URL=https://cmsweb-testbed.cern.ch/dbs/prod/global/DBSReader
  export DBS_WRITER_URL=https://cmsweb-testbed.cern.ch/dbs/prod/global/DBSWriter
}

setup_lifecycle_agent()
{
  if [ ! -d $SWAREA ]; then
    echo "Software area not found in $SWAREA. Installing software ..."
    install_software
  fi

  source $SWAREA/$SCRAM_ARCH/cms/PHEDEX-lifecycle/$LIFECYCLEAGENTVERSION/etc/profile.d/init.sh
}

setup_dbs_lifecycle_tests()
{
  cd $WORKDIR
  wget $GITZIPBALL-O LifeCycleTests.zip
  unzip LifeCycleTests.zip
  cd giffels-LifeCycleTests*
  python setup.py install_system -s LifeCycleTests --prefix=$WORKDIR/LifeCycleTests
  cd $WORKDIR/LifeCycleTests
  export PYTHONPATH=$WORKDIR/LifeCycleTests/lib/python2.6/site-packages:$PYTHONPATH
}

echo "Starting time: $(date)"
echo "Running on $(/bin/hostname)"
echo "Having following processors:"
cat /proc/cpuinfo

## Copy proxy from private AFS space
copy_x509_proxy

## Check for a valid x509proxy
check_x509_proxy

## set-up dbs client
setup_dbs_client

## set-up dbs lifecycle tests
setup_dbs_lifecycle_tests

## cleaning-up workdir
cleanup_workingdir

echo "End time: $(date)"

