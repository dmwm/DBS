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

LIFECYCLEAGENTVERSION=1.0.5
LIFECYCLEAGENT=cms+PHEDEX-lifecycle+$LIFECYCLEAGENTVERSION

DBS3LIFECYCLEVERSION=0.0.1
DBS3LIFECYCLE=cms+dbs3-lifecycle+$DBS3LIFECYCLEVERSION

PRIVATEDIR=$HOME/private

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
    check_success "Cleaning up $WORKINGDIR" $?
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
  apt-get install $DBS3CLIENT $DBS3CLIENTDOC $DATAPROVIDER $LIFECYCLEAGENT $DBS3LIFECYCLE -y
  check_success "Install $DBS3CLIENT, $DBS3CLIENTDOC, $DATAPROVIDER, $LIFECYCLEAGENT and $DBS3LIFECYCLE" $?
}

setup_dbs_lifecylce()
{
  if [ ! -d $SWAREA ]; then
    echo "Software area not found in $SWAREA. Installing software ..."
    install_software
  fi

  source $SWAREA/$SCRAM_ARCH/cms/dbs3-lifecycle/$DBS3LIFECYCLEVERSION/etc/profile.d/init.sh
  ## setup dbs_client
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

run_dbs_lifecycle_tests()
{
  ## remove potential fifo pipe
  rm -rf /tmp/dbs3fifo
  StatsServer.py -n -o $WORKINGDIR/Output.db -i /tmp/dbs3fifo &> $WORKINGDIR/StatsServer.log &

  if [ 'x$WORKFLOW' == 'x' ]; then
    WORKFLOW=$DBS3_LIFECYCLE_ROOT/conf/DBS3AnalysisLifecycle.conf
  fi

  Lifecycle.pl --config $WORKFLOW &> $WORKINGDIR/LifeCycle.log &

  ## LifeCycleTests can be aborted by an external file
  create_steering_file

  ## check every 5 seconds, if process should be aborted
  while true; do
    check_steering_file
    if [ $? -eq 0 ]; then
      echo "Kill Applications"
      kill_lifecycle_tests
      break
    fi
    sleep 5
  done
}

stage_out_output()
{
  echo "Create directory $SCRATCH/LifeCycleResults"
  mkdir -p $SCRATCH/LifeCycleResults
  echo "Copy data from $WORKINGDIR $SCRATCH/LifeCycleResults"
  cp -a $WORKINGDIR/LifeCycle.log $SCRATCH/LifeCycleResults/LifeCycle_$JOBNUM.log
  cp -a $WORKINGDIR/StatsServer.log $SCRATCH/LifeCycleResults/StatsServer_$JOBNUM.log
  cp -a $WORKINGDIR/Output.db $SCRATCH/LifeCycleResults/Output_$JOBNUM.db
}

create_steering_file()
{
  STEERINGFILE=$PRIVATEDIR/LifeCycleJob_$JOBNUM.cmd
  echo 1 > $STEERINGFILE
}

check_steering_file()
{
  return $(cat $STEERINGFILE)
}

kill_lifecycle_tests()
{
   for PID in $(pgrep -u $(id -u) -f "Lifecycle.pl"); do
       echo "Kill $PID"
       kill -9 $PID
   done

   ## give processes some time to clean-up
   sleep 10

   for PID in $(pgrep -u $(id -u) -f "StatsServer"); do
       echo "Kill $PID"
       kill -1 $PID
   done
   
   ## give processes some time to clean-up and flush filesystem buffers
   sleep 10
   sync
}

if [ $# -lt 1 ]; then
  echo -e "$0: Takes at least one arguments.
               JobNumber (mandatory)
               Workflow (optional)\n"
  exit 1
fi

JOBNUM=$1

if [ $# -gt 1 ]; then
  WORKFLOW=$2
fi

echo "Starting time: $(date)"
echo "Running on $(/bin/hostname)"
echo "Having following processors:"
cat /proc/cpuinfo

## Copy proxy from private AFS space
copy_x509_proxy

## Check for a valid x509proxy
check_x509_proxy

## set-up dbs lifecylce tests
setup_dbs_lifecylce

## run dbs lifecycle tests
run_dbs_lifecycle_tests

## stage-out output
stage_out_output

## cleaning-up workdir
cleanup_workingdir

echo "End time: $(date)"