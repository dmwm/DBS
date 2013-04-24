#!/bin/bash

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

#On LSF is set to /pool/...
WORKING_DIR=$TMPDIR/DBS3_Life_Cycle_Agent_Test.$JOBNUM
PAYLOAD_DIR=$WORKING_DIR/Payloads.$JOBNUM
SCRAM_ARCH=slc5_amd64_gcc461
SWAREA=$WORKING_DIR/sw
REPO=comp.pre.giffels

GRIDENVSCRIPT=/afs/cern.ch/cms/LCG/LCG-2/UI/cms_ui_env.sh

DBS3CLIENTVERSION=3.1.2b
DBS3CLIENT=cms+dbs3-client+$DBS3CLIENTVERSION
DBS3CLIENTDOC=cms+dbs3-client-webdoc+$DBS3CLIENTVERSION

DATAPROVIDERVERSION=1.0.5p
DATAPROVIDER=cms+lifecycle-dataprovider+$DATAPROVIDERVERSION

LIFECYCLEAGENTVERSION=1.1.0
LIFECYCLEAGENT=cms+PHEDEX-lifecycle+$LIFECYCLEAGENTVERSION

DBS3LIFECYCLEVERSION=0.0.4p
DBS3LIFECYCLE=cms+dbs3-lifecycle+$DBS3LIFECYCLEVERSION

PRIVATEDIR=$HOME/private
AFSWORKSPACE=/afs/cern.ch/work/g/giffels

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
    #create $WORKING_DIR
    mkdir -p $WORKING_DIR
    cp -a $PRIVATEDIR/$proxy_file $WORKING_DIR/$proxy_file
    export X509_USER_PROXY=$WORKING_DIR/$proxy_file
    check_success "Copying X509 Proxy failed" $?
  fi
}

cleanup_workingdir()
{
  ## Clean up pre-exists workdir
  local proxy_file=x509up_u$(id -u)

  if [ -d $WORKING_DIR ]; then
    echo "$WORKING_DIR already exists. Cleaning up ..."
    rm -rf $WORKING_DIR
    check_success "Cleaning up $WORKING_DIR" $?
    rm -rf $WORKING_DIR/$proxy_file
    check_success "Cleaning up $WORKING_DIR/$proxy_file" $?
    rm -rf $TMP_WORKFLOW
    check_success "Cleaning up $TMP_WORKLFOW" $?
    rm -rf $PAYLOAD_DIR
    check_success "Cleaning up $PAYLOAD_DIR" $?
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
  export DBS_READER_URL=https://cmsweb-testbed.cern.ch/dbs/dev/global/DBSReader
  export DBS_WRITER_URL=https://cmsweb-testbed.cern.ch/dbs/dev/global/DBSWriter
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
  rm -rf $PAYLOAD_DIR/dbs3fifo.$JOBNUM
  ##create payload directory
  mkdir $PAYLOAD_DIR
  StatsServer.py -n -o $WORKING_DIR/Output.db -i $PAYLOAD_DIR/dbs3fifo.$JOBNUM &> $WORKING_DIR/StatsServer.log &

  if [ 'x$WORKFLOW' == 'x' ]; then
    WORKFLOW=$DBS3_LIFECYCLE_ROOT/conf/DBS3AnalysisLifecycle.conf
  fi

  TMP_WORKFLOW=$(mktemp -p $PAYLOAD_DIR LifeCycleWorkflow.conf.XXXXXXXXX)

  ## change NamedPipe and TmpDir parameter in the Workflow
  SED_PAYLOAD_DIR=$(echo $PAYLOAD_DIR | sed -e "s,/,\\\\/,g")
  sed -e "s/@NamedPipe@/$SED_PAYLOAD_DIR\/dbs3fifo.$JOBNUM/g;s/@TmpDir@/$SED_PAYLOAD_DIR/g" $WORKFLOW &> $TMP_WORKFLOW

  Lifecycle.pl --config $TMP_WORKFLOW &> $WORKING_DIR/LifeCycle.log &

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
  echo "Create directory $AFSWORKSPACE/LifeCycleResults"
  mkdir -p $AFSWORKSPACE/LifeCycleResults
  echo "Copy data from $WORKING_DIR $AFSWORKSPACE/LifeCycleResults"
  cp -a $WORKING_DIR/LifeCycle.log $AFSWORKSPACE/LifeCycleResults/LifeCycle_$JOBNUM.log
  cp -a $WORKING_DIR/StatsServer.log $AFSWORKSPACE/LifeCycleResults/StatsServer_$JOBNUM.log
  cp -a $WORKING_DIR/Output.db $AFSWORKSPACE/LifeCycleResults/Output_$JOBNUM.db
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

echo "Starting time: $(date)"
echo "Running on $(/bin/hostname)"
echo "Having following processors:"
cat /proc/cpuinfo
echo "Working directory is $WORKING_DIR"

## cleaning-up workdir
cleanup_workingdir

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
