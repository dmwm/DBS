#!/bin/bash

#define commands
job_submit_cmd=/afs/cern.ch/cms/caf/scripts/cmsbsub
list_queue_cmd=/usr/bin/bqueues
list_jobs_cmd=/usr/bin/bjobs

stop_job_cmd=/usr/bin/bstop
resume_job_cmd=/usr/bin/bresume
kill_job_cmd=/usr/bin/bkill
log_peek_cmd=/usr/bin/bpeek
host_info_cmd=/usr/bin/bhosts

GRIDENVSCRIPT=/afs/cern.ch/cms/LCG/LCG-2/UI/cms_ui_env.sh
PRIVATEDIR=$HOME/private/

function usage
{
  echo -e "Wrapper script for the CERN LSF Batch System.
           Usage: submit_job.sh <argument> <options>

           Arguments: submit (Options -l <logfile> -q <queue> -x executable + job arguments)
                      bulksubmit (Options -l <logfile_template> -q <queue> -n <number_of_jobs> -x executable + job arguments)
                         * Logfiles will be enumerated by job number
                         * The job number will automatically added as first job argument
                      listjobs
                      listqueue (Options -q queue)
                      stop (Options -i <jobid>)
                      resume (Options -i <jobid>)
                      kill (Options -i <jobid>)
                      peeklog (Options -i <jobid>)
                      hostinfo (Options -q <queue>)

           All options above are mandatory.
           Options:
                      -c X509 credentials are needed by the job. Can be fetched from private AFS.\n"
}

function handle_x509_proxy
{
  if [ ! -e $GRIDENVSCRIPT ]; then
    echo "Grid environment cannot be set-up. $GRIDENVSCRIPT is missing."
    exit 2
  fi
  
  source $GRIDENVSCRIPT
  if ! voms-proxy-info --exists &> /dev/null; then
    echo "No valid x509 proxy found! Please, create one ..."
    if ! voms-proxy-init --voms cms --valid 192:00; then
      echo "Cannot create proxy certificate"
      exit 2
    fi
  fi

  local proxy_file=x509up_u$(id -u)
  cp -a /tmp/$proxy_file $PRIVATEDIR/$proxy_file
}

function check_running_jobs
{
  local local_first_job_number=1

  if ls $PRIVATEDIR/LifeCycleJob_*.cmd &> /dev/null; then
    local local_file_list=$(ls $PRIVATEDIR/LifeCycleJob_*.cmd | sort -k2 -t_ -n)

    for job in $local_file_list; do
      local local_job_running=$(cat $job)
      if [ $local_job_running -eq 1 ]; then
        let local_first_job_number=$(echo $job | tr -cd '[[:digit:]]')+1
      fi
    done
  fi

  echo $local_first_job_number
}

function bulk_submit_job
{
  local local_logfile_template=$1
  local local_queue=$2
  local local_number_of_jobs=$3
  local local_executable=$4
  local local_job_args=${@:5}

  local local_first_job_number=$(check_running_jobs)
  local local_last_job_number=$(expr $local_first_job_number + $local_number_of_jobs - 1)

  for job in $(seq $local_first_job_number $local_last_job_number); do
    submit_job $local_logfile_template.$job $local_queue $local_executable $job $local_job_args
  done
}

function submit_job
{
  local local_logfile=$1
  local local_queue=$2
  local local_executable=$3
  local local_job_args=${@:4}
 
  if [ "x$credentials" != "x" ]; then
    handle_x509_proxy
  fi

  $job_submit_cmd -o $local_logfile -q $local_queue $local_executable $local_job_args
}

function list_queue
{
  local local_queue=$1
  $list_queue_cmd $local_queue
}

function list_jobs
{
  $list_jobs_cmd
}

function stop_job
{
  local local_job_id=$1
  $stop_job_cmd $local_job_id
}

function resume_job
{
  local local_job_id=$1
  $resume_job_cmd $local_job_id
}

function kill_job
{
  local local_job_id=$1
  $kill_job_cmd $local_job_id
}

function log_peek
{
  local local_job_id=$1
  $log_peek_cmd $local_job_id
}

function host_info
{
  local local_queue=$1
  $host_info_cmd -R $local_queue
}

while [ $# -ge 1 ]; do
  case $1 in
    -q ) queue=$2; shift 2 ;;
    -i ) job_id=$2; shift 2 ;;
    -x ) executable=$2; shift 2 ;;
    -l ) logfile=$2; shift 2 ;;
    -c ) credentials=1; shift ;;
    -n ) number_of_jobs=$2; shift 2 ;;
    -h ) usage 1>&2; exit 1 ;;
    -* ) echo "$0: unrecognised option $1, use -h for help" 1>&2; exit 1 ;;
    *  ) args="$args;$1"; shift ;;
  esac
done

[ ${#args} -eq 0 ] || set $(echo $args | tr ";" "\n")

if [ $# -lt 1 ]; then
  echo "$0: Takes at least one argument, use -h for help" 1>&2
  exit 1
fi

case $1 in
  submit ) submit_job $logfile $queue $executable "${@:2}"; shift ;;
  bulksubmit ) bulk_submit_job $logfile $queue $number_of_jobs $executable "${@:2}"; shift ;;
  listjobs ) list_jobs; shift ;;
  listqueue ) list_queue $queue; shift ;;
  stop ) stop_job $job_id; shift ;;
  resume ) resume_job $job_id; shift ;;
  kill ) kill_job $job_id; shift ;;
  peeklog ) log_peek $job_id; shift ;;
  hostinfo ) host_info $queue; shift ;;
  * ) echo "$0: Unknown argument $1, use -h for help" 1>&2; exit 1 ;;
esac
