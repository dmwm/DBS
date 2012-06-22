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

function usage
{
  echo -e "Wrapper script for the CERN LSF Batch System.
           Usage: submit_job.sh <argument> <options>

           Arguments: submit (Options -l <logfile> -q <queue> -x executable + job arguments)
                      listjobs
                      listqueue (Options -q queue)
                      stop (Options -i <jobid>)
                      resume (Options -i <jobid>)
                      kill (Options -i <jobid>)
                      peeklog (Options -i <jobid>)
                      hostinfo (Options -q <queue>)

           All options are mandatory.\n"
}

function submit_job
{
  local local_logfile=$1
  local local_queue=$2
  local local_executable=$3
  local local_job_args=${@:4}
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
    -h ) usage 1>&2; exit 1 ;;
    -* ) echo "$0: unrecognised option $1, use -h for help" 1>&2; exit 1 ;;
    *  ) args="$args;$1"; shift ;;
  esac
done

[ ${#args} -eq 0 ] || set $(echo $args | tr ";" "\n")

if [ $# -lt 1 ]; then
  echo "$0: Takes at least one argument, use -h for help" 1>&2
fi

case $1 in
  submit ) submit_job $logfile $queue $executable "${@:2}"; shift ;;
  listjobs ) list_jobs; shift ;;
  listqueue ) list_queue $queue; shift ;;
  stop ) stop_job $job_id; shift ;;
  resume ) resume_job $job_id; shift ;;
  kill ) kill_job $job_id; shift ;;
  peeklog ) log_peek $job_id; shift ;;
  hostinfo ) host_info $queue; shift ;;
  * ) echo "$0: Unknown argument $1, use -h for help" 1>&2; exit 1 ;;
esac
