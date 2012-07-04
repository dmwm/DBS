#/bin/bash

function usage
{
  echo -e "Script to merge multiple Statistics SQLite3 databases.
           Usage: submit_job.sh -o Merged.db DB2Merge_1.db .... DB2Merge_n.db\n"
}

while [ $# -ge 1 ]; do
  case $1 in
    -o ) output=$2; shift 2 ;;
    -h ) usage 1>&2; exit 1 ;;
    -* ) echo "$0: unrecognised option $1, use -h for help" 1>&2; exit 1 ;;
    *  ) args="$args;$1"; shift ;;
  esac
done

[ ${#args} -eq 0 ] || set $(echo $args | tr ";" "\n")

if [ $# -lt 1 ] || [ "x$output" == "x" ]; then
  echo "$0: Takes at least one argument and one option, use -h for help" 1>&2
  exit 1
fi

tempfile=$(mktemp -t merge)

cat >> $tempfile <<EOF
BEGIN;
CREATE TABLE Statistics(Id INTEGER PRIMARY KEY, Query TEXT, ApiCall TEXT, ClientTiming DOUBLE, ServerTiming DOUBLE, ServerTimeStamp INT, ContentLength INT);
COMMIT;
EOF

for DB in ${@}; do
  cat >> $tempfile <<EOF
ATTACH DATABASE "$DB" as Merge;
BEGIN;
INSERT INTO Statistics (Query, ApiCall, ClientTiming, ServerTiming, ServerTimeStamp, ContentLength) SELECT Query, ApiCall, ClientTiming, ServerTiming, ServerTimeStamp, ContentLength FROM Merge.Statistics;
COMMIT;
DETACH DATABASE Merge;
EOF
done

sqlite3 $output < $tempfile
rm $tempfile