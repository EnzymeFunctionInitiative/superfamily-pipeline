#!/bin/bash

set +e

lock_file="`readlink -f $0`.lock"
#-gt is sometimes 2, sometimes 3?????
if [[ -f $lock_file || "`ps -ef | grep $0 | grep -v grep | wc -l`" -gt 3 ]]; then echo "Already running; exiting"; exit; fi
touch $lock_file

BASE_DIR=`dirname $(realpath "$0")`

### THE ONLY VARIABLES TO CHANGE ARE IN THIS FILE
source "$BASE_DIR/local_conf.sh"
source "$BASE_DIR/local_app_conf.sh"

BASE_ARGS="--master-dir $DATA_DIR --master-file $MASTER_FILE --queue $QUEUE"
BASE_ARGS="$BASE_ARGS --ignore-finalize-errors"

# Starts jobs if the run_collect process has completed
$EFI_PIPELINE_HOME/bin/master.pl $BASE_ARGS --load-dir $LOAD_DIR --efi-db $EFI_DATABASE --support-files $PROJECT_DIR/support --create-new-gnd-db --action check-final

rm $lock_file

