#!/bin/bash

lock_file="`readlink -f $0`.lock"
#-gt is sometimes 2, sometimes 3?????
if [[ -f $lock_file || "`ps -ef | grep $0 | grep -v grep | wc -l`" -gt 3 ]]; then echo "Already running; exiting"; exit; fi
touch $lock_file

BASE_DIR=`dirname $(realpath "$0")`

### THE ONLY VARIABLES TO CHANGE ARE IN THIS FILE
source "$BASE_DIR/local_conf.sh"
source "$BASE_DIR/local_app_conf.sh"

BASE_ARGS="--master-dir $DATA_DIR --master-file $MASTER_FILE --queue $QUEUE --load-dir $LOAD_DIR --min-cluster-size 3"

$EFI_PIPELINE_HOME/bin/master.pl $BASE_ARGS --action start-ascores --generate-dir $EFI_INPUT_JOB_DIR
$EFI_PIPELINE_HOME/bin/master.pl $BASE_ARGS --action check-completion --generate-dir $EFI_INPUT_JOB_DIR --check-type ca+cr
$EFI_PIPELINE_HOME/bin/master.pl $BASE_ARGS --action make-collect
$EFI_PIPELINE_HOME/bin/master.pl $BASE_ARGS --action check-collect
$EFI_PIPELINE_HOME/bin/master.pl $BASE_ARGS --action make-collect --split-ssns
$EFI_PIPELINE_HOME/bin/master.pl $BASE_ARGS --action check-collect --split-ssns
#TODO: populate database for new files

rm $lock_file

