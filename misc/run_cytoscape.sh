#!/bin/bash
set +e

lock_file="`readlink -f $0`.lock"
#-gt is sometimes 2, sometimes 3?????
if [[ -f $lock_file || "`ps -ef | grep $0 | grep -v grep | wc -l`" -gt 3 ]]; then echo "Already running; exiting"; exit; fi
touch $lock_file

BASE_DIR=`dirname $(realpath "$0")`
source "$BASE_DIR/local_conf.sh"
source "$BASE_DIR/local_app_conf.sh"
source "$PROJECT_DIR/cytoscape_config.sh"


if [[ -f "$CYTO_DIR/find.lock" ]]; then
    echo "Find lock exists; wait until find has finished."
    exit;
fi

job_info_db="$CYTO_DIR/job_info.sqlite"

find_lock_file="$jobInfoDb.find.lock";
touch $find_lock_file

echo "Running find_ssns.pl at $(date)"
/home/n-z/noberg/dev/cytoscape-util/batch_proc/bin/find_ssns.pl --db $job_info_db --ssn-root-dir $LOAD_DIR
echo "Finished find_ssns.pl at $(date)"

rm $find_lock_file

quit_arg=""
if [[ "$1" == "quit" ]]; then
    quit_arg="--quit-all"
fi

TEMP_DIR=$CY_TEMP_DIR

$CY_APP_HOME/bin/cyto_job_server.pl \
    --db $job_info_db \
    --script-dir $CYTO_DIR/scripts \
    --temp-dir $TEMP_DIR \
    --cyto-util-dir $CY_UTIL_DIR \
    --py4cy-image $CY_APP_SIF_IMAGE \
    --max-cy-jobs $CY_NUM_JOBS \
    --log-file $CYTO_DIR/log.txt \
    --cyto-config-home $CY_CONFIG_HOME \
    --cyto-app $CY_APP_MASTER \
    --image-conf verbose=verbose,style=style,zoom=400,name=ssn_lg \
    --queue $QUEUE \
    --ssn-root-dir $LOAD_DIR \
    --overwrite-images --cyto-multiple-mode --job-prefix cyto --cyto-job-uses $CY_NUM_USES \
    --delay $CY_DELAY $quit_arg
#    --image-conf verbose=no_verbose,style=style,zoom=400,crop=crop,name=ssn_lg \

rm $lock_file


