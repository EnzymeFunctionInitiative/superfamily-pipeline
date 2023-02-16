#!/bin/bash

lock_file="`readlink -f $0`.lock"

#-gt is sometimes 2, sometimes 3?????
if [[ -f $lock_file || "`ps -ef | grep $0 | grep -v grep | wc -l`" -gt 2 ]]; then echo "Already running; exiting"; exit; fi
touch $lock_file

source /etc/profile
module load Perl
module load efishared/devlocal
module load efiest/devlocal
module load efignt/devlocal

export EFI_TOOLS_HOME=/igbgroup/n-z/noberg/dev/EFITools

#source "$PWD/config.sh"
module load efidb/ip88
OUT_VER=0525
BASE_DIR=/private_stores/gerlt/databases/rsam/ip88/auto
JOB_OUTPUT_DIR="$BASE_DIR/output_$OUT_VER"
LOAD_DIR=/private_stores/gerlt/databases/rsam/ip88/rsam-4.0

BASE_ARGS="--master-dir $JOB_OUTPUT_DIR --master-file $BASE_DIR/master_${OUT_VER}.txt"

/home/n-z/noberg/dev/hmm/superfamily-pipeline/bin/master.pl $BASE_ARGS --action start-ascores --generate-dir /private_stores/gerlt/efi_test/results
/home/n-z/noberg/dev/hmm/superfamily-pipeline/bin/master.pl $BASE_ARGS --action check-completion --generate-dir /private_stores/gerlt/efi_test/results --check-type ca+cr --min-cluster-size 3
/home/n-z/noberg/dev/hmm/superfamily-pipeline/bin/master.pl $BASE_ARGS --action make-collect --output-collect-dir $LOAD_DIR --min-cluster-size 3
/home/n-z/noberg/dev/hmm/superfamily-pipeline/bin/master.pl $BASE_ARGS --action check-collect
/home/n-z/noberg/dev/hmm/superfamily-pipeline/bin/master.pl $BASE_ARGS --action make-collect --output-collect-dir $LOAD_DIR --split-ssns --min-cluster-size 3
/home/n-z/noberg/dev/hmm/superfamily-pipeline/bin/master.pl $BASE_ARGS --action check-collect --split-ssns
/home/n-z/noberg/dev/hmm/superfamily-pipeline/bin/master.pl $BASE_ARGS --action cytoscape --cyto-config $BASE_DIR/cytoscape_config.sh --cyto-run-script $JOB_OUTPUT_DIR/run_cytoscape.sh
/bin/bash $JOB_OUTPUT_DIR/run_cytoscape.sh

#/home/n-z/noberg/dev/hmm/superfamily-pipeline/bin/auto-ssn/start_ascore_jobs.pl --master-output-dir $JOB_OUTPUT_DIR --mode-2-master-file $BASE_DIR/master_$OUT_VER.txt --mode-2-input-dir /private_stores/gerlt/efi_test/results --uniref-version 90
#/home/n-z/noberg/dev/hmm/superfamily-pipeline/bin/auto-ssn/check_completion.pl --master-dir $JOB_OUTPUT_DIR --no-run-cr

rm $lock_file

