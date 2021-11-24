#!/bin/bash

BASEDIR=$1
IDFILE=$2
CMD=$3

if [[ "$CMD" == "" ]]; then
    echo "$0 base_dir id_file command"
    exit
fi


BINDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

INDIR=$BASEDIR/srcdata
TMPDIR=$BASEDIR/tmp
OUTDIR=$BASEDIR/output
BAKDIR=$BASEDIR/bak
TMPSRC=src_prefilled.txt

mkdir -p $INDIR
mkdir -p $TMPDIR
mkdir -p $OUTDIR
mkdir -p $BAKDIR

# Run this script in the directory up one from bin. Make sure the dirs above are created.


if [[ "$CMD" == "src" ]]; then
    sed -E 's/(Mega|cluster)-([^_]+)(_(.*))?\t/cluster-\2\t\4\t/i' $IDFILE > $INDIR/ids.cleaned.txt
    [ -f $OUTDIR/$TMPSRC ] && mv $OUTDIR/$TMPSRC $BAKDIR/$TMPSRC.bak
    echo "#CLUSTER	SSN_ID	FILT_ANALYSIS_ID	FULL_ANALYSIS_ID	FILT_COLOR_SSN_ID	FULL_COLOR_SSN_ID	MIN_LEN	MAX_LEN	E-VALUE" > $OUTDIR/$TMPSRC
    grep -v '^#' $INDIR/ids.cleaned.txt | awk '{print $2"\t"$1"\t\t\t\t\t\t"$4}' >> $OUTDIR/$TMPSRC
fi

if [[ "$CMD" == "sql-a" ]]; then
    # The analysis_eval > 20 thing is a hack to support the mega clusters that have two analysis jobs...
    grep -v '^#' $OUTDIR/$TMPSRC | cut -f1,2 | awk '{print "SELECT \""$1"\", COUNT(analysis_id) AS NUM_A, analysis_generate_id, analysis_id FROM analysis WHERE analysis_generate_id = "$2" AND analysis_status = \"FINISH\" AND ((\""$1"\" != \"cluster-1-1\" AND \""$1"\" != \"cluster-2-1\") OR analysis_evalue > 20) AND analysis_name LIKE \"%Unfiltered%\";"}' > $TMPDIR/tmp_aid_unfilt.sql
    echo "#CLUSTER	NUM_AID	ID	AID" > $TMPDIR/tmp_aid_unfilt.txt
    mysql -N efi_est_dev < $TMPDIR/tmp_aid_unfilt.sql >> $TMPDIR/tmp_aid_unfilt.txt
    grep -v '^#' $OUTDIR/$TMPSRC | cut -f1,2 | awk '{print "SELECT \""$1"\", COUNT(analysis_id) AS NUM_A, analysis_generate_id, analysis_id, analysis_min_length, analysis_max_length FROM analysis WHERE analysis_generate_id = "$2" AND analysis_status = \"FINISH\" AND ((\""$1"\" != \"cluster-1-1\" AND \""$1"\" != \"cluster-2-1\") OR analysis_evalue > 20) AND analysis_name NOT LIKE \"%Unfiltered%\";"}' > $TMPDIR/tmp_aid_filt.sql
    echo "#CLUSTER	NUM_AID	ID	AID" > $TMPDIR/tmp_aid_filt.txt
    mysql -N efi_est_dev < $TMPDIR/tmp_aid_filt.sql >> $TMPDIR/tmp_aid_filt.txt
    #grep -v '^#' $OUTDIR/$TMPSRC | cut -f2 | awk '{print "SELECT analysis_id FROM analysis LEFT JOIN generate ON generate.generate_id = analysis.analysis_generate_id WHERE analysis_generate_id = "$1" AND 
fi

if [[ "$CMD" == "sql-c" ]]; then
    grep -v '^#' $TMPDIR/tmp_aid_unfilt.txt | cut -f1,3,4 | awk '{print "SELECT \""$1"\", \""$2"\", \""$3"\", generate_id FROM generate WHERE generate_status = \"FINISH\" AND generate_params LIKE '\''%generate_color_ssn_source_id\":\""$3"\"%'\'';";}' > $TMPDIR/tmp_color_unfilt.sql
    echo "#CLUSTER	SSN_ID	UNFILT_AID	UNFILT_COLOR" > $TMPDIR/tmp_color_unfilt.txt
    mysql -N efi_est_dev < $TMPDIR/tmp_color_unfilt.sql >> $TMPDIR/tmp_color_unfilt.txt
    grep -v '^#' $TMPDIR/tmp_aid_filt.txt | cut -f1,3,4,5,6 | awk '{print "SELECT \""$1"\", \""$2"\", \""$3"\", generate_id, \""$4"\", \""$5"\" FROM generate WHERE generate_status = \"FINISH\" AND generate_params LIKE '\''%generate_color_ssn_source_id\":\""$3"\"%'\'';";}' > $TMPDIR/tmp_color_filt.sql
    echo "#CLUSTER	SSN_ID	FILT_AID	FILT_COLOR" > $TMPDIR/tmp_color_filt.txt
    mysql -N efi_est_dev < $TMPDIR/tmp_color_filt.sql >> $TMPDIR/tmp_color_filt.txt
fi

if [[ "$CMD" == "merge" ]]; then
    $BINDIR/merge_job_id_tables.pl $TMPDIR/tmp_color_filt.txt $TMPDIR/tmp_color_unfilt.txt $OUTDIR/source.txt
fi


