
package MasterActions;

use strict;
use warnings;

use Data::Dumper;
use AutoPipeline qw(do_mkdir do_sql is_job_finished run_job get_jobs_from_db);
use DataCollection;
use DataCollection::Commands;
use IdListParser;


sub new {
    my ($class, %args) = @_;

    my $self = {};
    bless $self, $class;

    $self->{dry_run} = $args{dry_run} // 0;
    $self->{get_dbh} = $args{get_dbh} or die "Need get_dbh";
    $self->{app_dir} = $args{app_dir} or die "Need app_dir";
    $self->{efi_tools_home} = $args{efi_tools_home} // $ENV{EFI_TOOLS_HOME};
    $self->{min_cluster_size} = $args{min_cluster_size} // 3;
    $self->{log_fh} = $args{log_fh};
    $self->{queue} = $args{queue} or die "Need queue";

    return $self;
}


sub createFinalDb {
    my $self = shift;
    my $masterData = shift;
    my $dbFile = shift;
    my $scriptDir = shift;
    my $efiDb = shift;
    my $dataDir = shift;
    my $efiMetaDir = shift;
    my $supportFilesDir = shift;
    my $getMetaOnly = shift;
    my $isSizeOnly = shift;
    my $postLoadFile = shift;

    my $metaBody = <<META;

module load Perl

export EFI_TOOLS_HOME=$self->{efi_tools_home}

META

    my $scriptFile;
    my $header;
    my @allMsg;

    if ($isSizeOnly) {
        $scriptFile = "$scriptDir/get_metadata_size.sh";
        $header = getHeader($scriptFile, "5gb", "create_rsam_db_size");
        $metaBody .= <<STUFF;
echo "Calculating size"
$self->{app_dir}/meta_to_sqlite.pl \\
            --sqlite-file $dbFile \\
            --mode sizes
STUFF
    } else {
        my $ecDescFile = "$supportFilesDir/enzclass.txt";
        my $tigrNames = "$supportFilesDir/tigr_names.txt";
        my $annoFile = "$supportFilesDir/annotations.txt";
        my $sfldDescFile = "$supportFilesDir/sfld_desc.txt";
        my $sfldColorFile = "$supportFilesDir/sfld_fam_color.txt";
        my $tigrIdFile = "$efiMetaDir/tigr_ids.txt";

        #TODO: handle region
        #TODO: handle make_ids id_mapping

        # This finds all of the sub-clusters in all of the diced clusters so it will take a long time
        my ($clusterAscoreMapFile, $msg2) = $self->getAscoreMapFile($efiMetaDir, $masterData);
        push @allMsg, @$msg2;

        my $getScriptFile = sub {
            my $prefix = shift;
            my $suffix = shift;
            return "$scriptDir/$prefix$suffix.sh";
        };

        my $postLoadFile = "$scriptDir/post_load.sh";

        my $getLoadFn = sub {
            my $suffix = shift // "";
            my $dicingArg = $suffix ? "--load-diced" : "";
            my $loadDicingArg = $suffix ? "--load-dicing-file $clusterAscoreMapFile" : "";

            my $consResFile = &$getScriptFile("load_cons_res", $suffix);
            my $convRatioFile = &$getScriptFile("load_conv_ratio", $suffix);
            my $idListFile = &$getScriptFile("load_id_list", $suffix);

            my $dicingMode = $suffix ? "dicing" : "";
            return <<STUFF;
echo "Loading cons-res,conv-ratio,$dicingMode,id-list"
$self->{app_dir}/meta_to_sqlite.pl \\
            --sqlite-file $dbFile \\
            --data-dir $dataDir \\
            --mode cons-res,conv-ratio,$dicingMode,id-list \\
            $dicingArg $loadDicingArg \\
            --load-cons-res-script $consResFile \\
            --load-conv-ratio-script $convRatioFile \\
            --load-id-list-script $idListFile

echo 'echo "Processing $consResFile"' >> $postLoadFile
echo '/bin/bash $consResFile' >> $postLoadFile
echo 'echo "Processing $convRatioFile"' >> $postLoadFile
echo '/bin/bash $convRatioFile' >> $postLoadFile
echo 'echo "Processing $idListFile"' >> $postLoadFile
echo '/bin/bash $idListFile' >> $postLoadFile
grep '#WRAPUP' $consResFile | sed 's/^#WRAPUP //' >> $postLoadFile
grep '#WRAPUP' $convRatioFile | sed 's/^#WRAPUP //' >> $postLoadFile
grep '#WRAPUP' $idListFile | sed 's/^#WRAPUP //' >> $postLoadFile

STUFF
        };

        $scriptFile = "$scriptDir/get_metadata.sh";
        $header = getHeader($scriptFile, "105gb", "create_rsam_db");

        my ($msg, $allIdFile) = $self->getUniProtIds($efiMetaDir, $masterData);
        push @allMsg, @$msg;

        my $unirefMappingFile = "$efiMetaDir/uniref-mapping.txt";
        my ($netInfoFile, $sfldInfoFile) = $self->getNetInfoFiles($efiMetaDir, $masterData, $sfldDescFile);

        (my $dbVer = $efiDb) =~ s/^(.*)://;

        my $dbConfigFile = "/igbgroup/n-z/noberg/dev/EST/efi.config";

        $metaBody .= <<META;
echo "Getting data from EFI database"
#TO GET DATABASE IMPORT FILES:
$self->{app_dir}/collect_metadata_from_efidb.pl \\
    --db-file $efiDb \\
    --data-dir $dataDir \\
    --kegg-output $efiMetaDir/kegg.txt \\
    --pdb-output $efiMetaDir/pdb.txt \\
    --ec-output $efiMetaDir/ec.txt \\
    --ec-desc-db $ecDescFile \\
    --taxonomy-output $efiMetaDir/tax.txt \\
    --swissprot-output $efiMetaDir/sp.txt \\
    --tigr-output $tigrIdFile \\
    --master-id-file $allIdFile

echo "Getting UniRef IDs"
#TODO: make this general purpose
module load efishared/devlocal
export EFI_DB=$dbVer
/home/n-z/noberg/dev/GNT/get_uniref_ids.pl --uniprot-ids $allIdFile --uniref-mapping $unirefMappingFile --v2 --config $dbConfigFile

rm -f $postLoadFile

META

    $metaBody .= &$getLoadFn("");
    $metaBody .= &$getLoadFn("_diced");

    $metaBody .= <<META;

echo "Loading kegg,swissprot,pdb,load-anno,taxonomy,enzymecode,uniref-map,netinfo,sfld-desc into database"
$self->{app_dir}/meta_to_sqlite.pl \\
            --sqlite-file $dbFile \\
            --data-dir $dataDir \\
            --mode kegg,swissprot,pdb,load-anno,taxonomy,enzymecode,uniref-map,netinfo,sfld-desc \\
            --load-kegg-file $efiMetaDir/kegg.txt \\
            --load-swissprot-file $efiMetaDir/sp.txt \\
            --load-pdb-file $efiMetaDir/pdb.txt \\
            --load-anno-file $annoFile \\
            --load-taxonomy-file $efiMetaDir/tax.txt \\
            --load-ec-file $ecDescFile \\
            --load-uniref-file $unirefMappingFile \\
            --load-netinfo-file $netInfoFile \\
            --load-sfld-desc-file $sfldInfoFile

# Happens after the meta_to_sqlite.pl steps, since we need to calculate sizes, and also map UniProt IDs to TIGR fams to optimize SQL query

echo "Calculating TIGR"
$self->{app_dir}/meta_to_sqlite.pl \\
            --sqlite-file $dbFile \\
            --mode tigr \\
            --load-tigr-names $tigrNames \\
            --load-tigr-file $tigrIdFile

# Load IDs and other data into database
/bin/bash $postLoadFile

$self->{app_dir}/meta_to_sqlite.pl \\
            --sqlite-file $dbFile \\
            --mode sizes

META
    }

    open my $fh, ">", $scriptFile or die "Unable to open script file $scriptFile: $!";
    $fh->print($header);
    $fh->print($metaBody);
    close $fh;

    return @allMsg;
}


sub createGndDb {
    my $self = shift;
    my ($efiDbModule, $scriptDir, $outputCollectDir, $unirefVersion, $newGndDb) = @_;

    my $gndTempDir = "$scriptDir/gnds_temp";
    mkdir($gndTempDir);
    my $gndOutputDir = "$outputCollectDir/gnds";
    mkdir($gndOutputDir);

    my $outputDb = "$gndOutputDir/gnd.sqlite";
    my $outputKey = "$gndOutputDir/gnd.key";
    my $makeJobScript = "$scriptDir/gen_gnd_job.sh";

    my $idFileBase = "$scriptDir/load_id_list.sh_load_ids";
    my $dicedIdFileBase = "$scriptDir/load_id_list_diced.sh_load_ids"; 

    my @mappingArgs;
    push @mappingArgs, "--id-map ${idFileBase}_uniprot.txt";
    push @mappingArgs, "--append-id-map ${dicedIdFileBase}_uniprot.txt";

    if (-f "${idFileBase}_uniref50.txt" or -f "${dicedIdFileBase}_uniref50.txt") {
        push @mappingArgs, "--id-map-uniref50 ${idFileBase}_uniref50.txt";
        push @mappingArgs, "--append-id-map-uniref50 ${dicedIdFileBase}_uniref50.txt";
    }
    if (-f "${idFileBase}_uniref90.txt" or -f "${dicedIdFileBase}_uniref90.txt") {
        push @mappingArgs, "--id-map-uniref90 ${idFileBase}_uniref90.txt";
        push @mappingArgs, "--append-id-map-uniref90 ${dicedIdFileBase}_uniref90.txt";
    }

    my $mappingArgs = join(" \\\n\t", @mappingArgs);

    my $dbArgs = not $newGndDb ? "--make-map-tables-only" : "";

    my $outputScript = "$scriptDir/gen_gnd_job_wrapper.sh";
    open my $fh, ">", $outputScript or do_exit(1);
    $fh->print(<<SCRIPT);
#!/bin/bash

module load Perl
module load efignt/devlocal

/usr/bin/uuidgen | tr -d '\n' > $outputKey

$self->{app_dir}/make_gnd_job.pl $dbArgs \\
    --script-file $makeJobScript \\
    --gnd-temp-dir $gndTempDir \\
    --output-db $outputDb \\
    --db-ver $efiDbModule \\
    $mappingArgs


SCRIPT
}


sub createHmmDb {
    my $self = shift;
    my ($masterData, $scriptDir, $outputCollectDir) = @_;

    my $hmmDbDir = "$outputCollectDir/hmms";
    mkdir $hmmDbDir;

    my $jobIdFile = "$scriptDir/hmm_job_list.txt";
    $self->getJobDirs($masterData, $jobIdFile, $outputCollectDir);

    my $dicedJobIdFile = "$scriptDir/hmm_diced_job_list.txt";
    $self->getDicedJobDirs($dicedJobIdFile);

    my $hmmBuildScript = "$scriptDir/gen_hmm_db.sh";

    my $allHmm = "$hmmDbDir/all.hmm";

    my $outputScript = "$scriptDir/gen_hmm_db_wrapper.sh";
    open my $fh, ">", $outputScript or do_exit(1);
    $fh->print(<<SCRIPT);
#!/bin/bash

module load Perl

$self->{app_dir}/collect_hmms.pl --data-dir $outputCollectDir --job-list-file $dicedJobIdFile --output-hmm-dir $hmmDbDir --by-ascore --diced
$self->{app_dir}/collect_hmms.pl --data-dir $outputCollectDir --job-list-file $jobIdFile --output-hmm $allHmm --by-ascore

echo "module load HMMER" > $hmmBuildScript
ls $hmmDbDir/*.hmm | sed 's/^/hmmpress /' >> $hmmBuildScript
echo "bash $hmmBuildScript"

SCRIPT
    close $fh;

    print "Run bash $outputScript\n";
}


sub getJobDirs {
    my $self = shift;
    my $masterData = shift;
    my $outFile = shift;
    my $outputCollectDir = shift;

    open my $fh, ">", $outFile;

    foreach my $clusterId (keys %$masterData) {
        my $kids = $masterData->{$clusterId}->{children};
        next if not $masterData->{$clusterId}->{job_id}; # Placeholder (i.e. parent cluster)
        my $dirPath = "$outputCollectDir/$clusterId";
        $fh->print(join("\t", $clusterId, "", $dirPath), "\n");
    }

    close $fh;
}


sub getDicedJobDirs {
    my $self = shift;
    my $outFile = shift;

    open my $fh, ">", $outFile;

    my $sql = <<SQL;
SELECT J.as_id, J.dir_path, A.cluster_id, A.ascore FROM collect_jobs AS J LEFT JOIN as_jobs AS A ON J.as_id = A.as_id WHERE J.collect_finished = 1
SQL
    my $dbh = &{$self->{get_dbh}}();
    my @jobs = get_jobs_from_db($sql, $dbh, $self->{dry_run}, $self->{log_fh});
    foreach my $job (@jobs) {
        $fh->print(join("\t", $job->{cluster_id}, $job->{ascore}, $job->{dir_path}), "\n");
    }

    close $fh;
}


sub getUniProtIds {
    my $self = shift;
    my $efiMetaDir = shift;
    my $masterData = shift;

    my $idFile = "$efiMetaDir/id_mapping_non_diced.txt";
    my $dicedIdFile = "$efiMetaDir/id_mapping_diced.txt";
    my $allIdFile = "$efiMetaDir/id_mapping_all.txt";

    my $dbh = &{$self->{get_dbh}}();
    my $useDiced = 1;
    my ($nonDicedDirs, $msg1) = $self->getFinishedJobPaths($dbh, $masterData, not $useDiced);
    my ($dicedDirs, $msg2) = $self->getFinishedJobPaths($dbh, $masterData, $useDiced);

    my ($nonDicedIds, $errorsND) = $self->getIdsFromDirs($nonDicedDirs);
    my ($dicedIds, $errorsD) = $self->getIdsFromDirs($dicedDirs);
    # Make unique
    my %allIds;
    map { $allIds{$_} = 1 } @$nonDicedIds;
    map { $allIds{$_} = 1 } @$dicedIds;

    open my $fh, ">", $allIdFile or die "Unable to write to all ID file $allIdFile: $!";
    foreach my $id (keys %allIds) {
        $fh->print("$id\n");
    }
    close $fh;

    my @msg = (@$msg1, @$msg2, @$errorsND, @$errorsD);
    return (\@msg, $allIdFile);
}


sub getAscoreMapFile {
    my $self = shift;
    my $efiMetaDir = shift;
    my $masterData = shift;
    
    my $dbh = &{$self->{get_dbh}}();
    my ($dicedDirs, $msg) = $self->getClusterAscoreJobPaths($dbh, $masterData);

    my $mapFile = "$efiMetaDir/cluster_ascore_map.txt";

    open my $fh, ">", $mapFile or die "Unable to open cluster ascore map file $mapFile: $!";

    foreach my $info (@$dicedDirs) {
        my $dirPath = $info->[0];
        my $clusterId = $info->[1];
        my $ascore = $info->[2];
        my $primaryAscore = $masterData->{$clusterId}->{primary_ascore};
        print "Finding diced sub clusters for $dirPath\n";
        my @subDirs = grep { -d $_ } glob("$dirPath/cluster-*");
        foreach my $subDir (@subDirs) {
            (my $subCluster = $subDir) =~ s%^.*/(cluster-[\d\-]+)$%$1%;
            $fh->print(join("\t", $clusterId, $primaryAscore, $ascore, $subCluster), "\n");
        }
    }

    close $fh;

    return ($mapFile, $msg);
}


sub getNetInfoFiles {
    my $self = shift;
    my $efiMetaDir = shift;
    my $masterData = shift;
    my $sfldDescFile = shift;

    my $sfldMap = {};
    if ($sfldDescFile and -f $sfldDescFile) {
        open my $fh, "<", $sfldDescFile or die "Unable to read SFLD desc file $sfldDescFile: $!";
        while (<$fh>) {
            chomp;
            my ($num, $desc, $color) = split(m/\t/);
            $sfldMap->{$num} = {desc => $desc, color => $color};
        }
        close $fh;
    }

    my @clusterIds = sort clusterIdSort keys %$masterData;

    my $outNetInfoFile = "$efiMetaDir/network_names.txt";
    my $outSfldMapFile = "$efiMetaDir/sfld_map.txt";
    open my $netInfoFh, ">", $outNetInfoFile;
    open my $sfldMapFh, ">", $outSfldMapFile;

    my $topLevel = "fullnetwork";
    
    foreach my $clusterId (@clusterIds) {
        my $md = $masterData->{$clusterId};
        (my $clusterName = $md->{cluster_name}) =~ s/^mega-/Megacluster-/i;
        my $parentId = $md->{job_id} ? "" : $topLevel; # This one is a placeholder for the top level children
        my $sfld = $md->{sfld};
        my $netInfoLine = join("\t", $clusterId, $clusterName, $sfld, "", $parentId);
        $netInfoFh->print($netInfoLine, "\n");
    
        my $sfldNum = $masterData->{$clusterId}->{sfld_num};
        if ($sfldNum and $sfldMap->{$sfldNum}->{color}) {
            my $color = $sfldMap->{$sfldNum}->{color};
            my $sfldMapLine = join("\t", $clusterId, $sfld, $color);
            $sfldMapFh->print($sfldMapLine, "\n");
        }
    }

    $netInfoFh->print(join("\t", $topLevel, "", "", "", ""), "\n");
    
    close $sfldMapFh;
    close $netInfoFh;

    return ($outNetInfoFile, $outSfldMapFile);
}


sub getIdsFromDirs {
    my $self = shift;
    my $dirs = shift;

    #TODO debug
    my @err;
    my @ids;
    foreach my $dir (@$dirs) {
        print "Getting IDs from $dir\n";
        my $file = "$dir/uniprot.txt";
        if ($self->{dry_run}) {
            print "Loading IDs from $file\n";
            next;
        }

        open my $fh, "<", $file or (push @err, "Unable to read ID file $file" and next); #die "Unable to read id file $file: $!";
        while (my $line = <$fh>) {
            chomp $line;
            push @ids, $line;
        }
        close $fh;
    }

    return (\@ids, \@err);
}


sub getHeader {
    my $scriptFile = shift;
    my $ram = shift;
    my $jobName = shift;

    return <<HEADER;
#!/bin/bash
#SBATCH --partition=efi
#SBATCH --nodes=1
#SBATCH --tasks-per-node=1
#SBATCH --mem=$ram
#SBATCH --job-name="$jobName"
#SBATCH --kill-on-invalid-dep=yes
#SBATCH -o $scriptFile.stdout.%j
#SBATCH -e $scriptFile.stderr.%j
HEADER
}


sub writeCytoscapeScript {
    my $self = shift;
    my $jobMasterDir = shift;
    my $configFile = shift;
    my $outputScript = shift;
    my $jobPrefix = shift // "";
    my $ssnListFileName = shift // "ssn_list.txt";

    my $ssnListFile = "$jobMasterDir/$ssnListFileName";
    my $bakFile = "$ssnListFile.bak0";
    my $c = 1;
    while (-f $bakFile) {
        $bakFile = "$ssnListFile.bak" . $c++;
    }
    rename $ssnListFile, $bakFile;

    my @files = $self->listSsnFiles();
    open my $fh, ">", $ssnListFile or die "Unable to write to $ssnListFile: $!\n";
    map { $fh->print($_, "\n"); } @files;
    close $fh;

    #my $prefixArg = $jobPrefix ? "--job-prefix $jobPrefix" : "";

    my $cmd = <<CMD;
#!/bin/bash

lock_file="`readlink -f \$0`.lock"

#-gt is sometimes 2, sometimes 3?????
if [[ -f \$lock_file || "`ps -ef | grep \$0 | grep -v grep | wc -l`" -gt 2 ]]; then exit; fi
touch \$lock_file

source /etc/profile
module load Perl
module load singularity

# Before the source
JOB_MASTER_DIR="$jobMasterDir"
CY_SSN_LIST_FILE="$ssnListFile"

source $configFile

if [[ ! -n \$CY_SSN_LIST_FILE ]]; then
    rm \$lock_file
    exit
fi

\$CY_APP_HOME/bin/cyto_job_server.pl \\
    --db \$CY_BASE_DIR/job_info.sqlite \\
    --script-dir \$CY_SCRIPT_DIR \\
    --ssn-list-file \$CY_SSN_LIST_FILE \\
    --py4cy-image \$CY_APP_SIF_IMAGE \\
    --max-cy-jobs \$CY_NUM_JOBS \\
    \$CY_DEBUG_FLAG \$CY_LOG_FILE_ARG \$CY_CHECK_SSN_DIRS_FLAG \$CY_DRY_RUN_FLAG \$CY_OVERWRITE_IMAGES_FLAG \\
    \$CY_CYTOSCAPE_CONFIG_MASTER_ARG \$CY_CYTOSCAPE_APP_MASTER_ARG \$CY_CYTOSCAPE_TEMP_HOME_ARG \$CY_JAVA_HOME_ARG \$CY_MAX_NODE_JOBS_ARG \$CY_MAX_JOBS_ARG \\
    \$CY_MODULE_ARG \$CY_EXPORT_ZOOM \$CY_IMAGE_CROP \$CY_APP_VERBOSE \$CY_IMAGE_NAME \$CY_QUEUE_ARG \$CY_JOB_PREFIX_ARG \$CY_MAX_RAM_THRESHOLD

rm \$lock_file
rm \$CY_SSN_LIST_FILE

CMD
    
    open my $outFh, ">", $outputScript or die "Unable to write to $outputScript: $!";
    $outFh->print($cmd);
    close $outFh;
}


sub listSsnFiles {
    my $self = shift;

    my @files;

    my $dbh = &{$self->{get_dbh}}();
    my @jobs = $self->getFinishedSplitJobs($dbh);

    foreach my $jobInfo (@jobs) {
        my $dir = $jobInfo->{dir_path};
        my $mainSsn = "$dir/ssn.xgmml";
        my $mainId = $jobInfo->{as_id};
        push @files, join("\t", $mainId, $mainSsn);

        my @dicings = grep { -d $_ } glob("$dir/cluster[\-_]*");
        foreach my $dicingDir (@dicings) {
            (my $dicingNum = $dicingDir) =~ s%^.*/cluster[\-_].*?(\d+)$%$1%;
            my $dicingId = "$mainId-$dicingNum";
            push @files, join("\t", $dicingId, "$dicingDir/ssn.xgmml");
        }

        my $sql = "UPDATE collect_jobs SET cyto_started = 1 WHERE as_id = '$mainId'";
        do_sql($sql, $dbh, $self->{dry_run}, $self->{log_fh});
    }

    return @files;
}


sub checkCollectFinished {
    my $self = shift;
    my $splitFlag = shift // 0;

    my $dbh = &{$self->{get_dbh}}();
    my @jobs = $self->getCollectionJobs($dbh, $splitFlag);

    my $key = $splitFlag == DataCollection::SPLIT ? "split_job_id" : "collect_job_id";

    my @messages;

    #TODO: this is checking for the presence of the main ssn.xgmml not the diced parent one.  This needs to be fixed.
    foreach my $jobData (@jobs) {
        my $asid = $jobData->{as_id};
        #my $slurmFinished = checkSlurmStatus($jobData->{split_job_id}) and checkSlurmStatus($jobData->{collect_job_id});
        push @messages, "No data for $asid" and next if not $jobData->{$key};
        my @ids = split(m/,/, $jobData->{$key});
        my $slurmFinished = checkSlurmStatus(@ids);
#        print "$asid\t" . join(",", @ids), "\t$slurmFinished\n";
        if ($slurmFinished) {
            my $finishFile = $splitFlag == DataCollection::SPLIT ? $jobData->{dir_path} . "/ssn.xgmml" : "";
            if ($finishFile and -f $finishFile) {
                push @messages, "$asid split finished";
                my $sql = "UPDATE collect_jobs SET split_finished = 1 WHERE as_id = '$asid'";
                do_sql($sql, $dbh, $self->{dry_run}, $self->{log_fh});
            } elsif ($splitFlag == DataCollection::COLLECT) {
                push @messages, "$asid collect finished";
                my $sql = "UPDATE collect_jobs SET collect_finished = 1 WHERE as_id = '$asid'";
                do_sql($sql, $dbh, $self->{dry_run}, $self->{log_fh});
            }
        }
    }

    return \@messages;
}


sub makeCollect {
    my $self = shift;
    my $masterData = shift;
    my $collectScriptDir = shift;
    my $outputCollectDir = shift;
    my $splitFlag = shift // 0;
    my $minClusterSize = shift // 3;
    my $jobPrefix = shift // "";

    do_mkdir($collectScriptDir);

    my $collect = new DataCollection(script_dir => $collectScriptDir, overwrite => 0, queue => $self->{queue}, app_dir => $self->{app_dir}, dry_run => $self->{dry_run}, efi_tools_home => $self->{efi_tools_home});

    my $dbh = &{$self->{get_dbh}}();
    my @jobs = $self->getFinishedJobsForCollection($dbh, $splitFlag, $minClusterSize);

    foreach my $jobData (@jobs) {
        my $clusterId = $jobData->{cluster_id};
        my $asid = $jobData->{as_id};

        my $ssnDir = $jobData->{ca_ssn_dir};
        my $inputSsn = "$ssnDir/$jobData->{ca_ssn_name}";
        my $outputDir = "$outputCollectDir/$clusterId"; 

        my $children = $masterData->{$clusterId}->{children};
        my $cd = {input_ca_dir => $ssnDir, input_ssn => $inputSsn, input_cr_dir => $jobData->{cr_dir}, output_dir => $outputDir,
            children => $children};
        $cd->{ascore} = $jobData->{ascore}; # Always dice, even if it's a parent cluster
        #$cd->{ascore} = $jobData->{ascore} if scalar @$children == 0;

        #$self->{log_fh}->print("Undefined cluster ID") if not $clusterId;
        #$self->{log_fh}->print("Undefined ascore for $clusterId") if not $cd->{ascore};
        my $dirPath = "$outputCollectDir/$clusterId";
        mkdir $dirPath if not -d $dirPath;
        if (scalar @$children == 0) {
            $dirPath .= "/dicing-$cd->{ascore}" if $cd->{ascore}; # Leaf cluster
            mkdir $dirPath;
        } elsif ($cd->{ascore} != $masterData->{$clusterId}->{primary_ascore}) {
            # Skip copying every AS except the primary one since this cluster has sub-clusters.
            next;
        }

        $collect->addCluster($asid, $clusterId, $cd, $dirPath, $splitFlag);
    }

    my ($jobMap, $messages) = $collect->finish($splitFlag, $jobPrefix);

    #TODO
    foreach my $asid (keys %$jobMap) {
        my $dirPath = $jobMap->{$asid}->{dir_path};
        my $sql = "";
        if ($splitFlag == DataCollection::COLLECT) {
            my $collectJobId = $jobMap->{$asid}->{collect_job_id};
            $collectJobId = join(",", @$collectJobId);
            $sql = "INSERT INTO collect_jobs (as_id, dir_path, started, collect_finished, split_finished, collect_job_id) VALUES ('$asid', '$dirPath', 1, 0, 0, '$collectJobId')";
        } else {
            my $splitJobId = $jobMap->{$asid}->{split_job_id};
            $splitJobId = join(",", @$splitJobId);
            $sql = "UPDATE collect_jobs SET split_job_id = '$splitJobId' WHERE as_id = '$asid'";
        }
        do_sql($sql, $dbh, $self->{dry_run}, $self->{log_fh});
    }

    return $messages;
}


sub getFinishedJobsForCollection {
    my $self = shift;
    my $dbh = shift;
    my $collectFlag = shift;
    my $minClusterSize = shift;

    my $collectClause = "(J.started IS NULL OR J.started = 0)";
    if ($collectFlag == DataCollection::SPLIT) {
        $collectClause = "(J.collect_finished = 1 AND (J.split_job_id IS NULL OR J.split_job_id = ''))";
    }

    my $sql = <<SQL;
SELECT C.as_id AS as_id, A.cluster_id AS cluster_id, A.uniref, A.ascore, C.dir_path AS ca_ssn_dir, C.ssn_name AS ca_ssn_name, R.dir_path AS cr_dir
    FROM ca_jobs AS C
    LEFT JOIN as_jobs AS A ON C.as_id = A.as_id
    LEFT JOIN cr_jobs AS R ON C.as_id = R.as_id
    LEFT JOIN collect_jobs AS J ON C.as_id = J.as_id
WHERE A.finished = 1 AND C.finished = 1 AND C.max_cluster_size >= $minClusterSize AND R.finished = 1 AND $collectClause
SQL
    return get_jobs_from_db($sql, $dbh, $self->{dry_run}, $self->{log_fh});
}


# Returns jobs that are still running
sub getCollectionJobs {
    my $self = shift;
    my $dbh = shift;
    my $collectFlag = shift;

    my $collectClause = "J.collect_finished = 0";
    if ($collectFlag == DataCollection::SPLIT) {
        $collectClause = "J.collect_finished = 1 AND J.split_finished = 0";
    }

    my $sql = <<SQL;
SELECT J.as_id AS as_id, J.split_job_id AS split_job_id, J.collect_job_id AS collect_job_id, J.dir_path AS dir_path
    FROM collect_jobs AS J
WHERE J.started = 1 AND $collectClause
SQL
    # Also add AND R.finished = 1 after testing
    return get_jobs_from_db($sql, $dbh, $self->{dry_run}, $self->{log_fh});
}


sub getFinishedSplitJobs {
    my $self = shift;
    my $dbh = shift;

    my $sql = <<SQL;
SELECT J.as_id AS as_id, J.split_job_id AS split_job_id, J.dir_path AS dir_path
    FROM collect_jobs AS J
WHERE J.split_finished = 1 AND (J.cyto_started IS NULL OR J.cyto_started = 0)
SQL
    # Also add AND R.finished = 1 after testing
    return get_jobs_from_db($sql, $dbh, $self->{dry_run}, $self->{log_fh});
}


sub getFinishedJobPaths {
    my $self = shift;
    my $dbh = shift;
    my $masterData = shift;
    my $useDiced = shift;

    my @clusterIds;
    foreach my $clusterId (keys %$masterData) {
        my $kids = $masterData->{$clusterId}->{children};
        next if not $masterData->{$clusterId}->{job_id}; # Placeholder (i.e. parent cluster)
        if (scalar @$kids and not $useDiced) {
            push @clusterIds, $clusterId;
        } elsif (not scalar @$kids and $useDiced) {
            push @clusterIds, $clusterId;
        }
    }

    my @messages;
    my @dirs;
    foreach my $clusterId (@clusterIds) {
        my $ascore = $masterData->{$clusterId}->{primary_ascore};
        my $sql = <<SQL;
SELECT J.dir_path
    FROM collect_jobs AS J
    LEFT JOIN as_jobs AS A ON J.as_id = A.as_id
WHERE J.collect_finished = 1 AND A.cluster_id = '$clusterId' AND A.ascore = $ascore
SQL
        my ($job) = get_jobs_from_db($sql, $dbh, $self->{dry_run}, $self->{log_fh});
        if ($job and $job->{dir_path} and -d $job->{dir_path}) {
            push @dirs, $job->{dir_path};
        } else {
            push @messages, "Error with $sql, nothing found";
        }
    }

    return (\@dirs, \@messages);
}


sub getClusterAscoreJobPaths {
    my $self = shift;
    my $dbh = shift;
    my $masterData = shift;

    my @clusterIds;
    foreach my $clusterId (keys %$masterData) {
        my $kids = $masterData->{$clusterId}->{children};
        next if not $masterData->{$clusterId}->{job_id}; # Placeholder (i.e. parent cluster)
        if (not scalar @$kids) {
            push @clusterIds, $clusterId;
        }
    }

    my @messages;
    my @dirs;
    foreach my $clusterId (@clusterIds) {
        my $sql = <<SQL;
SELECT J.dir_path, A.ascore
    FROM collect_jobs AS J
    LEFT JOIN as_jobs AS A ON J.as_id = A.as_id
WHERE J.collect_finished = 1 AND A.cluster_id = '$clusterId'
SQL
        my @jobs = get_jobs_from_db($sql, $dbh, $self->{dry_run}, $self->{log_fh});
        if (scalar @jobs) {
            foreach my $job (@jobs) {
                push @dirs, [$job->{dir_path}, $clusterId, $job->{ascore}];
            }
        } else {
            push @messages, "Error with $sql, nothing found";
        }
    }

    return (\@dirs, \@messages);
}


sub checkSlurmStatus {
    my @jobId = @_;
    return 1 if not scalar @jobId or not $jobId[0];
    my $isFinished = 1;
    foreach my $jobId (@jobId) {
        $isFinished = $isFinished && is_job_finished($jobId);
    }
    return $isFinished;
}


sub clusterIdSort {
    return IdListParser::clusterIdSort($a, $b);
}


1;

