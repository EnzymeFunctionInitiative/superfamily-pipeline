
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
    $self->{efi_config_file} = $args{efi_config_file} // $ENV{EFI_CONFIG};
    $self->{min_cluster_size} = $args{min_cluster_size} // 3;
    $self->{log_fh} = $args{log_fh};
    $self->{queue} = $args{queue} or die "Need queue";

    return $self;
}


sub createFinalDb {
    my $self = shift;
    my $masterData = shift;
    my $dataDbFile = shift;
    my $scriptDir = shift;
    my $efiDb = shift;
    my $dataDir = shift;
    my $efiMetaDir = shift;
    my $supportFilesDir = shift;
    my $getMetaOnly = shift;
    my $isSizeOnly = shift;
    my $masterScript = shift;


   (my $dbVer = $efiDb) =~ s/^(.*)://;

    my $metaBody = <<META;

rm -f $scriptDir/build.finished
touch $scriptDir/build.started

source /etc/profile.d/lmod.sh
source /home/groups/efi/apps/perl_env.sh
module load Perl
module load efishared/devlocal

export EFI_TOOLS_HOME=$self->{efi_tools_home}
export EFI_DB=$dbVer

META

    my $scriptFile;
    my $header;
    my @allMsg;

    my $jobName = "create_db" . ($ENV{OUTPUT_VERSION} ? "_" . $ENV{OUTPUT_VERSION} : "");

    if ($isSizeOnly) {
        $scriptFile = "$scriptDir/get_metadata_size.sh";
        $header = getHeader($scriptFile, "5gb", "${jobName}_size");
        $metaBody .= <<STUFF;
echo "Calculating size"
$self->{app_dir}/meta_to_sqlite.pl \\
            --sqlite-file $dataDbFile \\
            --mode sizes
STUFF
    } else {
        my $ecDescFile = "$supportFilesDir/enzclass.txt";
        my $annoFile = "$supportFilesDir/annotations.txt";

        #TODO: handle region
        #TODO: handle make_ids id_mapping

        # This finds all of the sub-clusters in all of the diced clusters so it will take a long time
        my ($clusterAscoreMapFile, $msg2) = $self->getAscoreMapFile($efiMetaDir, $masterData);
        push @allMsg, @$msg2;

        my $getLoadFn = sub {
            my $suffix = shift // "";
            my $dicingArg = $suffix ? "--load-diced" : "";
            my $loadDicingArg = $suffix ? "--load-dicing-file $clusterAscoreMapFile" : "";

            my $dicingMode = $suffix ? "dicing" : "";
            return <<STUFF;
echo "Loading cons-res,conv-ratio,$dicingMode,id-list"
$self->{app_dir}/meta_to_sqlite.pl \\
            --sqlite-file $dataDbFile \\
            --data-dir $dataDir \\
            --mode cons-res,conv-ratio,id-list,$dicingMode $dicingArg $loadDicingArg

STUFF
        };

        $scriptFile = $masterScript;
        $header = getHeader($scriptFile, "105gb", "$jobName");

        my ($msg, $allIdFile) = $self->getUniProtIds($efiMetaDir, $masterData);
        push @allMsg, @$msg;

        my $unirefMappingFile = "$efiMetaDir/uniref-mapping.txt";
        my ($netInfoFile, $subgroupInfoFile) = $self->getNetInfoFiles($efiMetaDir, $masterData);

        my $dbConfigFile = $self->{efi_config_file};

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
    --tigr-output $efiMetaDir/tigr_ids.txt \\
    --tigr-desc $efiMetaDir/tigr_info.txt \\
    --alphafold-desc $efiMetaDir/alphafold.txt \\
    --master-id-file $allIdFile

echo "Getting UniRef IDs"
/home/n-z/noberg/dev/GNT/get_uniref_ids.pl --uniprot-ids $allIdFile --uniref-mapping $unirefMappingFile --v2 --config $dbConfigFile

META

    $metaBody .= &$getLoadFn("");
    $metaBody .= &$getLoadFn("_diced");

    $metaBody .= <<META;

echo "Loading kegg,swissprot,pdb,load-anno,taxonomy,enzymecode,uniref-map,netinfo,subgroup-desc into database"
$self->{app_dir}/meta_to_sqlite.pl \\
    --sqlite-file $dataDbFile \\
    --data-dir $dataDir \\
    --mode kegg,swissprot,pdb,load-anno,taxonomy,enzymecode,uniref-map,netinfo,subgroup-desc,tigr,load-alphafolds \\
    --load-kegg-file $efiMetaDir/kegg.txt \\
    --load-swissprot-file $efiMetaDir/sp.txt \\
    --load-pdb-file $efiMetaDir/pdb.txt \\
    --load-anno-file $annoFile \\
    --load-taxonomy-file $efiMetaDir/tax.txt \\
    --load-ec-file $ecDescFile \\
    --load-netinfo-file $netInfoFile \\
    --load-subgroup-desc-file $subgroupInfoFile \\
    --load-tigr-info-file $efiMetaDir/tigr_info.txt \\
    --load-tigr-ids-file $efiMetaDir/tigr_ids.txt \\
    --load-uniref-file $unirefMappingFile \\
    --load-alphafold-file $efiMetaDir/alphafold.txt \\
#    --mode kegg,swissprot,pdb,load-anno,taxonomy,enzymecode,uniref-map,netinfo,subgroup-desc,tigr \\


$self->{app_dir}/meta_to_sqlite.pl \\
    --sqlite-file $dataDbFile \\
    --mode sizes

$self->{app_dir}/meta_to_sqlite.pl \\
    --sqlite-file $dataDbFile \\
    --data-dir $dataDir \\
    --mode cluster-index

touch $scriptDir/build.finished

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
    my ($efiDbModule, $dataDbFile, $loadDir, $scriptDir, $outputScript, $unirefVersion, $createNewGndDb) = @_;

    my $gndTempDir = "$scriptDir/gnds_temp";
    mkdir($gndTempDir);
    my $gndOutputDir = "$loadDir/gnds";
    mkdir($gndOutputDir);

    my $outputDb = "$gndOutputDir/gnd.sqlite";
    my $outputKey = "$gndOutputDir/gnd.key";

    `/usr/bin/uuidgen | tr -d '\n' > $outputKey`;

    my $dbArgs = not $createNewGndDb ? "--make-map-tables-only" : "";

    my $header = getHeader($outputScript, "30gb", "create_gnd_database");

    my %files;
    $files{uniprot} = {table => "id_mapping", column => "uniprot_id", output_file => "$gndTempDir/id_mapping_uniprot"};
    $files{uniref90} = {table => "id_mapping_uniref90", column => "uniref90_id", output_file => "$gndTempDir/id_mapping_uniref90"};
    $files{uniref50} = {table => "id_mapping_uniref50", column => "uniref50_id", output_file => "$gndTempDir/id_mapping_uniref50"} if $unirefVersion == 50;

    my $unirefIdsFile = "$gndTempDir/uniref_ids";
    my $allIdsFile = "$gndTempDir/all_ids";

    open my $fh, ">", $outputScript or die "Unable to write to create gnd database script $outputScript: $!";
    $fh->print($header);
    $fh->print(<<SCRIPT);

module load Perl
source /home/groups/efi/apps/perl_env.sh
module load efignt/devlocal

touch $scriptDir/gnd.started
mkdir -p $gndTempDir

# Get IDs from the id_mapping tables in the database, then sort them properly for the GNDs
#
SCRIPT

    foreach my $type (keys %files) {
        my $baseFile = "$files{$type}->{output_file}.ids";
        $fh->print(<<SCRIPT);
$self->{app_dir}/sqlite_to_text.pl --sqlite-file $dataDbFile --output-file $baseFile --table $files{$type}->{table} --columns '*'
$self->{app_dir}/sort_gnd_ids.pl --id-mapping $baseFile --out-sorted $baseFile.sorted
$self->{app_dir}/sqlite_to_text.pl --sqlite-file $dataDbFile --output-file $baseFile.diced --table diced_$files{$type}->{table} --columns '*'
$self->{app_dir}/sort_gnd_ids.pl --id-mapping $baseFile.diced --out-sorted $baseFile.diced.sorted
SCRIPT
    }

    $fh->print(<<SCRIPT);

cat $files{uniprot}->{output_file}.ids.sorted $files{uniprot}->{output_file}.ids.diced.sorted > $allIdsFile.sorted
awk '{print \$NF}' $allIdsFile.sorted | sort | uniq > $allIdsFile

# Get UniRef IDs
#
get_uniref_ids.pl --uniprot-ids $allIdsFile --uniref-mapping $unirefIdsFile --uniref-version 50


# Create the diagram database
#

create_diagram_db.pl \\
    --id-file $unirefIdsFile \\
    --do-id-mapping \\
    --uniref 50 \\
    --db-file $outputDb \\
    --job-type ID_LOOKUP \\
    --no-neighbor-file $gndTempDir/no_nb.txt \\
    --nb-size 20

$self->{app_dir}/make_gnd_cluster_map_tables.pl --db-file $outputDb --id-mapping $files{uniprot}->{output_file}.ids.sorted --seq-version uniprot --error-file $gndTempDir/error_uniprot.log
$self->{app_dir}/make_gnd_cluster_map_tables.pl --db-file $outputDb --id-mapping $files{uniref90}->{output_file}.ids.sorted --seq-version uniref90 --error-file $gndTempDir/error_uniref90.log
SCRIPT

    if ($unirefVersion == 50) {
        $fh->print(<<SCRIPT);
$self->{app_dir}/make_gnd_cluster_map_tables.pl --db-file $outputDb --id-mapping $files{uniref50}->{output_file}.ids.sorted --seq-version uniref50 --error-file $gndTempDir/error_uniref50.log
SCRIPT
    }

    $fh->print("touch $scriptDir/gnd.finished\n");

    close $fh;
}


sub createHmmDatabaseScript {
    my $self = shift;
    my ($masterData, $scriptDir, $outputScript, $loadDir) = @_;

    my $hmmDbDir = "$loadDir/hmms";
    mkdir $hmmDbDir;

    my $jobIdFile = "$scriptDir/hmm_job_list.txt";
    $self->getJobDirs($masterData, $jobIdFile, $loadDir);

    my $dicedJobIdFile = "$scriptDir/hmm_diced_job_list.txt";
    $self->getDicedJobDirs($dicedJobIdFile, $masterData);

    my $allHmm = "$hmmDbDir/all.hmm";

    my $header = getHeader($outputScript, "30gb", "create_hmm_databases");

    open my $fh, ">", $outputScript or die "Unable to write to create hmm databases script $outputScript: $!";
    $fh->print($header);
    $fh->print(<<SCRIPT);

module load Perl
source /home/groups/efi/apps/perl_env.sh

$self->{app_dir}/collect_hmms.pl --data-dir $loadDir --job-list-file $dicedJobIdFile --output-hmm-dir $hmmDbDir --by-ascore --diced
$self->{app_dir}/collect_hmms.pl --data-dir $loadDir --job-list-file $jobIdFile --output-hmm $allHmm --by-ascore

module load HMMER
for hmm_path in $hmmDbDir/*.hmm; do
    hmmpress \$hmm_path
done

SCRIPT
#echo "module load HMMER" > $hmmBuildScript
#ls $hmmDbDir/*.hmm | sed 's/^/hmmpress /' >> $hmmBuildScript
#echo "bash $hmmBuildScript"
    close $fh;
}


sub getJobDirs {
    my $self = shift;
    my $masterData = shift;
    my $outFile = shift;
    my $loadDir= shift;

    open my $fh, ">", $outFile;

    foreach my $clusterId (keys %$masterData) {
        my $kids = $masterData->{$clusterId}->{children};
        next if not $masterData->{$clusterId}->{job_id}; # Placeholder (i.e. parent cluster)
        #my $isDiced = @{$masterData->{$clusterId}->{ascores}} > 0;
        #next if $isDiced;
        my $dirPath = "$loadDir/$clusterId";
        $fh->print(join("\t", $clusterId, "", $dirPath), "\n");
    }

    close $fh;
}


sub getDicedJobDirs {
    my $self = shift;
    my $outFile = shift;
    my $masterData = shift;

    open my $fh, ">", $outFile;

    my $sql = <<SQL;
SELECT J.as_id, J.dir_path, A.cluster_id, A.ascore FROM collect_jobs AS J LEFT JOIN as_jobs AS A ON J.as_id = A.as_id WHERE J.collect_finished = 1
SQL
    my $dbh = &{$self->{get_dbh}}();
    my @jobs = get_jobs_from_db($sql, $dbh, $self->{dry_run}, $self->{log_fh});
    foreach my $job (@jobs) {
        my $md = $masterData->{$job->{cluster_id}};
        my $isDiced = @{$md->{ascores}} > 0;
        next if not $isDiced;
        next if $job->{ascore} == $md->{primary_ascore};
        $fh->print(join("\t", $job->{cluster_id}, $job->{ascore}, $job->{dir_path}), "\n");
    }

    close $fh;
}


sub getUniProtIds {
    my $self = shift;
    my $efiMetaDir = shift;
    my $masterData = shift;

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
            (my $subCluster = $subDir) =~ s%^.*/(cluster-[a-z\d\-]*[\d\-]+)$%$1%;
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

    my @clusterIds = sort clusterIdSort keys %$masterData;

    my $outNetInfoFile = "$efiMetaDir/network_names.txt";
    my $outSubgroupMapFile = "$efiMetaDir/subgroup_map.txt";
    open my $netInfoFh, ">", $outNetInfoFile;
    open my $subgroupMapFh, ">", $outSubgroupMapFile;

    my $topLevel = "fullnetwork";

    foreach my $clusterId (@clusterIds) {
        my $md = $masterData->{$clusterId};
        (my $clusterName = $md->{cluster_name}) =~ s/^mega-/Megacluster-/i;
        my $parentId = $self->getParentId($clusterId, $topLevel); # $md->{job_id} ? "" : $topLevel; # This one is a placeholder for the top level children
        my $subgroupId = $md->{subgroup_id};
        my $netInfoLine = join("\t", $clusterId, $clusterName, "", $md->{description}, $parentId, $subgroupId);
        $netInfoFh->print($netInfoLine, "\n");

        if ($md->{subgroup_id} and $md->{subgroup_color}) {
            my $color = $md->{subgroup_color};
            my $subgroupMapLine = join("\t", $clusterId, $md->{subgroup_id}, $md->{description}, $md->{subgroup_color});
            $subgroupMapFh->print($subgroupMapLine, "\n");
        }
    }

    $netInfoFh->print(join("\t", $topLevel, "", "", "", ""), "\n");

    close $subgroupMapFh;
    close $netInfoFh;

    return ($outNetInfoFile, $outSubgroupMapFile);
}


sub getParentId {
    my $self = shift;
    my $clusterId = shift;
    my $topLevel = shift;

    my $parentId = "";

    my @p = split(m/-/, $clusterId);
    if (@p < 3) {
        $parentId = $topLevel;
    } else {
        $parentId = join("-", @p[0..($#p-1)]);
    }

    return $parentId;
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
#SBATCH --mail-type=FAIL
HEADER
}


#sub writeCytoscapeScript {
#    my $self = shift;
#    my $jobMasterDir = shift;
#    my $loadDir = shift;
#    my $configFile = shift;
#    my $outputScript = shift;
#
#    return if -f $outputScript;
#
#	my $cyBaseDir = "$jobMasterDir/cytoscape";
#    my $cyScriptDir = "$cyBaseDir/scripts";
#    my $cyTempDir = "$cyBaseDir/temp";
#    my $jobInfoDb = "$cyBaseDir/job_info.sqlite";
#
#    if (not -f $configFile) {
#        open my $fh, ">", $configFile or die "Unable to open Cytoscape config file for writing: $!";
#        $fh->print(<<CMD);
##!/bin/bash
#export CY_APP_MASTER="/home/groups/efi/apps/Cytoscape_3_10_0"
#export CY_UTIL_DIR="/home/groups/efi/apps/cytoscape"
#export CY_CONFIG_HOME="\$CY_UTIL_DIR/init/CytoscapeConfiguration"
#export JAVA_HOME="/home/groups/efi/apps/jdk-17.0.6"
#export CY_APP_HOME="/home/n-z/noberg/dev/cytoscape-util/batch_proc"
#export CY_APP_SIF_IMAGE="\$CY_UTIL_DIR/py4cytoscape.sif"
#export CY_NUM_JOBS=40
#export CY_NUM_USES=15
#CMD
#        close $fh;
#    }
#
#    if (not -f "$jobMasterDir/find_ssns.sh") {
#        open my $fh, ">", "$jobMaster/find_ssns.sh" or die "Unable to open find_ssns.sh for writing: $!";
#        $fh->print(<<CMD);
##!/bin/bash
#set +e
#
#module load Perl
#source /home/groups/efi/apps/perl_env.sh
#
#lock_file="$jobInfoDb.find.lock";
#touch \$lock_file
#/home/n-z/noberg/dev/cytoscape-util/batch_proc/bin/find_ssns.pl --db $jobInfoDb --ssn-root-dir $loadDir
#rm \$lock_file
#
#CMD
#		close $fh;
#	}
#
#
#    my $cyDebug = ""; #--debug
#    my $cyDryRun = ""; #--dry-run
#    my $imageConf = "verbose=no_verbose,style=style,zoom=400,crop=crop,name=ssn_lg";
#
#    my $cmd = <<CMD;
##!/bin/bash
#set +e
#
#lock_file="`readlink -f \$0`.lock"
##-gt is sometimes 2, sometimes 3?????
#
#CYTO_DIR="$jobMasterDir/cytoscape"
#LOAD_DIR="$loadDir"
#JOB_INFO_DB="$jobInfoDb"
#
#if [[ -f "\$JOB_INFO_DB.find.lock" ]]; then
#    echo "Find lock exists; wait until find has finished."
#    exit;
#fi
#
#t1=`ps -ef`
#t2=`echo \$t1 | grep \$0`
#t3=`echo \$t3 | grep -v grep`
#t4=`echo \$t3 | wc -l`
#
#if [[ -f "\$lock_file" ]]; then
#    echo "Lock file exists"
#    exit
#elif [[ \$t4 -gt 2 ]]; then
#    echo "Too many processes"
#    exit;
#fi
#touch \$lock_file
#
#source /etc/profile
#module load Perl
#module load singularity
#source /home/groups/efi/apps/perl_env.sh
#
#
#source $configFile
#mkdir -p $cyScriptDir
#mkdir -p $cyTempDir
#
#
#\$CY_APP_HOME/bin/cyto_job_server.pl \
#    --db \$JOB_INFO_DB \
#    --script-dir \$CYTO_DIR/scripts \
#    --temp-dir \$CYTO_DIR/temp \
#    --cyto-util-dir \$CY_UTIL_DIR \
#    --py4cy-image \$CY_APP_SIF_IMAGE \
#    --max-cy-jobs \$CY_NUM_JOBS \
#    --log-file \$CYTO_DIR/log.txt \
#    --cyto-config-home \$CY_CONFIG_HOME \
#    --cyto-app \$CY_APP_MASTER \
#    --image-conf $imageConf \
#    --queue $self->{queue} \
#    --ssn-root-dir \$LOAD_DIR \
#    --overwrite-images --cyto-multiple-mode --job-prefix cyto --cyto-job-uses \$CY_NUM_USES
#
#rm \$lock_file
#    
#CMD
#    
#    open my $outFh, ">", $outputScript or die "Unable to write to $outputScript: $!";
#    $outFh->print($cmd);
#    close $outFh;
#}


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


sub updateCollectStatus {
    my $self = shift;
    my $optionFlag = shift // 0;

    my $dbh = &{$self->{get_dbh}}();
    my @jobs = $self->getCollectionJobs($dbh, $optionFlag);

    my $key = ($optionFlag & DataCollection::SPLIT) ? "split_job_id" : "collect_job_id";

    my @messages;

    #TODO: this is checking for the presence of the main ssn.xgmml not the diced parent one.  This needs to be fixed.
    foreach my $jobData (@jobs) {
        my $asid = $jobData->{as_id};
        #my $slurmFinished = checkSlurmStatus($jobData->{split_job_id}) and checkSlurmStatus($jobData->{collect_job_id});
        push @messages, "No data for $asid" and next if not $jobData->{$key};
        my @ids = split(m/,/, $jobData->{$key});
        my $slurmFinished = checkSlurmStatus(@ids);
        print "$asid $slurmFinished $jobData->{$key}\n";
        if ($slurmFinished) {
            my $finishFile = ($optionFlag & DataCollection::SPLIT) ? $jobData->{dir_path} . "/ssn.xgmml" : "";
            if ($finishFile and -f $finishFile) {
                push @messages, "$asid split finished";
                my $sql = "UPDATE collect_jobs SET split_finished = 1 WHERE as_id = '$asid'";
                do_sql($sql, $dbh, $self->{dry_run}, $self->{log_fh});
            } elsif ($optionFlag & DataCollection::COLLECT) {
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
    my $loadDir = shift;
    my $optionFlag = shift // 0;
    my $minClusterSize = shift // 3;
    my $jobPrefix = shift // "";

    my $messages1 = $self->makeCollect2($masterData, $collectScriptDir, $loadDir, $optionFlag, $minClusterSize, $jobPrefix);
    my $messages2 = [];
    if ($optionFlag & DataCollection::COLLECT) {
        # We pass XIMAGE here to process the image-only clusters.
        $messages2 = $self->makeCollect2($masterData, $collectScriptDir, $loadDir, $optionFlag | DataCollection::XIMAGE, $minClusterSize, $jobPrefix);
    }

    return [@$messages1, @$messages2];
}


sub makeCollect2 {
    my $self = shift;
    my $masterData = shift;
    my $collectScriptDir = shift;
    my $loadDir = shift;
    my $optionFlag = shift // 0;
    my $minClusterSize = shift // 3;
    my $jobPrefix = shift // "";

    my $isImageOnly = $optionFlag & DataCollection::XIMAGE;

    do_mkdir($collectScriptDir);

    my $collect = new DataCollection(script_dir => $collectScriptDir, overwrite => 0, queue => $self->{queue}, app_dir => $self->{app_dir}, dry_run => $self->{dry_run}, efi_tools_home => $self->{efi_tools_home});

    my $dbh = &{$self->{get_dbh}}();
    my @jobs = $self->getFinishedJobsForCollection($dbh, $optionFlag, $minClusterSize);

    foreach my $jobData (@jobs) {
        my $clusterId = $jobData->{cluster_id};
        my $asid = $jobData->{as_id};

        my $ssnDir = $jobData->{ca_ssn_dir};
        my $inputSsn = "$ssnDir/$jobData->{ca_ssn_name}";
        my $outputDir = "$loadDir/$clusterId"; 

        my $md = $masterData->{$clusterId};
        my $children = $md->{children};
        my $cd = {input_ca_dir => $ssnDir, input_ssn => $inputSsn, output_dir => $outputDir, children => $children};
        $cd->{input_cr_dir} = $jobData->{cr_dir} if not $isImageOnly;

        # No children (leaf) and multiple ascores ==> a diced SSN. A non-diced SSN will not have any ascores in the ascore field
        # (it has a primary_ascore value instead).
        #my $isDiced = (not @$children and @{$md->{ascores}} and not $isImageOnly);
        my $aaa = @{$md->{ascores}};
        my $isDiced = (@{$md->{ascores}} > 0 and not $isImageOnly);
        print("$asid $aaa is_diced=$isDiced is_image_only=$isImageOnly\n");
        print(Dumper($md));
        #$cd->{ascore} = $jobData->{ascore}; # Always dice, even if it's a parent cluster
        $cd->{ascore} = $jobData->{ascore} if $isDiced;

        #$self->{log_fh}->print("Undefined cluster ID") if not $clusterId;
        #$self->{log_fh}->print("Undefined ascore for $clusterId") if not $cd->{ascore};
        my $dirPath = "$loadDir/$clusterId";
        mkdir $dirPath if not -d $dirPath;
        if ($isDiced) {
            $dirPath .= "/dicing-$cd->{ascore}";
            mkdir $dirPath;
        }
        #} elsif ($cd->{ascore} != $md->{primary_ascore}) {
        #    # Skip copying every AS except the primary one since this cluster has sub-clusters.
        #    next;
        #}

        $collect->addCluster($asid, $clusterId, $cd, $dirPath, $optionFlag, $isDiced, $isImageOnly);
    }

    my ($jobMap, $messages) = $collect->finish($optionFlag, $jobPrefix);

    #TODO
    foreach my $asid (keys %$jobMap) {
        my $dirPath = $jobMap->{$asid}->{dir_path};
        my $sql = "";
        if ($optionFlag & DataCollection::COLLECT) {
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
    if ($collectFlag & DataCollection::SPLIT) {
        $collectClause = "(J.collect_finished = 1 AND (J.split_job_id IS NULL OR J.split_job_id = ''))";
    }
    my $crClause = "AND R.finished = 1";
    my $crDirPath = ", R.dir_path AS cr_dir";
    my $crJoin = "LEFT JOIN cr_jobs AS R on C.as_id = R.as_id";
    my $imageOnlyClause = "";
    if ($collectFlag & DataCollection::XIMAGE) {
        $crClause = "";
        $crDirPath = "";
        $crJoin = "";
        $imageOnlyClause = "AND A.image_only = 1";
    }

    my $sql = <<SQL;
SELECT C.as_id AS as_id, A.cluster_id AS cluster_id, A.uniref, A.ascore, C.dir_path AS ca_ssn_dir, C.ssn_name AS ca_ssn_name, A.image_only $crDirPath
    FROM ca_jobs AS C
    LEFT JOIN as_jobs AS A ON C.as_id = A.as_id
    $crJoin
    LEFT JOIN collect_jobs AS J ON C.as_id = J.as_id
WHERE A.finished = 1 AND C.finished = 1 AND C.max_cluster_size >= $minClusterSize $crClause AND $collectClause $imageOnlyClause
SQL
    return get_jobs_from_db($sql, $dbh, $self->{dry_run}, $self->{log_fh});
}


# Returns jobs that are still running
sub getCollectionJobs {
    my $self = shift;
    my $dbh = shift;
    my $collectFlag = shift;

    my $collectClause = "J.collect_finished = 0";
    if ($collectFlag & DataCollection::SPLIT) {
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


#
# Get the directory in the output structure that corresponds to the parent cluster (if diced) or the cluster (not diced).
# For example, if cluster-1-3 is diced, this would return "output_dir/cluster-1-3".
#
sub getFinishedJobPaths {
    my $self = shift;
    my $dbh = shift;
    my $masterData = shift;
    my $useDiced = shift;

    my @clusterIds;
    foreach my $clusterId (keys %$masterData) {
        my $kids = $masterData->{$clusterId}->{children};
        next if not $masterData->{$clusterId}->{job_id}; # Placeholder (i.e. parent cluster)
        push @clusterIds, $clusterId;
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
            push @messages, "Unable to find $job->{dir_path} for cluster $clusterId";
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
        next if not $masterData->{$clusterId}->{job_id}; # Placeholder (i.e. parent cluster)
        my $ascores = $masterData->{$clusterId}->{ascores};
        if (scalar @$ascores) {
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

