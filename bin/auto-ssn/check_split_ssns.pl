#!/bin/env perl

use strict;
use warnings;

use FindBin;
use Data::Dumper;
use File::Find;
use FindBin;
use Getopt::Long;

use lib "$FindBin::Bin/../../lib";

use AutoPipeline qw(do_mkdir do_sql get_job_dir);
use IdListParser;


my ($masterDir, $dryRun);
my $result = GetOptions(
    "master-dir=s"      => \$jobMasterDir,
    "dry-run|dryrun"    => \$dryRun,
);

die "Need --master-dir" if not $jobMasterDir or not -d $jobMasterDir;

my $dbFile = "$jobMasterDir/data.sqlite";
my $dbh = DBI->connect("dbi:SQLite:dbname=$dbFile", "", "");
die "No database connection\n" if not $dbh;

createSchema();


my $app = "$FindBin::Bin/../split_ssn.pl";


update_job_status("split_ssns");

my @newJobs = getNewSplitJobs($dbh);

foreach my $job (@newJobs) {
    my $asid = $job->{as_id};
    my $inputSsn = $job->{input_ssn_dir} . "/" . $job->{input_ssn_path};
    my $outputDir = get_job_dir($jobMasterDir, $job->{cluster_id}, $job->{uniref});
    $outputDir .= "/split_ssns/" . $asid;
    my $fileClusterId = make_file_cluster_id($job->{cluster_id});
    my $jobNum = makeScript($inputSsn, $outputDir, $fileClusterId);
    my $sql = "INSERT OR REPLACE INTO split_ssns SET started = 1, job_id = $jobNum WHERE as_id = '$asid'";
    do_sql($sql, $dbh, $dryRun);
}






sub makeScript {
    my $inputSsn = shift;
    my $outputDir = shift;
    my $fileClusterId = shift;

    my $outputDirPat = "$outputDir/$fileClusterId-";




sub createSchema {
    my $sql = "CREATE TABLE IF NOT EXISTS split_ssns (as_id TEXT, started INT, finished INT, job_id INT, dir_path TEXT, ssn_name TEXT, PRIMARY KEY(as_id))";
    do_sql($sql, $dbh, $dryRun);
}




if ($isDiced) {
    #TODO: implement this
#    #find(\&findDicedSsnWanted, $inDir);
#    my @dirs = glob("$inDir/cluster-*");
#    foreach my $dir (@dirs) {
#        my @ddirs = glob("$dir/dicing-*");
#        next if not scalar @ddirs;
#
#        (my $num = $dir) =~ s%^.*/cluster-([\d\-]+)$%$1%;
#        my $sname = "${outScript}_$num";
#        open my $fh, ">", "$sname.sh";
#        $outFh = $fh;
#        $outFh->print(getHeader($sname));
#        foreach my $ddir (@ddirs) {
#            processDicedSsn("$ddir/ssn.zip", $ddir);
#        }
#        $outFh->close();
#    }
} else {
    my %data;
    
    open my $inFh, "<", $jobFile or die "Unable to read $jobFile: $!";
    while (<$inFh>) {
        chomp;
        next if m/^\s*$/ or m/^#/;
        my ($cluster, $parms) = IdListParser::parseLine($_);
        my @sourceNum = @{ $parms->{expandClusters} };
        my $sourceNum = $sourceNum[0][0] // 1;
        
        (my $clusterNum = $cluster) =~ s%^.*cluster[\-\d]*-(\d+)$%$1%;
        my $subCluster = $sourceNum;
        my $subClusterRemap = "$sourceNum:$clusterNum";
        
        my $colorDir = "$inDir/$parms->{ssnId}/output";
        my $targetDir = "$outDir/$cluster";
        my $ssnTempZip = "$targetDir/ssn_temp.zip";
        my $ssnTempXgmml = "$targetDir/ssn_temp.xgmml";
        writeSplitSsnJobLine("cp $colorDir/*_coloredssn.zip $ssnTempZip");
        writeSplitSsnJobLine("unzip -p $ssnTempZip > $ssnTempXgmml");
        writeSplitSsnJobLine("$app \\\n    --ssn-in $ssnTempXgmml \\\n    --sub-cluster $subClusterRemap \\\n    --output-file $targetDir/ssn.xgmml");
        writeSplitSsnJobLine("rm $ssnTempZip $ssnTempXgmml\n");
    }
    close $inFh;

#    foreach my $clusterId (sort keys %data) {
#        # If a cluster is manually partitioned (e.g. a child cluster exists already)
#        # then we skip partitioning the current one and copy it instead.
#        print "Skipping $clusterId because a child already exists\n" and next if $data{"$clusterId-1"};
#
#        my $targetDir = "$outDir/$clusterId"; 
#        my $ssnDir = "$inDir/$data{$clusterId}->{filt_cid}/output";
#    
#        # If there is only one cluster, copy the file to the output dir.
#        my @xfiles = glob("$ssnDir/cluster-data/uniprot-nodes/cluster_Uni*.txt");
#        if (scalar @xfiles == 1) {
#            doCopy($clusterId, $ssnDir, $targetDir);
#            next;
#        }
#
#        my $dissect = $data{$clusterId}->{dissect} ? ("--sub-clusters " . join(",", @{ $data{$clusterId}->{dissect} })) : "";
#    
#        my $ssnZipFile = getSsnZipFile($clusterId, $ssnDir);
#        my $outFile = "$targetDir/ssn";
#        $outFh->print("\n\n# PROCESSING   $clusterId  ####################################################################################################\n");
#        $outFh->print("mkdir -p $targetDir\n") if not -d $targetDir;
#        $outFh->print("unzip -p $ssnZipFile > $outFile.xgmml\n");
#        $outFh->print("$app --ssn-in $outFile.xgmml --mkdir $dissect --output-dir-pat $targetDir-\n");
#        if (-f "$outFile.zip") {
#            $outFh->print("rm $outFile.xgmml");
#        } else {
#            $outFh->print("rm -f $outFile.zip\n");
#            $outFh->print("zip -j $outFile.zip $outFile.xgmml\n");
#        }
#    }
}


close $outFh if $outScript;









sub writeSplitSsnJobLine {
    $outFh->print(join("\n", @_), "\n");
}


sub doCopy {
    my $clusterId = shift;
    my $ssnDir = shift;
    my $targetDir = shift;
    
    my $outFile = "$targetDir/ssn";

    print "Skipping $clusterId because it's already copied\n" and return if -f "$outFile.zip" or -f "$outFile.xgmml";
    
    $outFh->print("mkdir -p $targetDir\n") if not -d $targetDir;

    my $ssnZipFile = getSsnZipFile($clusterId, $ssnDir);

    $outFh->print("unzip -p $ssnZipFile > $outFile.xgmml\n");
    $outFh->print("rm -f $outFile.zip\n");
    $outFh->print("zip $outFile.zip $outFile.xgmml\n");
    $outFh->print("rm $outFile.xgmml\n");
}


sub getSsnZipFile {
    my $clusterId = shift;
    my $ssnDir = shift;
    my ($ssnZipFile) = glob("$ssnDir/*_coloredssn.zip");
    die "Unable to find zip file for $clusterId in $ssnDir/*_coloredssn.zip" if not $ssnZipFile;
    return $ssnZipFile;
}


sub findDicedSsnWanted {
    processDicedSsn($File::Find::name, $File::Find::dir);
}
sub processDicedSsn {
    my ($name, $dir) = @_;
    if ($name =~ m%dicing-\d+/ssn.zip%) {
        (my $cluster = $dir) =~ s/^.*(cluster[\-0-9]+).*$/$1/;
        $outFh->print("unzip -p $name > $name.tmp\n");
        $outFh->print("$app --ssn-in $name.tmp --output-dir-pat $dir/$cluster-\n");
        $outFh->print("rm $name.tmp\n");
    }
}


sub getHeader {
    my $logName = shift;
    return <<HEADER;
#!/bin/bash
#SBATCH --partition=efi-mem,efi
#SBATCH --nodes=1
#SBATCH --tasks-per-node=1
#SBATCH --mem=250gb
#SBATCH --job-name="split_ssns"
#SBATCH --kill-on-invalid-dep=yes
#SBATCH -o $logName.sh.stdout.%j
#SBATCH -e $logName.sh.stderr.%j
#set -e
module load Perl
export EFI_TOOLS_HOME=/home/n-z/noberg/dev/EFITools

HEADER
}


sub getDissect {
    my $str = shift;
    $str =~ s/ //g;
    my @p = split(m/,/, $str);
    my @clusters;
    for (my $pi = 0; $pi <= $#p; $pi++) {
        if ($p[$pi] =~ m/^[\d:]+$/) {
            push @clusters, $p[$pi];
        } elsif ($p[$pi] =~ m/\-/) {
            #my @s = split(m/\-/, $p[$pi]);
            my ($s, $e) = split(m/\-/, $p[$pi]);
            for (; $s <= $e; $s++) {
                push @clusters, $s;
            }
            #for (my $si = 0; $si <= $#s; $si++) {
            #    push @clusters, $s[$si];
            #}
        }
    }
    return @clusters;
}

