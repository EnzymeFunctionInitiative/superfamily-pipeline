#!/bin/env perl

use strict;
use warnings;

use Getopt::Long;
use Data::Dumper;
use FindBin;

print STDERR "DONE\n";
use lib "$FindBin::Bin/../lib";

use IdListParser;

my $AppDir = $FindBin::Bin;


my ($dataDir, $jobIdListFile, $scriptFile, $jobResultsDir, $jobType);
my $result = GetOptions(
    "data-dir=s"        => \$dataDir,
    "job-id-file=s"     => \$jobIdListFile,
    "script-file=s"     => \$scriptFile,
    "job-results-dir=s" => \$jobResultsDir,
    "type=s"            => \$jobType,
);

die "Need --data-dir" if not $dataDir or not -d $dataDir;
die "Need --job-id-file" if not $jobIdListFile or not -f $jobIdListFile;
die "Need --script-file" if not $scriptFile;
die "Need --job-results-dir" if not $jobResultsDir;
die "Need --type" if not $jobType;


my $clusters = {};

IdListParser::parseFile($jobIdListFile, \&parseLine);

my $outDirName = $jobType eq "nc" ? "nctemp" : "hmmtemp";


open my $outFh, ">", $scriptFile or die "Unable to write to $scriptFile: $!";

writeLine("module load efignt/devlocal");

foreach my $cluster (keys %$clusters) {
    my $jobId = $clusters->{$cluster}->{job_id};
    my $ssnDir = "$jobResultsDir/$jobId/output";
    my ($ssnFile) = glob("$ssnDir/*_coloredssn.xgmml");
    ($ssnFile) = glob("$ssnDir/*_coloredssn.zip") if not $ssnFile;
    warn "Unable to find $ssnDir" and next if not $ssnFile;

    my $outDir = "$dataDir/$cluster/$outDirName";
    writeLine("mkdir -p $outDir");

    if ($ssnFile =~ m/\.zip$/) {
        writeLine("unzip -p $ssnFile > $outDir/ssn.xgmml");
        $ssnFile = "$outDir/ssn.xgmml";
    }

    (my $jobName = $cluster) =~ s/\-//g;
    $jobName = "diced_$jobName";

    writeLine("cd $outDir");
    writeLine(getJobLine($ssnFile, $outDir, $jobName));
    writeLine("");
}

$outFh->close();




sub getJobLine {
    my ($ssnFile, $outDir, $jobName) = @_;
    $jobName //= "diced";
    my $sched = "--scheduler slurm --queue efi";
    if ($jobType eq "nc") {
        return "create_nb_conn_job.pl --ssn-in $ssnFile --output-name nc --output-path $outDir --dump-only";
    } else {
        return "make_colorssn_job.pl --ssn-in $ssnFile --ssn-out $outDir/ssn.xgmml --opt-msa-option HMM --job-id $jobName $sched";
    }
}


sub writeLine {
    map { $outFh->print("$_\n"); } @_;
}


sub parseLine {
    my ($cluster, $parms) = @_;
    $clusters->{$cluster} = {job_id => $parms->{ssnId}};
}
