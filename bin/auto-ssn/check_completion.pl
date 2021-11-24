#!/bin/env perl

use strict;
use warnings;

use Getopt::Long;
use Data::Dumper;
use DBI;
use Capture::Tiny qw(capture);


die "Needs EFI tools directory; the EFI_GNN environment variable must be set.\n" if not $ENV{EFI_GNN};
die "Needs EFI tools directory; the EFI_EST environment variable must be set.\n" if not $ENV{EFI_EST};


my ($jobMasterDir, $optAaList, $debug);
my $result = GetOptions(
    "job-master-dir=s"      => \$jobMasterDir,
    "opt-aa-list=s"         => \$optAaList,
    "debug"                 => \$debug,
);


my $dbFile = "$jobMasterDir/data.sqlite";
my $dbh = DBI->connect("dbi:SQLite:dbname=$dbFile", "", "");
die "No database connection\n" if not $dbh;


my $caStartApp = "$ENV{EFI_GNN}/make_colorssn_job.pl";
my $crStartApp = "$ENV{EFI_EST}/create_cluster_conv_ratio_job.pl";
my @defaultArgs = ("--queue $ENV{EFI_QUEUE}");
$optAaList = "C" if not $optAaList;

my $asBaseOutDir = "$jobMasterDir/as_job";
mkdir $asBaseOutDir;
my $caBaseOutDir = "$jobMasterDir/ca_jobs";
mkdir $caBaseOutDir;
my $crBaseOutDir = "$jobMasterDir/cr_jobs";
mkdir $crBaseOutDir;


updateJobStatus($dbh, "as");
updateJobStatus($dbh, "ca");
updateJobStatus($dbh, "cr");


my @caJobs = getNewCAJobs($dbh);

foreach my $job (@caJobs) {
    my $asid = $job->{as_id};
    my $outDir = "$caBaseOutDir/output_$asid";
    mkdir $outDir;
    my $outSsn = "ssn.xgmml";

    my @args = (@defaultArgs);
    push @args, "--opt-msa-option CR,HMM,WEBLOGO";
    push @args, "--opt-min-seq-msa 5";
    push @args, "--opt-max-seq-msa 750";
    push @args, "--opt-aa-list $optAaList";
    push @args, "--ssn-in $job->{path}";
    push @args, "--ssn-out $outSsn";
    push @args, "--job-id $asid";

    print "Running CA for $asid\n";
    runJob($asid, \@args, $caStartApp, $outDir, "ca", "Color SSN job is:");
}


my @crJobs = getNewCRJobs($dbh);

foreach my $job (@crJobs) {
    my $asid = $job->{as_id};
    # Start up a convergence ratio job
    my $outDir = "$crBaseOutDir/output_$asid";
    mkdir $outDir;

    my @args = (@defaultArgs);
    push @args, "--ssn-in $job->{path}";
    push @args, "--output-path $outDir";
    push @args, "--ascore $job->{ascore}";

    print "Running CR for $asid\n";
    runJob($asid, \@args, $crStartApp, $outDir, "cr", "Wait for BLAST job is");
}







sub runJob {
    my $asid = shift;
    my $args = shift;
    my $startApp = shift;
    my $outDir = shift;
    my $pfx = shift;
    my $grepText = shift;

    my $appStart = $startApp . " " . join(" ", @$args);
    my $cmd = <<CMD;
source /etc/profile
module load efiest/devlocal
curdir=\$PWD
cd $outDir
$appStart
CMD
    print "$cmd\n" and return if $debug;

    my ($result, $err) = capture { system($cmd); };
    my @lines = split(m/[\n\r]+/, $result);
    if (grep m/$grepText/, @lines) {
        (my $jobNum = $lines[$#lines]) =~ s/\D//g;
        my $sql = "UPDATE ${pfx}_jobs SET started = 1, job_id = $jobNum WHERE as_id = '$asid'";
        $dbh->do($sql);
    } else {
        print STDERR "Unable to submit $pfx $asid job: $result|$err\n";
    }
}

 














sub getNewCAJobs {
    my $dbh = shift;

    my $sql = <<SQL;
SELECT C.as_id AS as_id, A.path AS path, A.ascore AS ascore
    FROM ca_jobs AS C
    LEFT JOIN as_jobs AS A ON C.as_id = A.as_id
WHERE A.finished = 1 AND (C.started = 0 OR C.started IS NULL)
SQL
    return getNewJobs($sql);
}


sub getNewCRJobs {
    my $dbh = shift;

    my $sql = <<SQL;
SELECT R.as_id AS as_id, C.path AS path, A.ascore AS ascore
    FROM cr_jobs AS R
    LEFT JOIN ca_jobs AS C ON R.as_id = C.as_id
    LEFT JOIN as_jobs AS A ON R.as_id = A.as_id
WHERE C.finished = 1 AND (R.started = 0 OR R.started IS NULL)
SQL
    return getNewJobs($sql);
}


sub getNewJobs {
    my $sql = shift;

    my $sth = $dbh->prepare($sql);
    $sth->execute;

    my @jobs;
    while (my $row = $sth->fetchrow_hashref) {
        push @jobs, {path => $row->{path}, as_id => $row->{as_id}, ascore => $row->{ascore}};
    }

    return @jobs;
}


sub updateJobStatus {
    my $dbh = shift;
    my $pfx = shift;

    my $sql = "SELECT * FROM ${pfx}_jobs WHERE (finished IS NULL OR finished = 0)";
    my $sth = $dbh->prepare($sql);
    $sth->execute;

    while (my $row = $sth->fetchrow_hashref) {
        my $jobId = $row->{job_id};
        my $dir = getOutputDir($row, $pfx);
        my $path = "$dir/ssn.xgmml";
        
        my $finFileExists = 0;
        if ($pfx eq "as") {
            my $ascore = $row->{ascore};
            $path = "$dir/auto_AS${ascore}_full_ssn.xgmml";
            $finFileExists = -f "$dir/stats.tab.completed";
        } else {
            $finFileExists = -f "$dir/1.out.completed";
        }

        if ($jobId and isJobFinished($jobId) and $finFileExists) {
            print "$row->{as_id} $pfx has finished\n";
            my $sql = "UPDATE ${pfx}_jobs SET finished = 1, path = '$path' WHERE as_id = '$row->{as_id}'";
            $dbh->do($sql);
        }
    }
}


sub isJobFinished {
    my $id = shift;
    my $cmd = "/usr/bin/sacct -n -j $id -o State";
    my $result = `$cmd`;
    my @lines = split(m/[\r\n]+/s, $result);
    return 0 if not scalar @lines;
    $lines[0] =~ s/\s//g;
    return 1 if $lines[0] eq "COMPLETED";
    return 0;
}


sub getOutputDir {
    my $row = shift;
    my $pfx = shift;

    my $dir = "";

    if ($pfx eq "ca") {
        $dir = "$caBaseOutDir/output_$row->{as_id}/output";
    } elsif ($pfx eq "cr") {
        $dir = "$crBaseOutDir/output_$row->{as_id}/output";
    } elsif ($pfx eq "as") {
        my $localDir = "eval-$row->{ascore}-$row->{min_len}-$row->{max_len}";
        $dir = "$asBaseOutDir/output/$localDir";
    }

    return $dir;
}


