#!/bin/env perl

use strict;
use warnings;

use FindBin;
use Getopt::Long;
use Data::Dumper;
use DBI;

use lib "$FindBin::Bin/../../lib";

use AutoPipeline qw(do_mkdir do_sql get_job_dir update_job_status run_job get_num_running_jobs get_jobs_from_db wait_lock);


die "Needs EFI tools directory; the EFI_GNN environment variable must be set.\n" if not $ENV{EFI_GNN};
die "Needs EFI tools directory; the EFI_EST environment variable must be set.\n" if not $ENV{EFI_EST};


my ($jobMasterDir, $optAaList, $optMinSeqMsa, $debug, $mode, $jobPrefix, $dryRun, $maxCrJobs, $perlEnv);
my $result = GetOptions(
    "master-dir=s"          => \$jobMasterDir,
    "opt-aa-list=s"         => \$optAaList,
    "debug"                 => \$debug,
    "dry-run|dryrun"        => \$dryRun,
    "mode=s"                => \$mode,
    "min-cluster-size=i"    => \$optMinSeqMsa,
    "job-prefix=s"          => \$jobPrefix,
    "max-cr-jobs=i"         => \$maxCrJobs,
    "perl-env=s"            => \$perlEnv,
);


my $dbFile = "$jobMasterDir/data.sqlite";
my $dbh = DBI->connect("dbi:SQLite:dbname=$dbFile", "", "");
die "No database connection\n" if not $dbh;

$perlEnv = $ENV{EFI_PERL_ENV} // ($perlEnv // "");


my $caStartApp = "$ENV{EFI_GNN}/make_colorssn_job.pl";
my $crStartApp = "$ENV{EFI_EST}/create_cluster_conv_ratio_job.pl";
my @defaultArgs = ("--queue $ENV{EFI_QUEUE},$ENV{EFI_MEMQUEUE}");
$optAaList = "C" if not $optAaList;
$optMinSeqMsa = 3 if not $optMinSeqMsa;
$maxCrJobs = 5 if not $maxCrJobs;


open my $logFh, ">>", "$jobMasterDir/sql_log.txt";
$logFh->print("=====================> Starting SQL log ", scalar(localtime), " <=====================\n\n");

#my $lockFile = "$jobMasterDir/.lock";
#print "Already running\n" and exit(0) if -e $lockFile;
#`touch $lockFile`;

my $checkFinishFileOnly = 1;
update_job_status($dbh, "as_jobs", "stats.tab.completed", $dryRun, $logFh, undef, $checkFinishFileOnly);
update_job_status($dbh, "ca_jobs", "1.out.completed", $dryRun, $logFh, \&checkNumNodes);
update_job_status($dbh, "cr_jobs", "1.out.completed", $dryRun, $logFh);


print "MODE $mode\n";

if ($mode =~ m/ca/) {
    my @caJobs = getNewCAJobs($dbh);
    
    my $maxCaJobs = 25;
    my $caJobCount = get_num_running_jobs($dbh, "ca_jobs", $dryRun);
    
    if ($caJobCount >= $maxCaJobs) {
        print "Too many running CA jobs ($caJobCount / $maxCaJobs) to start any new ones.\n";
    } else {
        foreach my $job (@caJobs) {
            last if $caJobCount ++ > $maxCaJobs;
        
            my $asid = $job->{as_id};
            my $uniref = $job->{uniref};
            my $clusterId = $job->{cluster_id};
            my $outDir = get_job_dir($jobMasterDir, $clusterId, $uniref) . "/ca_jobs/$asid";
            do_mkdir($outDir, $dryRun);
        
            my $ssnIn = $job->{input_ssn_dir} . "/" . $job->{input_ssn_name};
            print "Waiting for a lock\n";
            wait_lock($ssnIn) or die "Unable to get a lock on input ssn for CA: $ssnIn";
            print "Lock acquired\n";
            my $ssnOut = "ssn.xgmml";
        
            my @args = (@defaultArgs);
            my $jidSuffix = "co";
            my $jidGrepText = "Color SSN job is:";
            if (not $job->{image_only}) {
                push @args, "--opt-msa-option CR,HMM,WEBLOGO,HIST";
                push @args, "--opt-min-seq-msa $optMinSeqMsa";
                push @args, "--opt-max-seq-msa 750";
                push @args, "--opt-aa-list $optAaList";
                push @args, "--opt-aa-threshold 0.9,0.8,0.7,0.6,0.5,0.4,0.3,0.2,0.1";
                $jidSuffix = "ca";
                $jidGrepText = "HMM and stuff job is:";
            }
            push @args, "--ssn-in $ssnIn";
            push @args, "--ssn-out $ssnOut";
            my $jid = "${asid}_$jidSuffix";
            $jid = "${jobPrefix}_$jid" if $jobPrefix;
            push @args, "--job-id $jid";
        
            print "Running CA for $asid\n";
            my $jobNum = run_job($asid, \@args, $caStartApp, $outDir, "ca_jobs", $jidGrepText, $dryRun, $perlEnv);
            if ($jobNum) {
                my $sql = "INSERT INTO ca_jobs (as_id, started, job_id, dir_path, ssn_name) VALUES ('$asid', 1, $jobNum, '$outDir/output', '$ssnOut')";
                do_sql($sql, $dbh, $dryRun, $logFh);
            }
        }
    }
}

#unlink($lockFile) and exit(0) if $noCr;

if ($mode =~ m/cr/) {
    my @crJobs = getNewCRJobs($dbh);
    
    my $crJobCount = get_num_running_jobs($dbh, "cr_jobs", $dryRun);
    #my $crJobCount = get_running_jobs("wait_CR_");
    
    if ($crJobCount >= $maxCrJobs) {
        print "Too many running CR jobs to start any new ones.\n";
    } else {
        foreach my $job (@crJobs) {
            last if $crJobCount ++ > $maxCrJobs;
        
            my $asid = $job->{as_id};
            my $uniref = $job->{uniref};
            my $clusterId = $job->{cluster_id};
            my $outDir = get_job_dir($jobMasterDir, $clusterId, $uniref) . "/cr_jobs/$asid";
            do_mkdir($outDir, $dryRun);
        
            my $ssnIn = $job->{input_ssn_dir} . "/" . $job->{input_ssn_name};
            wait_lock($ssnIn) or die "Unable to get a lock on input ssn for CR: $ssnIn";
        
            (my $crid = $asid) =~ s/(mega_|cluster_)//;
            $crid =~ s/AS//;
            $crid =~ s/ur//;
            my @args = (@defaultArgs);
            push @args, "--ssn-in $ssnIn";
            push @args, "--output-path $outDir";
            push @args, "--ascore $job->{ascore}";
            push @args, "--job-id $crid";
        
            print "Running CR for $asid\n";
            my $jobNum = run_job($asid, \@args, $crStartApp, $outDir, "cr_jobs", "Wait for BLAST job is", $dryRun, $perlEnv);
            if ($jobNum) {
                my $sql = "INSERT INTO cr_jobs (as_id, started, job_id, dir_path) VALUES ('$asid', 1, $jobNum, '$outDir/output')";
                do_sql($sql, $dbh, $dryRun, $logFh);
            }
        }
    }
}


#unlink($lockFile);





sub getNewCAJobs {
    my $dbh = shift;

#    my $sql = <<SQL;
#SELECT C.as_id AS as_id, A.dir_path AS input_ssn_dir, A.ssn_name AS input_ssn_name, A.cluster_id AS cluster_id, A.uniref
#    FROM ca_jobs AS C
#    LEFT JOIN as_jobs AS A ON C.as_id = A.as_id
#WHERE A.finished = 1 AND (C.started = 0 OR C.started IS NULL)
#SQL
    my $sql = <<SQL;
SELECT A.as_id AS as_id, A.dir_path AS input_ssn_dir, A.ssn_name AS input_ssn_name, A.cluster_id AS cluster_id, A.uniref, A.image_only
    FROM as_jobs AS A
    LEFT JOIN ca_jobs AS C ON A.as_id = C.as_id
WHERE A.finished = 1 AND (C.started = 0 OR C.started IS NULL)
SQL
    return get_jobs_from_db($sql, $dbh, $dryRun, $logFh);
}


sub getNewCRJobs {
    my $dbh = shift;

#    my $sql = <<SQL;
#SELECT R.as_id AS as_id, C.dir_path AS input_ssn_dir, C.ssn_name AS input_ssn_name, A.ascore AS ascore, A.cluster_id AS cluster_id, A.uniref
#    FROM cr_jobs AS R
#    LEFT JOIN ca_jobs AS C ON R.as_id = C.as_id
#    LEFT JOIN as_jobs AS A ON R.as_id = A.as_id
#WHERE C.finished = 1 AND (R.started = 0 OR R.started IS NULL)
#SQL
    my $sql = <<SQL;
SELECT C.as_id AS as_id, C.dir_path AS input_ssn_dir, C.ssn_name AS input_ssn_name, A.ascore AS ascore, A.cluster_id AS cluster_id, A.uniref
    FROM ca_jobs AS C
    LEFT JOIN cr_jobs AS R ON C.as_id = R.as_id
    LEFT JOIN as_jobs AS A ON C.as_id = A.as_id
WHERE C.finished = 1 AND C.max_cluster_size >= $optMinSeqMsa AND (R.started = 0 OR R.started IS NULL) AND A.image_only = 0
SQL
    return get_jobs_from_db($sql, $dbh, $dryRun, $logFh);
}


sub checkNumNodes {
    my $job = shift;

    my $dirPath = $job->{dir_path};
    my $filePath = "$dirPath/cluster_sizes.txt";
    my $colNum = 2; #0 = cluster number, 1 = uniprot cluster size, 2 = uniref90 cluster size

    my $max = 0;

    open my $fh, "<", $filePath or die "Unable to open $filePath to find num nodes";
    scalar <$fh>;
    while (my $line = <$fh>) {
        chomp $line;
        my @p = split(m/\t/, $line);
        $max = $p[$colNum] if $p[$colNum] > $max;
    }
    close $fh;

    return ("max_cluster_size = $max");
}


