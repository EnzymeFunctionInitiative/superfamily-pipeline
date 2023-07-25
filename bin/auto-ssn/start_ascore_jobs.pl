#!/bin/env perl

use strict;
use warnings;

use FindBin;
use Getopt::Long;
use DBI;
use File::Path qw(mkpath);
use Data::Dumper;

use lib "$FindBin::Bin/../../lib";

use AutoPipeline qw(do_mkdir do_sql get_job_dir get_num_running_jobs);
use MasterFile qw(parse_master_file);


die "Needs EFI tools directory; the EFI_EST environment variable must be set.\n" if not $ENV{EFI_EST};
die "Needs EFI database module name; the EFI_DB_MOD environment variable must be set.\n" if not $ENV{EFI_DB_MOD};


my ($ssnJobPath, $jobMasterDir, $ascoreList);
my ($webtoolsDbConf);
my ($dryRun, $removeExisting);
my ($masterFile, $masterInputDir, $unirefVersion, $jobPrefix);
my $result = GetOptions(
    "mode-1-ssn-job-dir=s"  => \$ssnJobPath,        # Mode 1  - directory of a specific SSN job
    "master-output-dir=s"   => \$jobMasterDir,      # Mode 1, 2
    "mode-1-ascores=s"      => \$ascoreList,        # Mode 1
    "mode-2-master-file=s"  => \$masterFile,        # Mode 2
    "mode-2-input-dir=s"    => \$masterInputDir,    # Mode 2  - directory where all of the jobs specified in the masterFile are located
    "webtools-db-conf=s"    => \$webtoolsDbConf,
    "remove-existing"       => \$removeExisting,
    "job-prefix=s"          => \$jobPrefix,
    "uniref-version=i"      => \$unirefVersion,
    "dry-run|dryrun"        => \$dryRun,
);


my $mode = ($masterFile ? 2 : 1);

die "Requires --ssn-job-path as input ($ssnJobPath) ($masterFile)\n" if $mode == 1 and ((not $ssnJobPath or not -d $ssnJobPath) and (not $masterFile or not -f $masterFile));
die "Requires --master-output-dir, and it must exist ($jobMasterDir)\n" if not $jobMasterDir or not -d $jobMasterDir;
die "Requires --mode-1-ascores (list of AS)\n" if (not $ascoreList and (not $masterFile or not -f $masterFile));

$unirefVersion = 90 if not $unirefVersion;

#my $lockFile = "$jobMasterDir/.lock";
#print "Already running\n" and exit(0) if -e $lockFile;
#`touch $lockFile`;


my $dbFile = "$jobMasterDir/data.sqlite";
my $dbh;
if (not $dryRun) {
    $dbh = DBI->connect("dbi:SQLite:dbname=$dbFile", "", "");
    #unlink($lockFile);
    die "No database connection\n" if not $dbh;
    createSchema();
}



my $startApp = "$ENV{EFI_EST}/create_analysis_job.pl";
my @defaultArgs = ("--filter eval", "--queue $ENV{EFI_QUEUE}", "--scheduler slurm", "--no-repnode", "--tmp output", "--keep-xgmml", "--maxfull 1000000000");


my $webDbh;
if ($webtoolsDbConf) {
    my $info = getDbConf($webtoolsDbConf);
    my $dsn = "DBI:mysql:database=$info->{database};host=$info->{host}";
    $webDbh = DBI->connect($dsn, $info->{username}, $info->{password});
}



if ($mode == 1) {
    my @mode1ascores = parseMode1Ascores($ascoreList);
    startAscoreJobs($ssnJobPath, $jobMasterDir, "auto", "", \@mode1ascores);
    #unlink($lockFile);
    exit(0);
}


my $masterData = parse_master_file($masterFile);

foreach my $clusterId (keys %$masterData) {
    my $md = $masterData->{$clusterId};
    my $clusterJobPath = get_job_dir($jobMasterDir, $clusterId, $unirefVersion);
    do_mkdir($clusterJobPath, $dryRun) if not -d $clusterJobPath;
    my $inDir = "$masterInputDir/" . $md->{job_id} . "/output";

    my $isDiced = @{$md->{children}} == 0 and @{$md->{ascores}};;
    my $finish = startAscoreJobs($inDir, $clusterJobPath, $clusterId, $md->{cluster_name}, $md, $isDiced);
    last if $finish;
}


#unlink($lockFile);



sub startAscoreJobs {
    my $jobPath = shift;
    my $masterDir = shift;
    my $clusterId = shift;
    my $clusterName = shift;
    my $ascoreData = shift;
    my $isDiced = shift;

    my $ssnNewParentDir = "$masterDir/as_job";
    do_mkdir($ssnNewParentDir, $dryRun);
    my $generateDir = "$ssnNewParentDir/output";
    do_mkdir($generateDir, $dryRun);
    
    my $uniref = checkForUniRef($jobPath);
    my @asJobArgs = @defaultArgs;
    push @asJobArgs, "--uniref-version $uniref" if $uniref;

    runShellCommand("rm -rf $generateDir/AS-*") if $removeExisting;
    if ($removeExisting or not -f "$generateDir/1.out.completed") {
        runShellCommand("rsync -a --exclude='*/' $jobPath/ $generateDir");
    }

    my @ascores;
    if (not $isDiced) {
        @ascores = ($ascoreData->{primary_ascore});
    } else {
        @ascores = @{$ascoreData->{ascores}};
        unshift @ascores, $ascoreData->{primary_ascore} if $ascores[0] != $ascoreData->{primary_ascore};
    }

    my $maxAsJobs = 25;
    my $asJobCount = get_num_running_jobs($dbh, "as_jobs", $dryRun);

    if ($asJobCount >= $maxAsJobs) {
        print "Too many running AS jobs to start any new ones.\n";
        return 1;
    }

    foreach my $ascore (@ascores) {

        my ($as, $minLen, $maxLen);
        if ($mode == 1) {
            $as = $ascore->[0];
            $minLen = $ascore->[1];
            $maxLen = $ascore->[2];
        } else {
            $as = $ascore;
            $minLen = $ascoreData->{min_len};
            $maxLen = $ascoreData->{max_len};
        }

        my $status = startJob($ssnNewParentDir, $generateDir, $clusterId, $clusterName, $uniref, $as, $minLen, $maxLen, \@asJobArgs, $isDiced);
        next if not $status;

        # Exit the loop if we are running the max number of jobs already.
        last if $asJobCount++ > $maxAsJobs;

    }

    return ($asJobCount >= $maxAsJobs);
}


sub startJob {
    my ($ssnNewParentDir, $generateDir, $clusterId, $clusterName, $uniref, $ascore, $minLen, $maxLen, $asJobArgs, $isDiced) = @_;

    #my $evalDirName = "eval-$ascore-$minLen-$maxLen";
    my $outputDirName = "AS-$ascore";
    my $outputPath = "$generateDir/$outputDirName";
    # Skip this one if it is already done.
    return if -f "$outputPath/stats.tab.completed";

    my $asid = lc($clusterId =~ s/[^a-z0-9_]/_/igr);
    $asid .= "_AS$ascore";
    #$asid .= "_ur$uniref" if $uniref;
    my $title = makeTitle($clusterId, $ascore, $uniref); #"${clusterId}_$asid";

    if ($removeExisting and -d $outputPath) {
        runShellCommand("rm -rf $outputPath");
    }

    my @args = @$asJobArgs;
    push @args, "--minlen $minLen";
    push @args, "--maxlen $maxLen";
    push @args, "--title $title";
    #push @args, "--job-id ${asid}_as"; # This becomes the file name
    push @args, "--minval $ascore";
    push @args, "--job-id $jobPrefix" if $jobPrefix;
    push @args, "--output-path $outputPath";

    my $appStart = $startApp . " " . join(" ", @args);
    my $cmd = <<CMD;
source /etc/profile
module load efiest/devlocal
module load $ENV{EFI_DB_MOD}\n
curdir=\$PWD
cd $ssnNewParentDir
# The PWD and cd stuff is because the scripts assume they are being run from the SSN directory.
$appStart
CMD
    my $result = runShellCommand("$cmd");
    print "Starting $title $outputDirName\n";
    if (not $dryRun) {
        my @lines = split(m/[\n\r]+/i, $result);
        my $foundStatsLine = 0;
        my $jobNum = 0;
        foreach my $line (@lines) {
            if ($line =~ m/Stats job is/i) {
                $foundStatsLine = 1;
            } elsif ($foundStatsLine) {
                ($jobNum = $line) =~ s/\D//g;
                last;
            }
        }
        my $ssnOutName = "${title}_full_ssn.xgmml";
        my $sql = "INSERT OR REPLACE INTO as_jobs (as_id, cluster_id, cluster_name, ascore, uniref, job_id, started, dir_path, ssn_name) VALUES ('$asid', '$clusterId', '$clusterName', $ascore, $uniref, $jobNum, 1, '$outputPath', '$ssnOutName')";
        do_sql($sql, $dbh, $dryRun);
    }
}


sub makeTitle {
    my ($clusterId, $asid, $uniref) = @_;
    my $title = "${clusterId}_$asid";
    $title .= "_ur$uniref" if $uniref;
    return $title;
}


sub createSchema {
    my $sql = "CREATE TABLE IF NOT EXISTS as_jobs (as_id TEXT, cluster_id TEXT, cluster_name TEXT, ascore INT, uniref INT, started INT, finished INT, job_id INT, dir_path TEXT, ssn_name TEXT, PRIMARY KEY(as_id))";
    do_sql($sql, $dbh, $dryRun);
    $sql = "CREATE TABLE IF NOT EXISTS ca_jobs (as_id TEXT, started INT, finished INT, job_id INT, dir_path TEXT, ssn_name TEXT, PRIMARY KEY(as_id))";
    do_sql($sql, $dbh, $dryRun);
    $sql = "CREATE TABLE IF NOT EXISTS cr_jobs (as_id TEXT, started INT, finished INT, job_id INT, dir_path TEXT, PRIMARY KEY(as_id))";
    do_sql($sql, $dbh, $dryRun);
    $sql = "CREATE TABLE IF NOT EXISTS collect_jobs (as_id TEXT, dir_path TEXT, started INT, finished INT, split_job_id INT, collect_job_id INT, PRIMARY KEY(as_id))";
    do_sql($sql, $dbh, $dryRun);
}


sub runShellCommand {
    my @o;
    foreach my $cmd (@_) {
        if ($dryRun) {
            print $cmd, "\n";
        } else {
            my $o = `$cmd`;
            push @o, $o;
        }
    }
    return join("\n", @o);
}


sub printHeader {
    ##!/bin/bash
    ##SBATCH --partition=efi
    ##SBATCH --nodes=1
    ##SBATCH --tasks-per-node=1
    ##SBATCH --mem=25gb
    ##SBATCH --job-name="get_gnds"
    ##SBATCH --kill-on-invalid-dep=yes
    ##SBATCH -o /igbgroup/n-z/noberg/dev/hmm/pipeline/ip82/load-3.0/scripts/get_gnds.sh.stdout.%j
    ##SBATCH -e /igbgroup/n-z/noberg/dev/hmm/pipeline/ip82/load-3.0/scripts/get_gnds.sh.stderr.%j
}


sub parseMode1Ascores {
    my $as = shift;

    my @ascores;

    my @as = split(m/,/, $as);
    foreach my $p (@as) {
        my @p = split(m/:/, $p);
        my $data = [$p[0], 0, 60000];
        $data->[1] = $p[1] if $p[1];
        $data->[2] = $p[2] if $p[2];
        push @ascores, $data;
    }

    return @ascores;
}


sub checkForUniRef {
    my $jobDir = shift;
    my $match = `head -200 $jobDir/fasta.metadata | grep -m 1 UniRef`;
    return "50" if $match =~ m/UniRef50/;
    return "90" if $match =~ m/UniRef90/;
    return "";
}


sub getDbConf {
    my $file = shift;
    open my $fh, "<", $file or die "Unable to open file $file: $!";
    my $conf = {};
    while (<$fh>) {
        chomp;
        if (m/^([^=]+)=(.*)\s*$/) {
            $conf->{$1} = $2;
        }
    }
    close $fh;
    return $conf;
}



