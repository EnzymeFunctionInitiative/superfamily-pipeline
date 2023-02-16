#!/bin/env perl

use strict;
use warnings;

die "Needs EFI directory; the EFI_EST environment variable must be set.\n" if not $ENV{EFI_EST};
die "Needs EFI_TOOLS_HOME directory; the EFI_TOOLS environment variable must be set.\n" if not $ENV{EFI_TOOLS_HOME};
die "Needs EFI database module name; the EFI_DB_MOD environment variable must be set.\n" if not $ENV{EFI_DB_MOD};

use FindBin;
use Getopt::Long qw(:config pass_through);
use DBI;
use Data::Dumper;
use Capture::Tiny qw(capture);

use lib "$FindBin::Bin/../lib";

use AutoPipeline qw(do_sql);
use MasterFile qw(parse_master_file);
use DataCollection;
use DataCollection::Commands;
use MasterActions;
use IdListParser;


my ($jobMasterDir, $masterMetaFile, $action, $unirefVersion, $queue, $dryRun, $showHelp, $lockFileName);
my $result = GetOptions(
    "master-dir=s"          => \$jobMasterDir,
    "master-file=s"         => \$masterMetaFile,
    "action=s"              => \$action,
    "uniref-version=i"      => \$unirefVersion,
    "queue=s"               => \$queue,
    "dry-run"               => \$dryRun,
    "help"                  => \$showHelp,
    "lock-file-name=s"      => \$lockFileName,
);


if ($showHelp) {
    showHelp();
    exit(0);
}


die "Need --master-dir\n" if not $jobMasterDir or not -d $jobMasterDir;
die "Need --master-file $masterMetaFile\n" if not $masterMetaFile or not -f $masterMetaFile;
die "Need --action\n" if not $action;
die "Need --queue\n" if not $queue or not $ENV{EFI_QUEUE};

#my %ACTIONS = ("start-ascores" => 1, "check-completion-ca" => 1, "check-completion-cr" => 1, "make-metadata" => 1, "make-collect" => 1);
#die "Invalid --action" if not $ACTIONS{$action};


my $dbFile = "$jobMasterDir/data.sqlite";
my $collectScriptDir = "$jobMasterDir/collect_scripts";
my $appDir = $FindBin::Bin;
$queue = $ENV{EFI_QUEUE} if not $queue;
$unirefVersion = 90 if not $unirefVersion;
$lockFileName = ".lock" if not $lockFileName;


my $lockFile = "$jobMasterDir/$lockFileName";
print "Already running master.pl\n" and exit(0) if -e $lockFile;
`touch $lockFile`;

my $logFile = "$jobMasterDir/log.txt";
open my $logFh, ">>", $logFile;
my $logger = new MasterLogger($logFh);


my $masterData = parse_master_file($masterMetaFile);
my $actions = new MasterActions(dry_run => $dryRun, get_dbh => getDbhFn(), app_dir => $appDir, efi_tools_home => $ENV{EFI_TOOLS_HOME}, log_fh => $logger, queue => $queue);

print_log("=====================> Starting master.pl log " . scalar(localtime) . " <=====================");
print_log("ACTION: $action");


if ($action eq "start-ascores") {
    initDatabase($dbFile, 1);

    my ($generateDir);
    GetOptions("--generate-dir=s" => \$generateDir);
    print "Needs --generate-dir\n" and do_exit(1) if not $generateDir or not -d $generateDir;

    my $app = "$appDir/auto-ssn/start_ascore_jobs.pl";
    my @args;
    push @args, "--master-output-dir $jobMasterDir";
    push @args, "--mode-2-master-file $masterMetaFile";
    push @args, "--mode-2-input-dir $generateDir";
    push @args, "--uniref-version $unirefVersion";
    push @args, "--dry-run" if $dryRun;
    my $args = join(" ", @args);

    my $cmd = "$app $args";
    my ($result, $err) = capture { system($cmd); };
    print_log("WARNING: There was an error running $app $args: $err") and do_exit(1) if $err;
    print_log($result);


#########################################################################################
} elsif ($action eq "check-completion") {
    initDatabase($dbFile, 1);

    my ($checkType, $minClusterSize, $maxCrJobs);
    GetOptions("--check-type=s", \$checkType, "--min-cluster-size=i" => \$minClusterSize, "max-cr-jobs=i" => \$maxCrJobs);
    print "Need --check-type = ca|cr|ca+cr\n" and do_exit(1) if not $checkType;

    my $app = "$appDir/auto-ssn/check_completion.pl";
    my @args;
    push @args, "--master-dir $jobMasterDir";
    push @args, "--mode $checkType";
    push @args, "--dry-run" if $dryRun;
    push @args, "--min-cluster-size $minClusterSize" if $minClusterSize;
    push @args, "--max-cr-jobs $maxCrJobs" if $maxCrJobs;

    my $args = join(" ", @args);
    my $cmd = "$app $args";
    print_log($cmd);

    my ($result, $err) = capture { system($cmd); };
    print_log("WARNING: There was an error running $app $args: $err") and do_exit(1) if $err;
    print_log($result);


#########################################################################################
} elsif ($action eq "cytoscape") {
    my ($cytoConfig, $outputRunScript, $ssnListFileName);
    GetOptions("--cyto-config=s" => \$cytoConfig, "--cyto-run-script=s" => \$outputRunScript, "--ssn-list-file-name=s" => \$ssnListFileName);
    $actions->writeCytoscapeScript($jobMasterDir, $cytoConfig, $outputRunScript, "", $ssnListFileName);


#########################################################################################
} elsif ($action eq "make-ssn-list") {
    my ($outputCollectDir, $outputFile);
    GetOptions("--output-collect-dir=s" => \$outputCollectDir, "--ssn-list=s" => \$outputFile);
    #print "Needs --output-collect-dir\n" and do_exit(1) if not $outputCollectDir or not -d $outputCollectDir;
    print "Needs --ssn-list\n" and do_exit(1) if not $outputFile;

    my @files = $actions->listSsnFiles();
    open my $fh, ">", $outputFile or die "Unable to write to $outputFile: $!";
    foreach my $file (@files) {
        $fh->print("$file\n");
    }
    close $fh;


#########################################################################################
} elsif ($action eq "make-collect") {
    my ($outputCollectDir, $splitSsns, $minClusterSize);
    GetOptions("--output-collect-dir=s" => \$outputCollectDir, "--split-ssns" => \$splitSsns, "min-cluster-size=i" => \$minClusterSize);
    print "Needs --output-collect-dir\n" and do_exit(1) if not $outputCollectDir or not -d $outputCollectDir;

    $minClusterSize = 3 if not $minClusterSize;
    my $flag = $splitSsns ? DataCollection::SPLIT : DataCollection::COLLECT;
    my $messages = $actions->makeCollect($masterData, $collectScriptDir, $outputCollectDir, $flag, $minClusterSize);
    print_log(@$messages);


#########################################################################################
} elsif ($action eq "check-collect") {
    my ($splitSsns);
    GetOptions("--split-ssns" => \$splitSsns);

    my $flag = $splitSsns ? DataCollection::SPLIT : DataCollection::COLLECT;
    my $messages = $actions->checkCollectFinished($flag);
    print_log(@$messages);


#########################################################################################
} elsif ($action eq "build-db" or $action eq "build-db-post") {
    my $isSizeOnly = $action eq "build-db-post";
    my ($outputCollectDir, $efiDb, $ecDescFile, $getMetaOnly, $supportFiles);
    GetOptions("--output-collect-dir=s" => \$outputCollectDir, "--get-metadata-only" => \$getMetaOnly,
                "--efi-db=s" => \$efiDb, "--ec-desc-file=s" => \$ecDescFile, "--support-files=s" => \$supportFiles);
    print "Needs --output-collect-dir\n" and do_exit(1) if not $isSizeOnly and not $outputCollectDir or not -d $outputCollectDir;
    print "Needs --efi-db\n" and do_exit(1) if not $isSizeOnly and not $efiDb;
    #print "Needs --ec-desc-file\n" and do_exit(1) if not $ecDescFile or not -f $ecDescFile;
    print "Needs env EFI_TOOLS_HOME\n" and do_exit(1) if not $ENV{EFI_TOOLS_HOME};

    my $scriptDir = "$jobMasterDir/build_scripts";
    mkdir($scriptDir);
    my $efiMetaDir = "$jobMasterDir/efi_meta";
    mkdir ($efiMetaDir);

    my $outputDb = "$outputCollectDir/data.sqlite";
    print "$outputDb already exists; please remove it before continuing\n" and do_exit(1) if not $isSizeOnly and -f $outputDb;

    my @messages = $actions->createFinalDb($masterData, $outputDb, $scriptDir, $efiDb, $outputCollectDir, $efiMetaDir, $supportFiles, $getMetaOnly, $isSizeOnly);

    if (scalar @messages) {
        print "There were errors:\n\t" . join("\n\t", @messages), "\n";
    }


#########################################################################################
} elsif ($action eq "get-gnds") {
    my ($outputCollectDir, $efiDbModule, $newGndDb);
    GetOptions("--output-collect-dir=s" => \$outputCollectDir, "--efi-db-module=s" => \$efiDbModule, "--new-db" => \$newGndDb);
    print "Needs --output-collect-dir\n" and do_exit(1) if not $outputCollectDir or not -d $outputCollectDir;
    print "Needs --efi-db-module\n" and do_exit(1) if not $efiDbModule;

    my $scriptDir = "$jobMasterDir/build_scripts";
    mkdir($scriptDir);

    $actions->createGndDb($efiDbModule, $scriptDir, $outputCollectDir, $unirefVersion, $newGndDb);


#########################################################################################
} elsif ($action eq "build-hmms") {
    my ($outputCollectDir);
    GetOptions("--output-collect-dir=s" => \$outputCollectDir);

    my $scriptDir = "$jobMasterDir/build_scripts";
    mkdir($scriptDir);

    $actions->createHmmDb($masterData, $scriptDir, $outputCollectDir);


#########################################################################################
} else {
    print "Invalid --action\n";
}


unlink($lockFile);







sub createSchema {
    my $dbh = shift;
    my $sql = "CREATE TABLE IF NOT EXISTS as_jobs (as_id TEXT, cluster_id TEXT, cluster_name TEXT, ascore INT, uniref INT, started INT, finished INT, job_id INT, dir_path TEXT, ssn_name TEXT, PRIMARY KEY(as_id))";
    do_sql($sql, $dbh, $dryRun);
    $sql = "CREATE TABLE IF NOT EXISTS ca_jobs (as_id TEXT, started INT, finished INT, job_id INT, dir_path TEXT, ssn_name TEXT, max_cluster_size INT, PRIMARY KEY(as_id))";
    do_sql($sql, $dbh, $dryRun);
    $sql = "CREATE TABLE IF NOT EXISTS cr_jobs (as_id TEXT, started INT, finished INT, job_id INT, dir_path TEXT, PRIMARY KEY(as_id))";
    do_sql($sql, $dbh, $dryRun);
    $sql = "CREATE TABLE IF NOT EXISTS collect_jobs (as_id TEXT, dir_path TEXT, started INT, collect_finished INT, split_finished INT, cyto_started INT, collect_job_id TEXT, split_job_id TEXT, PRIMARY KEY(as_id))";
    do_sql($sql, $dbh, $dryRun);
}


sub getDbhFn {
    return sub {
        die "Missing db file in master dir\n" if not -f $dbFile;
        my $dbh;
        if (not $dryRun) {
            $dbh = initDatabase($dbFile);
            die "No database connection\n" if not $dbh;
        }
        return $dbh;
    };
}


sub initDatabase {
    my $dbFile = shift;
    my $initOnly = shift // 0;
    my $dbh = DBI->connect("dbi:SQLite:dbname=$dbFile", "", "");
    createSchema($dbh);
    if ($initOnly) {
        $dbh = undef;
    } else {
        return $dbh;
    }
}


sub do_exit {
    my $status = shift // 0;
    unlink($lockFile) if -f $lockFile;
    exit($status);
}


sub print_log {
    $logger->print_log(@_);
}


sub showHelp {
    print <<HELP;

This program can be grouped into three sets of actions.  The first set collects all of the data and creates
SSN images.  The second step builds the internal databases that power the 'Explore' pages and various search
functions, and retrieves the GNDs.  The third step builds the new website, integrating the 'Explore' data
pages into the main site.

PART 1

This part requires a list of generate jobs.  All of these jobs can be run in a cron script.  An example
is given in misc/run_collect.sh.

1. start-ascores creates all of the SLURM jobs (AS) that make diced SSNs from the generate jobs.

2. check-completion checks for completion of AS SSN jobs (step 1) and starts up convergence ratio (CR) and
   cluster analysis (CA) jobs for completed AS SSN jobs.

3. make-collect collects any jobs that have finished both CA and CR and collects the various files (id lists,
   HMMs, etc) into appropriately-named directories.

4. check-collect checks for any completed collect jobs and marks them as complete.

5. make-collect --split-ssns creates jobs that split up diced SSNs into the individual diced clusters.

6. check-collect --split-ssns checks for any completed split SSN jobs.

7. cytoscape checks for any completed jobs and starts up the image generation process using Cytoscape.



PART 2

This can be run after all steps in Part 1 are completed with the exception of cytoscape, which can run
in parallel.  These should not be run in a cron script, and should be run in sequence.

1. build-db creates the database the powers the 'Explore' pages.  It consists of a single .sqlite
   database file.  It actually creates a SLURM script that is submitted.

   --action build-db --output-collect-dir <PATH_TO_RSAM_OUTPUT_DIR> --efi-db mysql:efi_NNNN
       --ec-desc-file <PATH_TO_enzclass.txt> [--get-metadata-only]

   --get-metadata-only doesn't ZZZZ

   --ec-desc-file the file comes from https://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/complete/docs/enzclass.txt

2. build-hmms creates the HMM files necessary for searching by sequence.

3. build-gnds creates a SLURM job that retrieves GNDs for all of the IDs in the database.



PART 3

TBD

HELP
}


package MasterLogger;
use strict;
use warnings;
use POSIX qw(strftime);
sub new {
    my $class = shift;
    my $fh = shift;
    my $self = {fh => $fh};
    bless $self, $class;
    return $self;
}
sub print {
    my $self = shift;
    $self->print_log(@_);
}
sub flush {
    my $self = shift;
    $self->{fh}->flush();
}
sub print_log {
    my $self = shift;
    my @args = @_;
    my $dt = strftime("[%Y-%m-%d %H:%M:%S]", localtime);
    my $pad = length($dt);
    $pad = " " x $pad;
    my $p = "";
    foreach my $line (@args) {
        $line =~ s/[\r\n]+$//gs;
        $line =~ s/[\r\n]/\n$pad /gs;
        if ($p) {
            $self->{fh}->print("$pad $line\n");
        } else {
            $p = $pad;
            $self->{fh}->print("$dt $line\n");
        }
    }
}
1;

