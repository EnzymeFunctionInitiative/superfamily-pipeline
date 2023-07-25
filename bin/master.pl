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
use File::Path qw(make_path);


my ($projectDir, $jobMasterDir, $loadDir, $masterMetaFile, $subgroupInfoFile, $action, $unirefVersion, $queue, $dryRun, $showHelp, $lockFileName, $efiConfigFile);
my $result = GetOptions(
    "project-dir=s"         => \$projectDir,
    "master-dir=s"          => \$jobMasterDir,
    "load-dir=s"            => \$loadDir,   # Where the output files go after being collected
    "master-file=s"         => \$masterMetaFile,
    "subgroup-info-file=s"  => \$subgroupInfoFile,
    "action=s"              => \$action,
    "uniref-version=i"      => \$unirefVersion,
    "queue=s"               => \$queue,
    "dry-run"               => \$dryRun,
    "help"                  => \$showHelp,
    "lock-file-name=s"      => \$lockFileName,
    "efi-config-file=s"     => \$efiConfigFile,
);


if ($showHelp) {
    showHelp();
    exit(0);
}


$efiConfigFile //= $ENV{EFI_CONFIG};

die "Need --master-dir\n" if not $jobMasterDir or not -d $jobMasterDir;
die "Need --master-file $masterMetaFile\n" if not $masterMetaFile or not -f $masterMetaFile;
die "Need --action\n" if not $action;
die "Need --queue\n" if not $queue or not $ENV{EFI_QUEUE};
die "Need --efi-config-file\n" if not $efiConfigFile or not -f $efiConfigFile;

#my %ACTIONS = ("start-ascores" => 1, "check-completion-ca" => 1, "check-completion-cr" => 1, "make-metadata" => 1, "make-collect" => 1);
#die "Invalid --action" if not $ACTIONS{$action};

$projectDir = "$jobMasterDir/.." if not $projectDir or not -d $projectDir;

my $dbFile = "$jobMasterDir/data.sqlite";
my $collectScriptDir = "$jobMasterDir/collect_scripts";
my $buildScriptDir = "$jobMasterDir/build_scripts";
my $appDir = $FindBin::Bin;
$queue = $ENV{EFI_QUEUE} if not $queue;
$unirefVersion = 90 if not $unirefVersion;
$lockFileName = ".lock" if not $lockFileName;


my $lockFile = "$jobMasterDir/$lockFileName";
print "Already running master.pl\n" and exit(0) if -e $lockFile;
`touch $lockFile`;

make_path($collectScriptDir) if not -d $collectScriptDir;
make_path($buildScriptDir) if not -d $buildScriptDir;

my $logFile = "$jobMasterDir/log.txt";
open my $logFh, ">>", $logFile;
my $logger = new MasterLogger($logFh);


my $masterData = parse_master_file($masterMetaFile, $subgroupInfoFile // "");
my $actions = new MasterActions(dry_run => $dryRun, get_dbh => getDbhFn(), app_dir => $appDir, efi_tools_home => $ENV{EFI_TOOLS_HOME}, log_fh => $logger, queue => $queue, efi_config_file => $efiConfigFile);

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
    push @args, "--perl-env $ENV{EFI_PERL_ENV}" if $ENV{EFI_PERL_ENV};

    my $args = join(" ", @args);
    my $cmd = "$app $args";
    print_log($cmd);

    my ($result, $err) = capture { system($cmd); };
    print_log("WARNING: There was an error running $app $args: $err") and do_exit(1) if $err;
    print_log($result);


##########################################################################################
#} elsif ($action eq "create-cytoscape") {
#    print "Need --load-dir" and do_exit(1) if (not $loadDir or not -d $loadDir);
#    my $cytoConfig = "$projectDir/cytoscape_config.sh";
#    my $outputRunScript = "$jobMasterDir/run_cytoscape.sh";
#    $actions->writeCytoscapeScript($jobMasterDir, $loadDir, $cytoConfig, $outputRunScript);
#
#
##########################################################################################
#} elsif ($action eq "run-cytoscape") {
#    my $runScript = "$jobMasterDir/run_cytoscape.sh";
#    print_log("WARNING: Cytoscape run script $runScript does not exist") and do_exit(1) if not -f $runScript;
#    my ($result, $err) = capture { system("/bin/bash", $runScript); };
#    print_log("WARNING: There was an error running $runScript: $err") and do_exit(1) if $err;


##########################################################################################
#} elsif ($action eq "make-ssn-list") {
#    my ($outputCollectDir, $outputFile);
#    GetOptions("--output-collect-dir=s" => \$outputCollectDir, "--ssn-list=s" => \$outputFile);
#    #print "Needs --output-collect-dir\n" and do_exit(1) if not $outputCollectDir or not -d $outputCollectDir;
#    print "Needs --ssn-list\n" and do_exit(1) if not $outputFile;
#
#    my @files = $actions->listSsnFiles();
#    open my $fh, ">", $outputFile or die "Unable to write to $outputFile: $!";
#    foreach my $file (@files) {
#        $fh->print("$file\n");
#    }
#    close $fh;


#########################################################################################
} elsif ($action eq "make-collect") {
    my ($splitSsns, $minClusterSize);
    GetOptions("--split-ssns" => \$splitSsns, "min-cluster-size=i" => \$minClusterSize);

    $minClusterSize = 3 if not $minClusterSize;
    my $flag = $splitSsns ? DataCollection::SPLIT : DataCollection::COLLECT;
    my $messages = $actions->makeCollect($masterData, $collectScriptDir, $loadDir, $flag, $minClusterSize);
    print_log(@$messages);


#########################################################################################
} elsif ($action eq "check-collect") {
    my ($splitSsns);
    GetOptions("--split-ssns" => \$splitSsns);

    my $flag = $splitSsns ? DataCollection::SPLIT : DataCollection::COLLECT;
    my $messages = $actions->updateCollectStatus($flag);
    print_log(@$messages);


#########################################################################################
} elsif ($action =~ m/^check-final/) {

    my $isSizeOnly = $action eq "check-final-size";

    my $gndScriptDir = "$buildScriptDir/gnd_build";

    my ($isCollectFinished, $step, $message) = checkCollectStatus();
    my $isBuildStarted = -f "$buildScriptDir/build.started" || 0;
    my $isBuildFinished = -f "$buildScriptDir/build.finished" || 0;
    my $isGndStarted = -f "$gndScriptDir/gnd.started" || 0;
    my $isGndFinished = -f "$gndScriptDir/gnd.finished" || 0;

    my $text = $isCollectFinished ? "" : " step=$step message=$message";

    #print "collect_finished=$isCollectFinished$text | build_started=$isBuildStarted | build_finished=$isBuildFinished | gnd_started=$isGndStarted | gnd_finished=$isGndFinished\n";

    my $buildDb = "$loadDir/data.sqlite";

    if ($isCollectFinished and not $isBuildStarted) {
        my ($efiDb, $getMetaOnly, $supportFiles);
        GetOptions("--get-metadata-only" => \$getMetaOnly, "--efi-db=s" => \$efiDb, "--support-files=s" => \$supportFiles);
        print "Needs --efi-db\n" and do_exit(1) if (not $isSizeOnly and not $efiDb);
        print "Needs env EFI_TOOLS_HOME\n" and do_exit(1) if not $ENV{EFI_TOOLS_HOME};

        my $efiMetaDir = "$jobMasterDir/efi_meta";
        make_path($efiMetaDir) if not -d $efiMetaDir;
        my $masterScript = "$buildScriptDir/master_build.sh";

        my @messages = $actions->createFinalDb($masterData, $buildDb, $buildScriptDir, $efiDb, $loadDir, $efiMetaDir, $supportFiles, $getMetaOnly, $isSizeOnly, $masterScript);
        if (scalar @messages) {
            print("There were errors creating the final job:\n\t" . join("\n\t", @messages) . "\n") and do_exit(1);
        } else {
            my ($result, $err) = capture { system("/usr/bin/sbatch", $masterScript); };
            print "There was an error submitting master script $masterScript: $err\n" and do_exit(1) if $err;
        }

        my $hmmScriptDir = "$buildScriptDir/hmm_build";
        make_path($hmmScriptDir) if not -d $hmmScriptDir;
        my $hmmScript = "$hmmScriptDir/create_hmm_databases.sh";

        $actions->createHmmDatabaseScript($masterData, $hmmScriptDir, $hmmScript, $loadDir);

        my ($result, $err) = capture { system("/usr/bin/sbatch", $hmmScript); };
        print "There was an error submitting HMM $hmmScript: $err\n" and do_exit(1) if $err;

    } elsif ($isBuildStarted and $isBuildFinished and not $isGndStarted) {
        print "Starting GND\n";
        my ($efiDbModule, $createNewGndDb);
        GetOptions("--efi-db=s" => \$efiDbModule, "--create-new-gnd-db" => \$createNewGndDb);

        make_path($gndScriptDir) if not -d $gndScriptDir;
        my $gndScript = "$gndScriptDir/create_gnd_database.sh";

        $actions->createGndDb($efiDbModule, $buildDb, $loadDir, $gndScriptDir, $gndScript, $unirefVersion, $createNewGndDb);

        if ($gndScript) {
            my ($result, $err) = capture { system("/usr/bin/sbatch", $gndScript); };
            print "There was an error submitting GND $gndScript: $err\n" and do_exit(1) if $err;
        } else {
            print "Failed to create GND script\n";
        }

    }


#########################################################################################
} else {
    print "Invalid --action $action\n";
}


unlink($lockFile);












sub checkCollectStatus {
    my $dbhFn = getDbhFn();
    my $dbh = &$dbhFn;

    my $sqlFn = sub { my $table = shift; my $finCol = shift || "finished"; return "SELECT COUNT(CASE WHEN $finCol = 1 THEN 1 END) AS num_finished, COUNT(CASE WHEN started = 1 THEN 1 END) AS num_started FROM $table"; };
    my ($sql, $row, $sth);

    $sql = &$sqlFn("as_jobs");
    $sth = $dbh->prepare($sql);
    $sth->execute();
    $row = $sth->fetchrow_hashref();
    return (0, "as_jobs", "num_finished=$row->{num_finished} num_started=$row->{num_started}") if not $row or $row->{num_finished} != $row->{num_started};

    $sql = &$sqlFn("ca_jobs");
    $sth = $dbh->prepare($sql);
    $sth->execute();
    $row = $sth->fetchrow_hashref();
    return (0, "ca_jobs", "num_finished=$row->{num_finished} num_started=$row->{num_started}") if not $row or $row->{num_finished} != $row->{num_started};

    $sql = &$sqlFn("cr_jobs");
    $sth = $dbh->prepare($sql);
    $sth->execute();
    $row = $sth->fetchrow_hashref();
    return (0, "cr_jobs", "num_finished=$row->{num_finished} num_started=$row->{num_started}") if not $row or $row->{num_finished} != $row->{num_started};

    $sql = &$sqlFn("collect_jobs", "collect_finished");
    $sth = $dbh->prepare($sql);
    $sth->execute();
    $row = $sth->fetchrow_hashref();
    return (0, "collect_jobs", "num_finished=$row->{num_finished} num_started=$row->{num_started}") if not $row or $row->{num_finished} != $row->{num_started};

    return (1, "", "");
}


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

