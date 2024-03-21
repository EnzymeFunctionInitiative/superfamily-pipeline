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
    startAscoreJobs($ssnJobPath, [$jobMasterDir, $jobMasterDir], "auto", "", \@mode1ascores);
    #unlink($lockFile);
    exit(0);
}


my $masterData = parse_master_file($masterFile);


foreach my $clusterId (keys %$masterData) {
    print "Skipping cluster $clusterId: no job information\n" and next if (not $masterData->{$clusterId}->{job_id} or not $masterData->{$clusterId}->{primary_ascore});
    my $md = $masterData->{$clusterId};
    my $clusterJobPath = get_job_dir($jobMasterDir, $clusterId, $unirefVersion);
    do_mkdir($clusterJobPath, $dryRun) if not -d $clusterJobPath;

    my $isDiced = (@{$md->{children}} == 0 and @{$md->{ascores}}) ? 1 : 0;

    my $imageJobPath = $clusterJobPath;
    if ($md->{primary_uniref} and $md->{primary_uniref} != $unirefVersion) {
        $imageJobPath = get_job_dir($jobMasterDir, $clusterId, $md->{primary_uniref});
        do_mkdir($imageJobPath, $dryRun) if not -d $imageJobPath;
    }

    my $inDir = "$masterInputDir/" . $md->{job_id} . "/output";
    my $imageInDir = "$masterInputDir/" . (($isDiced and $md->{image_job}) ? $md->{image_job} : $md->{job_id}) . "/output";

    $md->{cluster_id} = $clusterId;
    $md->{is_diced} = $isDiced;
    $md->{input_job_path} = $inDir;
    $md->{input_uniref} = checkForUniRef($inDir);
    $md->{output_path} = $clusterJobPath;
    $md->{image_job_path} = $imageInDir;
    $md->{image_uniref} = $md->{primary_uniref};
    $md->{image_output_path} = $imageJobPath;

    #my $finish = startAscoreJobs($inDir, [$clusterJobPath, $imageJobPath], $clusterId, $md->{cluster_name}, $md, $isDiced, $imageInDir);
    my $finish = startAscoreJobs($md);
    #                             cluster_id => $clusterId,
    #                             #cluster_name => $md->{cluster_name},
    #                             metadata => $md,
    #                             is_diced => $isDiced,
    #                             generate_input_path => $inDir,
    #                             generate_uniref => $inputUniref,
    #                             cluster_output_path => $clusterJobPath,
    #                             image_output_path => $imageJobPath, $clusterId, $md->{cluster_name}, $md, $isDiced, $imageInDir, [$uniref, $md->{primary_uniref}]);
    last if $finish;
}


#unlink($lockFile);



sub startAscoreJobs {
    my $md = shift;

    #my $jobPath = shift;
    #my $masterDir = shift;
    #my $clusterId = shift;
    #my $clusterName = shift;
    #my $ascoreData = shift;
    #my $isDiced = shift;
    #my $imageInDir = shift;
    #my $unirefVersion = checkForUniRef($md->{input_job_path});

    my @asJobArgs = @defaultArgs;
    push @asJobArgs, "--uniref-version $md->{input_uniref}" if $md->{input_uniref};

    my $maxAsJobs = 25;
    my $asJobCount = get_num_running_jobs($dbh, "as_jobs", $dryRun);
    if ($asJobCount >= $maxAsJobs) {
        print "Too many running AS jobs to start any new ones.\n";
        return 1;
    }

    my $startJobList = sub {
        my $ascores = shift;
        my $sourceDir = shift;
        my $outputTopLevelPath = shift;
        my $unirefVersion = shift || 0;
        my $imageOnly = shift || 0;

        my $outputDirName = $imageOnly ? "output_image" : "output";
        my $outputPath = "$outputTopLevelPath/$outputDirName";
        do_mkdir($outputPath);

        runShellCommand("rm -rf $outputPath/AS-*") if $removeExisting;
        if ($removeExisting or not -f "$outputPath/1.out.completed") {
            runShellCommand("rsync -a --exclude='*/' $sourceDir/ $outputPath");
        }

        foreach my $ascore (@$ascores) {
            my ($as, $minLen, $maxLen);
            if ($mode == 1) {
                $as = $ascore->[0];
                $minLen = $ascore->[1];
                $maxLen = $ascore->[2];
            } else {
                $as = $ascore;
                $minLen = $md->{min_len};
                $maxLen = $md->{max_len};
            }

            my $asDirName = "AS-$ascore";
            my $outputAscorePath = "$outputPath/$asDirName";
            do_mkdir($outputAscorePath);

            my $status = startJob($outputTopLevelPath, $outputAscorePath, $outputDirName, $md->{cluster_id}, $md->{cluster_name}, $unirefVersion, $as, $minLen, $maxLen, \@asJobArgs, $md->{is_diced}, $imageOnly);
            ++$asJobCount if $status;

            # Exit the loop if we are running the max number of jobs already.
            last if $asJobCount > $maxAsJobs;
        }
    };

    my @ascores;
    if (not $md->{is_diced}) {
        @ascores = ($md->{primary_ascore});
    } else {
        @ascores = @{$md->{ascores}};
    }

    if (($md->{is_diced} and $ascores[0] != $md->{primary_ascore}) or $md->{image_output_path} ne $md->{input_job_path}) {
        my $imageTopLevelPath = "$md->{image_output_path}/as_job";
        do_mkdir($imageTopLevelPath, $dryRun);
        my $imageOnly = 1;
        &$startJobList([$md->{primary_ascore}], $md->{image_job_path}, $imageTopLevelPath, $md->{image_uniref}, $imageOnly);
    }

    my $outputTopLevelPath = "$md->{output_path}/as_job";
    do_mkdir($outputTopLevelPath, $dryRun);
    &$startJobList(\@ascores, $md->{input_job_path}, $outputTopLevelPath, $md->{input_uniref});

    return ($asJobCount >= $maxAsJobs) ? 1 : 0;
}


sub startJob {
    my ($outputTopLevelPath, $outputPath, $outputDirName, $clusterId, $clusterName, $uniref, $ascore, $minLen, $maxLen, $asJobArgs, $isDiced, $imageOnly) = @_;

    #my $evalDirName = "eval-$ascore-$minLen-$maxLen";

    my $asid = lc($clusterId =~ s/[^a-z0-9_]/_/igr);
    $asid .= "_AS$ascore";

    # Skip this one if it is already done.
    return 0 if -f "$outputPath/stats.tab.completed";

    # Skip this one if it is in progress.
    my $sql = "SELECT * FROM as_jobs WHERE as_id = ?";
    my $sth = $dbh->prepare($sql);
    $sth->execute($asid);
    my $row = $sth->fetchrow_hashref;
    return 0 if $row;

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
    push @args, "--results-dir-name $outputDirName";

    my $appStart = $startApp . " " . join(" ", @args);
    my $cmd = <<CMD;
source /etc/profile
module load efiest/devlocal
module load $ENV{EFI_DB_MOD}\n
curdir=\$PWD
cd $outputTopLevelPath
# The PWD and cd stuff is because the scripts assume they are being run from the SSN directory.
$appStart
CMD
    my $result = runShellCommand("$cmd");
    print "Starting $title AS-$ascore\n";
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
        my $unirefCol = $uniref ? "uniref," : "";
        my $unirefVal = $uniref ? "$uniref," : "";
        my $ssnOutName = "${title}_full_ssn.xgmml";
        my $sql = "INSERT OR REPLACE INTO as_jobs (as_id, cluster_id, cluster_name, ascore, $unirefCol job_id, started, dir_path, ssn_name, image_only) VALUES ('$asid', '$clusterId', '$clusterName', $ascore, $unirefVal $jobNum, 1, '$outputPath', '$ssnOutName', $imageOnly)";
        do_sql($sql, $dbh, $dryRun);
    }

    return 1;
}


sub makeTitle {
    my ($clusterId, $asid, $uniref) = @_;
    my $title = "${clusterId}_$asid";
    $title .= "_ur$uniref" if $uniref;
    return $title;
}


sub createSchema {
    my $sql = "CREATE TABLE IF NOT EXISTS as_jobs (as_id TEXT, cluster_id TEXT, cluster_name TEXT, ascore INT, uniref INT, started INT, finished INT, job_id INT, dir_path TEXT, ssn_name TEXT, image_only INT, PRIMARY KEY(as_id))";
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



