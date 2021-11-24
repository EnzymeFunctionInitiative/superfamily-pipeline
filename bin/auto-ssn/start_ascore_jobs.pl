#!/bin/env perl

use strict;
use warnings;

use Getopt::Long;
use DBI;


die "Needs EFI tools directory; the EFI_EST environment variable must be set.\n" if not $ENV{EFI_EST};
die "Needs EFI database module name; the EFI_DB_MOD environment variable must be set.\n" if not $ENV{EFI_DB_MOD};


my ($ssnJobPath, $jobMasterDir, $ascoreList);
my $result = GetOptions(
    "ssn-job-path=s"        => \$ssnJobPath,
    "job-master-dir=s"      => \$jobMasterDir,
    "ascores=s"             => \$ascoreList,
);


die "Requires --ssn-job-path as input\n" if not $ssnJobPath or not -d $ssnJobPath;
die "Requires --job-master-dir, and it must exist\n" if not $jobMasterDir or not -d $jobMasterDir;
die "Requires --ascores (list of AS)\n" if not $ascoreList;


my $dbFile = "$jobMasterDir/data.sqlite";
my $dbh = DBI->connect("dbi:SQLite:dbname=$dbFile", "", "");
die "No database connection\n" if not $dbh;
createSchema();


my @ascores = parseAscores($ascoreList);
my $uniRef = checkForUniRef($ssnJobPath);

my $startApp = "$ENV{EFI_EST}/create_analysis_job.pl";
my @defaultArgs = ("--filter eval", "--queue $ENV{EFI_QUEUE}", "--scheduler slurm", "--no-repnode", "--tmp output", "--keep-xgmml", "--maxfull 1000000000");
push @defaultArgs, "--uniref-version $uniRef" if $uniRef;


my $ssnNewParentDir = "$jobMasterDir/as_job";
mkdir $ssnNewParentDir;
my $generateDir = "$ssnNewParentDir/output";
mkdir $generateDir;
#printHeader();
#print "source /etc/profile\n";
#print "module load efiest/devlocal\n";
#print "module load $ENV{EFI_DB_MOD}\n\n";
#print "rsync -a --exclude='*/' $ssnJobPath/ $generateDir\n";
#print "curdir=\$PWD\n";
#print "cd $jobOutputDir\n\n";


`rm -rf $generateDir/eval-*`;

foreach my $ascore (@ascores) {
    my $as = $ascore->[0];
    my $asid = "AS$as";
    my $minLen = $ascore->[1];
    my $maxLen = $ascore->[2];
    my $title = "auto_$asid";

    my @args = @defaultArgs;
    push @args, "--minlen $minLen";
    push @args, "--maxlen $maxLen";
    push @args, "--title $title";
    #push @args, "--job-id auto_$asid";
    push @args, "--minval $as";

    my $appStart = $startApp . " " . join(" ", @args);
    my $cmd = <<CMD;
source /etc/profile
module load efiest/devlocal
module load $ENV{EFI_DB_MOD}\n
rsync -a --exclude='*/' $ssnJobPath/ $generateDir
curdir=\$PWD
cd $ssnNewParentDir
# The PWD and cd stuff is because the scripts assume they are being run from the SSN directory.
$appStart
CMD
    my $result = `$cmd`;
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
    #print join("\t", @$ascore, $jobNum), "\n";
    my $ssnOutPath = "$generateDir/eval-$as-$minLen-$maxLen/${title}_full_ssn.xgmml";
    my $sql = "INSERT OR REPLACE INTO as_jobs (path, as_id, ascore, job_id) VALUES ('$ssnOutPath', '$asid', $as, $jobNum)";
    $dbh->do($sql);
    $sql = "INSERT OR REPLACE INTO ca_jobs (as_id) VALUES ('$asid')";
    $dbh->do($sql);
    $sql = "INSERT OR REPLACE INTO cr_jobs (as_id) VALUES ('$asid')";
    $dbh->do($sql);
}








sub createSchema {
    my $sql = "CREATE TABLE IF NOT EXISTS as_jobs (as_id TEXT PRIMARY KEY, path TEXT, ascore INT, started INT, finished INT, job_id INT)";
    $dbh->do($sql);
    $sql = "CREATE TABLE IF NOT EXISTS ca_jobs (as_id TEXT PRIMARY KEY, path TEXT, started INT, finished INT, job_id INT)";
    $dbh->do($sql);
    $sql = "CREATE TABLE IF NOT EXISTS cr_jobs (as_id TEXT PRIMARY KEY, path TEXT, started INT, finished INT, job_id INT)";
    $dbh->do($sql);
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


sub parseAscores {
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


