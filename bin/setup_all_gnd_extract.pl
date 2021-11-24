#!/bin/env perl

use strict;
use warnings;

use Getopt::Long;
use Data::Dumper;
use FindBin;

use lib "$FindBin::Bin/../lib";

use IdListParser;

my $AppDir = $FindBin::Bin;


my ($dataDir, $jobIdListFile, $scriptDir, $numScripts, $masterDb);
my $result = GetOptions(
    "data-dir=s"        => \$dataDir,
    "job-id-file=s"     => \$jobIdListFile,
    "script-dir=s"      => \$scriptDir,
    "num-scripts=i"     => \$numScripts,
    "master-db=s"       => \$masterDb,
);

die "Need --data-dir" if not $dataDir or not -d $dataDir;
die "Need --job-id-file" if not $jobIdListFile or not -f $jobIdListFile;
die "Need --script-dir" if not $scriptDir or not -d $scriptDir;
die "Need --master-db" if not $masterDb or not -f $masterDb;


my $logDir = "$scriptDir/logs";
mkdir($logDir) if not -d $logDir;

$numScripts = 5 if not $numScripts;


my $clusters = IdListParser::parseFile($jobIdListFile, $dataDir);
my %clusters = %$clusters;


my @clusters = keys %clusters;
my $numPerScript = int(scalar(@clusters) / $numScripts + $numScripts);


my $scriptFileCount = 1;
my $getScriptNameFn = sub { return "split_up_gnds_${scriptFileCount}"; };
my $getScriptPath = sub { my $path = "$scriptDir/" . &$getScriptNameFn() . ".sh"; return $path; };

open my $scriptFh, ">", &$getScriptPath();
my $jobName = &$getScriptNameFn();
$scriptFh->print(getJobHeader($jobName));
my $c = 1;
foreach my $clusterId (@clusters) {
    if ($c++ % $numPerScript == 0) {
        close $scriptFh;
        $scriptFileCount++;
        open my $fh, ">", &$getScriptPath();
        $scriptFh = $fh;
        my $jobName = &$getScriptNameFn();
        $scriptFh->print(getJobHeader($jobName));
    }
    $scriptFh->print(getLine($clusterId));
}
close $scriptFh;




sub getLine {
    my $clusterId = shift;
    my $outDb = "$dataDir/$clusterId/gnd.sqlite";
    my @parms;
    push @parms, "--cluster-id-file $dataDir/$clusterId/uniprot.txt";
    push @parms, "--output-db $outDb";
    push @parms, "--log $logDir/$clusterId.log";
    push @parms, "--master-db $masterDb";
    return "$AppDir/extract_gnd_db.pl " . join(" ", @parms) . "\n";
}


sub getJobHeader {
    my $jobName = shift;
    return <<HEADER;
#!/bin/bash
#SBATCH --partition=efi
#SBATCH --nodes=1
#SBATCH --tasks-per-node=1
#SBATCH --mem=5gb
#SBATCH --job-name="$jobName"
#SBATCH --kill-on-invalid-dep=yes
#SBATCH -o $scriptDir/$jobName.sh.stdout.%j
#SBATCH -e $scriptDir/$jobName.sh.stderr.%j
#set -e

module unload MariaDB
module load Perl

HEADER
}



