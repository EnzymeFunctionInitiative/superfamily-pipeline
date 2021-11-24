#!/bin/env perl

use strict;
use warnings;

use Getopt::Long;
use Data::Dumper;
use File::Find;
use FindBin;

use lib "$FindBin::Bin/../lib";

use IdListParser;




my ($dataDir, $jobIdListFile);
#my ($doDicing, $defaultAscore, $ascoreFile);
my $result = GetOptions(
    "data-dir=s"        => \$dataDir,
#    "do-dicing"         => \$doDicing,
#    "ascore-file=s"     => \$ascoreFile,
    "job-id-list=s"     => \$jobIdListFile,
#    "use-existing"      => \$useExistingFile,
);

die "Need --data-dir" if not $dataDir or not -d $dataDir;
die "Need --job-id-list" if not $jobIdListFile or not -f $jobIdListFile;



my %clusters;

my $handleIdFn = sub {
    my ($cluster, $parms) = @_;
    (my $num = $cluster) =~ s/^.*?(\d+)$/$1/;
    $clusters{$cluster} = {base_dir => "$dataDir/$cluster", number => $num};
    if ($parms->{expandClusters}) {
        foreach my $ex (@{$parms->{expandClusters}}) {
            my $cNum = $ex->[1];
            my $subCluster = join("-", $cluster, $cNum);
            $clusters{$subCluster} = {base_dir => "$dataDir/$subCluster", number => $cNum};
        }
    }
};


IdListParser::parseFile($jobIdListFile, $handleIdFn);



foreach my $dir (glob("$dataDir/cluster-*")) {
    (my $cluster = $dir) =~ s%^.*/(cluster-[^/]+)$%$1%;
    if (not $clusters{$cluster}) {
        print "rm -rf $dir\n";
    }
}


