#!/bin/env perl

use strict;
use warnings;

use Getopt::Long;
use Data::Dumper;
use FindBin;

use lib "$FindBin::Bin/../lib";

use IdListParser;

my $AppDir = $FindBin::Bin;


my ($dataDir, $jobIdListFile, $dicedJobIdListFile, $idMappingFile, $byDepth, $dumpDepth, $diced, $dicedAll, $dicedAscoreDir, $seqVersion);
my $result = GetOptions(
    "data-dir=s"            => \$dataDir,
    "job-id-file=s"         => \$jobIdListFile,
    "diced-job-id-file=s"   => \$dicedJobIdListFile,
    "id-mapping=s"          => \$idMappingFile,
    "by-depth"              => \$byDepth,
    "dump-depth"            => \$dumpDepth,
    "diced"                 => \$diced,
    "diced-by-ascore"       => \$dicedAll,
    "diced-by-ascore-dir=s" => \$dicedAscoreDir,
    "seq-version=s"         => \$seqVersion,
);

die "Need --data-dir" if not $dataDir or not -d $dataDir;
die "Need --job-id-file" if not $jobIdListFile or not -f $jobIdListFile;
die "Need --id-mapping" if not $idMappingFile;


my $fileVersion = (not $seqVersion or $seqVersion !~ m/^uniref\d+$/) ? "uniprot" : $seqVersion;


my ($clusters) = IdListParser::parseFile($jobIdListFile, $dataDir);
my $dicedClusters = {};
my $parseAscoreLineFn = sub { return parseJobIdFileLine($dataDir, @_); }; 
IdListParser::parseFile($dicedJobIdListFile, $parseAscoreLineFn);
my $clusterList = $diced ? $dicedClusters : $clusters;
my $dicedIds = makeDicedIdList($dicedClusters); # to exclude diced ids from non-diced id output


my $outFh;
open $outFh, ">", $idMappingFile or die "Unable to write to $idMappingFile: $!" if not $dicedAll;

my %masterIds;
foreach my $cluster (sort clusterIdSort keys %$clusterList) {
    my $baseDir = $clusterList->{$cluster}->{base_dir};
    if ($diced) {
        foreach my $ascore (@{ $clusterList->{$cluster}->{ascore} }) {
            my $asDir = "$baseDir/dicing-$ascore";
            print "Couldn't find $asDir\n" and next if not -d $asDir;
            if ($dicedAll) {
                my $outFile = "$dicedAscoreDir/$cluster-$ascore.txt";
                writeClusterAscoreIds($cluster, $asDir, $ascore, $outFile);
            } else {
                handleId($cluster, $asDir, $ascore);
            }
        }
    } else {
        handleId($cluster, $baseDir);
    }
}


my %uniqueIds;
foreach my $id (keys %masterIds) {
    my @clusters = keys %{ $masterIds{$id} };
    foreach my $cluster (@clusters) {
        my @parts = split(m/-/, $cluster);
        my $parent = join("-", @parts[0..($#parts-1)]);
        if ($masterIds{$id}->{$parent}) {
            delete $masterIds{$id}->{$parent};
        }
    }
    @clusters = grep m/-/, keys %{ $masterIds{$id} };
    die ("multiple found for $id: " . join(",", @clusters)) if scalar @clusters > 1;
    my $theCluster = $clusters[0];
    next if not $diced and $dicedClusters->{$theCluster};
    $uniqueIds{$id} = $theCluster;
}

if (not $dicedAll) {
    foreach my $id (keys %uniqueIds) {
        $outFh->print(join("\t", $uniqueIds{$id}, $id), "\n");
    }
    
    close $outFh;
}








sub writeClusterAscoreIds {
    my $cluster = shift;
    my $baseDir = shift;
    my $ascore = shift;
    my $outFile = shift;

    open my $outFh, ">", $outFile;

    foreach my $dir (glob("$baseDir/$cluster-*")) {
        (my $subCluster = $dir) =~ s%^.*/($cluster-\d+)$%$1%;
        my $file = "$dir/$fileVersion.txt";
        next if not -f $file;
        my @subIds = readIdFile($file);
        map { $outFh->print(join("\t", $subCluster, $_), "\n") } @subIds;
    }

    close $outFh;
}


sub handleId {
    my $cluster = shift;
    my $baseDir = shift;
    my $ascore = shift || "";

    my $idFile = "$baseDir/$fileVersion.txt";
    print "READING $idFile ($cluster $ascore)\n";
    my @ids = readIdFile($idFile);
    if ($byDepth) {
        map { $masterIds{$_}->{$cluster} = 1 } @ids;
    } elsif ($diced) {
        writeClusterFileToMaster(\@ids, $cluster, $ascore);
        foreach my $dir (glob("$baseDir/$cluster-*")) {
            (my $subCluster = $dir) =~ s%^.*/($cluster-\d+)$%$1%;
            my $file = "$dir/$fileVersion.txt";
            next if not -f $file;
            my @subIds = readIdFile($file);
            writeClusterFileToMaster(\@subIds, $subCluster, $ascore);
        }
    } else {
        my @nonDicedIds = grep { not exists $dicedIds->{$_} } @ids;
        writeClusterFileToMaster(\@nonDicedIds, $cluster);
    }
}


sub readIdFile {
    my $idFile = shift;
    my @ids;
    my $rescode = open my $fh, "<", $idFile;
    print "Unable to open id file $idFile: $!\n" and return () if not $rescode;
    while (<$fh>) {
        chomp;
        next if m/^\s*$/ or m/^#/;
        push @ids, $_;
    }
    close $fh;
    return @ids;
}


sub writeClusterFileToMaster {
    my $ids = shift;
    my $cluster = shift;
    my $extra = shift || "";
    foreach my $id (@$ids) {
        my @parms = ($cluster);
        push @parms, $extra if $extra;
        push @parms, $id;
        $outFh->print(join("\t", @parms), "\n");
    }
}

sub parseJobIdFileLine {
    my ($dataDir, $cluster, $parms) = @_;
    (my $num = $cluster) =~ s/^.*?(\d+)$/$1/;
    if ($dicedClusters->{$cluster} and $parms->{ascore}) {
        push @{ $dicedClusters->{$cluster}->{ascore} }, $parms->{ascore};
    } else {
        $dicedClusters->{$cluster} = {base_dir => "$dataDir/$cluster", number => $num, ascore => [$parms->{ascore}]};
    }
}


sub makeDicedIdList {
    my $diced = shift;
    my $ids = {};
    foreach my $cluster (keys %$diced) {
        my $info = $diced->{$cluster};
        my $file = "$info->{base_dir}/$fileVersion.txt";
        my @ids = readIdFile($file);
        map { $ids->{$_} = 1 } @ids;
    }
    return $ids;
}


sub clusterIdSort {
    return IdListParser::clusterIdSort($a, $b);
}


sub uniprotIdSort {
    my $ca = $uniqueIds{$a};
    my $cb = $uniqueIds{$b};
    my $comp = IdListParser::clusterIdSort($ca, $cb);
    return $a cmp $b if not $comp;
}


sub loadDicingFile {
    my $file = shift;

    my %dicing;

    open my $fh, "<", $file;

    while (<$fh>) {
        chomp;
        next if m/^#/;
        my ($cluster, $ascore) = split(m/\t/);
        push @{ $dicing{$cluster} }, $ascore;
    }

    close $fh;

    return \%dicing;
}


