#!/bin/env perl

use strict;
use warnings;

use Getopt::Long;
use Data::Dumper;


my ($source, $outputDirPat, $subClusters, $makeDir, $namePat, $outputName, $subCluster, $subClusterMapFile);
my $result = GetOptions(
    "source=s"          => \$source,

    "output-dir-pat=s"  => \$outputDirPat,
    "sub-clusters=s"    => \$subClusters,
    "sub-cluster-map-file=s"    => \$subClusterMapFile,
    "name-pat=s"        => \$namePat,
    "mkdir"             => \$makeDir,

    "output-name=s"     => \$outputName,
    "sub-cluster=s"     => \$subCluster,
);


die "Need --source" if not $source or not -f $source;
die "Need --output-dir-pat or --output-name" if not $outputDirPat and not $outputName;
die "Need --name-pat" if $outputDirPat and not $namePat;
die "Need --sub-cluster" if (not $subCluster and not $subClusterMapFile) and $outputName;


my %subClusters;
my $findSingle = 0;
if ($outputDirPat and ($subClusters or $subClusterMapFile)) {
    if ($subClusterMapFile) {
        my $subs = parseSubClusterMapFile($subClusterMapFile);
        %subClusters = %$subs;
    } else {
        $subClusters = parseSubClusters($subClusters);
        %subClusters = map { $_->[0] => $_->[1] } @$subClusters if $subClusters;
    }
} elsif ($outputName and $subCluster) {
    my $subs = parseSubClusters($subCluster);
    die "No subs" if not $subs;
    $subClusters{$subs->[0]->[0]} = $subs->[0]->[1];
    $subCluster = $subs->[0]->[1];
    $findSingle = 1;
}



open my $sourceFh, "<", $source or die "Unable to read --source $source: $!";

my $header = "";
my %clusterData;
while (my $line = <$sourceFh>) {
    chomp $line;
    next if $line =~ m/^\s*$/ or $line =~ m/^#/;
    if (not $header) {
        $header = $line;
        next;
    }
    my ($num, $lineData) = split(m/\t/, $line, 2);
    if ($findSingle) {
        my $cNum = $subClusters{$num} // 0;
        if ($cNum) {
            push @{$clusterData{$cNum}}, $lineData;
        }
    } else {
        my $cNum = $subClusters{$num} ? $subClusters{$num} : $num;
        if ($cNum >= 0) {
            push @{$clusterData{$cNum}}, $lineData;
        }
    }
}

close $sourceFh;



if ($outputDirPat) {
    foreach my $num (sort keys %clusterData) {
        my $outDir = "$outputDirPat$num";
        if (not -d $outDir) {
            if ($makeDir) {
                mkdir $outDir;
            } else {
                print "Skipping $outDir; doesn't exist\n";
                next;
            }
        }
        my $outFile = "$outDir/$namePat";

        outputData($header, $outFile, $num, $clusterData{$num});
    }
} else {
    my $data = $clusterData{$subCluster};
    my $outFile = $outputName;
    outputData($header, $outFile, $subCluster, $data);
}










sub outputData {
    my $header = shift;
    my $outFile = shift;
    my $num = shift;
    my $data = shift;

    print "Writing to $outFile\n";
    open my $fh, ">", $outFile or die "Unable to write to $outFile: $!";
    $fh->print($header, "\n");
    foreach my $line (@$data) {
        $fh->print(join("\t", $num, $line), "\n");
    }
    close $fh;
}


sub parseSubClusterMapFile {
    my $file = shift;

    open my $sizeFh, "<", $file or print "Unable to read map file $file: $!";
    my $sizeHeader = <$sizeFh>;
    my %renumber;
    while (<$sizeFh>) {
        chomp;
        my @p = split(m/\t/);
        my $snum = $p[0];
        my $nnum = $p[1];
        $renumber{$snum} = $nnum;
    }
    close $sizeFh;

    return \%renumber;
}


sub parseSubClusters {
    my $str = shift;
    return undef if not $str;
    my @p = split(m/,/, $str);
    my @c;
    foreach my $p (@p) {
        my @a = split(/\-/, $p);
        if (scalar @a > 1) {
            my ($s, $e) = ($a[0], $a[$#a]);
            for ($s..$e) {
                push @c, [$_, $_];
            }
        } else {
            my $a = $a[0];
            if ($a =~ m/^(.*):(.*)$/) {
                push @c, [$1, $2];
            } else {
                push @c, [$a, $a];
            }
        }
    }
    return scalar @c ? \@c : undef;
}

