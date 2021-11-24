#!/bin/env perl

use strict;
use warnings;

use Getopt::Long;
use Data::Dumper;

use lib "$FindBin::Bin/../lib";

use IdListParser;


my ($inputIdFile, $outputIdFile);
my $result = GetOptions(
    "input=s"       => \$inputIdFile,
    "output=s"      => \$outputIdFile,
);


die "Need --input file" if not $inputIdFile or not -f $inputIdFile;
die "Need --output file" if not $outputIdFile;


open my $inFh, "<", $inputIdFile or die "Unable to open input $inputIdFile: $!";

my $hasAscore = 0;
my $ids = {};
my $minSet = {};
while (<$inFh>) {
    chomp;
    my @p = split(m/[\|\t]/);
    if ($#p >= 2) {
        $hasAscore = 1;
        push @{$ids->{$p[0]}->{$p[1]}}, $p[2];
        push @{$minSet->{$p[0]}->{$p[1]}}, $p[2];
    } else {
        push @{$ids->{$p[0]}}, $p[1];
        push @{$minSet->{$p[0]}}, $p[1];
    }
}

close $inFh;


foreach my $cluster (keys %$ids) {
    my $idFn = sub {
        my ($master, $min, $cluster) = @_;
        my @p = split(m/-/, $cluster);
        my $parent = join("-", @p[0..($#p-1)]);
        if ($master->{$parent}) {
            delete $min->{$parent};
        }
    };
    if ($hasAscore) {
        foreach my $ascore (keys %{$ids->{$cluster}}) {
            &$idFn($ids, $minSet, $cluster);
        }
    } else {
        &$idFn($ids, $minSet, $cluster);
    }
}


open my $outFh, ">", $outputIdFile or die "Unable to write to output $outputIdFile: $!";

foreach my $cluster (sort clusterIdSort keys %$minSet) {
    my $outputFn = sub {
        my ($S, @cols) = @_;
        foreach my $id (@$S) {
            $outFh->print(join("\t", @cols, $id), "\n");
        }
    };
    if ($hasAscore) {
        foreach my $ascore (sort { $a <=> $b } keys %{$minSet->{$cluster}}) {
            &$outputFn($minSet->{$cluster}->{$ascore}, $cluster, $ascore);
        }
    } else {
        &$outputFn($minSet->{$cluster}, $cluster);
    }
}

close $outFh;



sub clusterIdSort {
    return IdListParser::clusterIdSort($a, $b);
}

