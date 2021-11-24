#!/bin/env perl

use strict;
use warnings;

use Getopt::Long;


my ($inFile, $outFile);
my $result = GetOptions(
    "in-file=s"         => \$inFile,
    "out-file=s"        => \$outFile,
);

die "Need --in-file" if not $inFile or not -f $inFile;
die "Need --out-file" if not $outFile;


open my $inFh, "<", $inFile or die "Unable to read $inFile: $!";
open my $outFh, ">", $outFile or die "Unable to write $outFile: $!";

my $start = 0;
while (<$inFh>) {
    $start = 1 if m/^Cluster\s+Number\s+Convergence\s+Ratio/;
    $outFh->print($_) if $start;
}

close $outFh;
close $inFh;


