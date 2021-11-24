#!/bin/env perl

use strict;
use warnings;


use Getopt::Long;


while (<>) {
    chomp;
    next if m/^\s*#/;
    my (@parts) = split(m/\t/);
    my $clusterId = $parts[0];
    my $filtSsnId = $parts[4] // 0;
    my $fullSsnId = $parts[5] // 0;

    my $jobId = $filtSsnId;
    next if not $jobId;

    my @files = glob("/private_stores/gerlt/efi_test/results/$jobId/output/*.xgmml");
    next if not scalar @files;

    print join("\t", $jobId, $clusterId, $files[0]), "\n";
}


