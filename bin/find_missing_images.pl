#!/bin/env perl

use strict;
use warnings;


use Data::Dumper;


my $dir = $ARGV[0];




my %missingFromSsns;
my %missingExpected;
my $totalFoundSsns = 0;
my $totalExpected = 0;

foreach my $primaryCluster (glob("$dir/cluster-*")) {
    my @dicings = glob("$primaryCluster/dicing-*");
    if (@dicings) {
        foreach my $dicing (@dicings) {
            my @parts = split(m/\//, $dicing);
            my $cluster = $parts[$#parts - 1];
            (my $ascore = $parts[$#parts]) =~ s/^.*dicing-(\d+)$/$1/;
            my $asid = "$cluster-AS$ascore";

            my $pngExists = -f "$dicing/ssn_lg.png";
            my $ssnExists = -f "$dicing/ssn.xgmml";
            $missingFromSsns{$asid} = 1 if ($ssnExists and not $pngExists);
            $missingExpected{$asid} = 1 if not $ssnExists;

            $totalFoundSsns++ if $ssnExists;
            $totalExpected++;

            foreach my $subCluster (glob("$dicing/cluster-*")) {
                (my $cluster = $subCluster) =~ s/^.*\/(cluster-[0-9\-AS]+)$/$1/;
                my $asid = "$cluster-AS$ascore";

                $pngExists = -f "$subCluster/ssn_lg.png";
                $ssnExists = -f "$subCluster/ssn.xgmml";
                $missingFromSsns{$asid} = 1 if ($ssnExists and not $pngExists);
                $missingExpected{$asid} = 1 if not $ssnExists;

                $totalFoundSsns++ if -f "$subCluster/ssn.xgmml";
                $totalExpected++;
            }
        }
    }
}


foreach my $asid (sort keys %missingFromSsns) {
    print "MISSING IMAGE $asid\n";
}
foreach my $asid (sort keys %missingExpected) {
    print "MISSING SSN $asid\n";
}
my $missingFromSsns = keys %missingFromSsns;
my $missingExpected = keys %missingExpected;

#print "\nTotal expected: $totalExpected\nTotal SSNs found: $totalFoundSsns\nMissing images (out of expected): $missingExpected\nMissing images (out of SSNs found): $missingFromSsns\n";



