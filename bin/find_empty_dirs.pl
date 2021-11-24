#!/bin/env perl

use strict;
use warnings;

use Getopt::Long;


my ($dir);
my $result = GetOptions(
    "dir=s"         => \$dir,
);


my @ddirs = glob("$dir/dicing-*");
foreach my $ddir (@ddirs) {
    my @cdirs = glob("$ddir/cluster-*");
    foreach my $cdir (@cdirs) {
        my @f = glob("$cdir/*");
        print "rm -rf $cdir\n" if $#f < 2;
    }
}


