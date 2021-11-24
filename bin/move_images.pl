#!/bin/env perl

use strict;
use warnings;

use Getopt::Long;


my ($indir, $outdir);
my $result = GetOptions(
    "in-dir=s"      => \$indir,
    "out-dir=s"     => \$outdir,
);


die "Need --in-dir" if not $indir or not -d $indir;
die "Need --out-dir" if not $outdir or not -d $outdir;


my @files = glob("$indir/cluster-*.png");

foreach my $file (@files) {
    (my $name = $file) =~ s%^.*/(cluster-[\-\d]+)([_smlg]+)?.png$%$1%;
    print "cp $file $outdir/$name/${name}_lg.png\n";
    print "cp $file $outdir/$name/${name}_sm.png\n";
}



