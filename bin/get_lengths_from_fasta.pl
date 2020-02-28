#!/usr/bin/env perl


use strict;
use warnings;

use Getopt::Long;
use FindBin;
use lib "$FindBin::Bin/../lib";

use EST::LengthHistogram;



my ($fastaFile, $incfrac, $outputFile, $trimZeros);
my $result = GetOptions(
    "fasta=s"               => \$fastaFile,
    "output=s"              => \$outputFile,
    "incfrac=f"             => \$incfrac,
    "trim"                  => \$trimZeros,
);


die "Requires input --fasta argument for sequence lengths" if not $fastaFile or not -f $fastaFile;
die "Requires output --output length file argument" if not $outputFile;

$incfrac = 0.99 if not defined $incfrac or $incfrac !~ m/^[\.\d]+$/;
$trimZeros = defined $trimZeros;



my $histo = new EST::LengthHistogram(incfrac => $incfrac, trim => $trimZeros);

my $id = "";
my $seq = "";

open my $fh, "<", $fastaFile;

while (<$fh>) {
    chomp;
    next if m/^\s*$/;

    if (m/^>(\S+)/) {
        $id = $1;
        $histo->addData(length($seq)) if $seq;
        $seq = "";
    } else {
        $seq .= $_;
    }
}

close $fh;

$histo->saveToFile($outputFile);


