#!/bin/env perl

use strict;
use warnings;


use Getopt::Long;



my ($idFile, $cluster, $ascore, $outputFile);
my $result = GetOptions(
    "id-file=s"         => \$idFile,
    "cluster=s"         => \$cluster,
    "ascore=i"          => \$ascore,
    "output-file=s"     => \$outputFile,
);


die "Need --id-file" if not $idFile or not -f $idFile;
die "Need --cluster" if not $cluster;
die "Need --ascore" if not $ascore;
die "Need --output-file" if not $outputFile;


open my $fh, "<", $idFile;

my @ids;

while (<$fh>) {
    chomp;
    my @parts = split(m/\t/);
    next if $#parts < 2;
    push @ids, $parts[2] if $parts[0] =~ m/^$cluster-/ and $parts[1] eq $ascore;
}

close $fh;


open my $outfh, ">", $outputFile;

map { $outfh->print("$_\n"); } @ids;

close $outfh;



