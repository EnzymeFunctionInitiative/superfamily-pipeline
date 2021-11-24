#!/bin/env perl

use strict;
use warnings;

use Getopt::Long;



my ($dataDir, $outFile, $onlyDups);
my $result = GetOptions(
    "data-dir=s"        => \$dataDir,
    "output-file=s"     => \$outFile,
    "only-dups"         => \$onlyDups,
);

die "Need --data-dir" if not $dataDir or not -d $dataDir;
die "Need --output-file" if not $outFile;



my %sizes;
my %reverseSizes;

(my $cluster = $dataDir) =~ s%^.*/(cluster-[\-0-9]+)$%$1%;

my @dicings = glob("$dataDir/dicing*");

foreach my $dicing (@dicings) {
    (my $dice = $dicing) =~ s%^.*/dicing-(\d+)$%$1%;
    my @dirs = glob("$dicing/cluster-*");
    foreach my $dir (@dirs) {
        (my $subCluster = $dir) =~ s%^.*/(cluster-[\-\d]+)$%$1%;
        next if not -f "$dir/uniprot.txt";
        my $uplc = lineCount("$dir/uniprot.txt");
        my $urlc = lineCount("$dir/uniref90.txt");
        my $key = "$subCluster";
        $sizes{$dice}->{$key}->{uniprot} = $uplc;
        $sizes{$dice}->{$key}->{uniref90} = $urlc;
        push @{ $reverseSizes{$dice}->{$urlc} }, $key;
    }
}




open my $outFh, ">", $outFile;

if ($onlyDups) {
    my @dicings = sort { $a <=> $b } keys %reverseSizes;
    foreach my $dice (@dicings) {
        print $outFh "Dicing=$dice\n";
        my @sizes = sort { $a <=> $b } keys %{ $reverseSizes{$dice} };
        #@sizes = grep { $_ > 10 } @sizes;
        foreach my $size (@sizes) {
            my @sizeKeys = @{ $reverseSizes{$dice}->{$size} };
            my $numDups = scalar @sizeKeys;
            if ($numDups > 1) {
                print $outFh "\tDups=$numDups\n";
                my @sz = map { join("\t", "", "", $_, $sizes{$dice}->{$_}->{uniprot}, $sizes{$dice}->{$_}->{uniref90}) } sort @sizeKeys;
                print $outFh join("\n", @sz), "\n";
            }
        }
    }
} else {
    foreach my $dice (keys %sizes) {
        foreach my $key (keys %{ $sizes{$dice} }) {
            print $outFh join("\t", $dice, $key, $sizes{$dice}->{$key}->{uniprot}, $sizes{$dice}->{$key}->{uniref90}), "\n";
        }
    }
}

close $outFh;









sub lineCount {
    my $file = shift;

    my $c = 0;
    open my $fh, "<", $file or die "Unable to read $file: $!";
    while (<$fh>) {
        next if m/^\s*$/;
        $c++;
    }
    close $fh;

    return $c;
}



