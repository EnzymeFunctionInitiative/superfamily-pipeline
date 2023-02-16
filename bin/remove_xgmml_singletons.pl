#!/bin/env perl

use strict;
use warnings;

use Getopt::Long;


my ($inputSsn, $outputSsn, $inputList);
my $result = GetOptions(
    "input=s"       => \$inputSsn,
    "input-list=s"  => \$inputList,
    "output=s"      => \$outputSsn,
);



if ($inputList) {
    open my $fh, "<", $inputList;
    while (<$fh>) {
        chomp;
        next if m/^#/;
        my ($id, $file) = split(m/\t/);
        print "Processing $id = $file\n";
        processFile($file, "$file.new");
    }
    close $fh;
} else {
    processFile($inputSsn, $outputSsn);
}




sub processFile {
    my $inputFile = shift;
    my $outputFile = shift;

    my $degrees = {};
    
    open my $inFh, "<", $inputFile or die "Unable to open input SSN $inputFile: $!";
    
    while (my $line = <$inFh>) {
        chomp $line;
        if ($line =~ m/<node.*id="([^"]+)"/) {
            $degrees->{$1} = 0;
        } elsif ($line =~ m/<edge/) {
            (my $source = $line) =~ s/^.*source="([^"]+)".*$/$1/;
            (my $target = $line) =~ s/^.*target="([^"]+)".*$/$1/;
            (my $id = $line) =~ s/^.*id="([^"]+)".*$/$1/;
            #print "$source $target $id\n";
            $degrees->{$source}++;
            $degrees->{$target}++;
            #$edges->{$id} = [$source, $target];
        }
    }
    
    close $inFh;
    
    
    open $inFh, "<", $inputFile or die "Unable to open input SSN $inputFile: $!";
    open my $outFh, ">", $outputFile or die "Unable to open output SSN $outputFile: $!";
    
    my $delNode = 0;
    my $delEdge = 0;
    while (my $line = <$inFh>) {
        if ($line =~ m/<node.*id="([^"]+)"/) {
            if ($degrees->{$1} == 0) {
                $delNode = 1;
            } else {
                $delNode = 0;
                print $outFh $line;
            }
        } elsif ($line =~ m/<\/node>/) {
            print $outFh $line if not $delNode;
            $delNode = 0;
        } else {
            print $outFh $line if not $delNode;
        }
    }
    
    close $outFh;
    close $inFh;

}



