#!/bin/env perl

use strict;
use warnings;


my $filtFile = shift @ARGV;
my $fullFile = shift @ARGV;
my $mergeFile = shift @ARGV;


my @clusters;
my %fdata;
my %udata;

open my $ffh, "<", $filtFile or die "Unable to open length filtered file $filtFile: $!";
open my $ufh, "<", $fullFile or die "Unable to open full file $fullFile: $!";

parseFile($ffh, \%fdata);
parseFile($ufh, \%udata);

close $ufh;
close $ffh;


open my $mergeFh, ">", $mergeFile or die "unable to write to merge file $mergeFile: $!";
print $mergeFh "#CLUSTER	SSN_ID	FILT_ANALYSIS_ID	FULL_ANALYSIS_ID	FILT_COLOR_SSN_ID	FULL_COLOR_SSN_ID	MIN_LEN	MAX_LEN	E-VALUE\n";

foreach my $cluster (@clusters) {
    warn "Unable to find $cluster in unfiltered file" if not $udata{$cluster};
    warn "Unable to find $cluster in filtered file" if not $fdata{$cluster};
    warn "$cluster full genId=$udata{$cluster}->[0] filt genId=$fdata{$cluster}->[0]" if ($udata{$cluster} and $fdata{$cluster} and $udata{$cluster}->[0] != $fdata{$cluster}->[0]);
    
    my @row = ($cluster);
    push @row, ($fdata{$cluster} ? $fdata{$cluster}->[0] : $udata{$cluster}->[0]); # SSN ID
    push @row, ($fdata{$cluster} ? $fdata{$cluster}->[1] : "");
    push @row, ($udata{$cluster} ? $udata{$cluster}->[1] : "");
    push @row, ($fdata{$cluster} ? $fdata{$cluster}->[2] : "");
    push @row, ($udata{$cluster} ? $udata{$cluster}->[2] : "");
    push @row, (($fdata{$cluster} and defined $fdata{$cluster}->[3]) ? $fdata{$cluster}->[3] : "");
    push @row, (($fdata{$cluster} and defined $fdata{$cluster}->[4]) ? $fdata{$cluster}->[4] : "");
    push @row, "";

    print $mergeFh join("\t", @row), "\n";
}


close $mergeFh;






sub parseFile {
    my $fh = shift;
    my $data = shift;

    while (<$fh>) {
        chomp;
        s/^\s*(.*?)\s*/$1/;
        next if m/^#/;
        my ($clusterId, $genId, $aid, $colorId, @extra) = split(m/\t/);
        push @clusters, $clusterId;
        $data->{$clusterId} = [$genId, $aid, $colorId, @extra];
    }
}


