#!/bin/env perl

# Merges HMMs for all sub-clusters.

use strict;
use warnings;

use File::Find;
use Getopt::Long;
use FindBin;
use Data::Dumper;



my ($diced, $dataDir, $jobListFile, $outputHmmFile, $outputHmmDir, $byAscore);
my $result = GetOptions(
    "data-dir=s"        => \$dataDir,
    "job-list-file=s"   => \$jobListFile,
    "output-hmm=s"      => \$outputHmmFile,
    "output-hmm-dir=s"  => \$outputHmmDir,
    "diced"             => \$diced,
    "by-ascore"         => \$byAscore,
);


die "Need --data-dir" if not $dataDir or not -d $dataDir;
die "Need --job-list-file" if not $jobListFile or not -f $jobListFile;
die "Need --output-hmm or --output-hmm-dir" if not $outputHmmFile and not $outputHmmDir;


my $clusters = loadJobList($jobListFile);

#if ($diced) {
#    my $parseAscoreLineFn = sub { return parseJobIdFileLine($dataDir, @_); };
#    IdListParser::parseFile($jobListFile, $parseAscoreLineFn);
#} else {
#    $clusters = IdListParser::parseFile($jobListFile, $dataDir);
#}


my $OutFh; # global
openFile();

my %dbEntries;

foreach my $cluster (keys %$clusters) {
    if ($diced) {
        openFile($cluster) if not $byAscore;
        foreach my $info (@{ $clusters->{$cluster} }) {
            my $asDir = $info->{base_dir};
            my $ascore = $info->{ascore};
            if ($byAscore) {
                my $filePath = openFile($cluster, $ascore);
                push @{ $dbEntries{$cluster} }, [$ascore, $filePath];
            }
            foreach my $subDir (glob("$asDir/cluster-*")) {
                (my $subCluster = $subDir) =~ s%^.*/(cluster-[^/]+)$%$1%;
                processHmm("$subDir/hmm.hmm", $subCluster, $ascore);
            }
        }
    } else {
        my ($info) = @{ $clusters->{$cluster} };
        my $dir = $info->{base_dir};
        processHmm("$dir/hmm.hmm", $cluster);
    }
}


close $OutFh if $OutFh;


if ($outputHmmDir) {
    foreach my $cluster (keys %dbEntries) {
        open my $fh, ">", "$outputHmmDir/diced-$cluster.txt";
        foreach my $data (@{ $dbEntries{$cluster} }) {
            print $fh join("\t", $cluster, @$data), "\n";
        }
        close $fh;
    }
}








sub openFile {
    my $cluster = shift || "";
    my $ascore = shift || "";

    $ascore = "-AS$ascore" if $ascore;

    if ($outputHmmDir and $cluster) {
        close $OutFh if $OutFh;
        my $file = "$outputHmmDir/$cluster$ascore.hmm";
        open my $fh, ">", $file or die "Unable to write to HMM database $file: $!";
        $OutFh = $fh;
        return "$cluster$ascore.hmm";
    } elsif ($outputHmmFile) {
        open my $fh, ">", $outputHmmFile or die "Unable to write to HMM database $outputHmmFile: $!";
        $OutFh = $fh;
    }
}


sub processHmm {
    my $hmmFile = shift;
    my $cluster = shift;
    my $ascore = shift || "";

    print "Processing $hmmFile $cluster $ascore\n";

    $ascore = "-AS$ascore" if $ascore;

    open my $fh, "<", $hmmFile or print "Unable to read hmm file $hmmFile: $!\n" and return;
    while (<$fh>) {
        chomp;
        if (m/NAME  .*/) {
            s/NAME  .*/NAME  $cluster$ascore/;
        }
        print $OutFh "$_\n";
    }
    close $fh;
}


#sub parseJobIdFileLine {
#    my ($dataDir, $cluster, $parms) = @_;
#    (my $num = $cluster) =~ s/^.*?(\d+)$/$1/;
#    if ($clusters->{$cluster} and $parms->{ascore}) {
#        push @{ $clusters->{$cluster}->{ascore} }, $parms->{ascore};
#    } else {
#        $clusters->{$cluster} = {base_dir => "$dataDir/$cluster", number => $num, ascore => [$parms->{ascore}]};
#    }
#}


sub loadJobList {
    my $file = shift;

    my $data = {};

    open my $fh, "<", $file;

    while (<$fh>) {
        chomp;
        next if m/^\s*$/ or m/^#/;
        my ($cluster, $ascore, $path) = split(m/\t/);
        push @{ $data->{$cluster} }, {base_dir => $path, ascore => $ascore};
    }

    close $fh;

    return $data;
}



