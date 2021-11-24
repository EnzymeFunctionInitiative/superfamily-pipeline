#!/bin/env perl

use strict;
use warnings;

use File::Find;
use Getopt::Long;
use FindBin;
use Data::Dumper;

use lib "$FindBin::Bin/../lib";

use IdListParser;


my ($diced, $dataDir, $jobIdFile, $outputHmmFile, $outputHmmDir, $byAscore);
my $result = GetOptions(
    "data-dir=s"        => \$dataDir,
    "job-id-file=s"     => \$jobIdFile,
    "output-hmm=s"      => \$outputHmmFile,
    "output-hmm-dir=s"  => \$outputHmmDir,
    "diced"             => \$diced,
    "by-ascore"         => \$byAscore,
);


die "Need --data-dir" if not $dataDir or not -d $dataDir;
die "Need --job-id-file" if not $jobIdFile or not -f $jobIdFile;
die "Need --output-hmm or --output-hmm-dir" if not $outputHmmFile and not $outputHmmDir;


my $clusters = {};
if ($diced) {
    my $parseAscoreLineFn = sub { return parseJobIdFileLine($dataDir, @_); };
    IdListParser::parseFile($jobIdFile, $parseAscoreLineFn);
} else {
    $clusters = IdListParser::parseFile($jobIdFile, $dataDir);
}


my $OutFh; # global
openFile();

my %dbEntries;

foreach my $cluster (keys %$clusters) {
    my $dir = $clusters->{$cluster}->{base_dir};
    if ($diced) {
        openFile($cluster) if not $byAscore;
        foreach my $ascore (@{ $clusters->{$cluster}->{ascore} }) {
            my $asDir = "$dir/dicing-$ascore";
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




#sub getCommand {
#    my $cluster = shift;
#    my $dir = shift;
#
#    return "cat $dir/hmm.hmm | sed 's/NAME  .*/NAME  $cluster/' >> $outputHmmFile";
#}


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

sub parseJobIdFileLine {
    my ($dataDir, $cluster, $parms) = @_;
    (my $num = $cluster) =~ s/^.*?(\d+)$/$1/;
    if ($clusters->{$cluster} and $parms->{ascore}) {
        push @{ $clusters->{$cluster}->{ascore} }, $parms->{ascore};
    } else {
        $clusters->{$cluster} = {base_dir => "$dataDir/$cluster", number => $num, ascore => [$parms->{ascore}]};
    }
}


