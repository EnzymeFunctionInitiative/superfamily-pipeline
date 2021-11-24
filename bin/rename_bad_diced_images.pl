#!/bin/env perl

use strict;
use warnings;

use Getopt::Long;
use FindBin;
use Data::Dumper;

use lib "$FindBin::Bin/../lib";

use IdListParser;


my ($dataDir, $outDir, $idListFile, $jobScript, $ascoreFile);
my $result = GetOptions(
    "data-dir=s"            => \$dataDir,
    "output-dir=s"          => \$outDir,
    "id-file=s"             => \$idListFile,
    "job-script=s"          => \$jobScript,
    "ascore-file=s"         => \$ascoreFile,
);

die "Require data dir" if not $dataDir or not -d $dataDir;
die "Require output dir" if not $outDir or not -d $outDir;
die "Require id file" if not $idListFile or not -f $idListFile;
die "Require ascore file" if not $ascoreFile or not -f $ascoreFile;
die "Require job-script" if not $jobScript;

$jobScript = $ENV{PWD} . "/" . $jobScript if $jobScript !~ m%/%;
(my $scriptName = $jobScript) =~ s%^.*?([^/]+)?$%$1%;
$scriptName =~ s/\.sh$//;
(my $scriptDir = $jobScript) =~ s%^(/.*?)(/[^/]+)$%$1%;


my $ascoreData = IdListParser::loadAlignmentScoreFile($ascoreFile);


open my $jobScriptFh, ">", $jobScript or die "Unable to write to job script $jobScript: $!";

open my $fh, "<", $idListFile or die "Unable to open id file $idListFile: $!";

while (<$fh>) {
    chomp;
    next if m/^\s*$/;
    next if m/^\s*#/;
    
    my ($cluster, $parms) = IdListParser::parseLine($_);

    my $jobId = $parms->{fullColorId};

    die "What? $cluster does not exist in ascore" if not $ascoreData->{$cluster};

    foreach my $row (@{$ascoreData->{$cluster}}) {
        my $ssnDir = "$dataDir/$row->{job_id}/output";
        process($cluster, $row->{ascore}, $row->{job_id}, $parms->{ssnId}, $ssnDir);
    }
}



sub process {
    my ($cluster, $ascore, $jobId, $ssnId, $ssnDir) = @_;

    my $mapFile = "$ssnDir/cluster_num_map.txt";
    print "$mapFile\n";
    open my $mapFh, "<", $mapFile or die "Unable to find $mapFile: $!";
    my $header = <$mapFh>;
    my %mapping;
    while (<$mapFh>) {
        chomp;
        my ($from, $to) = split(m/\t/);
        next if $from == $to;
        $mapping{$from} = $to;
    }
    close $mapFh;

    my $mainTargetDir = "$outDir/$cluster";;
    my $dicedMainDir = "$mainTargetDir/dicing-$ascore";

    foreach my $from (sort {$a<=>$b} keys %mapping) {
        my $to = $mapping{$from};
        my $fromName = "$cluster-$from";
        my $toName = "$cluster-$to";
        next if not -d "$dicedMainDir/$fromName" or not -d "$dicedMainDir/$toName";
        doMove($dicedMainDir, $fromName, $toName);
    }
}


sub doMove {
    my ($mainDir, $fromName, $toName) = @_;
    #my ($cluster, $from, $to, $fromName, $toName) = @_;

    foreach my $suffix ("sm", "lg") {
        my $ff = "${fromName}_$suffix.png";
        my $tf = "${toName}_$suffix.png";
        if (not -f "$mainDir/$fromName/$ff") {
            #writeJobLine("# $mainDir/$fromName/$ff does not exist");
            next;
        }
        writeJobLine("mv $mainDir/$fromName/$ff $mainDir/$toName/$tf.tmp");
        writeJobLine("mv $mainDir/$toName/$tf $mainDir/$fromName/$ff");
        writeJobLine("mv $mainDir/$toName/$tf.tmp $mainDir/$toName/$tf");
    }
}


sub writeJobLine {
    print $jobScriptFh join("", @_), "\n";
}


