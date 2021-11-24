#!/bin/env perl

use strict;
use warnings;

use Getopt::Long;
use Data::Dumper;
use File::Find;
use FindBin;

use lib "$FindBin::Bin/../lib";

use IdListParser;




my ($dataDir, $doDicing, $jobScript, $defaultAscore, $jobIdListFile, $ascoreFile, $useExistingFile);
my $result = GetOptions(
    "data-dir=s"            => \$dataDir,
    "do-dicing"             => \$doDicing,
    "job-script=s"          => \$jobScript,
    "default-ascore=i"      => \$defaultAscore,
    "ascore-file=s"         => \$ascoreFile,
    "job-id-list=s"         => \$jobIdListFile,
    "use-existing"          => \$useExistingFile,
);

die "Need --data-dir" if not $dataDir or not -d $dataDir;
die "Need --job-script" if not $jobScript;
die "Need --job-id-list" if not $jobIdListFile or not -f $jobIdListFile;


$defaultAscore ||= 25;
my $BUFFER = "";

my $ascoreData = {};
if ($ascoreFile and -f $ascoreFile) {
    my $data = IdListParser::loadAlignmentScoreFile($ascoreFile);
    foreach my $cluster (keys %$data) {
        map { push @{$ascoreData->{$cluster}}, $_->{ascore}; } @{$data->{$cluster}};
    }
}




my $clusters = IdListParser::parseFile($jobIdListFile, $dataDir);
my %clusters = %$clusters;



open my $jobScriptFh, ">", $jobScript or die "Unable to write to --job-script $jobScript: $!";

addAction("module load efiest/devlocal");
addAction("module load efidb/ip82");
addAction("");


## Use existing CR file, and then 
#if ($useExisting) {
#    find(\&processFile, $dataDir);
#
## Calculate the CR
#} else {
    foreach my $cluster (sort keys %clusters) {
        my $cData = $clusters{$cluster};
        my $baseDir = $cData->{base_dir};
        if (-f "$baseDir/conv_ratio.txt") {
            addAction("rm $baseDir/conv_ratio.txt");
        #    print "Skipping $cluster; already exists\n";
        #    next;
        }
        my $result = handleClusterDir($baseDir, $cluster, $cData->{number});
        if (not $result) {
            print "There was no acore for $cluster; you may want to look into that\n";
        }
    }
#}


close $jobScriptFh;





sub processFile {
    if ($_ eq "conv_ratio.txt") {
        my $dir = $File::Find::dir;
        (my $cluster = $dir) =~ s%^.*(cluster[\-\d]+)/?$%$1%;
        handleClusterDir($dir, $cluster);
    }
}


sub handleClusterDir {
    my ($dir, $cluster, $num) = @_;
    return 0 if not $ascoreData->{$cluster};
    my $ascore = $ascoreData->{$cluster}->[0] // $defaultAscore;
    return if $doDicing and $dir !~ m/dicing-(\d+)/;
    $ascore = $doDicing ? ($1 // $ascore) : $ascore;
    return if not $doDicing and $dir =~ m/dicing-(\d+)/;

    my $ascoreArg = $ascore ? "--ascore $ascore" : "";

    my $fastaFile = "";
    my $idList = "";
    if (-f "$dir/uniref90.fasta") {
        $fastaFile = "$dir/uniref90.fasta";
        $idList = "$dir/uniref90.txt";
    } elsif (-f "$dir/uniref50.fasta") {
        $fastaFile = "$dir/uniref50.fasta";
        $idList = "$dir/uniref50.txt";
    } else {
        $fastaFile = "$dir/uniprot.fasta";
        $idList = "$dir/uniprot.txt";
    }
    my $tmpDir = "$dir/crtemp";
    addAction("mkdir -p $tmpDir");
    addAction("sed 's/\$/\t$num/' $idList > $tmpDir/idlist.txt");
    addAction("create_cluster_conv_ratio_job.pl --id-list-in $tmpDir/idlist.txt --fasta-in $fastaFile --output-path $tmpDir --output-file $dir/conv_ratio.txt --scheduler slurm --queue efi --ram 30 $ascoreArg");
    addAction("");

    return 1;
}



sub addAction {
    my $action = shift;
    $jobScriptFh->print("$action\n");
    #$BUFFER .= $action . "\n";
}




