#!/bin/env perl

use strict;
use warnings;

use FindBin;
use Getopt::Long;
use File::Find;

my ($loadDir, $scriptDir, $estAppDir);
my $results = GetOptions(
    "load-dir=s"    => \$loadDir,
    "script-dir=s"  => \$scriptDir,
    "est-app-dir=s" => \$estAppDir,
);

die "Need --load-dir" if not $loadDir or not -d $loadDir;
die "Need --script-dir" if not $scriptDir or not -d $scriptDir;
die "Need --est-app-dir" if not $estAppDir or not -d $estAppDir;



my @allActions;

my @dirs = glob("$loadDir/cluster-*");

foreach my $dir (@dirs) {
    push @allActions, makeHistogramAction($dir) if -f "$dir/uniprot.fasta";
    my @dicings = glob("$dir/dicing*");
    foreach my $dicing (@dicings) {
        my @clusters = glob("$dicing/cluster-*");
        foreach my $cluster (@clusters) {
            push @allActions, makeHistogramAction($cluster) if -f "$cluster/uniprot.fasta";
        }
    }
}


my $scriptNum = 0;

my @files;

while (@allActions) {
    my @actions = splice(@allActions, 0, 20000);
    my $file = "$scriptDir/histo_$scriptNum.sh";
    makeJob($file, \@actions);
    push @files, $file;
    $scriptNum++;
}


foreach my $file (@files) {
    print "sbatch $file\n";
}






sub makeJob {
    my $file = shift;
    my $actions = shift;

    open my $fh, ">", $file or die "Unable to write to $file: $!";
    $fh->print(<<SCRIPT);
#!/bin/bash
#SBATCH --partition=efi
#SBATCH --nodes=1
#SBATCH --tasks-per-node=1
#SBATCH --mem=20gb
#SBATCH --job-name="filt_histo"
#SBATCH --kill-on-invalid-dep=yes
#SBATCH -o $file.stdout.%j
#SBATCH -e $file.stderr.%j
#set -e

source /etc/profile
module purge
module load R
module load Perl
source /home/groups/efi/apps/perl_env.sh

SCRIPT

    map { $fh->print("$_\n"); } @$actions;
}


sub makeHistogramAction {
    my $inputDir = shift;
    
    my @actions;

    my $outputFn = sub {
        my $urType = shift;
        my $fileType = lc($urType);
        my $outHisto = "$inputDir/length_histogram_$fileType.txt";
        my $outImage = "$inputDir/length_histogram_${fileType}_lg.png";
        push @actions, "$estAppDir/make_length_histo.pl --seq-file $inputDir/$fileType.fasta --histo-file $outHisto";
        push @actions, "Rscript /home/n-z/noberg/dev/EST/Rgraphs/hist-length.r legacy $outHisto $outImage 0 'Full-$urType' 700 315";
    };
    &$outputFn("UniProt") if -f "$inputDir/uniprot.fasta";
    &$outputFn("UniRef90") if -f "$inputDir/uniref90.fasta";
    &$outputFn("UniRef50") if -f "$inputDir/uniref50.fasta";

    return @actions;
}


