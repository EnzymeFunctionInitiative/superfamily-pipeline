#!/bin/env perl

use strict;
use warnings;

use Data::Dumper;
use File::Find;
use Getopt::Long;


my ($dir, $outputScript, $outputDir, $isDiced, $skipExisting);
my $result = GetOptions(
    "input-dir=s"       => \$dir,
    "output-script=s"   => \$outputScript,
    "output-dir=s"      => \$outputDir,
    "diced"             => \$isDiced,
    "skip-existing"     => \$skipExisting,
);

die "Need --input-dir" if not $dir or not -d $dir;


my $outFh = \*STDOUT;
open $outFh, ">", $outputScript or die "Unable to write to --output $outputScript: $!" if $outputScript;
$outFh->print(getHeader("prep_zip_ssns.sh"));

my $findSub = $isDiced ? \&wantedDiced : \&wantedNormal;
find($findSub, $dir);


close $outFh if $outputScript;


sub wantedDiced {
    my $name = $File::Find::name;
    my $dir = $File::Find::dir;
    if ($name =~ m%(cluster-[\-0-9]+)/dicing-(\d+)/(cluster\-\d+\-\d+\-\d+)/ssn\.xgmml$%) {
        (my $zipName = $name) =~ s%^(.*)\.xgmml$%$1.zip%;
        return if $skipExisting and -f $zipName;
        my $parent = $1;
        my $as = $2;
        my $cname = $3;
        my $newfile = "$dir/$cname-AS$as.xgmml";
        my $outDir = $outputDir ? "$outputDir/$parent/dicing-$as/$cname" : $dir;
        #my $zipName = "$outDir/ssn.zip";
        doZip($outDir, $name, $newfile, $zipName);
    }
}


sub wantedNormal {
    my $name = $File::Find::name;
    my $dir = $File::Find::dir;
    if ($name !~ m/dicing-/ and $name =~ m%(cluster\-[\d\-]+)/ssn\.xgmml$%) {
        my $cname = $1;
        my $newfile = "$dir/$cname.xgmml";
        (my $zipName = $name) =~ s%^(.*)\.xgmml$%$1.zip%;
        doZip($dir, $name, $newfile, $zipName);
    }
}


sub doZip {
    my ($dir, $name, $newfile, $zipName) = @_;
    $outFh->print("rm -f $zipName\n") if -f $zipName;
    $outFh->print("mv $name $newfile\n");
    $outFh->print("zip -j $dir/ssn.zip $newfile\n");
    $outFh->print("mv $newfile $name\n\n");
}


sub getHeader {
    my $logName = shift;
    return <<HEADER;
#!/bin/bash
#SBATCH --partition=efi-mem,efi
#SBATCH --nodes=1
#SBATCH --tasks-per-node=1
#SBATCH --mem=150gb
#SBATCH --job-name="zip_ssns"
#SBATCH --kill-on-invalid-dep=yes
#SBATCH -o $logName.stdout.%j
#SBATCH -e $logName.stderr.%j
#set -e

module load Perl
EFI_TOOLS_HOME=/home/n-z/noberg/dev/EFITools

HEADER
}


