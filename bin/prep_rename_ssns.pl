#!/bin/env perl

use strict;
use warnings;

use Data::Dumper;
use File::Find;
use Getopt::Long;


my ($dir, $isDiced, $outputScript);
my $result = GetOptions(
    "dir=s"         => \$dir,
    "diced"         => \$isDiced,
    "output-script=s"   => \$outputScript,
);

die "Need --dir" if not $dir or not -d $dir;



my $outFh = \*STDOUT;
open $outFh, ">", $outputScript or die "Unable to write to --output $outputScript: $!" if $outputScript;
$outFh->print(getHeader("prep_zip_ssns.sh"));


if ($isDiced) {
    #TODO:
} else {
    my @dirs = grep { -d $_ } glob("$dir/cluster-*");
    foreach my $dir (@dirs) {
        (my $clusterName = $dir) =~ s%^.*/(cluster-[^/]+)$%$1%;
        processDir($dir, $clusterName);
    }
}


sub processDir {
    my $dir = shift;
    my $clusterName = shift;
    my $ssn = "$dir/ssn.xgmml";
    my $namedSsn = "$dir/$clusterName.xgmml";
    my $zipSsn = "$dir/ssn.zip";
    if (-s $ssn) {
        print "rm -f $zipSsn\n";
        print "mv $ssn $namedSsn\n";
        print "zip -j $zipSsn $namedSsn\n";
        print "mv $namedSsn $ssn\n";
        print "\n";
    }
}




#find(\&dicedWanted, $dir);
#
#
#sub dicedWanted {
#    my $name = $File::Find::name;
#    if ($name =~ m/dicing\-(\d+)\/(cluster[\-0-9]+)\/ssn.xgmml/) {
#        my $as = $1;
#        my $cluster = $2;
#        my $dir = $File::Find::dir;
#        print "mv $name $dir/$cluster-AS$as.xgmml\n";
#    }
#}


sub getHeader {
    my $logName = shift;
    return <<HEADER;
#!/bin/bash
#SBATCH --partition=efi-mem,efi
#SBATCH --nodes=1
#SBATCH --tasks-per-node=1
#SBATCH --mem=10gb
#SBATCH --job-name="zip_ssns"
#SBATCH --kill-on-invalid-dep=yes
#SBATCH -o $logName.stdout.%j
#SBATCH -e $logName.stderr.%j
#set -e

module load Perl
EFI_TOOLS_HOME=/home/n-z/noberg/dev/EFITools

HEADER
}


