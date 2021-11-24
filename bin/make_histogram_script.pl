#!/bin/env perl

use strict;
use warnings;

use FindBin;
use Getopt::Long;
use File::Find;

my ($appDir, $opts, $collectDir);
my $results = GetOptions(
    "app-dir=s"     => \$appDir,
    "graph-opts=s"  => \$opts,
    "collect-dir=s" => \$collectDir,
#    "id-file=s"     => \$listFile,
);

$appDir = $FindBin::Bin if not $appDir or not -d $appDir;
$opts = "--incfrac 1 --trim" if not $opts;

#die "Need --id-file" if not $listFIle or not -f $listFile;
die "Need --collect-dir" if not $collectDir or not -d $collectDir;


my $pwd = $ENV{PWD};

print <<HEADER;
#!/bin/bash
#SBATCH --partition=efi-mem,efi
#SBATCH --nodes=1
#SBATCH --tasks-per-node=1
#SBATCH --mem=5gb
#SBATCH --job-name="filt_histo"
#SBATCH --kill-on-invalid-dep=yes
#SBATCH -o $pwd/histo.sh.stdout.%j
#SBATCH -e $pwd/histo.sh.stderr.%j
#set -e

APPDIR=$appDir
OPTS=\"$opts\"

module load Perl
module load R

HEADER

find(\&wanted, $collectDir);



sub wanted {
    if (m/filtseq.fa$/) {
        #(my $dir = $_) =~ s/filtseq.fa$//;
        my $dir = $File::Find::dir;
        print "\$APPDIR/get_lengths_from_fasta.pl --fasta $File::Find::name --output $dir/filt_histo.tab \$OPTS\n";
        print "Rscript \$APPDIR/hist-length.r legacy $dir/filt_histo.tab $dir/length_histogram_filtered_sm.png 0 'Length Filtered' 700 315\n";
        print "Rscript \$APPDIR/hist-length.r legacy $dir/filt_histo.tab $dir/length_histogram_filtered_lg.png 0 'Length Filtered' 1800 900\n"
    }
}


