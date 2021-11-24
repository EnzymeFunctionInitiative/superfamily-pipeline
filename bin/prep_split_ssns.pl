#!/bin/env perl

use strict;
use warnings;

use Data::Dumper;
use File::Find;
use FindBin;
use Getopt::Long;

use lib "$FindBin::Bin/../lib";

use IdListParser;


my ($inDir, $outDir, $jobFile, $outScript, $isDiced);
my $result = GetOptions(
    "input-dir=s"       => \$inDir,  # job dir
    "output-dir=s"      => \$outDir, # output database dir
    "job-file=s"        => \$jobFile,
    "output=s"          => \$outScript,
    "diced"             => \$isDiced,
);

die "Need --input-dir" if not $inDir or not -d $inDir;
die "Need --job-file" if not $isDiced and (not $jobFile or not -f $jobFile);
die "Need --output script pattern" if $isDiced and not $outScript;


my $app = "$FindBin::Bin/split_ssn.pl";


my $outFh = \*STDOUT;
if ($outScript and not $isDiced) {
    open my $fh, ">", $outScript or die "Unable to write to $outScript: $!";
    $outFh = $fh;
}
$outFh->print(getHeader("prep_split_ssns.sh")) if not $isDiced;


if ($isDiced) {
    #TODO: implement this
#    #find(\&findDicedSsnWanted, $inDir);
#    my @dirs = glob("$inDir/cluster-*");
#    foreach my $dir (@dirs) {
#        my @ddirs = glob("$dir/dicing-*");
#        next if not scalar @ddirs;
#
#        (my $num = $dir) =~ s%^.*/cluster-([\d\-]+)$%$1%;
#        my $sname = "${outScript}_$num";
#        open my $fh, ">", "$sname.sh";
#        $outFh = $fh;
#        $outFh->print(getHeader($sname));
#        foreach my $ddir (@ddirs) {
#            processDicedSsn("$ddir/ssn.zip", $ddir);
#        }
#        $outFh->close();
#    }
} else {
    my %data;
    
    open my $inFh, "<", $jobFile or die "Unable to read $jobFile: $!";
    while (<$inFh>) {
        chomp;
        next if m/^\s*$/ or m/^#/;
        my ($cluster, $parms) = IdListParser::parseLine($_);
        my @sourceNum = @{ $parms->{expandClusters} };
        my $sourceNum = $sourceNum[0][0] // 1;
        
        (my $clusterNum = $cluster) =~ s%^.*cluster[\-\d]*-(\d+)$%$1%;
        my $subCluster = $sourceNum;
        my $subClusterRemap = "$sourceNum:$clusterNum";
        
        my $colorDir = "$inDir/$parms->{ssnId}/output";
        my $targetDir = "$outDir/$cluster";
        my $ssnTempZip = "$targetDir/ssn_temp.zip";
        my $ssnTempXgmml = "$targetDir/ssn_temp.xgmml";
        writeSplitSsnJobLine("cp $colorDir/*_coloredssn.zip $ssnTempZip");
        writeSplitSsnJobLine("unzip -p $ssnTempZip > $ssnTempXgmml");
        writeSplitSsnJobLine("$app \\\n    --ssn-in $ssnTempXgmml \\\n    --sub-cluster $subClusterRemap \\\n    --output-file $targetDir/ssn.xgmml");
        writeSplitSsnJobLine("rm $ssnTempZip $ssnTempXgmml\n");
    }
    close $inFh;

#    foreach my $clusterId (sort keys %data) {
#        # If a cluster is manually partitioned (e.g. a child cluster exists already)
#        # then we skip partitioning the current one and copy it instead.
#        print "Skipping $clusterId because a child already exists\n" and next if $data{"$clusterId-1"};
#
#        my $targetDir = "$outDir/$clusterId"; 
#        my $ssnDir = "$inDir/$data{$clusterId}->{filt_cid}/output";
#    
#        # If there is only one cluster, copy the file to the output dir.
#        my @xfiles = glob("$ssnDir/cluster-data/uniprot-nodes/cluster_Uni*.txt");
#        if (scalar @xfiles == 1) {
#            doCopy($clusterId, $ssnDir, $targetDir);
#            next;
#        }
#
#        my $dissect = $data{$clusterId}->{dissect} ? ("--sub-clusters " . join(",", @{ $data{$clusterId}->{dissect} })) : "";
#    
#        my $ssnZipFile = getSsnZipFile($clusterId, $ssnDir);
#        my $outFile = "$targetDir/ssn";
#        $outFh->print("\n\n# PROCESSING   $clusterId  ####################################################################################################\n");
#        $outFh->print("mkdir -p $targetDir\n") if not -d $targetDir;
#        $outFh->print("unzip -p $ssnZipFile > $outFile.xgmml\n");
#        $outFh->print("$app --ssn-in $outFile.xgmml --mkdir $dissect --output-dir-pat $targetDir-\n");
#        if (-f "$outFile.zip") {
#            $outFh->print("rm $outFile.xgmml");
#        } else {
#            $outFh->print("rm -f $outFile.zip\n");
#            $outFh->print("zip -j $outFile.zip $outFile.xgmml\n");
#        }
#    }
}


close $outFh if $outScript;









sub writeSplitSsnJobLine {
    $outFh->print(join("\n", @_), "\n");
}


sub doCopy {
    my $clusterId = shift;
    my $ssnDir = shift;
    my $targetDir = shift;
    
    my $outFile = "$targetDir/ssn";

    print "Skipping $clusterId because it's already copied\n" and return if -f "$outFile.zip" or -f "$outFile.xgmml";
    
    $outFh->print("mkdir -p $targetDir\n") if not -d $targetDir;

    my $ssnZipFile = getSsnZipFile($clusterId, $ssnDir);

    $outFh->print("unzip -p $ssnZipFile > $outFile.xgmml\n");
    $outFh->print("rm -f $outFile.zip\n");
    $outFh->print("zip $outFile.zip $outFile.xgmml\n");
    $outFh->print("rm $outFile.xgmml\n");
}


sub getSsnZipFile {
    my $clusterId = shift;
    my $ssnDir = shift;
    my ($ssnZipFile) = glob("$ssnDir/*_coloredssn.zip");
    die "Unable to find zip file for $clusterId in $ssnDir/*_coloredssn.zip" if not $ssnZipFile;
    return $ssnZipFile;
}


sub findDicedSsnWanted {
    processDicedSsn($File::Find::name, $File::Find::dir);
}
sub processDicedSsn {
    my ($name, $dir) = @_;
    if ($name =~ m%dicing-\d+/ssn.zip%) {
        (my $cluster = $dir) =~ s/^.*(cluster[\-0-9]+).*$/$1/;
        $outFh->print("unzip -p $name > $name.tmp\n");
        $outFh->print("$app --ssn-in $name.tmp --output-dir-pat $dir/$cluster-\n");
        $outFh->print("rm $name.tmp\n");
    }
}


sub getHeader {
    my $logName = shift;
    return <<HEADER;
#!/bin/bash
#SBATCH --partition=efi-mem,efi
#SBATCH --nodes=1
#SBATCH --tasks-per-node=1
#SBATCH --mem=250gb
#SBATCH --job-name="split_ssns"
#SBATCH --kill-on-invalid-dep=yes
#SBATCH -o $logName.sh.stdout.%j
#SBATCH -e $logName.sh.stderr.%j
#set -e
module load Perl
export EFI_TOOLS_HOME=/home/n-z/noberg/dev/EFITools

HEADER
}


sub getDissect {
    my $str = shift;
    $str =~ s/ //g;
    my @p = split(m/,/, $str);
    my @clusters;
    for (my $pi = 0; $pi <= $#p; $pi++) {
        if ($p[$pi] =~ m/^[\d:]+$/) {
            push @clusters, $p[$pi];
        } elsif ($p[$pi] =~ m/\-/) {
            #my @s = split(m/\-/, $p[$pi]);
            my ($s, $e) = split(m/\-/, $p[$pi]);
            for (; $s <= $e; $s++) {
                push @clusters, $s;
            }
            #for (my $si = 0; $si <= $#s; $si++) {
            #    push @clusters, $s[$si];
            #}
        }
    }
    return @clusters;
}

