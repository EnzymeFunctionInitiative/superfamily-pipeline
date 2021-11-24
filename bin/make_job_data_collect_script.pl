#!/bin/env perl

use strict;
use warnings;

use Getopt::Long;
use FindBin;
use Data::Dumper;

use lib "$FindBin::Bin/../lib";

use IdListParser;


my ($dataDir, $outDir, $idListFile, $diceJobs, $jobScript, $ascoreFile, $splitMulti, $splitSsnScript);
my ($includeParent);
my $result = GetOptions(
    "data-dir=s"            => \$dataDir,
    "out-dir=s"             => \$outDir,
    "id-file=s"             => \$idListFile,
    "dice-if-necessary"     => \$diceJobs,
    "job-script=s"          => \$jobScript,
    "ascore-file=s"         => \$ascoreFile,
    "split-multi"           => \$splitMulti,
    "split-ssn-script=s"    => \$splitSsnScript,
    "include-parent"        => \$includeParent,
);


die "Require data dir" if not $dataDir or not -d $dataDir;
die "Require target dir" if not $outDir;
die "Require file" if not $idListFile or not -f $idListFile;
die "Require job-script" if not $jobScript;

$diceJobs = defined $diceJobs;

$jobScript = $ENV{PWD} . "/" . $jobScript if $jobScript !~ m%/%;
(my $scriptName = $jobScript) =~ s%^.*?([^/]+)?$%$1%;
$scriptName =~ s/\.sh$//;
(my $scriptDir = $jobScript) =~ s%^(/.*?)(/[^/]+)$%$1%;
my $dicingFile = $diceJobs ? "$scriptDir/${scriptName}_dice.sh" : "";
my $cpCmd = "cp -u";
my $appDir = "$FindBin::Bin";


my $ascoreData = {};
if ($ascoreFile and -f $ascoreFile) {
    $ascoreData = IdListParser::loadAlignmentScoreFile($ascoreFile);
}


my $numLines = 0;
my $fileNum = 1;
open my $jobScriptFh, ">", $jobScript or die "Unable to write to job script $jobScript: $!";
my $splitSsnScriptFh;
open $splitSsnScriptFh, ">", $splitSsnScript or die "Unable to write to SSN split job script $splitSsnScript: $!" if $splitSsnScript;

my @dicedClusters;
my $diceCollectFh;
if ($dicingFile) {
    $diceCollectFh = $jobScriptFh;
}

writeJobLine(getJobHeader("$scriptName"));
writeSplitSsnJobLine(getJobHeader("${scriptName}_split_ssn")) if $splitSsnScript;


my @jobInfo;
my %parent;
my %children;

open my $fh, "<", $idListFile or die "Unable to open id file $idListFile: $!";

while (<$fh>) {
    chomp;
    next if m/^\s*$/;
    next if m/^\s*#/;
    
    my ($cluster, $parms) = IdListParser::parseLine($_);

    my @sourceNum = @{ $parms->{expandClusters} };
    my $sourceNum = $sourceNum[0][0] // 1;
    push @jobInfo, [$cluster, $parms, $sourceNum];
}

close $fh;

foreach my $jobInfo (@jobInfo) {
    my ($cluster, $parms, $sourceNum) = @$jobInfo;

    my $ssnDir = "";
    $ssnDir = "$dataDir/$parms->{ssnId}/output";
    print STDERR "$cluster: $ssnDir not found; ignoring\n" and return if not -d $ssnDir;

    if ($diceJobs and $parms->{ascore}) {
        processJob($cluster, $parms->{ascore}, $parms->{ssnId}, $ssnDir, $parms->{crJobId}, $sourceNum);
    } else {
        processJob($cluster, "", $parms->{ssnId}, $ssnDir, $parms->{crJobId}, $sourceNum); #$parms->{expandClusters});
    }
}





sub processJob {
    my ($cluster, $ascore, $ssnId, $ssnDir, $crJobId, $sourceNum) = @_;
    $crJobId = 0 if not $crJobId;

    # Input
    my $colorDir = "$dataDir/$ssnId/output";

    # Output
    my $targetDir = "$outDir/$cluster";
    my $dicedMainDir = $ascore ? "$targetDir/dicing-$ascore" : "";
    my $dicedTargetDir = $ascore ? "$dicedMainDir/$cluster" : "";

    (my $clusterNum = $cluster) =~ s%^.*cluster[\-\d]*-(\d+)$%$1%;
    my $subCluster = $sourceNum;
    my $subClusterRemap = "$sourceNum:$clusterNum";
    
    my $processSubClusters = sub {};

    # Create directories
    writeJobLine("\n\n\n#################################################################################");
    writeJobLine("# PROCESSING $cluster");
    writeJobLine("mkdir -p $targetDir");

    # Collect HMMs and length histograms.
    if (not $diceJobs) {
        my $parentNum = 0;
        my $parentColorDir = "";
        copyHmmFiles($ssnId, $colorDir, $targetDir, $subCluster, $subClusterRemap);

        if (-d $colorDir) {
            copyIdFiles($colorDir, $targetDir, $subCluster);
        } elsif (not -d $colorDir) {
            writeJobLine("echo \"Unable to find FASTA and ID data for $ssnId ($colorDir)\"");
        }
    }

    my $ssnTempZip = "$targetDir/ssn_temp.zip";
    my $ssnTempXgmml = "$targetDir/ssn_temp.xgmml";

    # Copy for main cluster
    writeSplitSsnJobLine("$cpCmd $colorDir/*_coloredssn.zip $ssnTempZip");
    writeSplitSsnJobLine("unzip -p $ssnTempZip > $ssnTempXgmml");
    if ($diceJobs) {
        writeSplitSsnJobLine("$appDir/split_ssn.pl --ssn-in $ssnTempXgmml --output-dir-pat $dicedTargetDir-");
    } else {
        writeSplitSsnJobLine("$appDir/split_ssn.pl --ssn-in $ssnTempXgmml --sub-cluster $subClusterRemap --output-file $targetDir/ssn.xgmml");
    }
    writeSplitSsnJobLine("rm $ssnTempZip $ssnTempXgmml");

    # Copy the convergence ratio file.
    if ($crJobId) {
        my $convRatioJobDir = "$dataDir/$crJobId/output";
        my $targetCR = "$targetDir/conv_ratio.txt";
        if ($subCluster) {
            my $tDir = $diceJobs ? $dicedTargetDir : $targetDir;
            writeJobLine("$appDir/split_tab_file.pl --source $convRatioJobDir/conv_ratio.txt --output-name $targetCR --sub-cluster $subClusterRemap");
        }
    }
    
    # Copy all of the HMM from the diced jobs. The parent files have already been copied.
    if ($diceJobs) {
        copyIdFiles($colorDir, $dicedMainDir, "All");
        writeJobLine("#### PROCESSING DICING");
        my $hDir = "$colorDir/cluster-data/hmm/full/normal";
        my $convRatioJobDir = "";
        $convRatioJobDir = "$dataDir/$crJobId/output" if $crJobId;
        processDicedHmm($cluster, $colorDir, $hDir, $dicedTargetDir, $ascore, $convRatioJobDir);
    }
}

close $fh;


if ($diceJobs) {
    open my $fh, ">", "$dicingFile.ascore-cluster-ids.txt" or die "Unable to write to $dicingFile.ascore-cluster-ids.txt: $!";
    map { print $fh "$_\n"; } @dicedClusters;
    close $fh;
    close $diceCollectFh;
}






sub copyIdFiles {
    my ($colorDir, $targetDir, $subCluster) = @_;
    my $cDir = "$colorDir/cluster-data/";
    my @urFiles50 = glob("$cDir/uniref50-nodes/*");
    my @urFiles90 = glob("$cDir/uniref90-nodes/*");
    my $hasUniRef = scalar @urFiles50 > 2 ? 50 : (scalar @urFiles90 > 2 ? 90 : 0);

    # Multiple clusters
    my @idFiles = glob("$cDir/uniprot-nodes/cluster_UniProt_IDs_*.txt");
    my $hasMultipleClusters = scalar @idFiles == 1 ? 0 : 1;
    if (not $hasMultipleClusters or $includeParent) {
        # Copy parent
        writeJobLine(getCopyIdFiles($colorDir, $targetDir, $subCluster, $hasUniRef));
    }
}


sub copyHmmFiles {
    my ($ssnId, $colorDir, $targetDir, $subCluster, $subClusterRemap) = @_;
    my $hDir = "$colorDir/cluster-data/hmm/full/normal";
    my @hmmFiles = glob("$hDir/hmm/cluster_*.hmm");
    if (-d $hDir) {
        writeJobLine(getCopyHistoFiles($hDir, $targetDir, $subCluster));
        writeJobLine(getCopyHmmFiles($hDir, $targetDir, $subCluster));
        writeJobLine(getCopyConsResFiles($colorDir, $targetDir, $subCluster, 0, $subClusterRemap));
    } else {
        writeJobLine("echo \"Unable to find HMM data for $ssnId ($colorDir)\"");
    }
}


sub getCopyIdFiles {
    my $colorDir = shift;
    my $targetDir = shift;
    my $num = shift;
    my $hasUniRef = shift;

    my @lines;

    my $allPfx = "";
    my $fastaFile = "cluster_${num}.fasta";
    if ($num eq "All") {
        $num = "";
        $allPfx = "All_";
        $fastaFile = "all.fasta";
    } else {
        $num = "_$num";
    }

    my $iDir = "$colorDir/cluster-data";
    my $fDir = "$colorDir/cluster-data/fasta";
    push @lines, "$cpCmd $fDir/$fastaFile $targetDir/uniprot.fasta\n";
    push @lines, "$cpCmd $fDir-uniref50/$fastaFile $targetDir/uniref50.fasta\n" if $hasUniRef == 50;
    push @lines, "$cpCmd $fDir-uniref90/$fastaFile $targetDir/uniref90.fasta\n" if $hasUniRef >= 50;
    push @lines, "$cpCmd $iDir/uniprot-nodes/cluster_${allPfx}UniProt_IDs${num}.txt $targetDir/uniprot.txt\n";
    push @lines, "$cpCmd $iDir/uniref50-nodes/cluster_${allPfx}UniRef50_IDs${num}.txt $targetDir/uniref50.txt\n" if $hasUniRef == 50;
    push @lines, "$cpCmd $iDir/uniref90-nodes/cluster_${allPfx}UniRef90_IDs${num}.txt $targetDir/uniref90.txt\n" if $hasUniRef >= 50;

    return @lines;
}


sub getCopyHmmFiles {
    my $hDir = shift;
    my $targetDir = shift;
    my $num = shift;

    my @lines;
    push @lines, "$cpCmd $hDir/weblogo/cluster_${num}.png $targetDir/weblogo.png\n";
    push @lines, "$cpCmd $hDir/hmm/cluster_${num}.hmm $targetDir/hmm.hmm\n";
    push @lines, "$cpCmd $hDir/hmm/cluster_${num}.png $targetDir/hmm.png\n";
    push @lines, "$cpCmd $hDir/hmm/cluster_${num}.json $targetDir/hmm.json\n";
    push @lines, "$cpCmd $hDir/align/cluster_${num}.afa $targetDir/msa.afa\n";

    return @lines;
}


sub getCopyHistoFiles {
    my $hDir = shift;
    my $targetDir = shift;
    my $num = shift;

    my @lines;
    if (-f "$hDir/hist-uniprot/cluster_${num}.png") {
        push @lines, "$cpCmd $hDir/hist-uniprot/cluster_${num}.png $targetDir/length_histogram_uniprot_lg.png\n";
        push @lines, "$cpCmd $hDir/hist-uniprot/cluster_${num}.png $targetDir/length_histogram_uniprot_sm.png\n";
    }
    if (-f "$hDir/hist-uniref50/cluster_${num}.png") {
        push @lines, "$cpCmd $hDir/hist-uniref50/cluster_${num}.png $targetDir/length_histogram_uniref50_lg.png\n";
        push @lines, "$cpCmd $hDir/hist-uniref50/cluster_${num}.png $targetDir/length_histogram_uniref50_sm.png\n";
    }
    if (-f "$hDir/hist-uniref90/cluster_${num}.png") {
        push @lines, "$cpCmd $hDir/hist-uniref90/cluster_${num}.png $targetDir/length_histogram_uniref90_lg.png\n";
        push @lines, "$cpCmd $hDir/hist-uniref90/cluster_${num}.png $targetDir/length_histogram_uniref90_sm.png\n";
    }

    return @lines;
}


sub getCopyConsResFiles {
    my $colorDir = shift;
    my $targetDir = shift;
    my $subCluster = shift;
    my $isDiced = shift || 0;
    my $subClusterRemap = shift || "";
    my $subClusterRemapFile = shift || "";

    my $subCArg = "";
    if ($isDiced and $subClusterRemapFile) {
        $subCArg = "--sub-cluster-map-file $subClusterRemapFile";
    }

    my @lines;
    foreach my $consFile (glob("$colorDir/*ConsensusResidue_*Position*")) {
        (my $res = $consFile) =~ s/^.*_ConsensusResidue_([A-Z])_.*$/$1/;
        my $file = "consensus_residue_${res}_position.txt";
        if (not $isDiced and not $subCluster) {
            push @lines, "$cpCmd $consFile $targetDir/$file\n";
        }
        if ($isDiced) {
            push @lines, "$appDir/split_tab_file.pl --source $consFile --mkdir --output-dir-pat $targetDir- --name-pat $file $subCArg";
        } elsif ($subClusterRemap) {
            push @lines, "$appDir/split_tab_file.pl --source $consFile --output-name $targetDir/$file --sub-cluster $subClusterRemap";
        }
    }
    push @lines, "";
    
    return @lines;
}

sub processDicedHmm {
    my $cluster = shift;
    my $colorDir = shift;
    my $hDir = shift;
    my $locOutDir = shift;
    my $ascore = shift;
    my $convRatioJobDir = shift || "";

    my @urFiles50 = glob("$colorDir/cluster-data/uniref50-nodes/*");
    my @urFiles90 = glob("$colorDir/cluster-data/uniref90-nodes/*");
    my $hasUniRef = scalar @urFiles50 > 2 ? 50 : (scalar @urFiles90 > 2 ? 90 : 0);

    my $sizeFile = "$colorDir/cluster_num_map.txt";
    open my $sizeFh, "<", $sizeFile or print "Unable to read $sizeFile: $!";
    my $sizeHeader = <$sizeFh>;
    my %renumber;
    while (<$sizeFh>) {
        chomp;
        my @p = split(m/\t/);
        my $snum = $p[0];
        my $nnum = $p[1];
        $renumber{$snum} = $nnum;
    }
    close $sizeFh;

    my @subClusters;
    my @files = sort {
            (my $aa = $a) =~ s/^.*cluster_(\d+).*$/$1/;
            (my $bb = $b) =~ s/^.*cluster_(\d+).*$/$1/;
            $aa = $renumber{$aa};
            $bb = $renumber{$bb};
            return $aa <=> $bb;
        } glob("$hDir/hmm/cluster_*.hmm");
    foreach my $hmm (@files) {
        (my $subNum = $hmm) =~ s%^.*hmm/cluster_(\d+)\.hmm$%$1%;
        push @subClusters, $subNum;
        my $clusterId = "$cluster-$subNum";
        my $targetNum = $renumber{$subNum};
        my $targetId = "$cluster-$targetNum";
        my $targetDir = "$locOutDir-$targetNum";
        my $fastaFile = "$colorDir/cluster-data/fasta/cluster_$subNum.fasta";
        my $afaFile = "$hDir/align/cluster_$subNum.afa";
        (my $hmmPngFile = $hmm) =~ s/\.hmm$/.png/;
        (my $hmmJsonFile = $hmm) =~ s/\.hmm$/.json/;

        push @dicedClusters, "$cluster\t$ascore\t$targetId";
        writeDicedJobLine("# PROCESSING $clusterId->$targetId");
        writeDicedJobLine("mkdir -p $targetDir");
        
        writeDicedJobLine(getCopyHistoFiles($hDir, $targetDir, $subNum));
        writeDicedJobLine(getCopyIdFiles($colorDir, $targetDir, $subNum, $hasUniRef));
        writeDicedJobLine(getCopyHmmFiles($hDir, $targetDir, $subNum));
    }

    # Parent has already been processed.
    writeDicedJobLine(getCopyConsResFiles($colorDir, $locOutDir, \@subClusters, 1, "", $sizeFile));

    #TODO: split conv_ratio
    my $convRatioFile = "$convRatioJobDir/conv_ratio.txt";
    my $line = "$appDir/split_tab_file.pl --source $convRatioFile --output-dir-pat $locOutDir- --name-pat conv_ratio.txt --sub-cluster-map-file $sizeFile";
    writeDicedJobLine($line);
}


sub getJobHeader {
    my $jobName = shift;
    my $pwd = $ENV{PWD};
    return <<HEADER;
#!/bin/bash
#SBATCH --partition=efi-mem,efi
#SBATCH --nodes=1
#SBATCH --tasks-per-node=1
#SBATCH --mem=5gb
#SBATCH --job-name="collect"
#SBATCH --kill-on-invalid-dep=yes
#SBATCH -o $pwd/$jobName.sh.stdout.%j
#SBATCH -e $pwd/$jobName.sh.stderr.%j
#set -e

HEADER
}


sub writeSplitSsnJobLine {
    return if not $splitSsnScriptFh;
    $splitSsnScriptFh->print(join("", @_), "\n");
}
sub writeDicedJobLine {
    writeJobLine(@_);
}
sub writeJobLine {
    print $jobScriptFh join("", @_), "\n";
}


