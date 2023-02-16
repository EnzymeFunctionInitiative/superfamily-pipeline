
package DataCollection::Commands;

use strict;
use warnings;


sub new {
    my $class = shift;
    my %args = @_;

    my $self = {};
    bless $self, $class;

    $self->{cp_cmd} = "cp -u";
    $self->{app_dir} = $args{app_dir} or die "Need app_dir";
    $self->{include_parent} = $args{include_parent} // 0;

    return $self;
}


# Per cluster
sub getNonDicedCollectCommands {
    my $self = shift;
    my $clusterId = shift;
    my $data = shift;
    my $targetDir = shift;

    $self->{_commands} = [];
    $self->{_diced_clusters} = [];

    my $ascore = "";
    my $sourceNum;
    $self->processJob($clusterId, $data, $ascore, $targetDir, 0, $sourceNum, 0);
    
    return $self->{_commands};
}


# Per cluster
sub getNonDicedSplitCommands {
    my $self = shift;
    my $clusterId = shift;
    my $data = shift;
    my $targetDir = shift;

    $self->{_split} = [];
    $self->{_diced_clusters} = [];

    my $ascore = "";
    my $sourceNum;
    $self->processJob($clusterId, $data, $ascore, $targetDir, 0, $sourceNum, 1);
    
    return $self->{_split};
}


# Per cluster
sub getDicedCollectCommands {
    my $self = shift;
    my $clusterId = shift;
    my $data = shift;
    my $targetDir = shift;

    $self->{_commands} = [];
    $self->{_diced_clusters} = [];

    my $ascore = $data->{ascore} // die "Need ascore";
    my $sourceNum = "";
    $self->processJob($clusterId, $data, $ascore, $targetDir, 1, $sourceNum, 0);
    
    return $self->{_commands};
}


# Per cluster
sub getDicedSplitCommands {
    my $self = shift;
    my $clusterId = shift;
    my $data = shift;
    my $targetDir = shift;

    $self->{_split} = [];
    $self->{_diced_clusters} = [];

    my $ascore = $data->{ascore} // die "Need ascore";
    my $sourceNum = "";
    $self->processJob($clusterId, $data, $ascore, $targetDir, 1, $sourceNum, 1);
    
    return $self->{_split};
}


sub processJob {
    my $self = shift;
    my $cluster = shift;
    my $jobData = shift;
    my $ascore = shift;
    my $targetDir = shift;
    my $diceJobs = shift // 0;
    my $sourceNum = shift // "";
    my $ssnOnly = shift // 0;

    my $cpCmd = $self->{cp_cmd};
    my $appDir = $self->{app_dir};

    my $inputCaDir = $jobData->{input_ca_dir};
    my $inputSsn = $jobData->{input_ssn};
    my $inputCrDir = $jobData->{input_cr_dir};

    # Output
    my $dicedMainDir = $ascore ? "$targetDir" : "";
    my $dicedTargetDir = $ascore ? "$dicedMainDir/$cluster" : "";

    # Relic of old way of doing things
    #(my $clusterNum = $cluster) =~ s%^.*cluster[_\-\d]*[\-_](\d+)$%$1%;
    #my $subCluster = (defined $sourceNum and length $sourceNum) ? $sourceNum : $clusterNum;
    #my $subClusterRemap = "$subCluster:$clusterNum";

    my $subCluster = 1;
    my $subClusterRemap = "1:1";

    if (not $ssnOnly) {
        # Create directories
        $self->writeJobLine("");
        $self->writeJobLine("");
        $self->writeJobLine("");
        $self->writeJobLine("#################################################################################");
        $self->writeJobLine("# PROCESSING $cluster");

        # Collect HMMs and length histograms.
        #TODO
        if (not $diceJobs) {
            $self->writeJobLine("mkdir -p $targetDir");
            $self->copyHmmFiles($inputCaDir, $targetDir, $subCluster, $subClusterRemap);
    
            if (-d $inputCaDir) {
                $self->copyParentIdFiles($inputCaDir, $targetDir, $subCluster);
            } elsif (not -d $inputCaDir) {
                $self->writeJobLine("echo \"Unable to find FASTA and ID data for $inputCaDir\"");
            }
        } else {
            $self->writeJobLine("mkdir -p $dicedMainDir");
        }
    }

    if ($ssnOnly) {
        # Copy for main cluster
        my $theSsn = -f "$inputSsn.no_singletons" ? "$inputSsn.no_singletons" : $inputSsn;
        if ($diceJobs) {
            $self->writeSplitSsnJobLine("$cpCmd $theSsn $dicedMainDir/ssn.xgmml");
            $self->writeSplitSsnJobLine("$appDir/split_ssn.pl --ssn-in $theSsn --output-dir-pat $dicedTargetDir-");
        } else {
            $self->writeSplitSsnJobLine("$appDir/split_ssn.pl --ssn-in $theSsn --sub-cluster $subClusterRemap --output-file $targetDir/ssn.xgmml");
        }
    } else {
        # Remove singletons
        #$self->writeJobLine("mv $inputSsn $inputSsn.bak");
        $self->writeJobLine("$appDir/remove_xgmml_singletons.pl --input $inputSsn --output $inputSsn.no_singletons");
    }

    if (not $ssnOnly) {
        # Copy the convergence ratio file for the parent job.
        if ($inputCrDir) {
            my $targetCR = "$targetDir/conv_ratio.txt";
            if ($subCluster) {
                my $tDir = $diceJobs ? $dicedTargetDir : $targetDir;
                $self->writeJobLine("$appDir/split_tab_file.pl --source $inputCrDir/conv_ratio.txt --output-name $targetCR --sub-cluster $subClusterRemap");
            }
        }
        
        # Copy all of the HMM from the diced jobs. The parent files have already been copied.
        if ($diceJobs) {
            $self->copyParentIdFiles($inputCaDir, $dicedMainDir, "All");
            $self->writeJobLine("#### PROCESSING DICING");
            my $hDir = "$inputCaDir/cluster-data/hmm/full/normal";
            my $convRatioJobDir = $inputCrDir // "";
            $self->processDicedHmm($cluster, $inputCaDir, $hDir, $dicedTargetDir, $ascore, $convRatioJobDir);
        }
    }
}


sub copyParentIdFiles {
    my $self = shift;
    my ($inputCaDir, $targetDir, $subCluster) = @_;
    my $cDir = "$inputCaDir/cluster-data/";
    my @urFiles50 = glob("$cDir/uniref50-nodes/*");
    my @urFiles90 = glob("$cDir/uniref90-nodes/*");
    my $hasUniRef = scalar @urFiles50 > 2 ? 50 : (scalar @urFiles90 > 2 ? 90 : 0);

    # Multiple clusters
    #my @idFiles = glob("$cDir/uniprot-nodes/cluster_UniProt_IDs_*.txt");
    #my $hasMultipleClusters = scalar @idFiles == 1 ? 0 : 1;
    #if (not $hasMultipleClusters or $self->{include_parent}) {
        # Copy parent
        $self->writeJobLine($self->getCopyIdFiles($inputCaDir, $targetDir, $subCluster, $hasUniRef));
    #}
}


sub getCopyIdFiles {
    my $self = shift;
    my $inputCaDir = shift;
    my $targetDir = shift;
    my $num = shift;
    my $hasUniRef = shift;

    my $cpCmd = $self->{cp_cmd};

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

    my $iDir = "$inputCaDir/cluster-data";
    my $fDir = "$inputCaDir/cluster-data/fasta";
    push @lines, "$cpCmd $fDir/$fastaFile $targetDir/uniprot.fasta";
    push @lines, "$cpCmd $fDir-uniref50/$fastaFile $targetDir/uniref50.fasta" if $hasUniRef == 50;
    push @lines, "$cpCmd $fDir-uniref90/$fastaFile $targetDir/uniref90.fasta" if $hasUniRef >= 50;
    push @lines, "$cpCmd $iDir/uniprot-nodes/cluster_${allPfx}UniProt_IDs${num}.txt $targetDir/uniprot.txt";
    push @lines, "$cpCmd $iDir/uniref50-nodes/cluster_${allPfx}UniRef50_IDs${num}.txt $targetDir/uniref50.txt" if $hasUniRef == 50;
    push @lines, "$cpCmd $iDir/uniref90-nodes/cluster_${allPfx}UniRef90_IDs${num}.txt $targetDir/uniref90.txt" if $hasUniRef >= 50;

    return @lines;
}


sub copyHmmFiles {
    my $self = shift;
    my ($inputCaDir, $targetDir, $subCluster, $subClusterRemap) = @_;

    my $hDir = "$inputCaDir/cluster-data/hmm/full/normal";
    my @hmmFiles = glob("$hDir/hmm/cluster_*.hmm");
    if (-d $hDir) {
        $self->writeJobLine($self->getCopyHistoFiles($hDir, $targetDir, $subCluster));
        $self->writeJobLine($self->getCopyHmmFiles($hDir, $targetDir, $subCluster));
        $self->writeJobLine($self->getCopyConsResFiles($inputCaDir, $targetDir, $subCluster, 0, $subClusterRemap));
    } else {
        $self->writeJobLine("echo \"Unable to find HMM data for $inputCaDir\"");
    }
}


sub getCopyHistoFiles {
    my $self = shift;
    my $hDir = shift;
    my $targetDir = shift;
    my $num = shift;

    my $cpCmd = $self->{cp_cmd};

    my @lines;
    if (-f "$hDir/hist-uniprot/cluster_${num}.png") {
        push @lines, "$cpCmd $hDir/hist-uniprot/cluster_${num}.png $targetDir/length_histogram_uniprot_lg.png";
        push @lines, "$cpCmd $hDir/hist-uniprot/cluster_${num}.png $targetDir/length_histogram_uniprot_sm.png";
    }
    if (-f "$hDir/hist-uniref50/cluster_${num}.png") {
        push @lines, "$cpCmd $hDir/hist-uniref50/cluster_${num}.png $targetDir/length_histogram_uniref50_lg.png";
        push @lines, "$cpCmd $hDir/hist-uniref50/cluster_${num}.png $targetDir/length_histogram_uniref50_sm.png";
    }
    if (-f "$hDir/hist-uniref90/cluster_${num}.png") {
        push @lines, "$cpCmd $hDir/hist-uniref90/cluster_${num}.png $targetDir/length_histogram_uniref90_lg.png";
        push @lines, "$cpCmd $hDir/hist-uniref90/cluster_${num}.png $targetDir/length_histogram_uniref90_sm.png";
    }

    return @lines;
}


sub getCopyHmmFiles {
    my $self = shift;
    my $hDir = shift;
    my $targetDir = shift;
    my $num = shift;

    my $cpCmd = $self->{cp_cmd};

    my @lines;
    push @lines, "$cpCmd $hDir/weblogo/cluster_${num}.png $targetDir/weblogo.png";
    push @lines, "$cpCmd $hDir/hmm/cluster_${num}.hmm $targetDir/hmm.hmm";
    push @lines, "$cpCmd $hDir/hmm/cluster_${num}.png $targetDir/hmm.png";
    push @lines, "$cpCmd $hDir/hmm/cluster_${num}.json $targetDir/hmm.json";
    push @lines, "$cpCmd $hDir/align/cluster_${num}.afa $targetDir/msa.afa";

    return @lines;
}


sub getCopyConsResFiles {
    my $self = shift;
    my $inputCaDir = shift;
    my $targetDir = shift;
    my $subCluster = shift;
    my $isDiced = shift || 0;
    my $subClusterRemap = shift || "";
    my $subClusterRemapFile = shift || "";

    my $cpCmd = $self->{cp_cmd};
    my $appDir = $self->{app_dir};

    my $subCArg = "";
    if ($isDiced and $subClusterRemapFile) {
        $subCArg = "--sub-cluster-map-file $subClusterRemapFile";
    }

    my @lines;
    foreach my $consFile (glob("$inputCaDir/*ConsensusResidue_*Position*")) {
        (my $res = $consFile) =~ s/^.*_ConsensusResidue_([A-Z])_.*$/$1/;
        my $file = "consensus_residue_${res}_position.txt";
        if (not $isDiced and not $subCluster) {
            push @lines, "$cpCmd $consFile $targetDir/$file";
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
    my $self = shift;
    my $cluster = shift;
    my $inputCaDir = shift;
    my $hDir = shift;
    my $locOutDir = shift;
    my $ascore = shift;
    my $convRatioJobDir = shift || "";

    my $appDir = $self->{app_dir};

    my @urFiles50 = glob("$inputCaDir/cluster-data/uniref50-nodes/*");
    my @urFiles90 = glob("$inputCaDir/cluster-data/uniref90-nodes/*");
    my $hasUniRef = scalar @urFiles50 > 2 ? 50 : (scalar @urFiles90 > 2 ? 90 : 0);

    my $sizeFile = "$inputCaDir/cluster_num_map.txt";
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
        my $fastaFile = "$inputCaDir/cluster-data/fasta/cluster_$subNum.fasta";
        my $afaFile = "$hDir/align/cluster_$subNum.afa";
        (my $hmmPngFile = $hmm) =~ s/\.hmm$/.png/;
        (my $hmmJsonFile = $hmm) =~ s/\.hmm$/.json/;

        push @{$self->{_diced_clusters}}, "$cluster\t$ascore\t$targetId";
        $self->writeDicedJobLine("# PROCESSING $clusterId->$targetId");
        $self->writeDicedJobLine("mkdir -p $targetDir");
        
        $self->writeDicedJobLine($self->getCopyHistoFiles($hDir, $targetDir, $subNum));
        $self->writeDicedJobLine($self->getCopyIdFiles($inputCaDir, $targetDir, $subNum, $hasUniRef));
        $self->writeDicedJobLine($self->getCopyHmmFiles($hDir, $targetDir, $subNum));
    }

    # Parent has already been processed.
    $self->writeDicedJobLine($self->getCopyConsResFiles($inputCaDir, $locOutDir, \@subClusters, 1, "", $sizeFile));

    #TODO: split conv_ratio
    my $convRatioFile = "$convRatioJobDir/conv_ratio.txt";
    my $line = "$appDir/split_tab_file.pl --source $convRatioFile --output-dir-pat $locOutDir- --name-pat conv_ratio.txt --sub-cluster-map-file $sizeFile";
    $self->writeDicedJobLine($line);
}


sub writeSplitSsnJobLine {
    my $self = shift;
    push @{ $self->{_split} }, @_;
}
sub writeDicedJobLine {
    my $self = shift;
    $self->writeJobLine(@_);
}
sub writeJobLine {
    my $self = shift;
    push @{ $self->{_commands} }, @_;
}



1;

