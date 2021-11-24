#!/bin/env perl

use strict;
use warnings;

use Getopt::Long;
use Data::Dumper;
use FindBin;

use lib "$FindBin::Bin/../lib";

use IdListParser;

my $AppDir = $FindBin::Bin;


my ($rawMapFile, $rawRegionsFile, $outSfldMapFile, $outNetInfoFile, $outRegionsFile, $outJobIds, $outDicedJobIds, $outAscore, $rawDicedIdFile);
my $result = GetOptions(
    "raw-map=s"             => \$rawMapFile, # raw mapping file from excel: cluster_id<TAB>name_including_SFLD_num
    "regions=s"             => \$rawRegionsFile, # raw regions file from excel: cluster_id<TAB>regions...
    "diced=s"               => \$rawDicedIdFile,
    "out-sfld-map=s"        => \$outSfldMapFile,
    "out-regions=s"         => \$outRegionsFile,
    "out-net-info=s"        => \$outNetInfoFile,
    "out-job-ids=s"         => \$outJobIds,
    "out-diced-job-ids=s"   => \$outDicedJobIds,
#    "out-diced-ascore=s"    => \$outAscore,
);

die "Need --raw-map" if not $rawMapFile or not -f $rawMapFile;
die "Need --regions" if not $rawRegionsFile or not -f $rawRegionsFile;
die "Need --diced id file" if not $rawDicedIdFile or not -f $rawDicedIdFile;
die "Need --out-regions" if not $outRegionsFile;
die "Need --out-sfld-map" if not $outSfldMapFile;
die "Need --out-net-info" if not $outNetInfoFile;
die "Need --out-job-ids" if not $outJobIds;
die "Need --out-diced-job-ids" if not $outDicedJobIds;
#die "Need --out-diced-ascore" if not $outAscore;



open my $dicedFh, "<", $rawDicedIdFile;

my %dicedInfo;

while (<$dicedFh>) {
    chomp;
    next if m/^#/ or m/^\s*$/;
    my ($cluster, $ascore, $clusterJob, $crJob) = split(m/\t/);
    $cluster = clusterNameToId($cluster);
    $ascore = formatAscore($ascore);
    push @{ $dicedInfo{$cluster} }, {ascore => $ascore, cluster_job => $clusterJob, cr_job => $crJob};
}

close $dicedFh;




my %clusterNames;

open my $mapFh, "<", $rawMapFile;

my $headerRow = <$mapFh>;
my %cols = parseHeaderRow($headerRow);

my @nonDicedJobOut;
my @dicedJobOut;
my $fullNetJobId = 0;

# my %cols = (cluster => [0, 4], uniprot_cluster_num => 5, ascore => [6, 9], color_job => 0, cluster_job => 0, cr_job => 0, uniref_type => 50, sfld => "");
while (<$mapFh>) {
    chomp;
    next if m/^#/ or m/^\s*$/;
    my ($parentMega, @p) = split(m/\t/);
    my $parentMegaId = clusterNameToId($parentMega);
    my ($clusterId, $clusterName, $ascore) = getClusterId($cols{cluster}, $cols{ascore}, @p);
    if (not $clusterId) {
        $clusterId = $parentMegaId;
        $clusterName = $parentMega;
    }
    my $colorJob = $p[$cols{color_job}] // "";
    my $clusterJob = $p[$cols{cluster_job}] // "";
    my $crJob = $p[$cols{cr_job}] // "";
    my $clusterNum = getClusterNum($clusterId, \@p, $cols{ssn_uniprot_cluster_num}, $cols{ca_uniprot_cluster_num});
    (my $uniRefType = $p[$cols{uniref_type}] // "") =~ s/\D//g;
    my $sfld = $p[$cols{sfld}] // "";

    my $mainJobId = $clusterJob ? $clusterJob : $colorJob;
    #my ($ssnId, $aId, $fullAId, $colorId, $fullColorId) = ($mainJobId, 0, 0, $mainJobId, $mainJobId);
    my @nonDicedLine = ($clusterId, $mainJobId);
    #push @nonDicedLine, ($mainJobId, 0, 0, $mainJobId, $mainJobId);
    #my ($minLen, $maxLen) = (0, 0);
    #push @nonDicedLine, (0, 0);
    #my $expandClusters = $clusterNum;
    push @nonDicedLine, $clusterNum;
    # $crJobIda
    push @nonDicedLine, $crJob;
    # $ascore
    push @nonDicedLine, $ascore;

    if ($dicedInfo{$clusterId}) {
        foreach my $info (@{ $dicedInfo{$clusterId} }) {
            #my @dicedLine = ($clusterId, $info->{cluster_job}, 0, 0, $info->{cluster_job}, $info->{cluster_job});
            #push @dicedLine, (0, 0);
            my @dicedLine = ($clusterId, $info->{cluster_job});
            #push @dicedLine, (0, 0);
            push @dicedLine, 1;
            push @dicedLine, $info->{cr_job};
            push @dicedLine, $info->{ascore};
            push @dicedJobOut, \@dicedLine;
        }
    }

    push @nonDicedJobOut, \@nonDicedLine;
    
    $clusterNames{$clusterId} = {num => 0, short_name => $clusterName, name => $sfld};
    if ($sfld =~ s/\[(\d+)\]//) {
        $clusterNames{$clusterId}->{num} = $1;
    }

    $fullNetJobId = $mainJobId if not $fullNetJobId;
}

close $mapFh;

{ # Handle full network
    my @nonDicedLine = ("fullnetwork", $fullNetJobId);
    #push @nonDicedLine, ($fullNetJobId, 0, 0, $fullNetJobId, $fullNetJobId);
    #push @nonDicedLine, (0, 0);
    push @nonDicedLine, 1;
    push @nonDicedLine, "";
    push @nonDicedLine, "";
    push @nonDicedJobOut, \@nonDicedLine;
    $clusterNames{"fullnetwork"} = {num => 0, short_name => "", name => ""};
}




my @clusterIds = sort clusterIdSort keys %clusterNames;

open my $netInfoFh, ">", $outNetInfoFile;
open my $sfldMapFh, ">", $outSfldMapFile;

foreach my $cluster (@clusterIds) {
    my $netInfoLine = join("\t", $cluster, $clusterNames{$cluster}->{short_name}, $clusterNames{$cluster}->{name});
    $netInfoFh->print($netInfoLine, "\n");
    my $sfldNum = $clusterNames{$cluster}->{sfld_num};
    if ($sfldNum) {
        my $sfldMapLine = join("\t", $cluster, $sfldNum);
        $sfldMapFh->print($sfldMapLine, "\n");
    }
}

close $sfldMapFh;
close $netInfoFh;




open my $jobIdFh, ">", $outJobIds;

foreach my $row (@nonDicedJobOut) {
    $jobIdFh->print(join("\t", @$row), "\n");
}

close $jobIdFh;


open my $dicedJobIdFh, ">", $outDicedJobIds;

foreach my $row (@dicedJobOut) {
    $dicedJobIdFh->print(join("\t", @$row), "\n");
}

close $dicedJobIdFh;




open my $rawRegionsFh, "<", $rawRegionsFile;
open my $regionsFh, ">", $outRegionsFile;

my $curKey = "";
my $regionIndex = 1;
while (<$rawRegionsFh>) {
    chomp;
    my $line = $_;
    next if m/^\s*$/ or m/^#/;
    my ($cluster, $subCluster, @regions) = split(m/[\s]+/);
    if ($curKey eq $cluster) {
        $regionIndex++;
    } else {
        $curKey = $cluster;
        $regionIndex = 0;
    }

    print "$subCluster [$line] from regions not found in cluster name file; not including in regions file\n" and next if not $clusterNames{$subCluster}->{short_name};
    my $shortName = $clusterNames{$subCluster}->{short_name};
    $shortName =~ s/Megacluster-/Mega-/;
    $shortName =~ s/Cluster-//;

    my $regionLine = join("\t", $cluster, $subCluster, $shortName, $regionIndex, @regions);
    $regionsFh->print($regionLine, "\n"); 
}

close $regionsFh;
close $rawRegionsFh;









sub clusterIdSort {
    return IdListParser::clusterIdSort($a, $b);
}


sub parseHeaderRow {
    my $row = shift;
    chomp $row;
    my (@parts) = split(m/\t/, $row);
    # All are indexes
    my %cols = (cluster => [0, 4], ssn_uniprot_cluster_num => 5, ca_uniprot_cluster_num => 6, ascore => [7, 9], color_job => 0, cluster_job => 0, cr_job => 0, uniref_type => 50, sfld => "");
    my $offset = 1; # to account for first col in job line being the ID
    for (my $i = 0; $i <= $#parts; $i++) {
        my $oi = $i - $offset;
        if ($parts[$i] =~ m/ssn\s+uniprot\s+cluster/i) {
            $cols{cluster}->[1] = $oi - 1;
            $cols{ssn_uniprot_cluster_num} = $oi;
        } elsif ($parts[$i] =~ m/ca\s+uniprot\s+cluster/i) {
            $cols{ca_uniprot_cluster_num} = $oi;
        } elsif ($parts[$i] =~ m/alignment\s+score/i) {
            $cols{ascore}->[0] = $oi;
        } elsif ($parts[$i] =~ m/color\s+ssn\s+job/i) {
            $cols{ascore}->[1] = $oi - 1;
            $cols{color_job} = $oi;
        } elsif ($parts[$i] =~ m/cluster\s+analysis\s+job/i) {
            $cols{cluster_job} = $oi;
        } elsif ($parts[$i] =~ m/cr\s+job/i) {
            $cols{cr_job} = $oi;
        } elsif ($parts[$i] =~ m/unirefnn/i) {
            $cols{uniref_type} = $oi;
        } elsif ($parts[$i] =~ m/sfld/i) {
            $cols{sfld} = $oi;
        }
    }
    return %cols;
}


sub getClusterNum {
    my ($clusterId, $p, $ssnNum, $caNum) = @_;
    if ($p->[$caNum]) {
        return $p->[$caNum];
    } elsif ($p->[$ssnNum]) {
        return $p->[$ssnNum];
    } else {
        return 1;
    }
}


sub clusterNameToId {
    my $name = shift;
    my $id = $name;
    $id =~ s/mega-/cluster-/i;
    $id =~ s/megacluster-/cluster-/i;
    $id =~ s/Cluster-/cluster-/i;
    return $id;
}


sub getClusterId {
    my $clusterRange = shift;
    my $ascoreRange = shift;
    my @p = @_;

    my $ascore = "";
    my $clusterName = "";
    for (my $i = $clusterRange->[0]; $i <= $clusterRange->[1]; $i++) {
        if ($p[$i] =~ m/\S/) {
            $clusterName = $p[$i];
            my $offset = $i - $clusterRange->[0];
            my $idx = $ascoreRange->[0] + $offset;
            #my $idx = $ascoreRange->[0];
            ($ascore = $p[$idx] // "") =~ s/\D//g;
            last;
        }
    }

    my $clusterId = clusterNameToId($clusterName);
    $clusterName =~ s/Mega-/Megacluster-/;
    $ascore = formatAscore($ascore);

    return ($clusterId, $clusterName, $ascore);
}


sub formatAscore {
    my $ascore = shift;
    $ascore =~ s/\D//g;
    return $ascore;
}













#sub processCluster {
#    my ($arc, $cluster, $clusters) {
#
#    my @clusterIds = sort clusterIdSort keys %$arc;
#
#    my $index = 1;
#    foreach my $cid (@clusterIds) {
#        $clusters->{$cid}->{region_index} = $index++;
#        processCluster($arc->{$cid}, $cid, $clusters);
#    }
#}




