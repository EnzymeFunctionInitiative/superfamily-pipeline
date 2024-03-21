#!/bin/perl

use strict;
use warnings;

use Getopt::Long;
use Data::Dumper;




my ($idMapping, $appendFile, $outFile);
my $result = GetOptions(
    "id-mapping=s@"     => \$idMapping,
    "append-mapping=s@" => \$appendFile,
    "out-sorted=s"      => \$outFile,
);


die "Need --id-mapping" if not $idMapping;
die "Need --out-sorted" if not $outFile;




open my $out, ">", $outFile;

my $masterRowIndex = 0;
foreach my $file (@$idMapping) {
    processFile($file, $out, \$masterRowIndex);
    print "ROW $masterRowIndex\n";
}



close $out;





sub processFile {
    my $file = shift;
    my $out = shift;
    my $rowIndexRef = shift;

    my $ids = readFile($file);

    sortClusters($ids);

    my $clusterLengthSortFn = sub {
        my $aid = $a;
        my $bid = $b;
        return clusterLengthSort($aid, $bid, $ids);
    };
    # sort each id by clusterLengthSort
    my @sorted = sort $clusterLengthSortFn keys %$ids;

    my $indexes = writeOutput($out, $ids, \@sorted, $rowIndexRef);

    outputIndexes($indexes);
}








sub outputIndexes {
    my $indexes = shift;
    foreach my $cluster (sort clusterSort keys %$indexes) {
        my $d = $indexes->{$cluster};
        my @parms = reverse split(m/,/, $cluster);
        push @parms, $d->{start}, $d->{end};
        print join("\t", @parms), "\n";
    }
}


sub writeOutput {
    my $out = shift;
    my $ids = shift;
    my $sorted = shift;
    my $rowNumRef = shift;

    my %indexes;
    my $rowNum = $$rowNumRef;
    my $lastRowNum = 0;
    my $lastCluster = "";
    
    foreach my $id (@$sorted) {
        my @cs = @{ $ids->{$id} };
        #my @clusterInfo = reverse split(m/,/, $cs[0]);
        #print $out join("\t", @clusterInfo, $id), "\n";
        foreach my $cluster (@cs) {
            my @clusterInfo = reverse split(m/,/, $cluster);
            print $out join("\t", @clusterInfo, $id), "\n";
            $indexes{$cluster}->{start} = $rowNum if not exists $indexes{$cluster};
            $indexes{$cluster}->{end} = $rowNum;
        }
        $rowNum++;
    }

    $$rowNumRef = $rowNum;

    return \%indexes;
}


sub readFile {
    my $file = shift;

    my %ids;

    open my $fh, "<", $file;
    print "Processing $file\n";
    while (my $line = <$fh>) {
        chomp $line;
        next if $line =~ m/^\s*$/ or $line =~ m/^#/;
        my @p = split(m/\t/, $line);
        my $cluster = $p[0];
        next if $cluster !~ m/^cluster/i;
        my $id = $p[$#p];
        $cluster = "$p[1],$cluster" if $#p > 1; # add ascore
        push @{ $ids{$id} }, $cluster;
    }
    close $fh;

    return \%ids;
}


sub sortClusters {
    my $ids = shift;
    foreach my $id (keys %$ids) {
        my @s = sort { length($b) <=> length($a) } @{ $ids->{$id} };
        $ids->{$id} = \@s;
    }
}


#sub clusterLengthSort {
#    my $aid = $a;
#    my $bid = $b;
#    return clusterLengthSortFn($aid, $bid, $ids);
#}
#sub clusterLengthSortAscore {
#    my $aid = $a;
#    my $bid = $b;
#    return clusterLengthSortFn($aid, $bid);
#}
sub clusterLengthSort {
    my ($aid, $bid, $ids) = @_;
    my @pa = @{$ids->{$a}}; # list of clusters for this ID
    my @pb = @{$ids->{$b}}; # list of clusters for this ID

#    # First compare alignment scores, if any
#    my ($asa, $asb);
#    if ($pa[$#pa] =~ s/,(\d+)$//) {
#        $asa = $1;
#    }
#    if ($pb[$#pb] =~ s/,(\d+)$//) {
#        $asb = $1;
#    }

#    my $debug = ($aid eq "A0A009H000" or $bid eq "A0A009H000");
#    print "$aid $bid ", join(",", @pa), " ", join(",", @pb) if $debug;

    my $compClusterPartsFn = sub {
        my ($ca, $cb) = @_;
        my @pca = split(m/-/, $ca);
        my @pcb = split(m/-/, $cb);
        my $maxIdx = $#pca > $#pcb ? $#pcb : $#pca;
        for (my $i = 0; $i <= $maxIdx; $i++) {
            my $r = 0;
#            print "      |$pca[$i]|\n" if $debug;
            if ($pca[$i] =~ m/^\d+$/) {
                $r = $pca[$i] <=> $pcb[$i];
#                print "                    what  $pca[$i] $pcb[$i] $r\n" if $debug;
            } else {
                $r = $pca[$i] cmp $pcb[$i];
            }
            return $r if $r;
        }
        return -1 if $#pca > $maxIdx;
        return 1 if $#pcb > $maxIdx;
        return $ca cmp $cb;
    };

    my $maxIdx = $#pa > $#pb ? $#pb : $#pa;
    for (my $ci = 0; $ci <= $maxIdx; $ci++) {
        my $r = &$compClusterPartsFn($pa[$ci], $pb[$ci]);
        return $r if $r;
    }

    return -1 if $#pa > $maxIdx;
    return 1 if $#pb > $maxIdx;
    
#    return -1 if $asa and not $asb;
#    return 1 if $asb and not $asa;
#    my $ret = ($asa and $asb) ? ($asa <=> $asb) : 0;
#    return $ret if $ret;

    return $aid cmp $bid;
}


sub clusterSort {
    my ($aid, @pa) = split(m/-/, $a);
    my ($bid, @pb) = split(m/-/, $b);
    
    my $maxIdx = $#pa > $#pb ? $#pb : $#pa;
    for (my $i = 0; $i <= $maxIdx; $i++) {
        #my $r = $pa[$i] cmp $pb[$i];
        #return $r if $r;
        my $r = 0;
        if ($pa[$i] =~ m/^\d+$/) {
            $r = $pa[$i] <=> $pb[$i];
        } else {
            $r = $pa[$i] cmp $pb[$i];
        }
        return $r if $r;
    }
    return 1 if $#pa > $maxIdx;
    return -1 if $#pb > $maxIdx;
    return $aid cmp $bid;
}



