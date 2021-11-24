

package IdListParser;


sub parseLine {
    my $line = shift;
    my $legacy = shift || 0;

    my ($cluster, @info) = split(m/\t/, $line, -1);
    print STDERR "$cluster: no info found; ignoring\n" and next if scalar @info < 3 or not $info[0];

    my %parms;
    if ($legacy) {
        my ($ssnId, $aId, $fullAId, $colorId, $fullColorId, $minLen, $maxLen, $expandClusters, $crJobId, $ascore) = @info;
        $minLen ||= 0;
        $maxLen ||= 0;
        $crJobId ||= 0;
        $ascore ||= 0;
        $expandClusters = parseExpandClusters($expandClusters);
    
        %parms = (
            ssnId => $ssnId,
            aId => $aId,
            fullAId => $fullAId,
            colorId => $colorId,
            fullColorId => $fullColorId,
            minLen => $minLen,
            maxLen => $maxLen,
            expandClusters => $expandClusters,
            crJobId => $crJobId,
            ascore => $ascore,
        );
    } else {
        my ($ssnId, $expandClusters, $crJobId, $ascore) = @info;
        $crJobId ||= 0;
        $ascore ||= 0;
        $expandClusters = parseExpandClusters($expandClusters);
    
        %parms = (
            ssnId => $ssnId,
            expandClusters => $expandClusters,
            crJobId => $crJobId,
            ascore => $ascore,
        );
    }

    return ($cluster, \%parms);
}


sub parseFileBase {
    my $file = shift;
    my $parseFn = shift;

    my $clusters = {};
    my $arc = {};
    if (not ref $parseFn) {
        my $dataDir = $parseFn;
        $parseFn = makeParseFn($dataDir, $clusters, $arc);
    }

    open my $fh, "<", $file or die "Unable to open id file $file: $!";
    
    while (<$fh>) {
        chomp;
        next if m/^\s*$/;
        next if m/^\s*#/;
        
        my ($cluster, $parms) = IdListParser::parseLine($_);

        &$parseFn($cluster, $parms);
    }

    close $fh;

    return ($clusters, $arc);
}


sub parseFile {
    my $file = shift;
    my $parseFn = shift;
    my ($clusters, $arc) = parseFileBase($file, $parseFn);
    return $clusters if $clusters;
}


sub parseFileExtra {
    my $file = shift;
    my $parseFn = shift;
    my ($clusters, $arc) = parseFileBase($file, $parseFn);
    return ($clusters, $arc) if $clusters;
}


sub makeParseFn {
    my $dataDir = shift;
    my $clusters = shift;
    my $arc = shift;
    my $parseFn = sub {
        my ($cluster, $parms) = @_;
        (my $num = $cluster) =~ s/^.*?(\d+)$/$1/;
        $clusters->{$cluster} = {base_dir => "$dataDir/$cluster", number => $num};
        addToArc($arc, $cluster);
#        if ($parms->{expandClusters}) {
#            foreach my $ex (@{$parms->{expandClusters}}) {
#                my $cNum = $ex->[1];
#                my $subCluster = join("-", $cluster, $cNum);
#                addToArc($arc, $subCluster);
#                $clusters->{$subCluster} = {base_dir => "$dataDir/$subCluster", number => $cNum};
#            }
#        }
    };
    return $parseFn;
}
sub addToArc {
    my ($arc, $cluster) = @_;
    my @parts = split(m/\-/, $cluster);
    my $topLevel = $parts[0];
    my $ds;
    if (not $arc->{$topLevel}) {
        $arc->{$topLevel} = {};
    }
    $ds = $arc->{$topLevel};
    foreach my $p (1..$#parts) {
        my $C = join("-", @parts[0..$p]);
        if (not $ds->{$C}) {
            $ds->{$C} = {};
        }
        $ds = $ds->{$C};
    }
}
        

sub parseExpandClusters {
    my $str = shift;
    return undef if not $str;
    my @p = split(m/,/, $str);
    my @c;
    foreach my $p (@p) {
        my @a = split(/\-/, $p);
        if (scalar @a > 1) {
            my ($s, $e) = ($a[0], $a[$#a]);
            for ($s..$e) {
                push @c, [$_, $_];
            }
        } else {
            my $a = $a[0];
            if ($a =~ m/^(.*):(.*)$/) {
                push @c, [$1, $2];
            } else {
                push @c, [$a, $a];
            }
        }
    }
    return scalar @c ? \@c : undef;
}


sub loadAlignmentScoreFile {
    my $file = shift;

    my $data = {};

    open my $fh, "<", $file;
    while (<$fh>) {
        chomp;
        next if m/^\s*#/;
        my @parts = split(m/\t/);
        my ($clusterId, $ascore, $jobId, $crJobId) = @parts;
        my $info = {ascore => $ascore, job_id => $jobId};
        $info->{cr_job} = $crJobId if $crJobId;
        push @{$data->{$clusterId}}, $info;
    }
    close $fh;

    return $data;
}


sub getClusterNumbers {
    my ($cluster, $parms) = @_;
    (my $num = $cluster) =~ s/^.*?(\d+)$/$1/;
    my $info = {};
    if ($parms->{expandClusters}) {
        foreach my $ex (@{$parms->{expandClusters}}) {
            my $cNum = $ex->[1];
            my $subCluster = join("-", $cluster, $cNum);
            $info->{$subCluster} = {number => $cNum};
        }
    } else {
        $info->{$cluster} = {number => $num};
    }
    return $info;
}


sub clusterIdSort {
    my ($a, $b) = @_;
    my @a = split(m/-/, $a);
    my @b = split(m/-/, $b);
    my $minIdx = $#a < $#b ? $#a : $#b;
    for (my $i = 1; $i <= $minIdx; $i++) {
        return -1 if $a[$i] < $b[$i];
        return 1 if $a[$i] > $b[$i];
    }
    return -1 if $#a < $#b;
    return 1 if $#a > $#b;
    return 0;
}


1;
