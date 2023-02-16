
package MasterFile;


use Exporter qw(import);


our @EXPORT_OK = qw(parse_master_file make_file_cluster_id);



sub parse_master_file {
    my $file = shift;
    my $unirefVersion = shift || 90;

    my $data = {};

    open my $fh, "<", $file or die "Unable to read master file $file: $!";

    chomp(my $header = <$fh>);
    die "Invalid header file $file (empty)" if not $header;
    $header =~ s/^#//;
    my %keymap = ("cluster" => "cluster_id", "uniref50 generate job" => "ur50_job", "uniref90 generate job" => "ur90_job", "subgroup [sfld]" => "sfld", "min len" => "min_len", "max len" => "max_len", "dicing as" => "dicing_as", "image as" => "image_as");
    my @cols = split(m/\t/, lc $header);
    my %hmap = map { ($keymap{$cols[$_]} // "") => $_ } 0..$#cols;

    while (my $line = <$fh>) {
        chomp $line;
        next if ($line =~ m/^\s*#/ or $line =~ m/^\s*$/);
        my @parts = split(m/\t/, $line);
        my ($clusterName, $uniref50JobId, $uniref90JobId, $minLen, $maxLen, $imageAscore, $ascores, $sfld) =
            map { $parts[$hmap{$_}] } ("cluster_id", "ur50_job", "ur90_job", "min_len", "max_len", "image_as", "dicing_as", "sfld");

        my $isPlaceholder = (not $uniref50JobId and not $uniref90JobId and $clusterName) ? 1 : 0;
        
        my $clusterId = make_file_cluster_id($clusterName);

        my @ascores;
        my $jobId = "";
        my $sfldNum = "";
        if ($isPlaceholder) {
            $sfld = "";
            $minLen = "";
            $maxLen = "";
            $imageAscore = "";
        } else {
            @ascores = parse_ascores(split(m/,/, $ascores));
            $jobId = $unirefVersion == 50 ? $uniref50JobId : $uniref90JobId;
            $sfld = "" if not $sfld;
            $sfldNum = $sfld ? ($sfld =~ s/^.*\[(\d+)\].*$/$1/r) : "";
        }
        $data->{$clusterId} = {cluster_name => $clusterName, job_id => $jobId, min_len => $minLen, max_len => $maxLen, ascores => \@ascores, primary_ascore => $imageAscore, children => [], sfld => $sfld, sfld_num => $sfldNum};
    }

    close $fh;

    foreach my $clusterId (keys %$data) {
        my @p = split(m/\-/, $clusterId);
        next if scalar @p <= 2;
        my $parentId = join("-", @p[0..($#p-1)]);
        if ($data->{$parentId}) {
            push @{$data->{$parentId}->{children}}, $clusterId;
        }
    }

    return $data;
}


sub parse_ascores {
    my @as = @_;
    my @ascores;
    foreach my $as (@as) {
        if ($as =~ m/^\d+$/) {
            push @ascores, $as;
        } elsif ($as =~ m/^(\d+):(\d+):(\d+)$/) {
            my $s = $1;
            my $e = $3;
            my $inc = $2;
            for (my $i = $s; $i < $e; $i += $inc) {
                push @ascores, $i;
            }
            push @ascores, $e;
        }
    }
    return @ascores;
}


sub make_file_cluster_id {
    my $clusterId = shift;
    $clusterId = lc $clusterId;
    $clusterId =~ s/^mega\-/cluster-/;
    return $clusterId;
}


1;

