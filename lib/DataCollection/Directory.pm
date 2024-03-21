
package DataCollection::Directory;

use strict;
use warnings;


sub new {
    my $class = shift;
    my %args = @_;

    my $self = {};
    $self->{load_dir} = $args{load_dir};

    bless $self, $class;

    return $self;
}


sub traverse {
    my $self = shift;
    my $findFn = shift;

    my $isClusterFn = sub {
        my $dir = shift;
        my $clusterId = $dir =~ s%^.*/([^/]+)$%$1%r // "";
        my $hasHmm = -f "$dir/hmm.hmm";
        my $hasSsn = -f "$dir/ssn_lg.png";
        my $info = {
            is_cluster => $hasHmm && $hasSsn,
            cluster_id => $clusterId,
        };
        return $info;
    };

    my @tldClusters = grep { -d } glob("$self->{load_dir}/cluster*");

    foreach my $tldClusterDir (@tldClusters) {
        my @dicingDirs = glob("$tldClusterDir/dicing-*");
        my @dicings = map { s%^/dicing-(\d+)$%$1%r } @dicingDirs;

        my $info = $isClusterFn->($tldClusterDir);
        $info->{is_diced} = @dicings > 0;
        $info->{dicings} = \@dicings if @dicings > 0;

        $findFn->($tldClusterDir, $info) if $info->{is_cluster};

        foreach my $dicedDir (@dicingDirs) {
            my $dicedDirInfo = $isClusterFn->($dicedDir);
            $dicedDirInfo->{is_diced} = 1;
            $dicedDirInfo->{dicings} = [];
            $findFn->($dicedDir, $dicedDirInfo) if $info->{is_cluster};

            my @subClusterDirs = glob("$dicedDir/cluster-*");

            foreach my $subClusterDir (@subClusterDirs) {
                my $subDirInfo = $isClusterFn->($subClusterDir);
                $findFn->($subClusterDir, $subDirInfo) if $info->{is_cluster};
            }
        }
    }
}


1;

