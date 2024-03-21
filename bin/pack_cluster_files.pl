#!/bin/env perl

use strict;
use warnings;

use Getopt::Long;
use FindBin;
use Archive::Tar;
use Cwd;

use lib "$FindBin::Bin/../lib";

use DataCollection::Directory;


my %args;
my $result = GetOptions(\%args,
    "load-dir=s",
);

die "Need --load-dir" if not $args{"load-dir"};
die "Need valid --load-dir" if not -d $args{"load-dir"};

my $dirHelper = DataCollection::Directory->new(load_dir => $args{"load-dir"});


my @packFiles = (
    "consensus_residue_C_position.txt",
    "conv_ratio.txt",
    "hmm.hmm",
    "hmm.json",
    "msa.afa",
    "uniprot.fasta",
    "uniprot.txt",
    "uniref90.fasta",
    "uniref90.txt",
);

my $curDir = getcwd();

my $findFn = sub {
    my $path = shift;
    my $info = shift;

    print("Processing $info->{cluster_id} $path\n");

    chdir($path);

    my $tar = Archive::Tar->new();
    $tar->add_files(@packFiles);
    $tar->write("packed_files.tar");

    my $oldDir = "$path/_Z_old";
    mkdir($oldDir);
    map { rename("$path/$_", "$oldDir/$_"); } @packFiles;

    chdir($curDir);
};


$dirHelper->traverse($findFn);



