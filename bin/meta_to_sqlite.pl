#!/bin/env perl

use strict;
use warnings;


use FindBin;
use Data::Dumper;
use File::Slurp;

use lib "$FindBin::Bin/../lib";

use EFI::HMM::Database qw(validateInputs);


my $parms = validateInputs(\&getUsage);

die getUsage("require --data-dir argument") if $parms->{mode} =~ m/swissprot/ and not -d $parms->{data_dir};
die getUsage("require --load-ec-file") if $parms->{mode} =~ m/enzymecode/ and not -f $parms->{load_ec_file};
die getUsage("require --load-kegg-file") if $parms->{mode} =~ m/kegg/ and not -f $parms->{load_kegg_file};

my $db = new EFI::HMM::Database($parms);


if ($parms->{mode} =~ m/swissprot/) {
    $db->swissProtsToSqlite($parms->{data_dir});
}
if ($parms->{mode} =~ m/enzymecode/) {
    $db->ecToSqlite($parms->{load_ec_file});
}
if ($parms->{mode} =~ m/kegg/) {
    $db->keggToSqlite($parms->{load_kegg_file});
}




sub getUsage {
    my $msg = shift || "";

    $msg = "ERROR: $msg\n\n" if $msg;
    return <<USAGE;
${msg}usage: $0 --data-dir <CLUSTER_DATA_DIR_PATH> --sqlite-file <OUTPUT_SQLITE_FILE_PATH>
    [--mode enzymecode --load-ec-file <EC_DESC_FILE_PATH>]
    [--mode swissprot]
    [--mode kegg]

USAGE
}

