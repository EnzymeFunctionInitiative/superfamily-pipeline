#!/bin/env perl

use strict;
use warnings;


use FindBin;
use Data::Dumper;
use File::Slurp;
use JSON;

use lib "$FindBin::Bin/../lib";

use EFI::HMM::Database qw(validateInputs cleanJson);


my $parms = validateInputs(\&getUsage);

die getUsage("require --json-file argument") if not $parms->{json_file};

my $db = new EFI::HMM::Database($parms);


my $json = {};
if ($parms->{in_place} and -f $parms->{json_file}) {
    my $jsonText = read_file($parms->{json_file});
    $jsonText = cleanJson($jsonText);
    $json = decode_json($jsonText);
}

if ($parms->{mode} =~ m/swissprot/) {
    $db->swissProtsToJson($json);
}
if ($parms->{mode} =~ m/enzymecode/) {
    $db->enzymeCodeToJson($json);
}
if ($parms->{mode} =~ m/kegg/) {
    $db->keggToJson($json);
}
if ($parms->{mode} =~ m/subgroup/) {
    $db->subgroupToJson($json);
}

my $JSON = JSON->new->allow_nonref;
my $jsonText = $JSON->pretty->encode($json);
write_file($parms->{json_file}, $jsonText);


sub getUsage {
    my $msg = shift || "";

    $msg = "ERROR: $msg\n\n" if $msg;
    return <<USAGE;
${msg}usage: $0 --json-file <OUTPUT_JSON_FILE_PATH> --sqlite-file <OUTPUT_SQLITE_FILE_PATH>
    [--mode swissprot]

USAGE
}

