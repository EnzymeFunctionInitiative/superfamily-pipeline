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

die getUsage("require --json-file argument") if not -f $parms->{json_file};

my $db = new EFI::HMM::Database($parms);


my $jsonText = read_file($parms->{json_file});
$jsonText = cleanJson($jsonText);
my $json = decode_json($jsonText);
    
$db->networkUiJsonToSqlite($json);


sub getUsage {
    my $msg = shift || "";

    $msg = "ERROR: $msg\n\n" if $msg;
    return <<USAGE;
${msg}usage: $0 --json-file <JSON_FILE_PATH> --sqlite-file <OUTPUT_SQLITE_FILE_PATH>

USAGE
}

