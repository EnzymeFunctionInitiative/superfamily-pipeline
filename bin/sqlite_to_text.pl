#!/bin/env perl

use strict;
use warnings;


use FindBin;
use Data::Dumper;

use lib "$FindBin::Bin/../lib";

use EFI::HMM::Database qw(validateInputs);


my $parms = validateInputs(\&getUsage);

die getUsage("require --output-file argument") if not $parms->{output_file};
die getUsage("require --columns argument") if not $parms->{columns};
die getUsage("require --table argument") if not $parms->{table};

my $db = new EFI::HMM::Database($parms);


my $data = $db->getTabularData($parms->{table}, $parms->{columns});


open my $fh, ">", $parms->{output_file};

foreach my $row (@$data) {
    $fh->print(join("\t", @$row), "\n");
}

close $fh;


sub getUsage {
    my $msg = shift || "";

    $msg = "ERROR: $msg\n\n" if $msg;
    return <<USAGE;
${msg}usage: $0 --output-file <OUTPUT_FILE_PATH> --sqlite-file <OUTPUT_SQLITE_FILE_PATH> --table <TABLE_NAME> --columns col_name[,col_name2,...]

USAGE
}

