#!/bin/perl

use strict;
use warnings;

use FindBin;
use Getopt::Long;
use Data::Dumper;

use lib "$FindBin::Bin/../lib";

use EFI::HMM::Output;


my ($hmmOutput, $idList);
my $result = GetOptions(
    "hmm-output=s"      => \$hmmOutput,
    "id-list=s"         => \$idList,
);

die getUsage("require --hmm-output input table") if not $hmmOutput or not -f $hmmOutput;
die getUsage("require --id-list output file") if not $idList;


my $hmmParser = new EFI::HMM::Output;

my $ids = $hmmParser->parse($hmmOutput);

open my $fh, ">", $idList or die "Unable to write to id list file $idList: $!";

foreach my $id (sort keys %$ids) {
    print $fh "$id\n";
}

close $fh;






sub getUsage {
    my $msg = shift || "";
    $msg = "ERROR: $msg\n\n" if $msg;
    return <<USAGE;
${msg}usage: $0 --hmm-output <HMMSEARCH_OUTPUT_TABLE> --id-list <PATH_TO_OUTPUT_FILE>

USAGE
}

