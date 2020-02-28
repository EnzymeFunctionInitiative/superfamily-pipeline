#!/usr/bin/env perl

use strict;
use warnings;


use Getopt::Long;
use Capture::Tiny qw(:all);
use File::Slurp;

my ($inputFile, $outputFile, $seqDbFile);
my $result = GetOptions(
    "input=s"       => \$inputFile,
    "output=s"      => \$outputFile,
    "seq-db=s"      => \$seqDbFile,
);


$seqDbFile = "" if not $seqDbFile;
$seqDbFile = $ENV{DFI_DB_PATH} if not $seqDbFile and $ENV{EFI_DB_PATH};

die getUsage("requires --seq-db or EFI_DB_PATH environment variable") if not $seqDbFile or not -f "$seqDbFile.pal";
die getUsage("requires --input file argument") if not $inputFile or not -f $inputFile;
die getUsage("requires --output file argument") if not $outputFile;


my @ids = map { $_ =~ s/[\r\n]//g; $_ } read_file($inputFile);

open my $fh, ">", $outputFile or die "Unable to open output file '$outputFile': $!";

while (scalar @ids) {
    my $batchLine = join(",", splice(@ids, 0, 1000));
    my $cmd = join(" ", "fastacmd", "-d", $seqDbFile, "-s", $batchLine);
    print $fh `$cmd`, "\n";
}

close $fh;




sub getUsage {
    my $msg = shift || "";
    $msg = "ERROR: $msg\n\n" if $msg;
    return <<USAGE;
${msg}usage: $0 --input <ID_LIST_FILE_PATH> --output <OUTPUT_FILE_PATH>
USAGE
}


