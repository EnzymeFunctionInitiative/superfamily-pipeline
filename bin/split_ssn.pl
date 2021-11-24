#!/bin/env perl

BEGIN {
    die "Need env var EFI_TOOLS_HOME\n" if not $ENV{EFI_TOOLS_HOME} or not -d $ENV{EFI_TOOLS_HOME};
}

use strict;
use warnings;

use FindBin;
use lib $ENV{EFI_TOOLS_HOME} . "/lib";

use Getopt::Long;

use EFI::SSN::Parser;
use EFI::SSN::Parser::Split;


my ($ssnIn, $baseOutName, $outFile, $theCluster, $doMkdir, $dissectStr);
my $result = GetOptions(
    "ssn-in=s"          => \$ssnIn,
    "mkdir"             => \$doMkdir,

    "sub-clusters=s"    => \$dissectStr,
    "output-dir-pat=s"  => \$baseOutName,

    "sub-cluster=s"     => \$theCluster,
    "output-file=s"     => \$outFile,
);

my $usage = <<USAGE;
$0 --ssn-in path_to_input_ssn --output-dir path_to_output_directory
USAGE

die "$usage\ninvalid --ssn-in" if not $ssnIn or not -f $ssnIn;
die "$usage\ninvalid --output-dir-pat or --output-file" if not $baseOutName and not $outFile;

$doMkdir = defined $doMkdir;

my $dissect;
if ($dissectStr) {
    $dissect = {};
    my @p = split(m/,/, $dissectStr);
    foreach my $p (@p) {
        if ($p =~ m/:/) {
            my ($s, $e) = split(m/:/, $p);
            $dissect->{$s} = $e;
        } else {
            $dissect->{$p} = 0;
        }
    }
} elsif ($theCluster) {
    my (@p) = split(m/:/, $theCluster);
    $dissect->{$p[0]} = (defined $p[1] ? $p[1] : $p[0]);
}


my %args;
if ($baseOutName) {
    $args{base_out_name} = $baseOutName;
} else {
    $args{out_file} = $outFile;
    print "PROCESSING $ssnIn -> $outFile ($theCluster)\n";
}

my $flags = EFI::SSN::Parser::OPT_EXCLUDE_METADATA;
my $ssn = EFI::SSN::Parser::Split::openSplitSsn($ssnIn, $doMkdir, %args);
$ssn->parseSplit($flags);
$ssn->writeSplit($dissect, $flags);


