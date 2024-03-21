#!/bin/env perl

use strict;
use warnings;


my $confFile = $ARGV[0];
my $siteDir = $ARGV[1];
my $dataDir = $ARGV[2];
my $projectName = $ARGV[3];
my $tempDir = $ARGV[4];


die "Require site dir" if not $siteDir;
die "Require data dir" if not $dataDir or not -d $dataDir;
die "Require project name" if not $projectName;
die "Require temp dir" if not $tempDir or not -d $tempDir;


my $bak = "$confFile.temp";

open my $in, "<", $confFile or die "Unable to open config file $confFile: $!";
open my $out, ">", $bak or die "Unable to open temp config file $bak for writing: $!";

while (my $line = <$in>) {
    if ($line =~ m/"__TEMP_DIR__"/) {
        $out->print("define(\"__TEMP_DIR__\",\"$tempDir\");\n");
    } elsif ($line =~ m/"__DATA_BASE_DIR__"/) {
        $out->print("define(\"__DATA_BASE_DIR__\",\"$dataDir\");\n");
    } elsif ($line =~ m/"__DATA_VERSION_PREFIX__"/) {
        $out->print("define(\"__DATA_VERSION_PREFIX__\",\"$projectName\");\n");
    } else {
        $out->print($line);
    }
}

close $out;
close $in;

unlink $confFile;
rename $bak, $confFile;


