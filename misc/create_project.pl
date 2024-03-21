#!/bin/env perl

use strict;
use warnings;

use Getopt::Long;
use Cwd qw(abs_path getcwd);
use FindBin;
use File::Path qw(make_path);
use File::Copy;
use HTTP::Tiny;


my $miscHome = abs_path("$FindBin::Bin/../misc");


my ($projectDir, $outVer, $loadVer, $cytoConfig);
my $result = GetOptions(
    "project-dir=s"         => \$projectDir,
    "output-version=s"      => \$outVer,
    "load-version=s"        => \$loadVer,
    "cytoscape-config=s"    => \$cytoConfig,
);


if (not $outVer) {
    my @dt = localtime();
    $outVer = sprintf("%02d%02d", $dt[4]+1, $dt[3]);
} elsif ($outVer !~ m/^\d\d\d\d$/) {
    die "--output-version must be in MMDD format.\n";
}

if (not $loadVer) {
    $loadVer = "1.0";
} elsif ($loadVer !~ m/^\d+\.\d+$/) {
    die "--load-version must be in #.# format.\n";
}

if (not $cytoConfig or not -f $cytoConfig) {
    $cytoConfig = "/home/groups/efi/apps/cytoscape/default_cytoscape_config.sh";
}




my @actions;

if (not $projectDir) {
    print "Use the current directory as the project directory? [Y/n] ";
    my $yn = <STDIN>;
    if ($yn =~ m/^\s*y/) {
        $projectDir = getcwd();
    } else {
        die "Requires --project-dir argument.\n";
    }
    push @actions, sub { make_path($projectDir); };
} elsif (-e $projectDir and not -d $projectDir) {
    die "The project dir $projectDir exists and it's not a directory\n";
} elsif (not -d $projectDir) {
    push @actions, sub { make_path($projectDir); };
}


my $dataDir = "$projectDir/data_$outVer";
my $loadDir = "$projectDir/load-$loadVer";
my $supportDir = "$projectDir/support";
push @actions, sub { make_path($dataDir); };
push @actions, sub { make_path($loadDir); };

push @actions, sub { copy("$miscHome/run_collect.sh", $projectDir); };
push @actions, sub { copy("$miscHome/local_conf.sh", $projectDir); };
push @actions, sub { copy("$miscHome/local_app_conf.sh.example", "$projectDir/local_app_conf.sh"); };
push @actions, sub { editLocalConf("$projectDir/local_conf.sh", $outVer, $loadVer, $projectDir); };
push @actions, sub { copy("$miscHome/sample_master.txt", "$projectDir/master_$outVer.txt"); };
push @actions, sub { copy($cytoConfig, "$projectDir/cytoscape_config.sh"); };
push @actions, sub { make_path("$dataDir/cytoscape/scripts"); make_path("$dataDir/cytoscape/temp"); };
push @actions, sub { copy("$miscHome/run_cytoscape.sh", $projectDir); };
push @actions, sub { copy("$miscHome/run_finalize.sh", $projectDir); };
push @actions, sub { make_path($supportDir); };
push @actions, sub { copy("$miscHome/sample_subgroup_info.txt", "$supportDir/subgroup_info.txt"); };
push @actions, sub { copy("$miscHome/tigr_names.txt", $supportDir); };
push @actions, sub { copy("$miscHome/enzclass.txt", $supportDir); };
push @actions, sub { copy("$miscHome/annotations.txt", $supportDir); };



foreach my $action (@actions) {
    &$action();
}


print <<TODO;

To-Do:

Add jobs to crontab:

    */5 * * * *    /bin/bash $projectDir/run_collect.sh >> $projectDir/run_collect.sh.cron.log 2>&1
    */2 * * * *    /bin/bash $projectDir/run_cytoscape.sh >> $projectDir/run_cytoscape.sh.cron.log 2>&1
    4,9,14,19,24,29,34,39,44,49,54,59 * * * *    /bin/bash $projectDir/run_finalize.sh >> $projectDir/run_finalize.sh.cron.log 2>&1

TODO










sub editLocalConf {
    my $file = shift;
    my $outVer = shift;
    my $loadVer = shift;
    my $projectDir = shift;

    open my $temp, ">", "$file.tmp";
    open my $fh, "<", $file;
    while (my $line = <$fh>) {
        if ($line =~ m/OUT_VER=/) {
            $line = "export OUT_VER=\"$outVer\"\n";
        } elsif ($line =~ m/LOAD_VER=/) {
            $line = "export LOAD_VER=\"$loadVer\"\n";
        } elsif ($line =~ m/PROJECT_DIR=/) {
            $line = "export PROJECT_DIR=\"$projectDir\"\n";
        }
        $temp->print($line);
    }
    close $fh;
    close $temp;

    unlink($file);
    rename("$file.tmp", $file);
}



=pod

Copy `misc/run_collect.sh` to `$DIR`.

Copy `misc/local_conf.sh` to `$DIR` and set the version numbers, in MMDD format and data version X.X format.

    OUT_VER=0601
    LOAD_VER=4.0

Copy `misc/local_app_conf.sh.example` to `$DIR/local_app_conf.sh` and update paths to the apps as necessary.
An example for IGB/EFI is included below.

Create the directories for intermediate and output directory:

    mkdir $DIR/data_$OUT_VER
    mkdir $DIR/load-$LOAD_VER

Copy a master spreadsheet to `$DIR/master_$OUT_VER.txt`.

The `$DIR/run_collect.sh` script can be run.  It is fine to add this script to cron.  Suggested periodicity
is 10 minutes.

## Parameters for EFI/IGB for `local_app_conf.sh`

    efi_tools_home="/home/n-z/noberg/dev/EFITools"
    efi_perl_env="/home/groups/efi/apps/perl_env.sh"
    efi_pipeline_home="/home/n-z/noberg/dev/superfamily-pipeline"
    efi_input_job_dir="/private_stores/gerlt/jobs/dev/est"


=cut


