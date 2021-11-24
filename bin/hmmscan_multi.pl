#!/bin/env perl

use strict;
use warnings;

$SIG{INT} = $SIG{TERM} = sub { exit; };

use Capture::Tiny 'capture';
use Getopt::Long;
use Data::Dumper;

if (not checkForHmmscan()) {
    die "Please load HMMER before running this script.";
}

my ($seqFile, $dbListFile, $numTasks, $tempDir, $outputFile, $debug);
my $result = GetOptions(
    "seq-file=s"        => \$seqFile,
    "db-list-file=s"    => \$dbListFile,
    "num-tasks=i"       => \$numTasks,
    "temp-dir=s"        => \$tempDir,
    "output-file=s"     => \$outputFile,
    "debug"             => \$debug,
);


die "Need --seq-file" if not -f ($seqFile//"");
die "Need --db-list-file" if not -f ($dbListFile//"");
die "Need --temp-dir" if not -d ($tempDir//"");

$numTasks = 1 if not $numTasks;
my $hmmCommand = "hmmscan";


my @hmms = getHmmList($dbListFile, $numTasks);

if ($debug) {
    my $hmms = join("\n", map { @$_ } @hmms);
    print <<DEBUG;
$0 --seq-file $seqFile --db-list-file $dbListFile --num-tasks $numTasks --temp-dir $tempDir --output-file $outputFile
DEBUG
}

my $parentPid = "$$";
my @children;


for (my $i = 0; $i < $numTasks; $i++) {
    my $pid = fork;
    if (not defined $pid) {
        warn "Failed to fork child $i: $!";
        kill "TERM", @children;
        exit;
    } elsif ($pid) {
        push @children, $pid;
        next;
    }
    doWork($hmmCommand, $hmms[$i], $seqFile, $tempDir, $pid);
    exit;
}

waitForWorkers();

processTableFiles($tempDir, $outputFile // "");



END {
    if ($parentPid and $parentPid == $$) {
        waitForWorkers();
    }
}




sub doWork {
    my $hmmCommand = shift;
    my $hmms = shift;
    my $seqFile = shift;
    my $outDir = shift;
    my $pid = shift;

    my @errors;

    foreach my $hmm (@$hmms) {
        my $outName;
        if ($hmm =~ m%^.*/(cluster-[\-0-9]+)(.*?)$%) {
            $outName = $1;
            my $theRest = $2;
            $outName =~ s%-+$%%;
            if ($theRest and $theRest =~ m/AS(\d+)/) {
                $outName = "$outName-AS$1";
            }
        } else {
            $outName = $pid;
        }
        my $outPath = "$outDir/${outName}_output.txt";
        my $tablePath = "$outDir/${outName}_results.txt";
#        print join(" ", ($hmmCommand, "-o", $outPath, "--tblout", $tablePath, $hmm, $seqFile), "\n");
        #$outFile =~ m/(cluster-[\-0-9])(-AS(\d+))?/;
        my ($out, $err, $ec) = capture {
            system($hmmCommand, "-o", $outPath, "--tblout", $tablePath, $hmm, $seqFile);
        };
        if ($ec != 0) {
            $err =~ s/[\r\n]/ /gs;
            push @errors, "There was an error running $hmm ($outPath, $tablePath): $err ($ec)\n";
            unlink $outPath;
            unlink $tablePath;
        }
    }

    warn "The following errors were found:\n" . join("\n", @errors) . "\n" if scalar @errors;
}


sub processTableFiles {
    my $dir = shift;
    my $output = shift;

    my $fh;
    if ($output) {
        open $fh, ">", $output or warn "Unable to write to $output: $!" and return;
    } else {
        $fh = \*STDOUT;
    }

    my @files = glob("$dir/*_results.txt");
   
    my @data;
    foreach my $file (@files) {
        my ($cluster, $ascore, $matches) = processTableFile($file);
        push @data, [$cluster, $ascore, $matches];
    }

    foreach my $row (sort { $a->[1] <=> $b->[1] } @data) {
        my ($cluster, $ascore, $matches) = @$row;
        map { $fh->print(join("\t", $cluster, $ascore, @$_), "\n"); } @$matches;
    }

    close $fh if not $output;
    #map { unlink($_) } glob("$dir/*");
}

 
sub processTableFile {
    my $file = shift;

    my $ascore = "";
    my $cluster = "all";
    $file =~ m%^.*(cluster-[-\d]+)(AS(\d+))?_results.txt$%;
    if ($1) {
        $ascore = $3 // "";
        ($cluster = $1) =~ s%-$%%;
    }

    my @matches;
    my $maxResults = 11;
    my $maxLines = 10;

    open my $fh, "<", $file or warn "Unable to read $file: $!\n" and return;
    my $lineNum = 0;
    while (<$fh>) {
        next if $lineNum++ > $maxLines;
        chomp;
        next if m/^\s*$/ or m/^#/;
        my @parts = split(m/\s+/);
        if ($#parts >= 4) {
            (my $clusterTrim = $parts[0]) =~ s%-AS\d+$%%;
            my $evalue = $parts[4];
            push @matches, [$clusterTrim, $parts[4]] if $evalue < 1e-10;
            last if scalar @matches >= $maxResults;
        }
    }
    close $fh;

    return ($cluster, $ascore, \@matches);
}


sub waitForWorkers {
    while (scalar @children) {
        my $pid = $children[0];
        my $kid = waitpid $pid, 0;
        warn "Reaped $pid ($kid)\n" if $debug;
        shift @children;
    }
}


# Return a list of HMMs grouped by task
sub getHmmList {
    my $file = shift;
    my $numTasks = shift;

    (my $dirPath = $file) =~ s%^(.*)/([^/]+)$%$1%;

    my @hmms;
    open my $fh, "<", $file or die "Unable to open $file for reading: $!";
    while (<$fh>) {
        chomp;
        my @p = split(m/\t/);
        if ($#p > 0) {
            my $hmm = $p[$#p];
            $hmm = "$dirPath/$hmm" if $hmm !~ m%/%;
            push @hmms, $hmm;
        } else {
            push @hmms, $p[0];
        }
    }
    close $fh;

    my @tasks;
    for (my $i = 0; $i < $numTasks; $i++) {
        my $hi = $i;
        while ($hmms[$hi]) {
            push @{$tasks[$i]}, $hmms[$hi];
            $hi += $numTasks;
        }
    }

    return @tasks;
}


sub checkForHmmscan {
    my ($stdout, $stderr, $exit) = capture {
        system("hmmscan");
    };

    return $exit < 0 ? 0 : 1;
}


