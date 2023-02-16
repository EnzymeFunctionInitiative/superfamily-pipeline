
package DataCollection;

use strict;
use warnings;

use constant SPLIT => 1;
use constant COLLECT => 2;
use constant DEFAULT_NUM_SCRIPTS => 10;


use Exporter qw(import);
our @EXPORT_OK = qw();

use Data::Dumper;
use Capture::Tiny qw(capture);


sub new {
    my $class = shift;
    my %args = @_;

    my $self = {};
    bless $self, $class;

    $self->{script_dir} = $args{script_dir} or die "Need script_dir";
    $self->{overwrite} = $args{overwrite} // 0;
    $self->{num_scripts} = $args{num_simultaneous_scripts} // &DEFAULT_NUM_SCRIPTS;
    $self->{collect_clusters} = {};
    $self->{split_clusters} = {};
    $self->{command_line_sep} = $args{command_line_sep} // "\n";
    $self->{cluster_dir_paths} = {};
    $self->{queue} = $args{queue} or die "Need queue";
    $self->{ram} = $args{job_ram} // "25gb";  # end in gb
    $self->{dry_run} = $args{dry_run} // 0;
    $self->{efi_tools_home} = $args{efi_tools_home} // $ENV{EFI_TOOLS_HOME};

    my $appDir = $args{app_dir} or die "Need app_dir";
    my $includeParent = 0; #TODO
    my $dcc = new DataCollection::Commands(app_dir => $appDir, include_parent => $includeParent);
    $self->{dcc} = $dcc;

    return $self;
}


sub addCluster {
    my $self = shift;
    my $asid = shift;
    my $clusterId = shift;
    my $clusterData = shift;
    my $clusterOutputDirPath = shift;
    my $collectType = shift // COLLECT;

    #TODO: add this to the script list
    #we want to batch the stuff so that we don't have to split it up later manually when submitting

    $self->{cluster_dir_paths}->{$asid} = $clusterOutputDirPath;

    my $numChildren = scalar @{ $clusterData->{children} };
    # Non-diced cluster
    if ($numChildren > 0) {
        my $commands = [];
        if ($collectType == COLLECT) {
            $commands = $self->{dcc}->getNonDicedCollectCommands($clusterId, $clusterData, $clusterOutputDirPath);
            push @{ $self->{collect_clusters}->{$asid} }, "######### PROCESSING COLLECT FOR $clusterId", @$commands;
        } else {
            $commands = $self->{dcc}->getNonDicedSplitCommands($clusterId, $clusterData, $clusterOutputDirPath);
            push @{ $self->{split_clusters}->{$asid} }, "######### PROCESSING SPLIT $clusterId", @$commands;
        }
    } else {
        my $commands = [];
        if ($collectType == COLLECT) {
            $commands = $self->{dcc}->getDicedCollectCommands($clusterId, $clusterData, $clusterOutputDirPath);
            push @{ $self->{collect_clusters}->{$asid} }, "######### PROCESSING DICING COLLECT $clusterId AS $clusterData->{ascore}", @$commands;
        } else {
            $commands = $self->{dcc}->getDicedSplitCommands($clusterId, $clusterData, $clusterOutputDirPath);
            push @{ $self->{split_clusters}->{$asid} }, "######### PROCESSING DICING SPLIT $clusterId AS $clusterData->{ascore}", @$commands;
        }
    }
}


sub finish {
    my $self = shift;
    my $collectType = shift // COLLECT;
    my $jobPrefix = shift // "";

    my @batches;
    if ($collectType == COLLECT) {
        @batches = $self->divideIntoBatches3($self->{collect_clusters});
    } else {
        my $ramFn = sub {
            my @files = @_;
            my $maxRam = 0;
            foreach my $file (@files) {
                next if $file !~ m/split_ssn.pl.*--ssn-in ([^ ]+) .*$/;
                my $fileSize = -s $1;
                my $ram = int(0.02 * ($fileSize / 1024 / 1024) + 10 + 1);
                $maxRam = $ram if $ram > $maxRam;
            }
            return $maxRam;
        };
        @batches = $self->divideIntoBatches3($self->{split_clusters}, $ramFn);
    }

    my @messages;
    my $submitData = {};

    my $submitFn = sub {
        my $batches = shift;
        my $prefix = shift;
        my $jobIdKey = shift;
        my $numCommand = 0;
        my $lastJobId = 0;
        #foreach my $commands (@$batches) {
        for (my $ci = 0; $ci < scalar @$batches; $ci++) {
            my $commands = $batches->[$ci];

            my $jobName = "${prefix}_$numCommand";
            $jobName = "${jobPrefix}_$jobName" if $jobPrefix;
            my $scriptFile = $self->{script_dir} . "/$jobName.sh";
            while (-f $scriptFile) {
                $numCommand++;
                $jobName = "${prefix}_$numCommand";
                $scriptFile = $self->{script_dir} . "/$jobName.sh";
            }

            my $ram = $commands->{ram} ? "$commands->{ram}gb" : $self->{ram};
            my $depId = $commands->{dep} ? $lastJobId : 0;
            my $header = $self->getHeader($jobName, $ram, $depId);

            $self->saveToFile($scriptFile, $header, $commands->{commands});

            push @messages, "Submitting $scriptFile";
            my ($jobId, $message) = $self->submitJob($scriptFile);
            if (not $jobId) {
                push @messages, "ERROR: unable to submit $scriptFile: $message";
                next;
            }
            $lastJobId = $jobId;

            my $asid = $commands->{cluster_id};
            my $dirPath = $self->{cluster_dir_paths}->{$asid};
            $submitData->{$asid}->{dir_path} = $dirPath if not $submitData->{$asid}->{dir_path};
            push @{ $submitData->{$asid}->{$jobIdKey} }, $jobId;
            #foreach my $asid (keys %{ $commands->{cluster_ids} }) {
            #    my $dirPath = $self->{cluster_dir_paths}->{$asid};
            #    $submitData->{$asid}->{dir_path} = $dirPath if not $submitData->{$asid}->{dir_path};
            #    push @{ $submitData->{$asid}->{$jobIdKey} }, $jobId;
            #}
            $numCommand++;
        }
    };

    if ($collectType == COLLECT) {
        &$submitFn(\@batches, "collect", "collect_job_id");
    } else {
        &$submitFn(\@batches, "split", "split_job_id");
    }

    return $submitData, \@messages;
}


sub getHeader {
    my $self = shift;
    my $prefix = shift;
    my $ram = shift;
    my $depId = shift;

    my $depLine = $depId ? "#SBATCH --dependency=afterok:$depId" : "";

    my $header = <<HEADER;
#!/bin/bash
#SBATCH --partition=$self->{queue}
#SBATCH --nodes=1
#SBATCH --tasks-per-node=1
#SBATCH --mem=$ram
#SBATCH --job-name="$prefix"
#SBATCH --kill-on-invalid-dep=yes
#SBATCH -o $self->{script_dir}/$prefix.sh.stdout.%j
#SBATCH -e $self->{script_dir}/$prefix.sh.stderr.%j
$depLine

export EFI_TOOLS_HOME=$self->{efi_tools_home}

HEADER

    return $header;
}


sub saveToFile {
    my $self = shift;
    my $scriptFile = shift;
    my $header = shift;
    my $commands = shift;

    if ($self->{dry_run}) {
        print "Saving script commands to $scriptFile\n";
        return;
    }

    open my $fh, ">", $scriptFile or die "Unable to write to $scriptFile: $!";
    
    $fh->print($header);
    foreach my $cmd (@$commands) {
        $fh->print($cmd, "\n");
    }

    close $fh;
}


sub submitJob {
    my $self = shift;
    my $scriptFile = shift;
    
    if ($self->{dry_run}) {
        print "Submitting $scriptFile\n";
        return 0;
    }

    my $cmd = "/usr/bin/sbatch $scriptFile";
    my ($res, $err) = capture { system($cmd); };

    my $jobId = 0;
    my $msg = "";
    if ($err and $err =~ m/\S/) {
        $msg = "Unknown error submitting $cmd: $err";
    } else {
        ($jobId = $res) =~ s/\D//gs;
    }

    return ($jobId, $msg);
}


sub divideIntoBatches3 {
    my $self = shift;
    my $data = shift;
    my $ramFn = shift;

    my @batches;
    my $maxLines = 10000;

    my @clusterIds = sort clusterIdSort keys %$data;
    foreach my $clusterId (@clusterIds) {
        my @commands = getScriptCommands($data->{$clusterId});
        my $numCommands = scalar @commands;
        next if not $numCommands;
        
        my $ram = $ramFn ? &$ramFn(@commands) : 0;

        my @chunks;
        if ($numCommands > $maxLines) {
            for (my $mi = 0; $mi < $numCommands; $mi += $maxLines) {
                my @chunk;
                my $maxMi = ($mi + $maxLines) > $numCommands ? $numCommands : ($mi + $maxLines);
                for (my $ci = $mi; $ci < $maxMi; $ci++) {
                    push @chunk, $commands[$ci];
                }
                push @chunks, \@chunk;
            }
        } else {
            @chunks = (\@commands);
        }

        my $depC = 0;
        foreach my $chunk (@chunks) {
            push @batches, {commands => $chunk, ram => $ram, dep => $depC, cluster_id => $clusterId};
            $depC++;
        }
    }

    return @batches;
}

sub divideIntoBatches2 {
    my $self = shift;
    my $data = shift;
    my $ramFn = shift;

    my @clusterIds = sort clusterIdSort keys %$data;
    my @allLines;
    my %lineMap;
    my $maxRam = 0;
    my %ram;
    foreach my $clusterId (@clusterIds) {
        my @commands = getScriptCommands($data->{$clusterId});
        my $numCommands = scalar @commands;
        next if not $numCommands;
        
        my $ram = $ramFn ? &$ramFn(@commands) : 0;
        $ram{$clusterId} = $ram;

        my $lineNum = scalar @allLines;
        my $s = $lineNum;
        my $e = $s + $numCommands - 1;

        push @allLines, @commands;
        map { $lineMap{$_} = $clusterId; } ($s..$e);
    }

    my $numLines = scalar @allLines;
    my $frac = $numLines / $self->{num_scripts};
    my $numPerScript = int($frac + 1);
    $numPerScript = 1 if $numPerScript < 1;

    # If the number of lines per script is greater than 25000 we need to increase the number of scripts 
    # that are submitted because SLURM only processes scripts with an approximate maximum of 25000 lines.
    if ($numPerScript > 25000) {
        $numPerScript = 25000;
    }

    my @batches;
    my $c = 0;
    my $sidx = 0;
    my $lastMkdir = "";
    my $lastSplitTab = "";
    for (my $i = 0; $i < $numLines; $i++) {
        #my $clusterCommands = join($self->{command_line_sep}, @commands, $self->{command_line_sep});

        @batches = ({cluster_ids => {}, commands => [], ram => 0}) if scalar @batches == 0;
        $batches[$sidx]->{cluster_ids}->{$lineMap{$i}} = $sidx;
        if ($ramFn and $ram{$lineMap{$i}}) {
            $batches[$sidx]->{ram} = $ram{$lineMap{$i}} if $ram{$lineMap{$i}} > $batches[$sidx]->{ram};
        }
        push @{ $batches[$sidx]->{commands} }, $allLines[$i];

        $lastMkdir = $allLines[$i] if $allLines[$i] =~ m/^mkdir/;
        $lastSplitTab = $allLines[$i] if $allLines[$i] =~ m/split_tab_file\.pl/;

        $c++;
        my $totalCount = $sidx * $numPerScript + $c;
        if ($c >= $numPerScript and $totalCount < $numLines) {
            push @batches, {cluster_ids => {}, commands => [], ram => 0};
            $sidx++;
            $c = 0;
            # If we're splitting up and parallelizing, some commands may require the existence of directories
            # that belong to that cluster, but they may be separated for performance purposes.  So we
            # automatically include the last mkdir for safety's-sake.
            push @{ $batches[$sidx]->{commands} }, $lastMkdir if $lastMkdir;
            push @{ $batches[$sidx]->{commands} }, $lastSplitTab if $lastSplitTab;
            $lastMkdir = "";
            $lastSplitTab = "";
        }
    }

    return @batches;
}


sub divideIntoBatches {
    my $self = shift;
    my $data = shift;

    my @clusterIds = sort clusterIdSort keys %$data;
    my $frac = scalar @clusterIds / $self->{num_scripts};
    my $numPerScript = int($frac + 1);
    $numPerScript = 1 if $numPerScript < 1;

    my @batches;
    my $c = 0;
    my $sidx = 0;
    foreach my $clusterId (@clusterIds) {
        my $clusterCommands = join($self->{command_line_sep}, getScriptCommands($data->{$clusterId}), $self->{command_line_sep});

        @batches = ({cluster_ids => [], commands => []}) if scalar @batches == 0;
        push @{ $batches[$sidx]->{cluster_ids} }, $clusterId;
        push @{ $batches[$sidx]->{commands} }, $clusterCommands;

        $c++;
        my $totalCount = $sidx * $numPerScript + $c;
        if ($c >= $numPerScript and $totalCount < scalar @clusterIds) {
            push @batches, {cluster_ids => [], commands => []};
            $sidx++;
            $c = 0;
        }
    }

    return @batches;
}


sub getScriptCommands {
    my $data = shift;
    my @output = @$data;
    return @output;
}


sub clusterIdSort {
    my @a = split(m/[\-_]/, $a);
    my @b = split(m/[\-_]/, $b);

    my $maxIdx = $#a > $#b ? $#b : $#a;
    for (my $i = 0; $i <= $maxIdx; $i++) {
        my $result = (($a[$i] =~ m/\D/ or $b[$i] =~ m/\D/) ? $a[$i] cmp $b[$i] : $a[$i] <=> $b[$i]);
        return $result if $result;
    }

    return ($#a < $#b ? -1 : 1);
}


1;

