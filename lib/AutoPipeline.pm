
package AutoPipeline;

use strict;
use warnings;

use File::Path qw(mkpath);
use Capture::Tiny qw(capture);
use Fcntl qw(:flock);

use Exporter qw(import);

our @EXPORT_OK = qw(do_mkdir do_sql get_job_dir is_job_finished update_job_status run_job get_running_jobs_shell get_num_running_jobs get_jobs_from_db wait_lock);


sub do_mkdir {
    my $dir = shift;
    my $dryRun = shift || 0;
    if ($dryRun) {
        print "mkdir $dir\n";
    } else {
        mkpath($dir) if not -d $dir;
    }
}


# Tries X number of times to get a lock on a file.  Once the lock is obtained, it is immediately released,
# because the purpose of this function is to ensure that SSNs are fully written before they are read.
sub wait_lock {
    my $file = shift;
    my $numAttempts = shift || 100;
    open my $fh, "+<", $file or die "Unable to read $file in wait_lock: $!";
    my $c = 0;
    while (!flock($fh, LOCK_EX | LOCK_NB) and $c++ < $numAttempts) {
        sleep(1);
    }
    close $fh;
    return $c < $numAttempts;
}


sub do_sql {
    my $sql = shift;
    my $dbh = shift or die;
    my $dryRun = shift || 0;
    my $logFh = shift || 0;
    if ($logFh) {
        $logFh->print("$sql\n");
        $logFh->flush();
    }
    if ($dryRun) {
        print "$sql\n";
    } else {
        my $retval = $dbh->do($sql);
        die "Invalid $sql" if not $retval;
    }
}


sub get_job_dir {
    my $jobMasterDir = shift;
    my $clusterId = shift;
    my $uniref = shift || "";
    my $suffix = $uniref ? "_ur$uniref" : "";
    return "$jobMasterDir/${clusterId}$suffix";
}


sub update_job_status {
    my $dbh = shift;
    my $table = shift;
    my $finishFile = shift;
    my $dryRun = shift || 0;
    my $logFh = shift || 0;
    my $extraSqlFn = shift || undef;
    my $finishFileOnly = shift || 0;

    my $sql = "SELECT * FROM $table WHERE started = 1 AND (finished IS NULL OR finished = 0)";
    print "$sql\n" if $dryRun;
    $logFh->print("$sql\n") if $logFh;
    my $sth = $dbh->prepare($sql);
    $sth->execute;

    while (my $row = $sth->fetchrow_hashref) {
        my $jobId = $row->{job_id};
        my $dir = $row->{dir_path};

        my $isFinished = 0;
        if (ref $finishFile eq "CODE") {
            &$finishFile($jobId, $row);
        } else {
            $isFinished = $finishFile ? -f "$dir/$finishFile" : 1;
        }

        if (($finishFileOnly or ($jobId and is_job_finished($jobId))) and $isFinished) {
            print "$row->{as_id} $table has finished\n";
            my $extra = "";
            if ($extraSqlFn and ref $extraSqlFn eq "CODE") {
                $extra = ", " . &$extraSqlFn($row);
            }
            my $sql = "UPDATE $table SET finished = 1 $extra WHERE as_id = '$row->{as_id}'";
            do_sql($sql, $dbh, $dryRun, $logFh);
        }
    }
}


sub is_job_finished {
    my $id = shift;
    my $cmd = "/usr/bin/sacct -n -j $id -o State";
    my $result = `$cmd`;
    my @lines = split(m/[\r\n]+/s, $result);
    return 0 if not scalar @lines;
    $lines[0] =~ s/\s//g;
    return 1 if $lines[0] eq "COMPLETED";
    return 0;
}


sub run_job {
    my $asid = shift;
    my $args = shift;
    my $startApp = shift;
    my $outDir = shift;
    my $pfx = shift;
    my $grepText = shift;
    my $dryRun = shift || 0;
    my $perlEnv = shift || "";

    $perlEnv = $ENV{EFI_PERL_ENV} if $ENV{EFI_PERL_ENV};
    $perlEnv = "export EFI_PERL_ENV=$perlEnv" if $perlEnv;

    my $appStart = $startApp . " " . join(" ", @$args);
    my $cmd = <<CMD;
source /etc/profile
module load efiest/devlocal
curdir=\$PWD
cd $outDir
$perlEnv
$appStart
CMD

    if ($dryRun) {
        print $cmd;
        return 0;
    }

    my ($result, $err) = capture { system($cmd); };
    my @lines = split(m/[\n\r]+/, $result);
    my $jobNum = 0;
    if (grep m/$grepText/, @lines) {
        #($jobNum = $lines[$#lines]) =~ s/\D//g;
        my @nums = split(m/,/, $lines[$#lines]);
        $jobNum = $nums[$#nums];
        die "Couldn't find job ID in $result : $err" if not $jobNum;
    } else {
        print STDERR "Unable to submit $pfx $asid job: $result|$err\n";
    }
    return $jobNum;
}


sub get_running_jobs_shell {
    my $pattern = shift;
    my $cmd = '/usr/bin/squeue -o "%.18i %9P %25j %.8u %.2t %.10M %.6D %R %m" | grep ' . $pattern;
    my $result = `$cmd`;
    my @lines = split(m/\n/, $result);
    return scalar @lines;
}


sub get_num_running_jobs {
    my $dbh = shift;
    my $table = shift;
    my $dryRun = shift || 0;
    my $sql = <<SQL;
SELECT as_id FROM $table WHERE started = 1 AND (finished = 0 OR finished IS NULL)
SQL
    my @jobs = get_jobs_from_db($sql, $dbh, $dryRun);
    return scalar @jobs;
}


sub get_jobs_from_db {
    my $sql = shift;
    my $dbh = shift;
    my $dryRun = shift || 0;
    my $logFh = shift || 0;

    #if ($dryRun) {
    #    print "$sql\n";
    #    return ();
    #}

    $logFh->print("$sql\n") if $logFh;
    my $sth = $dbh->prepare($sql);
    $sth->execute;

    my @jobs;
    while (my $row = $sth->fetchrow_hashref) {
        push @jobs, $row;
    }

    return @jobs;
}


1;

