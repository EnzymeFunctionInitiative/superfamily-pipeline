#!/bin/env perl

use strict;
use warnings;

use Getopt::Long;
use Data::Dumper;
use FindBin;

use lib "$FindBin::Bin/../lib";

use IdListParser;

my $AppDir = $FindBin::Bin;


my ($dataDir, $scriptFile, $dbFile, $tempDir, $dbVer, $mapTablesOnly);
my ($mapFile, $appendMapFile, $mapFileUniRef90, $appendMapFileUniRef90, $mapFileUniRef50, $appendMapFileUniRef50);
#my ($dataDir, $jobIdListFile, $scriptFile, $dbFile, $tempDir, $isDiced, $dbVer, $mapFile, $mapFileUniRef90, $dicedIdDir, $dicedOutputDir, $mapTablesOnly);
#my ($dataDir, $jobIdListFile, $scriptFile, $dbFile, $tempDir, $isDiced, $dbVer, $mapFile, $dicedIdDir, $dicedOutputDir, $dicedScriptDir);
my $result = GetOptions(
    "data-dir=s"            => \$dataDir,
    "script-file=s"         => \$scriptFile,
    "output-db=s"           => \$dbFile,
    "gnd-temp-dir=s"        => \$tempDir,
    "db-ver=s"              => \$dbVer,
    "id-map=s@"             => \$mapFile, # map cluster -> id
    "append-id-map=s@"      => \$appendMapFile, # map cluster -> id
    "id-map-uniref90=s@"    => \$mapFileUniRef90, # map cluster -> id
    "append-id-map-uniref90=s@" => \$appendMapFileUniRef90, # map cluster -> id
    "id-map-uniref50=s@"    => \$mapFileUniRef50, # map cluster -> id
    "append-id-map-uniref50=s@" => \$appendMapFileUniRef50, # map cluster -> id
    "make-map-tables-only"  => \$mapTablesOnly,
);

die "Need --data-dir" if not $dataDir or not -d $dataDir;
die "Need --script-file" if not $scriptFile;
die "Need --output-db" if not $dbFile;
die "Need --gnd-temp-dir" if not $tempDir or not -d $tempDir;
die "Need --id-map" if not $mapFile;

$dbVer = $ENV{EFI_DB_RELEASE} if not $dbVer;
die "Need --db-ver" if not $dbVer;
$dbVer =~ s/\D//g;



# DON'T DO THIS: unlink $dbFile if -f $dbFile;
my $fileArgs = {uniprot => $mapFile};
$fileArgs->{append_uniprot} = $appendMapFile if $appendMapFile;
$fileArgs->{uniref90} = $mapFileUniRef90 if $mapFileUniRef90;
$fileArgs->{append_uniref90} = $appendMapFileUniRef90 if $appendMapFileUniRef90;
$fileArgs->{uniref50} = $mapFileUniRef50 if $mapFileUniRef50;
$fileArgs->{append_uniref50} = $appendMapFileUniRef50 if $appendMapFileUniRef50;
writeScript($scriptFile, $tempDir, $fileArgs, "", $dbFile);








sub writeScript {
    my $scriptFile = shift;
    my $tempDir = shift;
    my $mapFileArgs = shift;
    my $jobName = shift;
    my $dbFile = shift;

    (my $scriptDir = $scriptFile) =~ s%^(.*)/([^/]+).sh$%$1%;
    $jobName = $2 if not $jobName;

    open my $scriptFh, ">", $scriptFile;
    
    $scriptFh->print(getJobHeader($scriptDir, $jobName));
    $scriptFh->print(getGndHeader($tempDir, $mapFileArgs));
    $scriptFh->print(getScriptCode($dbFile)); 
    
    close $scriptFh;

    print "sbatch $scriptFile\n";
}


sub getJobHeader {
    my $scriptDir = shift;
    my $jobName = shift;
    return <<HEADER;
#!/bin/bash
#SBATCH --partition=efi
#SBATCH --nodes=1
#SBATCH --tasks-per-node=1
#SBATCH --mem=25gb
#SBATCH --job-name="$jobName"
#SBATCH --kill-on-invalid-dep=yes
#SBATCH -o $scriptDir/$jobName.sh.stdout.%j
#SBATCH -e $scriptDir/$jobName.sh.stderr.%j
set -e

module unload MariaDB
module load Perl

module load efignt/devlocal
module load efidb/ip$dbVer

APPDIR=$AppDir

HEADER
}


sub getGndHeader {
    my $tempDir = shift;
    my $mapFileArgs = shift;

    my $mapFile = join(" ", map { "--id-mapping $_" } @{ $mapFileArgs->{uniprot} });
    my $mapFileUniRef90 = "";
    my $mapFileUniRef50 = "";
    if ($mapFileArgs->{uniref90}) {
        my @a = @{ $mapFileArgs->{uniref90} };
        $mapFileUniRef90 = join(" ", map { "--id-mapping $_" } @a);
    }
    if ($mapFileArgs->{uniref50}) {
        my @a = @{ $mapFileArgs->{uniref50} };
        $mapFileUniRef50 = join(" ", map { "--id-mapping $_" } @a);
    }

    my $extraUniProtFiles = join(" ", @{ $mapFileArgs->{append_uniprot} }) if $mapFileArgs->{append_uniprot};
    my $extraUniRef90Files = join(" ", @{ $mapFileArgs->{append_uniref90} }) if $mapFileArgs->{append_uniref90};
    my $extraUniRef50Files = join(" ", @{ $mapFileArgs->{append_uniref50} }) if $mapFileArgs->{append_uniref50};

    #my $colNum = $isDiced ? 3 : 2;
    my $colNum = 2;

    my $script = <<SCRIPT;
BASEDIR="$tempDir"
MASTERIDFILEARGS="$mapFile"
SCRIPT
    if (not $mapTablesOnly) {
        $script .= "echo rm -rf \$BASEDIR/*\n";
    }
    $script .= "MASTERIDFILE90ARGS=\"$mapFileUniRef90\"\n" if $mapFileUniRef90;
    $script .= "MASTERIDFILE50ARGS=\"$mapFileUniRef50\"\n" if $mapFileUniRef50;
    $script .= <<SCRIPT;
UNIREFIDS="\$BASEDIR/uniref_ids.txt"
IDFILE="\$BASEDIR/sorted_ids"
IDFILE90="\$BASEDIR/sorted_ids_uniref90"
IDFILE50="\$BASEDIR/sorted_ids_uniref50"

mkdir -p \$BASEDIR

\$APPDIR/sort_gnd_ids.pl \$MASTERIDFILEARGS --out-sorted \$IDFILE
SCRIPT
    $script .= "cat $extraUniProtFiles >> \$IDFILE\n" if $extraUniProtFiles;
    $script .= "\$APPDIR/sort_gnd_ids.pl \$MASTERIDFILE90ARGS --out-sorted \$IDFILE90\n" if $mapFileUniRef90;
    $script .= "cat $extraUniRef90Files >> \$IDFILE90\n" if $extraUniRef90Files;
    $script .= "\$APPDIR/sort_gnd_ids.pl \$MASTERIDFILE50ARGS --out-sorted \$IDFILE50\n" if $mapFileUniRef50;
    $script .= "cat $extraUniRef50Files >> \$IDFILE50\n" if $extraUniRef50Files;
    
    if (not $mapTablesOnly) {
        $script .= <<SCRIPT;

rm -f \$BASEDIR/stderr.log
touch \$BASEDIR/stderr.log

if [[ ! -f \$UNIREFIDS ]]; then
    awk '{print \$NF}' \$IDFILE | sort | uniq > \$BASEDIR/all_ids.txt
SCRIPT
#        $script .= "    awk '{print \$NF}' \$IDFILE90 > \$IDFILE90.tmp\n" if $mapFileArgs->{uniref90};
#        $script .= "    awk '{print \$NF}' \$IDFILE50 > \$IDFILE50.tmp\n" if $mapFileArgs->{uniref50};
        $script .= <<SCRIPT;
    get_uniref_ids.pl --uniprot-ids \$BASEDIR/all_ids.txt --uniref-mapping \$UNIREFIDS --uniref-version 50
fi

SCRIPT
    }

    return $script;
}


sub getScriptCode {
    my $dbFile = shift;

    my $script = <<SCRIPT;

OUTDB=$dbFile
SCRIPT

    if (not $mapTablesOnly) {
        $script .= <<SCRIPT;

create_diagram_db.pl \\
    --id-file \$UNIREFIDS \\
    --do-id-mapping \\
    --uniref 50 \\
    --db-file \$OUTDB \\
    --job-type ID_LOOKUP \\
    --no-neighbor-file \$BASEDIR/no_nb.txt \\
    --nb-size 20 \\
    --cluster-map \$IDFILE

SCRIPT
    }

    $script .= <<SCRIPT;

\$APPDIR/make_gnd_cluster_map_tables.pl --db-file \$OUTDB --id-mapping \$IDFILE --seq-version uniprot --error-file \$BASEDIR/error.log
\$APPDIR/make_gnd_cluster_map_tables.pl --db-file \$OUTDB --id-mapping \$IDFILE90 --seq-version uniref90 --error-file \$BASEDIR/error.log
\$APPDIR/make_gnd_cluster_map_tables.pl --db-file \$OUTDB --id-mapping \$IDFILE50 --seq-version uniref50 --error-file \$BASEDIR/error.log

SCRIPT

    return $script;
}


