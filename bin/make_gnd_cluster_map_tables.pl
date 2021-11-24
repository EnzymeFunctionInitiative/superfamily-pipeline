#!/bin/env perl

use strict;
use warnings;


use DBI;
use Getopt::Long;
use Data::Dumper;


my ($dbFile, $idMapping, $errorFile, $seqVersion, $appendToDb, $unirefMapFile, $debug, $updateUniRefOnly);
my $result = GetOptions(
    "db-file=s"         => \$dbFile,
    "id-mapping=s@"     => \$idMapping,
    "seq-version=s"     => \$seqVersion,
    "uniref-map=s"      => \$unirefMapFile,
    "error-file=s"      => \$errorFile,
    "append"            => \$appendToDb,
    "debug"             => \$debug,
    "update-uniref-only"    => \$updateUniRefOnly,
);


die "Need --db-file" if not $dbFile;
#die "Need --db-file" if not $dbFile or not -f $dbFile;
die "Need --id-mapping" if not $idMapping;
#die "Need --id-mapping" if not $idMapping or (not -f $idMapping and ref $idMapping ne "ARRAY");
die "Need --seq-version=uniprot|uniref50|uniref90" if not $seqVersion;


my $errorFh;
if ($errorFile) {
    open $errorFh, ">", $errorFile;
} else {
    my $fh = \*STDOUT;
    $errorFh = $fh;
}


my @err;
my $insCount = 0;
my $commitSize = 50000;

my $dbh = DBI->connect("dbi:SQLite:dbname=$dbFile", "", "", {RaiseError => 1});
print STDERR "DONE\n";

my $tableName = createSchema($seqVersion);
my $data = getClusterMapping($idMapping);
my $unirefMap = getUniRefMap($unirefMapFile) if $unirefMapFile and $seqVersion ne "uniprot";

$dbh->begin_work if not $debug;

if (not $updateUniRefOnly) {
    my $clusterIndexes = getClusterIndexes($seqVersion);
    my $memberMap = getMemberMap($data, $clusterIndexes);

    my $map = $seqVersion eq "uniref90" ? $unirefMap->{90} : ($seqVersion eq "uniref50" ? $unirefMap->{50} : undef);
    fillTables($tableName, $data, $clusterIndexes, $memberMap, $map);

    $dbh->commit if not $debug;
}

setUniRefSupport($data);

$dbh->commit if not $debug;

map {
    print $errorFh "Unable to find cluster_index for $_\n";
} @err;













sub setUniRefSupport {
    my $clusterData = shift;

    my $version = 100;
    $version = 90 if $seqVersion eq "uniref90";
    $version = 50 if $seqVersion eq "uniref50";

    my $processRow = sub {
        my $cluster = shift;
        my $sql = shift;
        my $ascore = shift || "";
        
        my $ascoreWhere = $ascore ? " AND ascore = '$ascore'" : "";
        my $ascoreCol = $ascore ? "ascore," : "";
        my $ascoreVal = $ascore ? "'$ascore'," : "";
        my $ascoreUpdate = ""; #$ascore ? " AND ascore = '$ascore'" : "";

        $sql .= $ascoreWhere;

        my $sth = $dbh->prepare($sql);
        $sth->execute;
        my $row = $sth->fetchrow_hashref;
        if (not $row) {
            $sql = "INSERT INTO cluster_id_uniref_support (cluster_id, $ascoreCol uniref_version) VALUES ('$cluster', $ascoreVal '$version')";
            doInsert($sql);
        } else {
            if ($row->{uniref_version} >= 50 and $version < $row->{uniref_version}) {
                $sql = "UPDATE cluster_id_uniref_support SET uniref_version = '$version' $ascoreUpdate WHERE cluster_id = '$cluster' $ascoreWhere";
                doInsert($sql);
            }
        }
    };

    foreach my $cluster (keys %$clusterData) {
        my $sql = "SELECT uniref_version FROM cluster_id_uniref_support WHERE cluster_id = '$cluster'";
        if (ref $clusterData->{$cluster} eq "HASH") {
            foreach my $ascore (keys %{$clusterData->{$cluster}}) {
                &$processRow($cluster, $sql, $ascore);
            }
        } else {
            &$processRow($cluster, $sql);
        }

    }
}


sub getClusterIndexes {
    my $version = shift;
    
    my $indexKey = "cluster_index";
    my $accKey = "accession";
    my $table = "attributes";
    if ($version =~ m/^uniref(\d+)$/) {
        $indexKey = "uniref_index";
        $accKey = "uniref_id";
        $table = "uniref${1}_range";
    }

    my $data = {};
    my $sql = "SELECT $indexKey, $accKey FROM $table";
    my $sth = $dbh->prepare($sql);
    $sth->execute;
    while (my $row = $sth->fetchrow_hashref) {
        $data->{$row->{$accKey}} = $row->{$indexKey};
    }

    return $data;
}


sub getMemberMap {
    my $data = shift;
    my $indexes = shift;

    my %members;
    foreach my $cluster (keys %$data) {
        my $ids = $data->{$cluster};
        if (ref $ids eq "ARRAY") {
            map { $members{$_} = $indexes->{$_}; } @$ids;
        } elsif (ref $ids eq "HASH") {
            foreach my $ascore (keys %$ids) {
                my $ascoreIds = $data->{$cluster}->{$ascore};
                map { $members{$_} = $indexes->{$_}; } @$ascoreIds;
            }
        }
    }

    return \%members;
}


sub fillTables {
    my $tableBaseName = shift;
    my $clusterData = shift;
    my $indexes = shift;
    my $memberMap = shift;
    my $unirefMap = shift;

    my $idIndex = 0;

    my $processIds = sub {
        my $ids = shift;
        my $ascore = shift || "";
        my $table = "${tableBaseName}_attr_index";
        my @ids = @$ids;
        
        my $sortFn = sub { $a cmp $b };
        if ($unirefMap) {
            $sortFn = sub {
                my $aa = $a; my $bb = $b;
                return -1 if not $unirefMap->{$aa};
                return 1 if not $unirefMap->{$bb};
                my $cmp = scalar(@{ $unirefMap->{$bb} }) <=> scalar(@{ $unirefMap->{$aa} });
                return $cmp if $cmp;
                return $aa cmp $bb;
            };
        }

        foreach my $id (sort $sortFn @ids) {
            my $clusterIndex = $indexes->{$id};
            if (not $clusterIndex) {
                push @err, $id;
                next;
            }

            my @cols = ("member_index", "cluster_index");
            my $cols = join(", ", @cols);
            my @vals = ("'$idIndex'", $clusterIndex);
            my $vals = join(", ", @vals);
            my $sql = "INSERT INTO $table ($cols) VALUES ($vals)";
            
            doInsert($sql);
            $idIndex++;
        }
    };

    my @clusterMap;
    my $clusterIdIndex = 0;

    foreach my $cluster (keys %$clusterData) {
        my $data = $clusterData->{$cluster};
        if (ref $data eq "ARRAY") {
            my $startIndex = $idIndex;
            &$processIds($data, "");
            push @clusterMap, {index => $clusterIdIndex, range => [$startIndex, $idIndex-1], name => $cluster, ascore => ""};
            $clusterIdIndex++;
        } elsif (ref $data eq "HASH") {
            foreach my $ascore (keys %$data) {
                my $startIndex = $idIndex;
                &$processIds($data->{$ascore}, $ascore);
                push @clusterMap, {index => $clusterIdIndex, range => [$startIndex, $idIndex-1], name => $cluster, ascore => $ascore};
                $clusterIdIndex++;
            }
        }
    }

    my $table = "${tableBaseName}_range";
    foreach my $data (@clusterMap) {
        my @cols = ("cluster_id", "start_index", "end_index");
        push @cols, "ascore" if $data->{ascore};
        my $cols = join(", ", @cols);

        my @vals = ("'$data->{name}'", @{ $data->{range} });
        push @vals, $data->{ascore} if $data->{ascore};
        my $vals = join(", ", @vals);

        my $sql = "INSERT INTO $table ($cols) VALUES ($vals)";

        doInsert($sql);
    }
}


sub createSchema {
    my $seqVersion = shift;

    my $baseName = "cluster_id_${seqVersion}";

    if (not $appendToDb) {
        my @statements;
        my $tableName = "${baseName}_range";
        push @statements, "DROP TABLE IF EXISTS $tableName";
        push @statements, "CREATE TABLE $tableName (cluster_id TEXT, ascore INTEGER, start_index INTEGER, end_index INTEGER)";
        push @statements, "CREATE INDEX ${tableName}_cluster_id ON $tableName (cluster_id)";
        push @statements, "CREATE INDEX ${tableName}_cluster_id_ascore ON $tableName (cluster_id, ascore)";
       
        $tableName = "${baseName}_attr_index";
        push @statements, "DROP TABLE IF EXISTS $tableName";
        push @statements, "CREATE TABLE $tableName (member_index INTEGER, cluster_index INTEGER)";
        push @statements, "CREATE INDEX ${tableName}_member_index ON $tableName (member_index)";

        $tableName = "cluster_id_uniref_support";
        #push @statements, "DROP TABLE IF EXISTS $tableName";
        push @statements, "CREATE TABLE IF NOT EXISTS $tableName (cluster_id TEXT, ascore INTEGER, uniref_version INTEGER)";
        push @statements, "CREATE INDEX IF NOT EXISTS ${tableName}_cluster_id ON $tableName (cluster_id, ascore)";
        map { $dbh->do($_) } @statements;
    }

    return $baseName;
}


sub getClusterMapping {
    my $mapFiles = shift;

    my $data = {};

    my $processFileFn = sub {
        my $mapFile = shift;
        print "processing $mapFile\n";
        open my $fh, "<", $mapFile;
        while (<$fh>) {
            chomp;
            next if m/^\s*$/ or m/^#/ or !m/^cluster/;
            my ($cluster, @p) = split(m/\t/);
            my ($ascore, $id);
            if ($#p > 0) {
                ($ascore, $id) = @p;
                push @{ $data->{$cluster}->{$ascore} }, $id;
            } else {
                $id = $p[0];
                push @{ $data->{$cluster} }, $id;
            }
        }
        close $fh;
    };

    map { &$processFileFn($_) } @$mapFiles;

    return $data;

#    my @entries = glob("$dir/*");
#
#    my ($upData, $urData) = ({}, {});
#
#    foreach my $entry (@entries) {
#        (my $dirName = $entry) =~ s%^.*/(cluster[^/]+)$%$1%;
#        next if not -d $entry or $dirName !~ m/^cluster/;
#
#        my $file = "$entry/uniprot.txt";
#        my $lc = getLineCount($file);
#        $upData->{$dirName} = {size => $lc, ids => [], file => $file};
#
#        $file = "$entry/uniref90.txt";
#        $lc = getLineCount($file);
#        $urData->{$dirName} = {size => $lc, ids => [], file => $file};
#    }
#
#    return ($upData, $urData);
}


sub getLineCount {
    my $file = shift;
    my $lc = `wc -l $file`;
    $lc =~ s/^(\d+)\D.*$/$1/gs;
    return $lc;
}


sub getIds {
    my $file = shift;
    open my $fh, "<", $file;
    my @ids;
    while (<$fh>) {
        chomp;
        next if m/^\s*$/ or m/^#/;
        push @ids, $_;
    }
    close $fh;
    return \@ids;
}


sub getUniRefMap {
    my $file = shift;

    open my $fh, "<", $file or die "Unable to read uniref map file $file: $!";

    my $data = {50 => {}, 90 => {}};

    while (<$fh>) {
        chomp;
        next if m/^\s*$/ or m/^#/;
        my ($uniprot, $uniref90, $uniref50) = split(m/\t/);
        push @{ $data->{90}->{$uniref90} }, $uniprot;
        push @{ $data->{50}->{$uniref50} }, $uniprot;
    }

    close $fh;

    return $data;
}


sub doInsert {
    my $sql = shift;
    if ($insCount++ > $commitSize) {
        print "Database commit\n";
        $dbh->commit if not $debug;
        $dbh->begin_work if not $debug;
        $insCount = 0;
    }
    $dbh->do($sql) if not $debug;
}




