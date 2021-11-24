#!/usr/bin/env perl
$|++;

use strict;
use warnings;

use Getopt::Long;
use FindBin;
use Data::Dumper;
use DBI;
use DBD::SQLite;
use Time::HiRes qw(time);


my ($dbFile, $clusterIdFile, $allIdFile, $outputDb, $logFile);
my $result = GetOptions(
    "cluster-id-file=s" => \$clusterIdFile,
    "all-id-file=s"     => \$allIdFile,
    "master-db=s"       => \$dbFile,
    "output-db=s"       => \$outputDb,
    "log=s"             => \$logFile,
);

my $defaultNbSize = 10;

die "Need --cluster-id-file or --all-id-file" if (not $clusterIdFile and not $allIdFile) or (not -f $clusterIdFile and not -f $allIdFile);
die "No --db-file provided" if not $dbFile or not -f $dbFile;
die "Need --output-db" if not $outputDb;


my %ids;
my @ids;
if ($allIdFile) {
    readIdFile($clusterIdFile, \%ids);
} else {
    @ids = readIdFile($clusterIdFile);
}


unlink $outputDb if -f $outputDb;

#my $indbh = DBI->connect("DBI:SQLite:dbname=$dbFile", "", "");
my $indbh = DBI->connect("DBI:mysql:database=gnd_temp;host=localhost;port=3306", "noberg", "Mykidsr0ck");
my $outdbh = DBI->connect("DBI:SQLite:dbname=$outputDb", "", "");
$outdbh->{AutoCommit} = 0;
my $numInsert = 0;


my $logFh = \*STDERR;
if ($logFile) {
    open my $fh, ">", $logFile or die "Unable to open log file $logFile: $!";
    $logFh = $fh;
}

createSchema($outdbh);
copyMetadata();

my ($newSortKey, $newSortOrder, $newClusterIndex) = (1, 0, 0);
my ($newNbSortKey) = (0, 0, 0);
my %urRange = (50 => {}, 90 => {});
my %idMap;
my %indexMap;
my %uniRef90Ids;
my %uniRef50Ids;

timer("");
foreach my $id (@ids) {
    my $row = getAttributesRow($id);
    writeLog("Unable to find $id in dataset") and next if not $row;
    my $sortKey = $row->{sort_key};
    my $clusterIndex = $row->{cluster_index};
    $indexMap{$clusterIndex} = $newClusterIndex;
    $idMap{$id} = $newClusterIndex;
    #$idMap{$id} = $newSortKey;
    $row->{sort_key} = $newSortKey;
    $row->{sort_order} = $newSortOrder;
    $row->{cluster_index} = $newClusterIndex;
    insertAttributesRow($row);

    my @rows = getNeighbors($sortKey);
    foreach my $row (@rows) {
        $row->{gene_key} = $newSortKey;
        $row->{sort_key} = $newNbSortKey;
        insertNeighborRow($row);
        $newNbSortKey++;
    }

    processUniRef($clusterIndex);

    $newSortKey++;
    $newSortOrder++;
    $newClusterIndex++;
}
insertClusterIndex(0, $newClusterIndex-1);


writeLog("Outputting UniRef90");
insertUniRefValues(\%uniRef90Ids, "uniref90");
writeLog("Outputting UniRef50");
insertUniRefValues(\%uniRef50Ids, "uniref50");

timer("num IDs: " . scalar(@ids));

$outdbh->commit;








sub insertUniRefValues {
    my $uniRefIds = shift;
    my $tableBaseName = shift;

    my $newUniRefMemberIndex = 0;
    my $newUniRefIndex = 0;
    foreach my $uniRefId (sort { $a <=> $b } keys %$uniRefIds) {
        my $rangeRow = $uniRefIds->{$uniRefId};
        my $uniRefUniProtId = $rangeRow->{uniref_id};
        my @oldClusterIndexes = getUniRefIndexes($rangeRow, "${tableBaseName}_index");
        my $ei = $rangeRow->{end_index};
        my $si = $rangeRow->{start_index};
        my $newStartUniRefIndex = $newUniRefMemberIndex;
        for (my $oldMemberIndex = $si; $oldMemberIndex <= $ei; $oldMemberIndex++) {
            my $oldClusterIndex = $oldClusterIndexes[$oldMemberIndex - $si];
            my $newClusterIndex = $indexMap{$oldClusterIndex};
            if (not defined $newClusterIndex) {
                writeLog("There was no index mapping for old $oldClusterIndex to new cluster index ($uniRefUniProtId); this means that the UniRef cluster contains IDs that are not in the input ID list file.");
            } else {
                $newUniRefMemberIndex++;
                insertUniRefClusterIndex($tableBaseName, $newUniRefMemberIndex, $newClusterIndex);
            }
        }
        my $attrTableClusterIndex = $rangeRow->{cluster_index};
        my $newUniProtIndex = $indexMap{$attrTableClusterIndex};
        if (not defined $newUniProtIndex) {
            writeLog("There was no index mapping for old $attrTableClusterIndex to new cluster index ($tableBaseName ID $uniRefUniProtId)");
        } else {
            insertUniRefRange($tableBaseName, $newUniRefIndex, $uniRefUniProtId, $newStartUniRefIndex, $newUniRefMemberIndex - 1, $newUniProtIndex);
            $newUniRefIndex++;
        }
    }

    insertUniRefCluster($tableBaseName, 0, $newUniRefIndex - 1);
}


#my $newUniRef90Index = 0;
#foreach my $uniRef90Id (sort { $a <=> $b } keys %uniRef90Ids) {
#    my $rangeRow = $uniRef90Ids{$uniRef90Id};
#    my @oldUniProtClusterIds = getUniRefIndexes($rangeRow, "uniref90_index");
#    my $ei = $rangeRow->{end_index};
#    my $si = $rangeRow->{start_index};
#    my $newStartUniRef90Index = $newUniRef90Index;
#    for (my $oldMemberIndex = $si; $oldMemberIndex <= $ei; $oldMemberIndex++) {
#        my $oldClusterIndex = $oldUniProtClusterIds[$mi-$si];
#        my $newClusterIndex = $indexMap{$oldClusterIndex};
#        $newUniRef90Index++;
#        #TODO: implement
#        insertUniRefClusterIndex(0, "uniref90_index", $newUniRef90Index, $newClusterIndex);
#    }
#    #TODO: implement
#    insertUniRefRange(0, "uniref90_range", $rangeRow, $newStartUniRef90Index, $newUniRef90Index - 1);
#}
#
#
#
#foreach my $ver (50, 90) {
#    my $s = 0;
#    my $memberIndex = 0;
#    my $rangeIndex = 0;
#    foreach my $id (sort keys %{$urRange{$ver}}) {
#        my @n = @{ $urRange{$ver}->{$id} };
#        my $e = $s;
#        foreach my $sortKey (@n) {
#            insertUniRefIndex($ver, $e, $sortKey);
#            $e++;
#        }
#        my $newSortKey = $idMap{$id};
#        writeLog("Warning: couldn't find UniRef$ver ID $id in idmap") and next if not $newSortKey;
#        insertUniRefRange($ver, $rangeIndex, $id, $s, $e, $newSortKey);
#        $rangeIndex++;
#        $s = $e;
#    }
#    insertUniRefCluster($ver, 0, $rangeIndex);
#}


sub processUniRef {
    my $clusterIndex = shift;

    my $newUniRef90IndexRow = getUniRefIndex("uniref90_index", $clusterIndex);
    writeLog("Couldn't find UniRef90 member index for $clusterIndex") and return if not $newUniRef90IndexRow;
    my $uniRef90MemberIndex = $newUniRef90IndexRow->{member_index};
    my $uniRef90RangeRow = getUniRefRange("uniref90_range", $uniRef90MemberIndex);
    my $uniRef90Id = $uniRef90RangeRow->{uniref_index};

    return if $uniRef90Ids{$uniRef90Id};

    $uniRef90Ids{$uniRef90Id} = $uniRef90RangeRow;
    
    my $uniRef50IndexRow = getUniRefIndex("uniref50_index", $uniRef90Id);
    writeLog("Couldn't find UniRef50 member index for $uniRef90MemberIndex/$clusterIndex") and return if not $uniRef50IndexRow;
    my $uniRef50MemberIndex = $uniRef50IndexRow->{member_index};
    my $uniRef50RangeRow = getUniRefRange("uniref50_range", $uniRef50MemberIndex);
    my $uniRef50Id = $uniRef50RangeRow->{uniref_index};

    return if $uniRef50Ids{$uniRef50Id};

    $uniRef50Ids{$uniRef50Id} = $uniRef50RangeRow;
}

#    push @{$uniRef50Members->{$uniRef50MemberIndex}}, $uniRef50IndexRow->{cluster_index};
#
#    push @{$uniRef90Members->{$uniRef90MemberIndex}}, $newUniRef90IndexRow->{cluster_index};
#
#    my $uniRef50IndexRow = getUniRefIndex($uniRef90MemberIndex, "uniref90_index");
#    writeLog("Couldn't find UniRef50 member index for $uniRef90MemberIndex/$clusterIndex") and next if not $uniRef50IndexRow;
#    my $uniRef50MemberIndex = $uniRef50IndexRow->{member_index};
#
#    push @{$uniRef50Members->{$uniRef50MemberIndex}}, $uniRef50IndexRow->{cluster_index};
#
#
#    my $uniRef90RangeRow = getUniRefRange($uniRef90MemberIndex, 90);
#    writeLog("Couldn't find $id in UniRef90 range table") and next if not $uniRef90RangeRow;
#
#    for (my $mi = $uniRef90RangeRow->{start_index}; $mi <= $uniRefRangeRow->{end_index}; $mi++) {
#        push @{ $urRange{$ver}->{$row->{uniref_id}} }, $newClusterIndex;
#    
#
#    foreach my $ver (50, 90) {
#        my $row = getUniRefIndex($clusterIndex, $ver);
#        writeLog("Couldn't find UniRef$ver member index for $clusterIndex") and next if not $row;
#        my $urIdx = $row->{member_index};
#        $row = getUniRefRange($urIdx, $ver);
#        writeLog("Couldn't find $id in UniRef$ver range table") and next if not $row;
#        push @{ $urRange{$ver}->{$row->{uniref_id}} }, $newClusterIndex;
#    }



my $TIMER;
sub timer {
    my $name = shift || "";
    if ($TIMER and $name) {
        printf("%-20s %.6f\n", $name, time - $TIMER);
    }
    $TIMER = time;
}


sub createSchema {
    my $cmd = "echo '.schema' | sqlite3 $dbFile | grep -v sqlite_sequence | sqlite3 $outputDb";
    `$cmd`;
}

sub insertTransaction {
    my $insertSql = shift;
    if ($numInsert++ > 50) {
        $outdbh->commit;
        $numInsert = 0;
    }
    $outdbh->do($insertSql);
}


sub copyMetadata {
    my $cluster = shift || "";
    my $sql = "SELECT neighborhood_size, type FROM metadata";
    my $sth = $indbh->prepare($sql);
    $sth->execute;
    my $row = $sth->fetchrow_arrayref;
    
    my $insertSql = "INSERT INTO metadata (name, neighborhood_size, type) VALUES (";
    $insertSql .= join(", ", map { $indbh->quote($_) } ($cluster, @$row));
    $insertSql .= ")";

    insertTransaction($insertSql);
}

sub insertAttributesRow {
    my ($row) = @_;
    insertRow($row, "attributes");
}

sub insertRow {
    my ($row, $table) = @_;
    my (@cols, @vals);
    foreach my $key (keys %$row) {
        next if not defined $row->{$key};
        push @cols, $key;
        push @vals, $outdbh->quote($row->{$key});
    }
    my $cols = join(",", @cols);
    my $vals = join(",", @vals);
    my $sql = "INSERT INTO $table ($cols) VALUES ($vals)";
    insertTransaction($sql);
}

sub getAttributesRow {
    my ($id) = @_;
    my $sql = "SELECT * FROM attributes WHERE accession = '$id'";
    my $sth = $indbh->prepare($sql);
    $sth->execute;
    my $row = $sth->fetchrow_hashref;
    return $row;
}

sub insertNeighborRow {
    my ($row) = @_;
    insertRow($row, "neighbors");
}

sub getNeighbors {
    my ($sortKey) = @_;
    my $sql = "SELECT * FROM neighbors WHERE gene_key = $sortKey";
    my $sth = $indbh->prepare($sql);
    $sth->execute;
    my @rows;
    while (my $row = $sth->fetchrow_hashref) {
        push @rows, $row;
    }
    return @rows;
}

sub getUniRefIndexes {
    my ($rangeRow, $table) = @_;
    my $sql = "SELECT * FROM $table WHERE member_index >= $rangeRow->{start_index} AND member_index <= $rangeRow->{end_index} ORDER BY member_index";
    my $sth = $indbh->prepare($sql);
    $sth->execute;
    my @clusterIds;
    while (my $row = $sth->fetchrow_hashref) {
        push @clusterIds, $row->{cluster_index};
    }
    return @clusterIds;
}

sub getUniRefIndex {
    my ($table, $clusterIndex) = @_;
    my $sql = "SELECT * FROM $table WHERE cluster_index = $clusterIndex";
    my $sth = $indbh->prepare($sql);
    $sth->execute;
    my $row = $sth->fetchrow_hashref;
    return $row;
}

sub getUniRefRange {
    my ($table, $idx) = @_;
    my $sql = "SELECT * FROM $table WHERE start_index <= $idx AND end_index >= $idx";
    #my $sql = "SELECT * FROM uniref${ver}_range WHERE start_index = $idx AND end_index = $idx";
    my $sth = $indbh->prepare($sql);
    $sth->execute;
    my $row = $sth->fetchrow_hashref;
    return $row;
}

sub insertUniRefClusterIndex {
    my ($tableBaseName, $memberIndex, $clusterIndex) = @_;
    my $sql = "INSERT INTO ${tableBaseName}_index VALUES ($memberIndex, $clusterIndex)";
    insertTransaction($sql);
}

sub insertUniRefRange {
    my ($tableBaseName, $newUniRefIndex, $uniProtId, $newStart, $newEnd, $uniProtClusterIndex) = @_;
    my $sql = "INSERT INTO ${tableBaseName}_range VALUES ($newUniRefIndex, '$uniProtId', $newStart, $newEnd, $uniProtClusterIndex)";
    insertTransaction($sql);
}

sub insertUniRefCluster {
    my ($tableBaseName, $start, $end) = @_;
    my $sql = "INSERT INTO ${tableBaseName}_cluster_index VALUES (1, $start, $end)";
    insertTransaction($sql);
}

sub insertClusterIndex {
    my ($start, $end) = @_;
    my $sql = "INSERT INTO cluster_index VALUES (1, $start, $end)";
    insertTransaction($sql);
}


sub readIdFile {
    my $file = shift;

    open my $fh, "<", $file or die "Unable to read --id-file $file: $!";

    my @ids;
    while (<$fh>) {
        chomp;
        next if m/^#/ or m/^\s*$/;
        push @ids, $_;
    }

    close $fh;

    return @ids;
}


sub writeLog {
    $logFh->print(join("\n", @_), "\n");
}


