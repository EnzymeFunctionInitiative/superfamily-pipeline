#!/bin/perl

use strict;
use warnings;

use DBI;
use Getopt::Long;


my ($dbFile, $tableName, $doDrop, $dryRun);
#my ($dbFile, $tableName, $idMapFile, $doDrop, $dryRun);
my $result = GetOptions(
    "db-file=s"     => \$dbFile,
    "table-name=s"  => \$tableName,
#    "id-mapping=s"  => \$idMapFile,
    "drop"          => \$doDrop,
    "dry-run"       => \$dryRun,
);


die "--db-file missing" if not $dbFile or not -f $dbFile;
die "--table table name missing" if not $tableName;
#die "--id-mapping missing" if not $idMapFile or not -f $idMapFile;

$doDrop = defined($doDrop);
$dryRun = defined($dryRun);


my $dbh = DBI->connect("DBI:SQLite:dbname=$dbFile","",""); 
my $doFn = sub {
    my $sql = shift;
    if ($dryRun) {
        print "$sql;\n";
    } else {
        $dbh->do($sql);
    }
};


my @tableNames = split(m/,/, $tableName);

foreach my $name (@tableNames) {
    processTable($name);
}








sub processTable {
    my $tableName = shift;

    my @cols = getTableInfo($tableName);
    my @indexes = getTableIndexes($tableName);
    
    my @newCols = ("cluster_id TEXT", "ascore INT");
    
    foreach my $col (@cols) {
        next if $col->{name} eq "cluster_id" or $col->{name} eq "ascore";
        my $type = "$col->{name} $col->{type}";
        $type .= " NOT NULL" if $col->{notnull};
        $type .= " DEFAULT $col->{dflt_value}" if $col->{dflt_value};
        #$type .= " PRIMARY KEY" if $col->{pk};
        push @newCols, $type;
    }
    
    
    my $idMapping = loadIdMapping();
    #my $idMapping = loadIdMapping($idMapFile);
    
    my $inserts = getData($tableName, \@cols, $idMapping);
    
    if ($doDrop) {
        my $sql = "DROP TABLE $tableName";
        &$doFn($sql);
    }
    
    my $sql = "CREATE TABLE $tableName (" . join(", ", @newCols) . ")";
    &$doFn($sql);
    createIndexes($tableName, \@indexes);
    
    $dbh->begin_work;
    
    foreach my $insert (@$inserts) {
        my $sql = "INSERT INTO $tableName VALUES (" . join(", ", @$insert) . ")";
        &$doFn($sql);
    }
    
    $dbh->commit;
}


sub createIndexes {
    my $tableName = shift;
    my $indexes = shift;
    my $sql = "CREATE INDEX ${tableName}_lookup_idx ON $tableName (cluster_id, ascore)";
    &$doFn($sql);
    foreach my $idx (@$indexes) {
        $sql = "CREATE INDEX $idx->{name} ON $tableName (" . join(", ", @{$idx->{cols}}) . ")";
        &$doFn($sql);
    }
}


sub getData {
    my $tableName = shift;
    my $cols = shift;
    my $idMapping = shift;

    my $sql = "SELECT * FROM $tableName";
    my $sth = $dbh->prepare($sql);
    $sth->execute;

    my @inserts;
    while (my $row = $sth->fetchrow_hashref) {
        foreach my $idrow (@{$idMapping->{$row->{uniprot_id}}}) {
            my @irow = ($dbh->quote($idrow->[0]), $dbh->quote($idrow->[1]));
            foreach my $col (@$cols) {
                next if $col->{name} eq "cluster_id" or $col->{name} eq "ascore";
                push @irow, $dbh->quote($row->{$col->{name}});
            }
            push @inserts, \@irow;
        }
    }

    return \@inserts;
}

sub loadIdMapping {
    my $data = {};

    my $sql = "SELECT * FROM id_mapping";
    my $sth = $dbh->prepare($sql);
    $sth->execute;
    while (my $row = $sth->fetchrow_hashref) {
        push @{$data->{$row->{uniprot_id}}}, [$row->{cluster_id}, ""];
    }
    
    $sql = "SELECT * FROM diced_id_mapping";
    $sth = $dbh->prepare($sql);
    $sth->execute;
    while (my $row = $sth->fetchrow_hashref) {
        push @{$data->{$row->{uniprot_id}}}, [$row->{cluster_id}, $row->{ascore}];
    }

    return $data;

    #my $file = shift;
    #my $data = {};
    #open my $fh, "<", $file or die "Unable to read id map $file: $!";
    #while (<$fh>) {
    #    chomp;
    #    my ($cid, $as, $id) = split(m/[\t\|]/);
    #    push @{$data->{$id}}, [$cid, $as];
    #}
    #close $fh;
    #return $data;
}


sub getTableInfo {
    my $tableName = shift;

    my $sql = "PRAGMA TABLE_INFO($tableName)";
    my $sth = $dbh->prepare($sql);
    $sth->execute;

    #cid         name        type        notnull     dflt_value  pk
    my @cols;
    while (my $row = $sth->fetchrow_hashref) {
        my %info = %$row;
        push @cols, \%info;
    }

    return @cols;
}


sub getTableIndexes {
    my $tableName = shift;

    my $sql = "PRAGMA INDEX_LIST($tableName)";
    my $sth = $dbh->prepare($sql);
    $sth->execute;

    #seq name
    my @indexes;
    while (my $row = $sth->fetchrow_hashref) {
        my $info = {name => $row->{name}};
        push @indexes, $info;
    }

    foreach my $idx (@indexes) {
        $sql = "PRAGMA INDEX_INFO($idx->{name})";
        $sth = $dbh->prepare($sql);
        $sth->execute;
        my @cols;
        while (my $row = $sth->fetchrow_hashref) {
            push @cols, $row->{name};
        }
        $idx->{cols} = \@cols;
    }

    return @indexes;
}



