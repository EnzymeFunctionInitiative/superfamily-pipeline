#!/bin/env perl

use strict;
use warnings;

use DBI;
use Getopt::Long;
use File::Find;
use Data::Dumper;

my ($dbFile, $dataDir, $ecListOut, $ecDbFile, $keggOut, $rewriteSP);
my $result = GetOptions(
    "db-file=s"         => \$dbFile,
    "data-dir=s"        => \$dataDir,
    "ec-desc-db=s"      => \$ecDbFile,
    "ec-list=s"         => \$ecListOut,
    "kegg-table=s"      => \$keggOut,
    "process-sp"        => \$rewriteSP,
);


die "Need --db-file" if not $dbFile or not -f $dbFile;
die "Need --data-dir" if not $dataDir or not -d $dataDir;



my $dbh = DBI->connect("DBI:SQLite:dbname=$dbFile","","");
my $self = {dbh => $dbh};
my $allEc = {};
my $kegg = {};

find(sub { processFile($self, $File::Find::name, $allEc, $kegg) if $_ eq "swissprot.txt"; }, $dataDir);



if ($ecDbFile and $ecListOut) {
    my $ecDb = readEcDb($ecDbFile);
    open my $out, ">", $ecListOut or die "Unable to write to --ec-list $ecListOut: $!";
    foreach my $code (sort keys %$allEc) {
        my ($h1, $h2, $h3, $h4) = split(m/\./, $code);
        print "WARNING: $code not found in DB\n" and next if not $ecDb->{"$h1.$h2.$h3.-"};
        my $desc = $ecDb->{"$h1.-.-.-"} . " // " . $ecDb->{"$h1.$h2.-.-"} . " // " . $ecDb->{"$h1.$h2.$h3.-"};
        print $out "$code\t$desc\n";
    }
    close $out;
}


if ($kegg) {
    open my $out, ">", $keggOut or die "Unable to write to --kegg-table $keggOut: $!";
    foreach my $cluster (sort keys %$kegg) {
        print $out join("\t", $cluster, join(";", @{$kegg->{$cluster}})), "\n";
    }
    close $out;
}





sub processFile {
    my $self = shift;
    my $spFile = shift;
    my $allEc = shift;
    my $kegg = shift;

    (my $clusterId = $spFile) =~ s%^.*?([^/]+)/([^/]+)$%$1%;

    open my $fh, "<", $spFile or die "Unable to read from $spFile: $!";
    my $hdr = <$fh>;
   
    my $out;
    if ($rewriteSP) {
        open $out, ">", "$spFile.tmp" or die "Unable to write to $spFile.tmp: $!";
        print $out $hdr;
    }

    while (<$fh>) {
        chomp;
        my ($clusterNum, $id, $oldSpDesc) = split(m/\t/);
        my $sql = "SELECT uniref.accession, STATUS, SwissProt_Description AS SP, KEGG, EC FROM uniref " .
            "LEFT JOIN annotations ON uniref.accession = annotations.accession " .
            "WHERE uniref50_seed = '$id'";
        my $sth = $self->{dbh}->prepare($sql);
        $sth->execute();
        my @newSpDesc;
        while (my $row = $sth->fetchrow_hashref()) {
            next if $row->{STATUS} ne "Reviewed";
            (my $spDesc = $row->{SP}) =~ s/Short.*$//;
            (my $ec = $row->{EC}) =~ s/^(\S+).*$/$1/;
            if ($ec =~ m/^\d+\.[^\.]+\.[^\.]+\./) {
                $spDesc .= "||$ec";
                $allEc->{$ec} = 1;
            }
            if ($row->{KEGG}) {
                push(@{$kegg->{$clusterId}}, $row->{KEGG});
            }
            push @newSpDesc, $spDesc;
        }
        print $out join("\t", $clusterNum, $id, join(";", @newSpDesc)), "\n" if $rewriteSP;
    }
    close $fh;

    if ($rewriteSP) {
        close $out;
        rename $spFile, "$spFile.bak" if not -f "$spFile.bak";
        rename "$spFile.tmp", $spFile;
    }
}


sub readEcDb {
    my $file = shift;

    my $db = {};

    open my $fh, "<", $file or die "Unable to read --ec-desc-db $file: $!";

    while (<$fh>) {
        next if not m/^[1-9]/;
        my $code = substr($_, 0, 9);
        my $desc = substr($_,  10);
        $desc =~ s/^\s*(.*?)\.*\s*$/$1/;
        $code =~ s/\s//g;
        #my ($h1, $h2, $h3, $h4) = split(m/\./, $code);
        $db->{$code} = $desc;
    }

    close $fh;

    return $db;
}


