#!/bin/env perl
$r = 1;

die "Needs EFI_TOOLS_HOME directory; the EFI_TOOLS environment variable must be set.\n" if not $ENV{EFI_TOOLS_HOME};

use strict;
use warnings;

use DBI;
use Getopt::Long;
use File::Find;
use Data::Dumper;
use Config::IniFiles;

use lib "$ENV{EFI_TOOLS_HOME}/lib";
use EFI::Annotations;


my ($dbFile, $dataDir, $ecListOut, $ecDbFile, $keggOut, $pdbOut, $taxOut, $spOut, $rewriteSP, $idFile, $masterIdFile, $tigrOut, $tigrDesc, $alphaDesc);
my $result = GetOptions(
    "db-file=s"             => \$dbFile, #sqlite
    "id-file=s"             => \$idFile, # the cluster ID list file to use; optionally put :# after the filename to pick col #
    "master-id-file=s"      => \$masterIdFile, # a list of UniProt IDs
    "data-dir=s"            => \$dataDir,
    "ec-desc-db=s"          => \$ecDbFile,
    "ec-output=s"           => \$ecListOut,
    "kegg-output=s"         => \$keggOut,
    "pdb-output=s"          => \$pdbOut,
    "taxonomy-output=s"     => \$taxOut,
    "swissprot-output=s"    => \$spOut,
    "tigr-output=s"         => \$tigrOut,
    "tigr-desc=s"           => \$tigrDesc,
    "alphafold-desc=s"      => \$alphaDesc,
    "fix-swissprot"         => \$rewriteSP,
);


die "Need --db-file" if not $dbFile;
die "Need --data-dir" if not $dataDir or not -d $dataDir;


my $dbh;
if (-f $dbFile) {
    $dbh = DBI->connect("DBI:SQLite:dbname=$dbFile","","");
} elsif ($dbFile =~ m/^mysql:(.*)$/) {
    my $dbName = $1;
    my $config = Config::IniFiles->new(-file => "/home/n-z/noberg/dev/EST/efi.config");
    my $user = $config->val("database", "user");
    my $password = $config->val("database", "password");
    my $host = $config->val("database", "host");
    $dbh = DBI->connect("DBI:mysql:database=$dbName;host=$host", $user, $password);
    die "Unable to connect to $dbName: $!" if not $dbh;
}

my $self = {dbh => $dbh};
my $data = {};
my $anno = new EFI::Annotations();

if ($idFile) {
    print "Processing from --id-file\n";
    my $colNum = 1;
    $idFile =~ s/^(.*):(\d+)$/$1/;
    $colNum = defined $2 ? $2 : 1;
    my @clusters = getClusters($idFile, $colNum);
    foreach my $cluster (@clusters) {
        my $dir = "$dataDir/$cluster";
        my $file = "${cluster}_uniprot.txt";
        $file = "uniprot.txt" if not -f "$dir/$file";
        processFile($self, "$dir", "$dir/$file", $data);
    }
} elsif ($masterIdFile) {
    print "Processing master $masterIdFile\n";
    processMaster($self, $masterIdFile, $data);
} else {
    print "Processing from finding directories\n";
    find(sub { processFile($self, $File::Find::dir, $File::Find::name, $data) if $_ =~ m/uniprot.txt$/; }, $dataDir);
}


if ($ecDbFile and $ecListOut) {
    my $ecDb = readEcDb($ecDbFile);
    open my $out, ">", $ecListOut or die "Unable to write to --ec-output $ecListOut: $!";
    foreach my $code (sort keys %{$data->{ec}}) {
        my ($h1, $h2, $h3, $h4) = split(m/\./, $code);
        print "WARNING: $code not found in DB\n" and next if not $ecDb->{"$h1.$h2.$h3.-"};
        my $desc = $ecDb->{"$h1.-.-.-"} . " // " . $ecDb->{"$h1.$h2.-.-"} . " // " . $ecDb->{"$h1.$h2.$h3.-"};
        print $out "$code\t$desc\n";
    }
    close $out;
}


if ($spOut) {
    print "Output sp\n";
    outputTable($spOut, $data->{sp});
}

if ($keggOut) {
    print "Output kegg\n";
    outputTable($keggOut, $data->{kegg});
}

if ($pdbOut) {
    print "Output pdb\n";
    outputTable($pdbOut, $data->{pdb});
}

if ($taxOut) {
    print "Output tax\n";
    outputTable($taxOut, $data->{tax});
}

if ($alphaDesc) {
    print "Output alphafold\n";
    outputTable($alphaDesc, $data->{af});
}

if ($tigrOut) {
    print "Output tigr\n";
    outputTable($tigrOut, $data->{tigr});

    if ($tigrDesc) {
        my $tdata = getTigrDesc($self, $data->{tigr});
        open my $fh, ">", $tigrDesc or die "Unable to open tigr desc $tigrDesc for writing: $!";
        foreach my $fam (sort keys %$tdata) {
            $fh->print(join("\t", $fam, $tdata->{$fam}), "\n");
        }
        close $fh;
    }
}


print "Done output\n";


sub outputTable {
    my $file = shift;
    my $data = shift;

    open my $out, ">", $file or die "Unable to write to output table $file: $!";
    foreach my $cluster (sort keys %$data) {
        foreach my $row (@{$data->{$cluster}}) {
            #print $out join("\t", $cluster, @$row), "\n";
            print $out join("\t", @$row), "\n";
        }
    }
    close $out;
}





sub processMaster {
    my $self = shift;
    my $idFile = shift;
    my $data = shift;

    print "|$idFile|\n";
    open my $fh, "<", $idFile or die "Unable to read id file $idFile: $!";

    my %ids;
    while (my $line = <$fh>) {
        chomp $line;
        next if $line =~ m/^\s*$/;
        my @p = split(m/\t/, $line);
        my $id = $p[$#p];
        $ids{$id} = 1;
    }

    close $fh;

    my @ids = sort keys %ids;
    my $max = scalar @ids;

    print "Found $max IDs\n";

    my $c = 0;
    my $step = int($max / 100);
    print "Progress: 0%\n";
    foreach my $id (@ids) {
        retrieveIdData($self, $id, "", $data);
        if ($c++ % $step == 0) {
            print "Progress: " . int($c / $step) . "%\n";
        }
    }
    print "Done\n";
}
sub retrieveIdData {
    my $self = shift;
    my $id = shift;
    my $clusterId = shift;
    my $data = shift;

    my $tigrCol = $tigrOut ? ", TIGRFAMs.id AS tigr_id" : "";
    my $tigrJoin = $tigrOut ? "LEFT JOIN TIGRFAMs ON annotations.accession = TIGRFAMs.accession" : "";

    my $sql = <<SQL;
SELECT swissprot_status, annotations.taxonomy_id, metadata, taxonomy.* $tigrCol
    FROM annotations
    LEFT JOIN taxonomy ON annotations.taxonomy_id = taxonomy.taxonomy_id
    $tigrJoin
WHERE annotations.accession = '$id'
SQL
    my $sth = $self->{dbh}->prepare($sql);
    $sth->execute();
    while (my $row = $sth->fetchrow_hashref()) {
        my $md = $anno->decode_meta_struct($row->{metadata});
        (my $sp = $md->{description}) =~ s/^\s*(.*?)\s*$/$1/;
        $sp .= "||$md->{ec_code}" if $md->{ec_code} and $sp;
        push @{$data->{sp}->{$clusterId}}, [$id, $sp] if $row->{swissprot_status} and $sp;
        push @{$data->{kegg}->{$clusterId}}, [$id, $md->{kegg}] if ($md->{kegg} and $md->{kegg} ne "None");
        push @{$data->{pdb}->{$clusterId}}, [$id, $md->{pdb}] if ($md->{pdb} and $md->{pdb} ne "None");
        push @{$data->{tigr}->{$clusterId}}, [$id, $row->{tigr_id}] if $row->{tigr_id};
        push @{$data->{af}->{$clusterId}}, [$id, $md->{alphafold}] if $md->{alphafold};
        if ($md->{organism} and $row->{taxonomy_id}) {
            my @vals;
            @vals = ($row->{taxonomy_id}, $row->{domain} // "", $row->{kingdom} // "", $row->{phylum} // "", $row->{class} // "", $row->{tax_order} // "", $row->{family} // "", $row->{genus} // "", $row->{species} // "");
            push @{$data->{tax}->{$clusterId}}, [$id, @vals];
        }
    }
}
sub processFile {
    my $self = shift;
    my $dir = shift;
    my $idFile = shift;
    my $data = shift;

    print "Processing $dir\n";

    (my $clusterId = $dir) =~ s%^.*?([^/]+)/*$%$1%;

    open my $fh, "<", $idFile or die "Unable to read id file $idFile: $!";

    while (my $id = <$fh>) {
        chomp $id;
        next if $id =~ m/^\s*$/;
        retrieveIdData($self, $id, $clusterId, $data);
    }

    close $fh;

    if ($rewriteSP) {
        my $spFile = "$dir/swissprot.txt";
        open my $fh, "<", $spFile or die "Unable to read from $spFile: $!";
        my $hdr = <$fh>;
        my $out;
        open $out, ">", "$spFile.tmp" or die "Unable to write to $spFile.tmp: $!";
        print $out $hdr;

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
                    $data->{ec}->{$ec} = 1;
                }
                if ($row->{KEGG}) {
                    push(@{$data->{kegg}->{$clusterId}}, $row->{KEGG});
                }
                push @newSpDesc, $spDesc;
            }
            print $out join("\t", $clusterNum, $id, join(";", @newSpDesc)), "\n" if $rewriteSP;
        }
        close $fh;
    
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


sub getTigrDesc {
    my $self = shift;
    my $clusterData = shift;
    my %fams;
    foreach my $cid (keys %$clusterData) {
        map { $fams{$_->[1]} = 1; } @{ $clusterData->{$cid} };
    }
    #my $sql = "SELECT long_name FROM family_info WHERE family = ?";
    #print "$sql\n";
    #my $sth = $self->{dbh}->prepare($sql);
    foreach my $fam (keys %fams) {
        my $sql = "SELECT long_name FROM family_info WHERE family = '$fam'";
        print("$sql\n");
        my $sth = $self->{dbh}->prepare($sql);
        $sth->execute();
        my $row = $sth->fetchrow_hashref();
        if ($row) {
            print "Has row\n";
            $fams{$fam} = $row->{long_name};
        }
    }
    return \%fams;
}


sub getClusters {
    my $file = shift;
    my $colNum = shift;

    $colNum--; # zero based
    my @clusters;

    open my $fh, "<", $file;

    while (<$fh>) {
        chomp;
        next if m/^\s*$/;
        next if m/^\s*#/;
        my @parts = split(m/\t/);
        push @clusters, $parts[$colNum];
    }

    close $fh;

    return @clusters;
}

