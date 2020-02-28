
package EFI::HMM::Database;

use strict;
use warnings;


use Exporter qw(import);
our @EXPORT_OK = qw(validateInputs cleanJson);

use Getopt::Long;
use DBI;
use File::Slurp;
use Data::Dumper;



sub new {
    my $class = shift;
    my %args = ($_[0] and ref $_[0] eq "HASH") ? %{$_[0]} : @_;

    my $self = {};
    bless $self, $class;

    $self->{sqlite} = $args{sqlite_file} // "";
    $self->{json_file} = $args{json_file} // "";

    $self->{dbh} = getHandle($self->{sqlite});

    return $self;
}


sub validateInputs {
    my $getUsageFn = shift;

    my %opts;
    my $result = GetOptions(
        \%opts,
        "json-file=s",
        "data-dir=s",
        "sqlite-file=s",
        "mode=s",
        "in-place",
        "load-ec-file=s",
        "load-kegg-file=s",
    );

    die &$getUsageFn("require --sqlite-file argument") if not $opts{"sqlite-file"};

    my %parms;
    $parms{json_file} = $opts{"json-file"} // "";
    $parms{data_dir} = $opts{"data-dir"} // "";
    $parms{sqlite_file} = $opts{"sqlite-file"};
    $parms{in_place} = defined($opts{"in-place"}) ? 1 : 0;
    $parms{mode} = $opts{"mode"} // "";
    $parms{load_ec_file} = $opts{"load-ec-file"} // "";
    $parms{load_kegg_file} = $opts{"load-kegg-file"} // "";

    return \%parms;
}


sub cleanJson {
    my $jsonText = shift;
    $jsonText =~ s/\s*\/\/.*?([\r\n])/$1/gs;
    $jsonText =~ s/^var +.*=\s*//s;
    $jsonText =~ s/,([\r\n]+)(\s*)}/$1$2}/gs;
    $jsonText =~ s/,([\r\n]+)(\s*)\]/$1$2]/gs;
    $jsonText =~ s/\t/ /gs;
    $jsonText =~ s/;[\r\n\s]*$//s;
    return $jsonText;
}


sub swissProtsToSqlite {
    my $self = shift;
    my $dataDir = shift;

    $self->createTable("swissprot");
    foreach my $dir (glob("$dataDir/cluster-*")) {
        print "PROCESSING $dir\n";
        my $sp = $self->getSwissProts($dir);
        (my $network = $dir) =~ s%^.*/([^/]+)$%$1%;
        $self->insertSwissProt($network, $sp);
    }
}


sub keggToSqlite {
    my $self = shift;
    my $keggFile = shift;

    $self->createTable("kegg");

    open my $fh, "<", $keggFile;

    while (<$fh>) {
        chomp;
        my ($clusterId, $keggVals) = split(m/\t/);
        foreach my $kegg (split(m/[,;]/, $keggVals)) {
            my $valStr = "\"$clusterId\", \"$kegg\"";
            my $sql = "INSERT INTO kegg (cluster_id, kegg) VALUES ($valStr)";
            $self->{dbh}->do($sql);
        }
    }

    close $fh;
}


sub ecToSqlite {
    my $self = shift;
    my $ecFile = shift;

    $self->createTable("enzymecode");

    open my $fh, "<", $ecFile;

    while (<$fh>) {
        chomp;
        my ($code, $desc) = split(m/\t/);
        my $sql = "INSERT INTO enzymecode (code_id, desc) VALUES (\"$code\", \"$desc\")";
        $self->{dbh}->do($sql);
    }

    close $fh;
}


sub networkUiJsonToSqlite {
    my $self = shift;
    my $json = shift;

    $self->createTable("size");
    $self->createTable("network");
    $self->createTable("region");
    
    $self->insertSizeData($json->{sizes});
    $self->insertNetworkData($json->{networks});
}


sub getSwissProts {
    my $self = shift;
    my $dir = shift;

    my $file = "$dir/swissprot.txt";
    
    my @lines = read_file($file);
    shift @lines; # discard header
    
    my %functions;
    foreach my $line (@lines) {
        my ($col, $uniprotId, $func) = split(m/\t/, $line);
        $func =~ s/[\s\n\r]+$//s;
        $func =~ s/[\s\t]+/ /g;
        
        my @funcs = split(m/;/, $func);
        map { s/^\s*(.*?)\s*$/$1/; s/Short.*$//; push @{$functions{$_}}, $uniprotId; } @funcs;
    }

    return \%functions;
}


sub swissProtsToJson {
    my $self = shift;
    my $json = shift;

    my @clusterIds;

    my $clusterSql = "SELECT DISTINCT cluster_id FROM swissprot";
    my $sth = $self->{dbh}->prepare($clusterSql);
    $sth->execute;
    while (my $row = $sth->fetchrow_hashref) {
        push @clusterIds, $row->{cluster_id};
    }

    foreach my $clusterId (@clusterIds) {
        my $sql = "SELECT DISTINCT function FROM swissprot WHERE cluster_id = '$clusterId'";
        my $sth = $self->{dbh}->prepare($sql);
        $sth->execute;
        while (my $row = $sth->fetchrow_hashref) {
            push @{$json->{swissprot}->{$clusterId}}, $row->{function};
        }
    }
}


sub enzymeCodeToJson {
    my $self = shift;
    my $json = shift;

    my $codeSql = "SELECT * FROM enzymecode";
    my $sth = $self->{dbh}->prepare($codeSql);
    $sth->execute;
    while (my $row = $sth->fetchrow_hashref) {
        $json->{enzymecode}->{$row->{code_id}} = $row->{desc};
    }
}


sub keggToJson {
    my $self = shift;
    my $json = shift;

    my @clusterIds;

    my $clusterSql = "SELECT DISTINCT cluster_id FROM kegg";
    my $sth = $self->{dbh}->prepare($clusterSql);
    $sth->execute;
    while (my $row = $sth->fetchrow_hashref) {
        push @clusterIds, $row->{cluster_id};
    }

    foreach my $clusterId (@clusterIds) {
        my $sql = "SELECT DISTINCT kegg FROM kegg WHERE cluster_id = '$clusterId'";
        my $sth = $self->{dbh}->prepare($sql);
        $sth->execute;
        while (my $row = $sth->fetchrow_hashref) {
            push @{$json->{kegg}->{$clusterId}}, $row->{kegg};
        }
    }
}


sub insertSwissProt {
    my $self = shift;
    my $clusterId = shift;
    my $spList = shift;

    foreach my $sp (sort keys %$spList) {
        $sp =~ s/"/'/g;
        foreach my $id (@{$spList->{$sp}}) {
            my $valStr = "\"$clusterId\", \"$id\", \"$sp\"";
            my $sql = "INSERT INTO swissprot (cluster_id, uniprot_id, function) VALUES ($valStr)";
            $self->{dbh}->do($sql);
        }
    }
}


sub insertSizeData {
    my $self = shift;
    my $data = shift;

    foreach my $clusterId (sort keys %$data) {
        my $valStr = "\"$clusterId\", ";
        $valStr .= getIntVal($data->{$clusterId}->{uniprot}) . ", ";
        $valStr .= getIntVal($data->{$clusterId}->{uniref90}) . ", ";
        $valStr .= getIntVal($data->{$clusterId}->{uniref50});
        my $sql = "INSERT INTO size (cluster_id, uniprot, uniref90, uniref50) VALUES ($valStr)";
        $self->{dbh}->do($sql);
    }
}


sub insertNetworkData {
    my $self = shift;
    my $data = shift;

    foreach my $clusterId (sort keys %$data) {
        my $net = $data->{$clusterId};

        my $valStr = join(", ", getStrVal($clusterId), getStrVal($net->{title}), getStrVal($net->{name}), getStrVal($net->{TESTtext}));
        my $sql = "INSERT INTO network (cluster_id, title, name, desc) VALUES ($valStr)";
        $self->{dbh}->do($sql);

        if ($net->{regions}) {
            $self->insertRegionData($clusterId, $net->{regions});
        }
    }
}


sub insertRegionData {
    my $self = shift;
    my $clusterId = shift;
    my $regions = shift;

    my $getCoords = sub {
        my $c = shift;
        return "\"\"" if not $c or not scalar @$c;
        return "\"" . join(",", @$c) . "\"";
    };

    my $idx = 0;
    foreach my $region (@$regions) {
        my $valStr = join(", ", getStrVal($clusterId), getStrVal($region->{id}), getStrVal($region->{name}), $idx, getStrVal($region->{number}), &$getCoords($region->{coords}));
        my $sql = "INSERT INTO region (network_id, region_id, region_index, name, number, coords) VALUES ($valStr)";
        $self->{dbh}->do($sql);
        $idx++;
    }
}


sub getIntVal {
    return $_[0] ? $_[0] : 0;
}
sub getStrVal {
    my $val = $_[0] ? $_[0] : "";
    return "\"$val\"";
}


sub createTable {
    my $self = shift;
    my $tableId = shift;

    my $schemas = {
        "size" => ["cluster_id TEXT", "uniprot INT DEFAULT 0", "uniref90 INT DEFAULT 0", "uniref50 INT DEFAULT 0"],
        "network" => ["cluster_id TEXT", "title TEXT", "name TEXT", "desc TEXT"],
        "region" => ["network_id TEXT", "region_id TEXT", "region_index INT", "name TEXT", "number TEXT", "coords TEXT"],
        "swissprot" => ["cluster_id TEXT", "uniprot_id TEXT", "function TEXT"],
        "enzymecode" => ["code_id TEXT", "desc TEXT"],
        "kegg" => ["cluster_id TEXT", "kegg TEXT"],
    };

    die "Unable to find table schema for $tableId" if not $schemas->{$tableId};

    my $colStr = join(", ", @{$schemas->{$tableId}});

    $self->{dbh}->do("DROP TABLE IF EXISTS $tableId");
    my $sql = "CREATE TABLE $tableId ($colStr)";
    $self->{dbh}->do($sql);
}


sub getHandle {
    my $file = shift;
    my $dbh = DBI->connect("DBI:SQLite:dbname=$file","","");
    return $dbh;
}


1;

