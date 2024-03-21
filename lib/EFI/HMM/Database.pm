
package EFI::HMM::Database;

use strict;
use warnings;


use Exporter qw(import);
our @EXPORT_OK = qw(validateInputs cleanJson);

use Getopt::Long;
use DBI;
use File::Slurp;
use Data::Dumper;
use File::Basename;



sub new {
    my $class = shift;
    my %args = ($_[0] and ref $_[0] eq "HASH") ? %{$_[0]} : @_;

    my $self = {};
    bless $self, $class;

    $self->{sqlite} = $args{sqlite_file} // "";
    $self->{json_file} = $args{json_file} // "";
    $self->{append_to_db} = $args{append_to_db} // "";
    $self->{dryrun} = $args{dryrun} // "";

    $self->{dbh} = getHandle($self->{sqlite}, $self->{dryrun});
    $self->{insert_count} = 0;
    $self->{exec_count} = 0;

    $self->{uniref_version} = $args{uniref_version} // 90;

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
        "load-swissprot-file=s",
        "load-pdb-file=s",
        "load-taxonomy-file=s",
        #"load-subgroup-map-file=s",
        "load-subgroup-desc-file=s",
        "load-id-list-script=s",
        "load-diced",
        "load-tigr-ids-file=s",
        "load-tigr-info-file=s",
        "load-region-file=s",
        "load-netinfo-file=s",
        "load-dicing-file=s",
        #"load-ssn-file=s",
        "load-uniref-file=s",
        "load-conv-ratio-script=s",
        "load-cons-res-script=s",
        "load-anno-file=s",
        "load-alphafold-file=s",
        "append-to-db", # don't recreate the table if it already exists
        "dryrun|dry-run",
        "uniref-version=i",
        "columns=s@",
        "output-file=s",
        "table=s",
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
    $parms{load_swissprot_file} = $opts{"load-swissprot-file"} // "";
    $parms{load_pdb_file} = $opts{"load-pdb-file"} // "";
    $parms{load_taxonomy_file} = $opts{"load-taxonomy-file"} // "";
    #$parms{load_subgroup_map_file} = $opts{"load-subgroup-map-file"} // "";
    $parms{load_subgroup_desc_file} = $opts{"load-subgroup-desc-file"} // "";
    $parms{load_id_list_script} = $opts{"load-id-list-script"} // "";
    $parms{load_diced} = defined $opts{"load-diced"} ? 1 : 0;
    $parms{load_tigr_ids_file} = $opts{"load-tigr-ids-file"} // "";
    $parms{load_tigr_info_file} = $opts{"load-tigr-info-file"} // "";
    $parms{load_region_file} = $opts{"load-region-file"} // "";
    $parms{load_netinfo_file} = $opts{"load-netinfo-file"} // "";
    $parms{load_dicing_file} = $opts{"load-dicing-file"} // "";
#    $parms{load_ssn_file} = $opts{"load-ssn-file"} // "";
    $parms{load_uniref_file} = $opts{"load-uniref-file"} // "";
    $parms{load_conv_ratio_script} = $opts{"load-conv-ratio-script"} // "";
    $parms{load_cons_res_script} = $opts{"load-cons-res-script"} // "";
    $parms{load_anno_file} = $opts{"load-anno-file"} // "";
    $parms{load_alphafold_file} = $opts{"load-alphafold-file"} // "";
    $parms{append_to_db} = $opts{"append-to-db"} // "";
    $parms{dryrun} = $opts{"dryrun"} // "";
    $parms{uniref_version} = $opts{"uniref-version"} // 90;
    $parms{columns} = $opts{"columns"} // "";
    $parms{output_file} = $opts{"output-file"} // "";
    $parms{table} = $opts{"table"} // "";

    return \%parms;
}


sub cleanJson {
    my $jsonText = shift;
    #$jsonText =~ s/\s*\/\/[^"\r\n]*?([\r\n])/$1/gs;
    $jsonText =~ s/^var \S+\s*=\s*//s;
    $jsonText =~ s/,([\r\n]+)(\s*)}/$1$2}/gs;
    $jsonText =~ s/,([\r\n]+)(\s*)\]/$1$2]/gs;
    $jsonText =~ s/\t/ /gs;
    $jsonText =~ s/;[\r\n\s]*$//s;
    return $jsonText;
}


sub genericToJson {
    my $self = shift;
    my $json = shift;
    my $tableName = shift;
    my $primaryCol = shift;

    my @clusterIds;

    my $clusterSql = "SELECT DISTINCT cluster_id FROM $tableName";
    my $sth = $self->{dbh}->prepare($clusterSql);
    $sth->execute;
    while (my $row = $sth->fetchrow_hashref) {
        push @clusterIds, $row->{cluster_id};
    }

    foreach my $clusterId (@clusterIds) {
        my $sql = "SELECT DISTINCT $primaryCol FROM $tableName WHERE cluster_id = '$clusterId'";
        my $sth = $self->{dbh}->prepare($sql);
        $sth->execute;
        while (my $row = $sth->fetchrow_hashref) {
            push @{$json->{$tableName}->{$clusterId}}, $row->{$primaryCol};
        }
    }
}


sub annoToSqlite {
    my $self = shift;
    my $file = shift;

    $self->createTable("annotations");

    return if not -f $file;

    open my $fh, "<", $file;

    while (my $line = <$fh>) {
        chomp $line;
        next if $line =~ m/^#/;
        my ($id, @doi) = split(m/\t/, $line);
        $id =~ s/^\s*(.*?)\s*$/$1/;
        next if $id !~ m/^[A-Z0-9]{6,10}$/i;
        my $doiStr = join("`", grep { m/^\S/ } map { s/^\s*(.*?)\s*$/$1/; $_ } @doi);
        my $sql = "INSERT OR IGNORE INTO annotations (uniprot_id, doi) VALUES(\"$id\", \"$doiStr\")";
        $self->batchInsert($sql);
    }

    close $fh;
}


# ALPHAFOLD ########################################################################################

sub alphafoldToSqlite {
    my $self = shift;
    my $file = shift;

    $self->createTable("alphafolds");

    open my $fh, "<", $file;

    my $sql = "INSERT INTO alphafolds (uniprot_id, alphafold_id) VALUES(?, ?)";
    my $sth = $self->{dbh}->prepare($sql);

    while (my $line = <$fh>) {
        chomp $line;
        next if $line =~ m/^#/;
        my ($id, $af) = split(m/\t/, $line);
        $id =~ s/^\s*(.*?)\s*$/$1/;
        next if $id !~ m/^[A-Z0-9]{6,10}$/i;
        $self->batchExec($sth, $id, $af);
    }

    close $fh;
}


# SWISSPROT METADATA ###############################################################################

sub swissprotsToSqlite {
    my $self = shift;
    my $spFile = shift;

    $self->createTable("swissprot");

    return if not -f $spFile;

    open my $fh, "<", $spFile;

    while (my $line = <$fh>) {
        chomp $line;
        my @parts = split(m/\t/, $line, -1);
        next if not $parts[$#parts];
        #my ($clusterId, $upId, $swissprotVals) = split(m/\t/);
        my $valStr = join(", ", map { "\"$_\"" } @parts);
        my $sql = "INSERT OR IGNORE INTO swissprot (uniprot_id, function) VALUES ($valStr)";
        $self->batchInsert($sql);
    }

    close $fh;
}


sub swissprotsToJson {
    my $self = shift;
    my $json = shift;
    return $self->genericToJson($json, "swissprot", "function");
}


#LEGACY WAY OF DOING THIS.  KEEPING AROUND FOR NOW, JIC.
#
#sub swissProtsToSqliteFromFileLegacy {
#    my $self = shift;
#    my $dataDir = shift;
#
#    $self->createTable("swissprot");
#    foreach my $dir (glob("$dataDir/cluster-*")) {
#        print "PROCESSING $dir\n";
#        my $sp = $self->getSwissProts($dir);
#        (my $network = $dir) =~ s%^.*/([^/]+)$%$1%;
#        $self->insertSwissProt($network, $sp);
#    }
#}
#
#
#sub getSwissProts {
#    my $self = shift;
#    my $dir = shift;
#
#    my $file = "$dir/swissprot.txt";
#    
#    my @lines = read_file($file);
#    shift @lines; # discard header
#    
#    my %functions;
#    foreach my $line (@lines) {
#        my ($col, $uniprotId, $func) = split(m/\t/, $line);
#        $func =~ s/[\s\n\r]+$//s;
#        $func =~ s/[\s\t]+/ /g;
#        
#        my @funcs = split(m/;/, $func);
#        map { s/^\s*(.*?)\s*$/$1/; s/Short.*$//; push @{$functions{$_}}, $uniprotId; } @funcs;
#    }
#
#    return \%functions;
#}
#
#
#sub insertSwissProt {
#    my $self = shift;
#    my $clusterId = shift;
#    my $spList = shift;
#
#    foreach my $sp (sort keys %$spList) {
#        $sp =~ s/"/'/g;
#        foreach my $id (@{$spList->{$sp}}) {
#            my $valStr = "\"$clusterId\", \"$id\", \"$sp\"";
#            my $sql = "INSERT INTO swissprot (cluster_id, uniprot_id, function) VALUES ($valStr)";
#            $self->{dbh}->do($sql);
#        }
#    }
#}


# ID LIST METADATA #################################################################################


sub idListsToSqlite2 {
    my $self = shift;
    my $mainDataDir = shift;
    my $isDiced = shift || 0;

    my $dicedPrefix = $isDiced ? "diced_" : "";
    my $tableName = "${dicedPrefix}id_mapping";
    my $tableNameUr50 = "${dicedPrefix}id_mapping_uniref50";
    my $tableNameUr90 = "${dicedPrefix}id_mapping_uniref90";
    $self->createTable($tableName);
    $self->createTable($tableNameUr50);
    $self->createTable($tableNameUr90);

    my @src = ("uniprot");
    push @src, "uniref50" if $self->{uniref_version} == 50;
    push @src, "uniref90";

    my $processFn = sub {
        my $dir = shift;
        my $ascore = shift || "";
        my $ascoreCol = $ascore ? "ascore," : "";
        my $ascorePH = $ascore ? "?," : "";
        foreach my $src (@src) {
            my $idTypeSuffix = $src ne "uniprot" ? "_$src" : "";
            my $idColName = "${src}_id";
            my $idFile = "$dir/$src.txt";
            next if not -f $idFile;
            (my $clusterId = $dir) =~ s%^.*/([^/]+)$%$1%;

            my $sql = "INSERT INTO $tableName$idTypeSuffix (cluster_id, $ascoreCol $idColName) VALUES(?, $ascorePH ?)";
            my $sth = $self->{dbh}->prepare($sql);

            my @data = ($clusterId);
            push @data, $ascore if $ascore;
            push @data, "";

            open my $ifh, "<", $idFile;
            while (my $line = <$ifh>) {
                chomp($line);
                $data[$#data] = $line;
                $self->batchExec($sth, @data);
            }
            close $ifh;
            $self->finish();
        }
    };
    my $processAllFn = sub {
        my $dataDir = shift;
        my $ascore = shift || "";
        foreach my $dir (glob("$dataDir/cluster-*")) {
            (my $clusterId = $dir) =~ s%^.*/([^/]+)$%$1%;
            &$processFn($dir, $ascore);
        }
    };

    if ($isDiced) {
        my @cdirs = glob("$mainDataDir/cluster-*");
        foreach my $cdir (@cdirs) {
            foreach my $dir (glob("$cdir/dicing-*")) {
                (my $ascore = $dir) =~ s/^.*dicing-(\d+)\/?$/$1/;
                &$processAllFn($dir, $ascore);
            }
        }
    } else {
        &$processAllFn($mainDataDir);
    }
}


sub createClusterIndexTable {
    my $self = shift;

    my $sql = "SELECT * FROM diced_id_mapping ORDER BY uniprot_id, ascore";
    my $sth = $self->{dbh}->prepare($sql);
    $sth->execute;

    my %ids;
    while (my $row = $sth->fetchrow_hashref) {
        push @{ $ids{$row->{uniprot_id}} }, [$row->{cluster_id}, $row->{ascore}];
    }

    my %dataNext;
    my %dataPrev;
    foreach my $id (keys %ids) {
        my @clusters = @{ $ids{$id} };
        for (my $i = 0; $i <= $#clusters; $i++) {
            my $cid = $clusters[$i]->[0] . "-" . $clusters[$i]->[1];
            if ($i < $#clusters) {
                my $nextCid = $clusters[$i + 1]->[0] . "-" . $clusters[$i + 1]->[1];
                $dataNext{$cid}->{$nextCid} = [$clusters[$i]->[0], $clusters[$i]->[1], $clusters[$i + 1]->[0], $clusters[$i + 1]->[1]];
            }
            if ($i > 0) {
                my $prevCid = $clusters[$i - 1]->[0] . "-" . $clusters[$i - 1]->[1];
                $dataPrev{$cid}->{$prevCid} = [$clusters[$i]->[0], $clusters[$i]->[1], $clusters[$i - 1]->[0], $clusters[$i - 1]->[1]];
            }
            #my $cid1 = $clusters[$i]->[0] . "-" . $clusters[$i]->[1];
            #my $cid2 = $clusters[$i + 1]->[0] . "-" . $clusters[$i + 1]->[1];
            #$data{$cid1}->{$cid2} = [$clusters[$i]->[0], $clusters[$i]->[1], $clusters[$i + 1]->[0], $clusters[$i + 1]->[1]];
        }
    }

    my $clusterSort = sub {
        my @ca = split(m/\-/, $a);
        my @cb = split(m/\-/, $b);

        my $upper = $#ca > $#cb ? $#cb : $#ca;

        for (my $i = 1; $i <= $upper; $i++) {
            my $res = $ca[$i] <=> $cb[$i];
            return $res if $res;
        }

        return $#cb > $#ca ? -1 : 0;
    };

    my $processData = sub {
        my $data = shift;
        my $table = shift;

        my @data;
        foreach my $cid (sort $clusterSort keys %$data) {
            foreach my $cid2 (sort $clusterSort keys %{ $data->{$cid} }) {
                push @data, $data->{$cid}->{$cid2};
            }
        }
    
        $self->createTable($table);
    
        $sql = "INSERT INTO $table (cluster_id, ascore, cluster_id2, ascore2) VALUES (?, ?, ?, ?)";
        $sth = $self->{dbh}->prepare($sql);
    
        foreach my $row (@data) {
            $self->batchExec($sth, @$row);
        }
    };

    &$processData(\%dataNext, "diced_cluster_index_next");
    &$processData(\%dataPrev, "diced_cluster_index_prev");

    #@data = sort {
    #                my @ca = split(m/\-/, $a->[0]);
    #                my @cb = split(m/\-/, $b->[0]);

    #                my $upper = $#ca > $#cb ? $#cb : $#ca;

    #                for (my $i = 1; $i <= $upper; $i++) {
    #                    my $res = $ca[$i] <=> $cb[$i];
    #                    return $res if $res;
    #                }

    #                return -1 if $#cb > $#ca;

    #                my $res = $a->[1] <=> $b->[1];
    #                return $res if $res;

    #                @ca = split(m/\-/, $a->[2]);
    #                @cb = split(m/\-/, $b->[2]);

    #                for (my $i = 1; $i <= $#ca; $i++) {
    #                    my $res = $ca[$i] <=> $cb[$i];
    #                    return $res if $res;
    #                }

    #                return 0;
    #             } @data;

    #print Dumper(\@data);
}


sub idListsToSqlite {
    my $self = shift;
    my $mainDataDir = shift;
    my $loadScript = shift;
    my $isDiced = shift || 0;
    my $jobIdListFile = shift || "";

    my $loadSqlFile = $loadScript . "_load.sql";

    open my $outFh, ">", $loadScript or die "Unable to write to id list script file $loadScript: $!";

    print $outFh <<SCRIPT;
echo '.separator "\\t"' > $loadSqlFile

SCRIPT

    my $dicedPrefix = $isDiced ? "diced_" : "";
    my @src;
    $self->createTable("${dicedPrefix}id_mapping");
    $self->createTable("${dicedPrefix}id_mapping_uniref50");
    $self->createTable("${dicedPrefix}id_mapping_uniref90");

    @src = ("uniprot");
    push @src, "uniref50" if $self->{uniref_version} == 50;
    push @src, "uniref90";

    my $masterLoadFileName = "${loadScript}_load_ids";
    unlink "${masterLoadFileName}_uniprot.txt";
    `touch ${masterLoadFileName}_uniprot.txt`; # empty file so we can avoid errors later
    unlink "${masterLoadFileName}_uniref50.txt" if $self->{uniref_version} == 50;
    `touch ${masterLoadFileName}_uniref50.txt` if $self->{uniref_version} == 50; # empty file so we can avoid errors later
    unlink "${masterLoadFileName}_uniref90.txt";
    `touch ${masterLoadFileName}_uniref90.txt`; # empty file so we can avoid errors later

    my $processFn = sub {
        my $dir = shift;
        my $ascore = shift || "";
        my $ascoreCol = $ascore ? "$ascore\\t" : "";
        foreach my $src (@src) {
            my $idTypeSuffix = $src ne "uniprot" ? "_$src" : "";
            my $idFile = "$dir/$src.txt";
            next if not -f $idFile;
            #$idFile = "$dir/$src.txt" if not $idFile;
            #my $ids = $self->getIdLists($dir);
            (my $network = $dir) =~ s%^.*/([^/]+)$%$1%;
            my $masterLoadFile = "${masterLoadFileName}_$src.txt";
            #$self->insertIdList($network, $ids);
#echo "############## PROCESSING $idFile"
#awk '{print "$network\\t$ascoreCol"\$1;}' $idFile > $idFile.load
#echo 'SELECT "LOADING $idFile.load";' >> $loadSqlFile
#echo '.import "$idFile.load" ${dicedPrefix}id_mapping${idTypeSuffix}' >> $loadSqlFile
            print $outFh <<SCRIPT;
awk '{print "$network\\t$ascoreCol"\$1;}' $idFile >> $masterLoadFile
SCRIPT
        }
    };
    my $processAllFn = sub {
        my $dataDir = shift;
        my $ascore = shift || "";
        #my $ascoreCol = $ascore ? "$ascore\\t" : "";
        foreach my $dir (glob("$dataDir/cluster-*")) {
            (my $clusterId = $dir) =~ s%^.*/([^/]+)$%$1%;
            &$processFn($dir, $ascore);
        }
    };

    if ($isDiced) {
        my @cdirs = glob("$mainDataDir/cluster-*");
        foreach my $cdir (@cdirs) {
            foreach my $dir (glob("$cdir/dicing-*")) {
                (my $ascore = $dir) =~ s/^.*dicing-(\d+)\/?$/$1/;
                &$processAllFn($dir, $ascore);
            }
        }
    } else {
        my $clusters;
        my @dirs;
        if ($jobIdListFile) {
            $clusters = {};
            my $handleIdFn = sub {
                my ($cluster, $parms) = @_;
                return if $cluster !~ m/^cluster/;
                push @dirs, "$mainDataDir/$cluster";
#                my $info = IdListParser::getClusterNumbers($cluster, $parms);
#                foreach my $key (keys %$info) {
#                    my $clusterName = "$cluster-$info->{$key}->{number}";
#                    push @dirs, "$mainDataDir/$clusterName";
#                }
            };
            IdListParser::parseFile($jobIdListFile, $handleIdFn);
        } else {
            push @dirs, $mainDataDir;
        }
        foreach my $dir (@dirs) {
            &$processFn($dir);
        }
        if (not scalar @dirs) {
        }
    }

    print $outFh <<SCRIPT;

echo '.import "${masterLoadFileName}_uniprot.txt" ${dicedPrefix}id_mapping' >> $loadSqlFile
SCRIPT
    if ($self->{uniref_version} == 50) {
        print $outFh <<SCRIPT;
echo '.import "${masterLoadFileName}_uniref50.txt" ${dicedPrefix}id_mapping_uniref50' >> $loadSqlFile
SCRIPT
    }
    print $outFh <<SCRIPT;
echo '.import "${masterLoadFileName}_uniref90.txt" ${dicedPrefix}id_mapping_uniref90' >> $loadSqlFile

echo 'sqlite3 $self->{sqlite} < $loadSqlFile'

SCRIPT

    close $outFh;
}


sub getIdLists {
    my $self = shift;
    my $dir = shift;

    my $file = glob("$dir/*uniprot.txt");

    my @ids = read_file($file);
    map { chomp } @ids;

    return \@ids;
}


sub insertIdList {
    my $self = shift;
    my $clusterId = shift;
    my $idList = shift;

    foreach my $id (sort @$idList) {
        my $valStr = "\"$clusterId\", \"$id\"";
        my $sql = "INSERT INTO id_mapping (cluster_id, uniprot_id) VALUES ($valStr)";
        $self->batchInsert($sql);
    }
}


# KEGG METADATA ####################################################################################

sub keggToSqlite {
    my $self = shift;
    my $keggFile = shift;

    $self->createTable("kegg");

    return if not -f $keggFile;

    open my $fh, "<", $keggFile;

    while (<$fh>) {
        chomp;
        #my ($clusterId, $upId, $keggVals) = split(m/\t/);
        my $valStr = join(", ", map { "\"$_\"" } split(m/\t/, $_, -1));
        my $sql = "INSERT INTO kegg (uniprot_id, kegg) VALUES ($valStr)";
        $self->batchInsert($sql);
        #foreach my $kegg (split(m/[,;]/, $keggVals)) {
        #    my $valStr = "\"$clusterId\", \"$kegg\"";
        #    my $sql = "INSERT INTO kegg (cluster_id, kegg) VALUES ($valStr)";
        #    $self->{dbh}->do($sql);
        #}
    }

    close $fh;
}


sub keggToJson {
    my $self = shift;
    my $json = shift;
    return $self->genericToJson($json, "kegg", "kegg");
}


# PDB METADATA #####################################################################################

sub pdbToSqlite {
    my $self = shift;
    my $pdbFile = shift;

    $self->createTable("pdb");

    return if not -f $pdbFile;

    open my $fh, "<", $pdbFile;

    while (<$fh>) {
        chomp;
        my ($upId, $pdbVals) = split(m/\t/);
        my $valStr = join(", ", map { "\"$_\"" } ($upId, $pdbVals));
        my $sql = "INSERT INTO pdb (uniprot_id, pdb) VALUES ($valStr)";
        $self->batchInsert($sql);
    }

    close $fh;
}


sub pdbToJson {
    my $self = shift;
    my $json = shift;
    return $self->genericToJson($json, "pdb", "pdb");
}


# PDB METADATA #####################################################################################

sub taxonomyToSqlite {
    my $self = shift;
    my $taxonomyFile = shift;

    $self->createTable("taxonomy");

    return if not -f $taxonomyFile;

    open my $fh, "<", $taxonomyFile;

    while (<$fh>) {
        chomp;
        #my ($clusterId, $upId, @taxonomyVals) = split(m/\t/);
        my @vals = split(m/\t/, $_, -1);
        if ($#vals < 9) {
            map { push(@vals, "") } (1..(9-$#vals));
        }
        my $valStr = join(", ", map { "\"$_\"" } @vals);
        my $sql = "INSERT INTO taxonomy (uniprot_id, tax_id, domain, kingdom, phylum, class, taxorder, family, genus, species) VALUES ($valStr)";
        $self->batchInsert($sql);
    }

    close $fh;
}


sub taxonomyToJson {
    my $self = shift;
    my $json = shift;
    return $self->genericToJson($json, "taxonomy", "species");
}


# SFLD METADATA ####################################################################################

#sub subgroupMapToSqlite {
#    my $self = shift;
#    my $subgroupFile = shift;
#
#    $self->createTable("subgroup_map");
#
#    open my $fh, "<", $subgroupFile;
#
#    while (<$fh>) {
#        chomp;
#        my ($clusterId, $subgroupVals) = split(m/\t/, $_, -1);
#        foreach my $subgroup (split(m/[,;]/, $subgroupVals)) {
#            my $valStr = "\"$clusterId\", \"$subgroup\"";
#            my $sql = "INSERT INTO subgroup_map (cluster_id, subgroup_id) VALUES ($valStr)";
#            $self->batchInsert($sql);
#        }
#    }
#
#    close $fh;
#}


sub subgroupDescToSqlite {
    my $self = shift;
    my $subgroupFile = shift;

    $self->createTable("subgroup_desc");

    return if not -f $subgroupFile;

    open my $fh, "<", $subgroupFile;

    while (<$fh>) {
        chomp;
        my ($subgroupId, $desc, $color) = split(m/\t/, $_, -1);
        my $valStr = "\"$subgroupId\", \"$desc\", \"$color\"";
        my $sql = "INSERT INTO subgroup_desc (subgroup_id, subgroup_desc, subgroup_color) VALUES ($valStr)";
        $self->batchInsert($sql);
    }

    close $fh;
}


#TODO:
sub subgroupToJson {
    my $self = shift;
    my $json = shift;

    $json->{subgroup_desc} = {};

    my $descSql = "SELECT * FROM subgroup_desc";
    my $sth = $self->{dbh}->prepare($descSql);
    $sth->execute;
    while (my $row = $sth->fetchrow_hashref) {
        $json->{subgroup_desc}->{$row->{subgroup_id}} = {desc => $row->{subgroup_desc}, color => $row->{subgroup_color}};
    }

    $json->{subgroup_map} = {};

    my $mapSql = "SELECT * FROM subgroup_map";
    $sth = $self->{dbh}->prepare($mapSql);
    $sth->execute;
    while (my $row = $sth->fetchrow_hashref) {
        push @{$json->{subgroup_map}->{$row->{cluster_id}}}, $row->{subgroup_id};
    }
}


# ENZYME CODE METADATA #############################################################################

sub ecToSqlite {
    my $self = shift;
    my $ecFile = shift;

    $self->createTable("enzymecode");

    return if not -f $ecFile;

    open my $fh, "<", $ecFile;

    while (<$fh>) {
        chomp;
        next if not m/^\d/;
        my $code = substr($_, 0, 10);
        my $desc = substr($_, 11);
        $desc =~ s/^\s*(.*?)\*$/$1/;
        #my ($code, $desc) = split(m/\t/, $_, -1);
        my $sql = "INSERT INTO enzymecode (code_id, desc) VALUES (\"$code\", \"$desc\")";
        $self->batchInsert($sql);
    }

    close $fh;
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


# CONVERGENCE RATIO METADATA #######################################################################

sub convRatioToSqlite2 {
    my $self = shift;
    my $mainDataDir = shift;
    my $isDiced = shift || 0;

    my $dicedPrefix = $isDiced ? "diced_" : "";
    print "IF YOU WANT TO NOT APPEND TO conv_ratio THEN YOU NEED TO DELETE THE TABLE FIRST.\n";
    my $tableName = "${dicedPrefix}conv_ratio";
    if (not $self->tableExists($tableName)) {
        $self->createTable($tableName);
    }

    my $processFn = sub {
        my $dataDirs = shift;
        my $ascore = shift || "";
        my $ascoreCol = $ascore ? "ascore," : "";
        my $ascorePH = $ascore ? "?," : "";
        foreach my $dir (@$dataDirs) {
            (my $clusterId = $dir) =~ s%^.*(cluster[\-\da-z]*[\-\d]+).*?$%$1%;
            my $crFile = "$dir/conv_ratio.txt";
            next if not -f $crFile;

            open my $cfh, "<", $crFile;
            my $header = <$cfh>;
            my $dataLine = <$cfh>;
            close $cfh;

            my ($cnum, @data) = split(m/\t/, $dataLine);
            my $sql = "INSERT INTO $tableName (cluster_id, $ascoreCol conv_ratio, num_ids, num_blast, node_conv_ratio, num_nodes, num_edges) VALUES (?, $ascorePH ?, ?, ?, ?, ?, ?)";
            my $sth = $self->{dbh}->prepare($sql);
            my @values = ($clusterId);
            push @values, $ascore if $ascore;
            push @values, @data;
            $self->batchExec($sth, @values);
        }
    };

    if ($isDiced) {
        my @dirs = glob("$mainDataDir/cluster-*");
        foreach my $dir (@dirs) {
            foreach my $ascoreDir (glob("$dir/dicing-*")) {
                (my $ascore = $ascoreDir) =~ s/^.*dicing-(\d+)\/?$/$1/;
                my @asCl = glob("$ascoreDir/cluster-*");
                &$processFn(\@asCl, $ascore);
            }
        }
    } else {
        my @dirs = glob("$mainDataDir/cluster-*");
        &$processFn(\@dirs);
    }

    $self->finish();
}


sub convRatioToSqlite {
    my $self = shift;
    my $mainDataDir = shift;
    my $loadScript = shift;
    my $isDiced = shift || 0;

    my $loadSqlFile = $loadScript . "_load.sql";

    open my $outFh, ">", $loadScript or die "Unable to write to conv ratio script file $loadScript: $!";

    my $masterLoadFile = "${loadScript}_load_conv_ratio.txt";

    print $outFh <<SCRIPT;
echo '.separator "\\t"' > $loadSqlFile

SCRIPT

    unlink $masterLoadFile;

    my $dicedPrefix = $isDiced ? "diced_" : "";
    print "IF YOU WANT TO NOT APPEND TO conv_ratio THEN YOU NEED TO DELETE THE TABLE FIRST.\n";
    my $tableName = "${dicedPrefix}conv_ratio";
    if (not $self->tableExists($tableName)) {
        $self->createTable($tableName);
    }

    my $compOp = $isDiced ? ">1" : "==2";
    my $processFn = sub {
        my $dataDirs = shift;
        my $ascore = shift || "";
        my $ascoreCol = $ascore ? "\t$ascore" : "";
        foreach my $dir (@$dataDirs) {
            (my $clusterId = $dir) =~ s%^.*(cluster[\-\da-z]*[\-\d]+).*?$%$1%;
            my $crFile = "$dir/conv_ratio.txt";
            print $outFh <<SCRIPT;
sed 's/^\\([0-9]\\+\\)/$clusterId$ascoreCol/' $crFile | awk 'NR$compOp {print \$0;}' >> $masterLoadFile
SCRIPT
        }
    };

    if ($isDiced) {
        my @dirs = glob("$mainDataDir/cluster-*");
        foreach my $dir (@dirs) {
            foreach my $ascoreDir (glob("$dir/dicing-*")) {
                (my $ascore = $ascoreDir) =~ s/^.*dicing-(\d+)\/?$/$1/;
                my @asCl = glob("$ascoreDir/cluster-*");
                &$processFn(\@asCl, $ascore);
            }
        }
    } else {
        my $sortFn = sub {
            (my $aa = $a) =~ s%^.*/(cluster-[^/]+)$%$1%;
            (my $bb = $b) =~ s%^.*/(cluster-[^/]+)$%$1%;
            my @a = split("-", $aa);
            my @b = split("-", $bb);
            my $c = scalar(@b) <=> scalar(@a);
            return $c if $c;
            for (my $i = 0; $i <= $#a; $i++) {
                my $c = $a[$i] cmp $b[$i];
                return $c if $c;
            }
            return 0;
        };

        my @dirs = glob("$mainDataDir/cluster-*");
        foreach my $dir (@dirs) {
            &$processFn([$dir]);
        }
    }

    print $outFh <<SCRIPT;

echo '.import "${masterLoadFile}" ${dicedPrefix}conv_ratio' >> $loadSqlFile

echo 'sqlite3 $self->{sqlite} < $loadSqlFile'

SCRIPT

    close $outFh;
}


# CONSENSUS RESIDUE METADATA #######################################################################

sub consResToSqlite2 {
    my $self = shift;
    my $mainDataDir = shift;
    my $isDiced = shift || 0;

    my $dicedPrefix = $isDiced ? "diced_" : "";
    my $tableName = "${dicedPrefix}cons_res";
    $self->createTable($tableName);

    my $processFn = sub {
        my $dataDir = shift;
        my $output = shift;
        my $ascore = shift || "";
        (my $clusterId = $dataDir) =~ s%^.*(cluster[\-\da-z][\-\d]+)$%$1%;

        # Find all of the available consensus residue results
        my %files = map { my $a = $_; $a =~ s%^.*residue_([A-Z])_.*$%$1%i; $_ => $a } grep { m/consensus_?residue_([A-Z])_position/i } glob("$dataDir/*.txt");

        foreach my $crFile (keys %files) {
            my $res = $files{$crFile};
            open my $fh, "<", $crFile or warn "Unable to read CR flie $crFile: $!" and return 0;
            my $header = <$fh>;
            while (my $line = <$fh>) {
                chomp($line);
                my ($a, $pct, $num) = split(m/\t/, $line);
                my $row = {id => $clusterId, cr => $res, pct => $pct, num => $num};
                $row->{as} = $ascore if $ascore;
                push @$output, $row;
            }
            close $fh;
        }
    };

    my $outputData = [];

    if ($isDiced) {
        my @dirs = glob("$mainDataDir/cluster-*");
        foreach my $dir (@dirs) {
            foreach my $ascoreDir (glob("$dir/dicing-*")) {
                (my $ascore = $ascoreDir) =~ s/^.*dicing-(\d+)\/?$/$1/;
                foreach my $cDir (glob("$ascoreDir/cluster-*")) {
                    &$processFn($cDir, $outputData, $ascore);
                }
            }
        }
    } else {
        my @dirs = glob("$mainDataDir/cluster-*");
        foreach my $dir (@dirs) {
            &$processFn($dir, $outputData);
        }
    }

    my $ascoreCol = $isDiced ? "ascore," : "";
    my $ascorePH = $isDiced ? "?," : "";
    foreach my $row (@$outputData) {
        my $sql = "INSERT INTO $tableName (cluster_id, $ascoreCol residue, percent, num_res) VALUES (?, $ascorePH ?, ?, ?)";
        my $sth = $self->{dbh}->prepare($sql);

        my @row = ($row->{id});
        push @row, $row->{as} if $isDiced;
        push @row, $row->{cr};
        push @row, $row->{pct};
        push @row, $row->{num};

        $self->batchExec($sth, @row);
    }

    $self->finish();
}

sub consResToSqlite {
    my $self = shift;
    my $mainDataDir = shift;
    my $loadScript = shift;
    my $isDiced = shift || 0;

    my $loadSqlFile = $loadScript . "_load.sql";

    open my $outFh, ">", $loadScript or die "Unable to write to conv ratio script file $loadScript: $!";

    print $outFh <<SCRIPT;
echo '.separator "\\t"' > $loadSqlFile

SCRIPT

    my $dicedPrefix = $isDiced ? "diced_" : "";
    $self->createTable("${dicedPrefix}cons_res");

    my $masterCrLoadFile = "${loadSqlFile}_master_cons_res.txt";
    unlink $masterCrLoadFile;

    my $processFn = sub {
        my $dataDir = shift;
        my $ascore = shift || "";
        my $ascoreDir = shift || "";
        my $ascoreCol = $ascore ? "$ascore\t" : "";
        (my $clusterId = $dataDir) =~ s%^.*(cluster[\-\da-z][\-\d]+)$%$1%;

        # Find all of the available consensus residue results
        my @files = glob("$dataDir/consensus_residue_*.txt");
        my @res = map { s%^.*consensus_residue_([A-Z])_.*$%$1%i; $_ } @files;

        foreach my $res (@res) {
            my $crFile = "$dataDir/consensus_residue_${res}_position.txt";
            open my $fh, "<", $crFile or warn "Unable to read CR flie $crFile: $!" and return 0;
            while (my $line = <$fh>) {
            print $outFh <<SCRIPT;
awk 'NR>1 {print "$clusterId\t$ascoreCol$res\t"\$2"\t"\$3;}' $crFile >> $masterCrLoadFile
SCRIPT
            }
            close $fh;
        }
    };

    if ($isDiced) {
        my @dirs = glob("$mainDataDir/cluster-*");
        foreach my $dir (@dirs) {
            foreach my $ascoreDir (glob("$dir/dicing-*")) {
                (my $ascore = $ascoreDir) =~ s/^.*dicing-(\d+)\/?$/$1/;
                foreach my $cDir (glob("$ascoreDir/cluster-*")) {
                    &$processFn($cDir, $ascore, $ascoreDir);
                }
            }
        }
    } else {
        my @dirs = glob("$mainDataDir/cluster-*");
        foreach my $dir (@dirs) {
            &$processFn($dir);
        }
    }

    print $outFh <<SCRIPT;

echo 'SELECT "LOADING $masterCrLoadFile";' >> $loadSqlFile
echo '.import "$masterCrLoadFile" ${dicedPrefix}cons_res' >> $loadSqlFile

echo 'sqlite3 $self->{sqlite} < $loadSqlFile'

SCRIPT

    close $outFh;
}


# NETWORK SIZE METADATA ############################################################################

sub networkSizeToSqlite {
    my $self = shift;

    $self->createTable("diced_size");
    $self->createTable("size");

    my ($dicedSizes, $parentSizeData) = $self->computeDicedClusterSizes();
    my $sizes = $self->computeClusterSizes();

    foreach my $dicedId (keys %$parentSizeData) {
        foreach my $sizeKey (keys %{ $parentSizeData->{$dicedId} }) {
            if ($parentSizeData->{$dicedId}->{$sizeKey}) {
                $sizes->{$dicedId}->{$sizeKey} = $parentSizeData->{$dicedId}->{$sizeKey};
            }
        }
    }

    $self->insertSizeDataDiced($dicedSizes);
    $self->insertSizeData($sizes);
}


sub computeDicedClusterSizes {
    my $self = shift;

    my $idSql = "SELECT cluster_id, ascore, parent_id, parent_ascore FROM diced_network";
    my %ascore;
    my $sth = $self->{dbh}->prepare($idSql);
    warn "Unable to prepare $idSql; ignoring" and return if not $sth;

    my %primaryAscore;
    $sth->execute;
    while (my $row = $sth->fetchrow_hashref) {
        my $pid = $row->{parent_id};
        my $asid = "$pid-$row->{ascore}";
        $ascore{$asid}->{parent} = $pid;
        $ascore{$asid}->{ascore} = $row->{ascore};
        push @{ $ascore{$asid}->{clusters} }, $row->{cluster_id};
        $primaryAscore{$pid} = $row->{parent_ascore} if not exists $primaryAscore{$pid};
    }

    my $tableBase = "diced_id_mapping";
    my $data = {};
    my $parentSizeData = {}; # What is the size of Mega-#-# when it's not diced?
    foreach my $type ("uniprot", "uniref50", "uniref90") {
        my $table = $type eq "uniprot" ? $tableBase : "${tableBase}_$type";
        #foreach my $ascoreKey (keys %ascore) {
        foreach my $asid (keys %ascore) {
            my $parent = $ascore{$asid}->{parent};
            my $ascore = $ascore{$asid}->{ascore};
            #(my $parent = $ascoreKey) =~ s/\-\d+$//;
            #(my $ascore = $ascoreKey) =~ s/^.*-(\d+)$/$1/;
            my $total = 0;
            #foreach my $cluster (@{$ascore{$ascoreKey}}) {
            foreach my $cluster (@{ $ascore{$asid}->{clusters} }) {
                my $col = "${type}_id";
                my $sql = "SELECT $col FROM $table WHERE cluster_id = '$cluster' AND ascore = '$ascore'";
                my $sth = $self->{dbh}->prepare($sql);
                $sth->execute;
                while (my $row = $sth->fetchrow_hashref) {
                    $data->{$cluster}->{$ascore}->{$type}++;
                    $total++;
                }
            }
            if ($primaryAscore{$parent} and $primaryAscore{$parent} == $ascore) {
                $parentSizeData->{$parent}->{$type} = $total;
            }
        }
    }

    return ($data, $parentSizeData);
}


sub evoTreeToSqlite {
    my $self = shift;

    my $getParent = sub {
        my @p = split(m/-/, $_[0]);
        my $parent = join("-", @p[0..($#p-1)]);
        return $parent;
    };

    my $ids = {};
    my $tree = {};

    my $clusterSql = "SELECT * FROM diced_id_mapping";
    my $sth = $self->{dbh}->prepare($clusterSql);
    $sth->execute;
    while (my $row = $sth->fetchrow_hashref) {
        my $parent = &$getParent($row->{cluster_id});
        $ids->{$row->{uniprot_id}}->{$parent}->{$row->{ascore}}->{$row->{cluster_id}} = 1;
        $tree->{$row->{cluster_id}}->{$row->{ascore}}->{$row->{uniprot_id}} = 1;
    }

    my $evo;

    foreach my $cluster (keys %$tree) {
        my $parent = &$getParent($cluster);
        my @as = sort keys %{$tree->{$cluster}};
        for (my $ai = 0; $ai < $#as; $ai++) {
            my $ascore = $as[$ai];
            my $nextAS = $as[$ai+1];
            foreach my $id (keys %{$tree->{$cluster}->{$ascore}}) {
                next if not $ids->{$id}->{$parent}->{$nextAS};
                my @c = keys %{$ids->{$id}->{$parent}->{$nextAS}};
                $evo->{$cluster}->{$ascore}->{$c[0]} = 1;
            }
        }
    }

    foreach my $cluster (sort keys %$evo) {
        foreach my $ascore (sort keys %{$evo->{$cluster}}) {
            foreach my $desc_cluster (sort keys %{$evo->{$cluster}->{$ascore}}) {
                print join("\t", $cluster, $ascore, $desc_cluster), "\n";
            }
        }
    }
}


sub computeClusterSizes {
    my $self = shift;
    my $computeParent = shift || 0;

    my $idSql = "SELECT DISTINCT cluster_id FROM id_mapping";
    my @clusters;
    my $sth = $self->{dbh}->prepare($idSql);
    $sth->execute;
    while (my $row = $sth->fetchrow_hashref) {
        push @clusters, $row->{cluster_id};
    }

    my $tableBase = "id_mapping";
    my $data = {};
    foreach my $type ("uniprot", "uniref50", "uniref90") {
        my $table = $type eq "uniprot" ? $tableBase : "${tableBase}_$type";
        foreach my $cluster (@clusters) {
            my $col = "${type}_id";
            my $sql = "SELECT $col FROM $table WHERE cluster_id = '$cluster'";
            my $sth = $self->{dbh}->prepare($sql);
            $sth->execute;
            while (my $row = $sth->fetchrow_hashref) {
                $data->{$cluster}->{$type}++;
            }
        }
    }

    my %needComp;
    foreach my $cluster (sort keys %$data) {
        my @p = split(m/-/, $cluster);
        next if scalar @p < 3;
        my $parent = join("-", @p[0..($#p-1)]);
        push @{$needComp{$parent}}, $cluster if not $data->{$parent};
    }

    my $sortFn = sub {
        my @a = split("-", $a);
        my @b = split("-", $b);
        my $c = scalar(@b) <=> scalar(@a);
        return $c if $c;
        for (my $i = 0; $i <= $#a; $i++) {
            my $c = $a[$i] cmp $b[$i];
            return $c if $c;
        }
        return 0;
    };

    foreach my $parent (sort $sortFn keys %needComp) {
        foreach my $cluster (@{$needComp{$parent}}) {
            foreach my $type (keys %{$data->{$cluster}}) {
                $data->{$parent}->{$type} += $data->{$cluster}->{$type};
            }
        }
    }

    return $data;
}


sub insertSizeDataDiced {
    my $self = shift;
    my $data = shift;

    foreach my $clusterId (sort keys %$data) {
        foreach my $ascore (sort keys %{$data->{$clusterId}}) {
            my $valStr = "\"$clusterId\", ";
            $valStr .= getIntVal($ascore) . ", ";
            $valStr .= getIntVal($data->{$clusterId}->{$ascore}->{uniprot}) . ", ";
            $valStr .= getIntVal($data->{$clusterId}->{$ascore}->{uniref90}) . ", ";
            $valStr .= getIntVal($data->{$clusterId}->{$ascore}->{uniref50});
            my $sql = "INSERT INTO diced_size (cluster_id, ascore, uniprot, uniref90, uniref50) VALUES ($valStr)";
            $self->batchInsert($sql);
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
        $self->batchInsert($sql);
    }
}


# NETWORK INFO METADATA ############################################################################

sub networkUiJsonToSqlite {
    my $self = shift;
    my $json = shift;

    $self->createTable("network");
    $self->createTable("region");

    $self->insertNetworkData($json->{networks});
}


sub insertNetworkData {
    my $self = shift;
    my $data = shift;

    my $sql = "INSERT INTO network (cluster_id, title, name, desc, parent_id, subgroup_id) VALUES (?, ?, ?, ?, ?, ?)";
    my $sth = $self->{dbh}->prepare($sql);

    foreach my $clusterId (sort keys %$data) {
        my $net = $data->{$clusterId};
        $self->batchExec($sth, $clusterId, $net->{title}, $net->{name}, $net->{desc}, $net->{parent}, $net->{subgroup_id});

        if ($net->{regions}) {
            $self->insertRegionData($clusterId, $net->{regions});
        }
        #if ($net->{tigr_families}) {
        #    $self->insertTigrDataForCluster($clusterId, $net->{tigr_families});
        #}
    }
}


sub netinfoToSqlite {
    my $self = shift;
    my $file = shift;

    $self->createTable("network");
    my %data;

    open my $fh, "<", $file or die "Unable to read netinfo file $file: $!";
    while (my $line = <$fh>) {
        chomp $line;
        my ($clusterId, $name, $title, $desc, $parentId, $subgroupId) = split(m/\t/, $line, -1);
        $data{$clusterId} = {title => $title, name => $name, desc => $desc, subgroup_id => $subgroupId // "", parent => $parentId // ""};
    }
    close $fh;

    $self->insertNetworkData(\%data);

    $self->finish;
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
        my $sql = "INSERT INTO region (cluster_id, region_id, name, region_index, number, coords) VALUES ($valStr)";
        $self->batchInsert($sql);
        $idx++;
    }
}


sub regionToSqlite {
    my $self = shift;
    my $file = shift;

    $self->createTable("region");

    my %data;

    open my $fh, "<", $file or die "Unable to read region file $file: $!";
    while (<$fh>) {
        chomp;
        my ($clusterId, $regionId, $name, $num, @coords) = split(m/\t/, $_, -1);
        #my $num = $data{$clusterId} ? scalar @{$data{$clusterId}}+1 : 1;
        #(my $name = $regionId) =~ s/^cluster-//;
        #$name = "Mega-$name" if $name =~ m/\d\-\d/;
        push @{$data{$clusterId}}, {id => $regionId, name => $name, number => $num, coords => \@coords};
    }
    close $fh;

    foreach my $cluster (keys %data) {
        $self->insertRegionData($cluster, $data{$cluster});
    }

    $self->finish;
}


# DICING ###########################################################################################

sub dicingToSqlite {
    my $self = shift;
    my $file = shift;

    $self->createTable("diced_network");

    open my $fh, "<", $file or die "Unable to read dicing file $file: $!";
    while (<$fh>) {
        chomp;
        next if m/^\s#/;
        next if m/^\s*$/;
        my ($parentId, $parentAscore, $ascore, $clusterId) = split(m/\t/, $_, -1);
        my $valStr = join(", ", getStrVal($clusterId), getStrVal($ascore), getStrVal($parentId), getStrVal($parentAscore));
        my $sql = "INSERT INTO diced_network (cluster_id, ascore, parent_id, parent_ascore) VALUES ($valStr)";
        $self->batchInsert($sql);
    }
    close $fh;
}


#sub ssnListToSqlite {
#    my $self = shift;
#    my $file = shift;
#
#    $self->createTable("diced_ssn");
#
#    open my $fh, "<", $file or die "Unable to read ssn file $file: $!";
#    while (<$fh>) {
#        chomp;
#        next if m/^\s#/;
#        next if m/^\s*$/;
#        my ($clusterId, $as) = split(m/\t/, $_, -1);
#        next if not $as;
#        my $valStr = join(", ", getStrVal($clusterId), getStrVal($as));
#        my $sql = "INSERT INTO diced_ssn (cluster_id, ascore) VALUES ($valStr)";
#        $self->batchInsert($sql);
#    }
#}


# UNIREF MAPPING ###################################################################################


sub unirefMappingToSqlite {
    my $self = shift;
    my $file = shift;

    $self->createTable("uniref_map");

    my $sql = "INSERT INTO uniref_map (uniprot_id, uniref90_id, uniref50_id) VALUES (?, ?, ?)";
    my $sth = $self->{dbh}->prepare($sql);

    open my $fh, "<", $file or die "Unable to read uniref-map file $file: $!";
    while (<$fh>) {
        chomp;
        my ($up, $ur90, $ur50) = split(m/\t/);
        if ($up and $ur90 and $ur50) {
            $self->batchExec($sth, $up, $ur90, $ur50);
        }
    }

    close $fh;
}


# FAMILY ###########################################################################################


sub insertTigrClusterDataRow {
    my $self = shift;
    my $clusterId = shift;
    my $uniProtId = shift;
    my $ascore = shift // 0;

    my @vals = (getStrVal($clusterId), getStrVal($uniProtId), getStrVal("TIGR"));
    push @vals, $ascore if $ascore;
    my $valStr = join(", ", @vals);

    my @cols = ("cluster_id", "family", "family_type");
    push @cols, "ascore" if $ascore;
    my $colStr = join(",", @cols);

    my $tableName = $ascore ? "families_diced" : "families";
    my $sql = "INSERT INTO $tableName ($colStr) VALUES ($valStr)";
    $self->batchInsert($sql);
}


sub tigrInfoToSqlite {
    my $self = shift;
    my $tigrIdsFile = shift;
    my $tigrInfoFile = shift;

    $self->createTable("tigr");

    if ($tigrIdsFile and -f $tigrIdsFile) {
        my $sql = "INSERT INTO tigr (uniprot_id, tigr) VALUES (?, ?)";
        my $idSth = $self->{dbh}->prepare($sql);

        open my $fh, "<", $tigrIdsFile or die "Unable to read tigr IDs $tigrIdsFile: $!";
        while (my $line = <$fh>) {
            chomp($line);
            $line =~ s/^\s*(.*?)\s*$/$1/;
            next if not $line;
            my ($id, $tigr) = split(m/\t/, $line);
            $self->batchExec($idSth, $id, $tigr);
        }
        close $fh;
    }

    $self->createTable("family_info");
    if ($tigrInfoFile and -f $tigrInfoFile) {
        open my $fh, "<", $tigrInfoFile or die "Unable to read tigr info $tigrInfoFile file: $!";
        my $sql = "INSERT OR IGNORE INTO family_info (family, description) VALUES (?, ?)";
        my $sth = $self->{dbh}->prepare($sql);
        while (my $line = <$fh>) {
            chomp($line);
            my ($fam, $desc) = split(m/\t/, $line);
            $self->batchExec($sth, $fam, $desc);
        }
        close $fh;
    }
    $self->finish;
}


# RETRIEVAL FUNCTIONS ##############################################################################


sub getTabularData {
    my $self = shift;
    my $table = shift;
    my $col = shift;

    my $cols = join(", ", @$col);

    my $sql = "SELECT $cols FROM $table";
    my $sth = $self->{dbh}->prepare($sql);
    $sth->execute();

    my @data;
    while (my $row = $sth->fetchrow_arrayref()) {
        push @data, [@$row];
    }

    $self->finish();

    return \@data;
}


# SQL UTILITY FUNCTIONS ############################################################################

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
        "network" => ["cluster_id TEXT", "title TEXT", "name TEXT", "desc TEXT", "parent_id TEXT", "subgroup_id TEXT"],
        "region" => ["cluster_id TEXT", "region_id TEXT", "region_index INT", "name TEXT", "number TEXT", "coords TEXT"],
        "id_mapping" => ["cluster_id TEXT", "uniprot_id TEXT"],
        "id_mapping_uniref50" => ["cluster_id TEXT", "uniref50_id TEXT"],
        "id_mapping_uniref90" => ["cluster_id TEXT", "uniref90_id TEXT"],
        "enzymecode" => ["code_id TEXT", "desc TEXT"],
        "swissprot" => ["uniprot_id TEXT", "function TEXT", "UNIQUE(uniprot_id, function)"],
        "kegg" => ["uniprot_id TEXT", "kegg TEXT"],
        "pdb" => ["uniprot_id TEXT", "pdb TEXT"],
        "tigr" => ["uniprot_id TEXT", "tigr TEXT"],
        "taxonomy" => ["uniprot_id TEXT", "tax_id TEXT", "domain TEXT", "kingdom TEXT", "phylum TEXT", "class TEXT", "taxorder TEXT", "family TEXT", "genus TEXT", "species TEXT"],
        "subgroup_desc" => ["subgroup_id TEXT", "subgroup_desc TEXT", "subgroup_color TEXT"],
#        "subgroup_map" => ["cluster_id TEXT", "subgroup_id TEXT"],
        "families" => ["cluster_id TEXT", "family TEXT", "family_type TEXT"],
#        "families_diced" => ["cluster_id TEXT", "ascore INT", "family TEXT", "family_type TEXT"],
        "family_info" => ["family TEXT PRIMARY KEY", "description TEXT"],
        "uniref_map" => ["uniprot_id TEXT", "uniref90_id TEXT", "uniref50_id TEXT"],
        "conv_ratio" => ["cluster_id TEXT", "conv_ratio REAL", "num_ids INT", "num_blast INT", "node_conv_ratio REAL", "num_nodes INT", "num_edges INT"],
        "cons_res" => ["cluster_id TEXT", "residue CHAR(1)", "percent INT", "num_res INT"],
        "annotations" => ["uniprot_id TEXT", "doi TEXT"],
        "alphafolds" => ["uniprot_id TEXT", "alphafold_id TEXT"],

        "diced_size" => ["cluster_id TEXT", "ascore INT", "uniprot INT DEFAULT 0", "uniref90 INT DEFAULT 0", "uniref50 INT DEFAULT 0"],
        "diced_network" => ["cluster_id TEXT", "ascore INT", "parent_id TEXT", "parent_ascore INT"],
        "diced_id_mapping" => ["cluster_id TEXT", "ascore INT", "uniprot_id TEXT"],
        "diced_id_mapping_uniref50" => ["cluster_id TEXT", "ascore INT", "uniref50_id TEXT"],
        "diced_id_mapping_uniref90" => ["cluster_id TEXT", "ascore INT", "uniref90_id TEXT"],
#        "diced_ssn" => ["cluster_id TEXT", "ascore TEXT"],
        "diced_conv_ratio" => ["cluster_id TEXT", "ascore INT", "conv_ratio REAL", "num_ids INT", "num_blast INT", "node_conv_ratio REAL", "num_nodes INT", "num_edges INT"],
        "diced_cons_res" => ["cluster_id TEXT", "ascore INT", "residue CHAR(1)", "percent INT", "num_res INT"],
        "diced_cluster_index_next" => ["cluster_id TEXT", "ascore INT", "cluster_id2 TEXT", "ascore2 INT"],
        "diced_cluster_index_prev" => ["cluster_id TEXT", "ascore INT", "cluster_id2 TEXT", "ascore2 INT"],
    };

    my $indexes = {
        "size" => [{name => "size_cluster_id_idx", cols => "cluster_id"}],
        "network" => [{name => "network_cluster_id_idx", cols => "cluster_id"}, {name => "network_subgroup_id", cols => "subgroup_id"}, {name => "network_parent_id", cols => "parent_id"}],
        "id_mapping" => [{name => "id_mapping_uniprot_id_idx", cols => "uniprot_id"}, {name => "id_mapping_cluster_id_idx", cols => "cluster_id"}],
        "id_mapping_uniref50" => [{name => "id_mapping_uniref50_id_idx", cols => "uniref50_id"}, {name => "id_mapping_uniref50_cluster_id_idx", cols => "cluster_id"}],
        "id_mapping_uniref90" => [{name => "id_mapping_uniref90_id_idx", cols => "uniref90_id"}, {name => "id_mapping_uniref90_cluster_id_idx", cols => "cluster_id"}],
        "swissprot" => [{name => "swissprot_uniprot_id_idx", cols => "uniprot_id"}],
        "kegg" => [{name => "kegg_uniprot_id_idx", cols => "uniprot_id"}],
        "tigr" => [{name => "tigr_uniprot_id_idx", cols => "uniprot_id"}, {name => "tigr_tigr_idx", cols => "tigr"}],
        "pdb" => [{name => "pdb_uniprot_id_idx", cols => "uniprot_id"}],
        "taxonomy" => [{name => "taxonomy_tax_uniprot_idx", cols => "uniprot_id"}, {name => "taxonomy_tax_id_idx", cols => "tax_id"}, {name => "taxonomy_domain_idx", cols => "domain"}, {name => "taxonomy_kingdom_idx", cols => "kingdom"}, {name => "taxonomy_phylum_idx", cols => "phylum"}, {name => "taxonomy_class_idx", cols => "class"}, {name => "taxonomy_taxorder_idx", cols => "taxorder"}, {name => "taxonomy_family_idx", cols => "family"}, {name => "taxonomy_genus_idx", cols => "genus"}, {name => "taxonomy_species_idx", cols => "species"}, {name => "taxonomy_uniprot_id_idx", cols => "uniprot_id"}],
        "families" => [{name => "families_cluster_id_idx", cols => "cluster_id"}],
#        "families_diced" => [{name => "families_cluster_id_idx", cols => "cluster_id, ascore"}],
        "family_info" => [{name => "family_info_name_idx", cols => "family"}],
        "uniref_map" => [{name => "uniref_map_up_idx", cols => "uniprot_id"}, {name => "uniref_map_ur90_idx", cols => "uniref90_id"}, {name => "uniref_map_ur50_idx", cols => "uniref50_id"}],
        "conv_ratio" => [{name => "conv_ratio_cluster_id", cols => "cluster_id"}],
        "cons_res" => [{name => "cons_res_cluster_id", cols => "cluster_id, num_res"}],
        "annotations" => [{name => "annotations_uniprot_id_idx", cols => "uniprot_id"}],
        "alphafolds" => [{name => "alphafolds_uniprot_id_idx", cols => "uniprot_id"}, {name => "alphafolds_alphafold_id_idx", cols => "alphafold_id"}],

        "diced_size" => [{name => "diced_size_cluster_id_idx", cols => "cluster_id, ascore"}],
        "diced_network" => [{name => "diced_cluster_id", cols => "cluster_id"}, {name => "diced_parent_id", cols => "parent_id"}, {name => "diced_ascore", cols => "ascore"}],
        "diced_id_mapping" => [{name => "diced_id_mapping_uniprot_id_idx", cols => "uniprot_id"}, {name => "id_mapping_diced_cluster_id_idx", cols => "cluster_id, ascore"}, {name => "id_mapping_diced_uniprot_ascore_idx", cols => "uniprot_id, ascore"}],
        "diced_id_mapping_uniref50" => [{name => "id_mapping_diced_uniref50_id_idx", cols => "uniref50_id"}, {name => "diced_id_mapping_uniref50_cluster_id_idx", cols => "cluster_id, ascore"}],
        "diced_id_mapping_uniref90" => [{name => "id_mapping_diced_uniref90_id_idx", cols => "uniref90_id"}, {name => "diced_id_mapping_uniref90_cluster_id_idx", cols => "cluster_id, ascore"}],
#        "diced_ssn" => [{name => "diced_ssn_cluster_id", cols => "cluster_id"}],
        "diced_conv_ratio" => [{name => "diced_conv_ratio_cluster_id", cols => "cluster_id, ascore"}],
        "diced_cons_res" => [{name => "diced_cons_res_cluster_id", cols => "cluster_id, ascore, num_res"}],
        "diced_cluster_index_next" => [{name => "diced_cluster_index_next_idx", cols => "cluster_id, ascore"}],
        "diced_cluster_index_prev" => [{name => "diced_cluster_index_prev_idx", cols => "cluster_id, ascore"}],
    };

    die "Unable to find table schema for $tableId" if not $schemas->{$tableId};

    my $colStr = join(", ", @{$schemas->{$tableId}});

    if (not $self->{append_to_db}) {
        $self->{dbh}->do("DROP TABLE IF EXISTS $tableId");
        my $sql = "CREATE TABLE $tableId ($colStr)";
        $self->{dbh}->do($sql);

        if ($indexes->{$tableId}) {
            foreach my $idxInfo (@{$indexes->{$tableId}}) {
                my $idxSql = "CREATE INDEX $idxInfo->{name} ON $tableId ($idxInfo->{cols})";
                $self->{dbh}->do($idxSql);
            }
        }
    }
}
sub tableExists {
    my $self = shift;
    my $tableName = shift;

    my $sql = "SELECT name FROM sqlite_master WHERE type='table' AND name='$tableName'";
    my $sth = $self->{dbh}->prepare($sql);
    $sth->execute;
    my $row = $sth->fetchrow_hashref;
    return $row ? 1 : 0;
}


sub getHandle {
    my $file = shift;
    my $dryRun = shift;
    my $dbh;
    if ($dryRun) {
        $dbh = DryRunDb->new();
    } else {
        $dbh = DBI->connect("DBI:SQLite:dbname=$file","","");
        $dbh->{AutoCommit} = 0;
    }
    return $dbh;
}


sub batchInsert {
    my $self = shift;
    my $sql = shift;

    if (++$self->{insert_count} % 100000 == 0) {
        $self->{insert_count} = 0;
        $self->{dbh}->commit;
    }
    $self->{dbh}->do($sql);
}


sub batchExec {
    my $self = shift;
    my $sth = shift;

    if (++$self->{exec_count} % 100000 == 0) {
        $self->{exec_count} = 0;
        $self->{dbh}->commit;
    }
    $sth->execute(@_);
}


sub finish {
    my $self = shift;
    $self->{dbh}->commit;
}





package DryRunDb;


sub new {
    my $class = shift;
    my $self = {};
    bless $self, $class;
    return $self;
}


sub do {
    my $self = shift;
    my @args = @_;
    print join("\n", @args), "\n";
}


sub prepare {
    my $self = shift;
    $self->do(@_);
    return new DryRunDb(); # For execute and fetchrow_hashref
}


sub execute {
    my $self = shift;
}


sub fetchrow_hashref {
    my $self = shift;
    return {};
}


1;

