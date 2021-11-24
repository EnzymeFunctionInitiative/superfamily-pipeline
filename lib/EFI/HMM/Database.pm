
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
        "load-sfld-map-file=s",
        "load-sfld-desc-file=s",
        "load-id-list-script=s",
        "load-diced",
        "load-tigr-file=s",
        "load-region-file=s",
        "load-netinfo-file=s",
        "load-dicing-file=s",
#        "load-ssn-file=s",
        "load-uniref-file=s",
        "load-conv-ratio-script=s",
        "load-cons-res-script=s",
        "load-anno-file=s",
        "job-id-file=s",
        "append-to-db", # don't recreate the table if it already exists
        "dryrun|dry-run",
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
    $parms{load_sfld_map_file} = $opts{"load-sfld-map-file"} // "";
    $parms{load_sfld_desc_file} = $opts{"load-sfld-desc-file"} // "";
    $parms{load_id_list_script} = $opts{"load-id-list-script"} // "";
    $parms{load_diced} = defined $opts{"load-diced"} ? 1 : 0;
    $parms{load_tigr_file} = $opts{"load-tigr-file"} // "";
    $parms{load_region_file} = $opts{"load-region-file"} // "";
    $parms{load_netinfo_file} = $opts{"load-netinfo-file"} // "";
    $parms{load_dicing_file} = $opts{"load-dicing-file"} // "";
#    $parms{load_ssn_file} = $opts{"load-ssn-file"} // "";
    $parms{load_uniref_file} = $opts{"load-uniref-file"} // "";
    $parms{load_conv_ratio_script} = $opts{"load-conv-ratio-script"} // "";
    $parms{load_cons_res_script} = $opts{"load-cons-res-script"} // "";
    $parms{load_anno_file} = $opts{"load-anno-file"} // "";
    $parms{job_id_file} = $opts{"job-id-file"} // "";
    $parms{append_to_db} = $opts{"append-to-db"} // "";
    $parms{dryrun} = $opts{"dryrun"} // "";

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


# SWISSPROT METADATA ###############################################################################

sub swissprotsToSqlite {
    my $self = shift;
    my $spFile = shift;

    $self->createTable("swissprot");

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
    @src = ("uniprot", "uniref50", "uniref90");

    my $masterLoadFileName = "${loadScript}_load_ids";
    unlink "${masterLoadFileName}_uniprot.txt";
    unlink "${masterLoadFileName}_uniref50.txt";
    unlink "${masterLoadFileName}_uniref90.txt";

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
    }

    print $outFh <<SCRIPT;

echo '.import "${masterLoadFileName}_uniprot.txt" ${dicedPrefix}id_mapping' >> $loadSqlFile
echo '.import "${masterLoadFileName}_uniref50.txt" ${dicedPrefix}id_mapping_uniref50' >> $loadSqlFile
echo '.import "${masterLoadFileName}_uniref90.txt" ${dicedPrefix}id_mapping_uniref90' >> $loadSqlFile

echo "WRAP UP BY EXECUTING sqlite3 $self->{sqlite} < $loadSqlFile"

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

    my $table = "pdb";
    $self->createTable($table);

    open my $fh, "<", $pdbFile;

    while (<$fh>) {
        chomp;
        my ($upId, $pdbVals) = split(m/\t/);
        my $valStr = join(", ", map { "\"$_\"" } ($upId, $pdbVals));
        my $sql = "INSERT INTO $table (uniprot_id, pdb) VALUES ($valStr)";
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

    open my $fh, "<", $taxonomyFile;

    while (<$fh>) {
        chomp;
        #my ($clusterId, $upId, @taxonomyVals) = split(m/\t/);
        my $valStr = join(", ", map { "\"$_\"" } split(m/\t/, $_, -1));
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

sub sfldMapToSqlite {
    my $self = shift;
    my $sfldFile = shift;

    $self->createTable("sfld_map");

    open my $fh, "<", $sfldFile;

    while (<$fh>) {
        chomp;
        my ($clusterId, $sfldVals) = split(m/\t/, $_, -1);
        foreach my $sfld (split(m/[,;]/, $sfldVals)) {
            my $valStr = "\"$clusterId\", \"$sfld\"";
            my $sql = "INSERT INTO sfld_map (cluster_id, sfld_id) VALUES ($valStr)";
            $self->batchInsert($sql);
        }
    }

    close $fh;
}


sub sfldDescToSqlite {
    my $self = shift;
    my $sfldFile = shift;

    $self->createTable("sfld_desc");

    open my $fh, "<", $sfldFile;

    while (<$fh>) {
        chomp;
        my ($sfldId, $desc, $color) = split(m/\t/, $_, -1);
        my $valStr = "\"$sfldId\", \"$desc\", \"$color\"";
        my $sql = "INSERT INTO sfld_desc (sfld_id, sfld_desc, sfld_color) VALUES ($valStr)";
        $self->batchInsert($sql);
    }

    close $fh;
}


#TODO:
sub sfldToJson {
    my $self = shift;
    my $json = shift;

    $json->{sfld_desc} = {};

    my $descSql = "SELECT * FROM sfld_desc";
    my $sth = $self->{dbh}->prepare($descSql);
    $sth->execute;
    while (my $row = $sth->fetchrow_hashref) {
        $json->{sfld_desc}->{$row->{sfld_id}} = {desc => $row->{sfld_desc}, color => $row->{sfld_color}};
    }

    $json->{sfld_map} = {};

    my $mapSql = "SELECT * FROM sfld_map";
    $sth = $self->{dbh}->prepare($mapSql);
    $sth->execute;
    while (my $row = $sth->fetchrow_hashref) {
        push @{$json->{sfld_map}->{$row->{cluster_id}}}, $row->{sfld_id};
    }
}


# ENZYME CODE METADATA #############################################################################

sub ecToSqlite {
    my $self = shift;
    my $ecFile = shift;

    $self->createTable("enzymecode");

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

sub convRatioToSqlite {
    my $self = shift;
    my $mainDataDir = shift;
    my $loadScript = shift;
    my $isDiced = shift || 0;
    my $jobIdListFile = shift || 0;

    my $loadSqlFile = $loadScript . "_load.sql";

    my $clusters;
    if ($jobIdListFile) {
        $clusters = {};
        my $handleIdFn = sub {
            my ($cluster, $parms) = @_;
            my $info = IdListParser::getClusterNumbers($cluster, $parms);
            foreach my $key (keys %$info) {
                $clusters->{$key} = $info->{$key};
            }
        };
        IdListParser::parseFile($jobIdListFile, $handleIdFn);
    }

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
            (my $clusterId = $dir) =~ s%^.*(cluster[\-\d]+).*?$%$1%;
            #print "$clusterId " . ($clusters->{$clusterId} ? "Y" : "N") . "\n";
            #return if ($clusters and not $clusters->{$clusterId});
            my $crFile = "$dir/conv_ratio.txt";
####sed 's/^\\([0-9]\\+\\)/$clusterId-\\1$ascoreCol/' $crFile | awk 'NR>1 {print \$0;}' > $crFile.load
#echo "############## PROCESSING $crFile"
#sed 's/^\\([0-9]\\+\\)/$clusterId$ascoreCol/' $crFile | awk 'NR$compOp {print \$0;}' > $crFile.load
#echo 'SELECT "LOADING $crFile.load";' >> $loadSqlFile
#echo '.import "$crFile.load" ${dicedPrefix}conv_ratio' >> $loadSqlFile
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

echo "WRAP UP BY EXECUTING sqlite3 $self->{sqlite} < $loadSqlFile"

SCRIPT

    close $outFh;
}

# CONSENSUS RESIDUE METADATA #######################################################################

sub consResToSqlite {
    my $self = shift;
    my $mainDataDir = shift;
    my $loadScript = shift;
    my $isDiced = shift || 0;
    my $jobIdListFile = shift || 0;

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
        my $ascoreCol = $ascore ? "$ascore\t" : "";
        (my $clusterId = $dataDir) =~ s%^.*(cluster[\-\d]+)$%$1%;
        my $crFile = "$dataDir/consensus_residue_C_position.txt";
        print $outFh <<SCRIPT;
awk 'NR>1 {print "$clusterId\t$ascoreCol"\$2"\t"\$3;}' $crFile >> $masterCrLoadFile
SCRIPT
    };

    if ($isDiced) {
        my @dirs = glob("$mainDataDir/cluster-*");
        foreach my $dir (@dirs) {
            foreach my $ascoreDir (glob("$dir/dicing-*")) {
                (my $ascore = $ascoreDir) =~ s/^.*dicing-(\d+)\/?$/$1/;
                foreach my $cDir (glob("$ascoreDir/cluster-*")) {
                    &$processFn($cDir, $ascore);
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
echo "WRAP UP BY EXECUTING sqlite3 $self->{sqlite} < $loadSqlFile"

SCRIPT

    close $outFh;
}


# NETWORK SIZE METADATA ############################################################################

sub networkSizeToSqlite {
    my $self = shift;

    $self->createTable("diced_size");
    $self->createTable("size");

    my ($dicedSizes, $primaryDicedSizes) = $self->computeDicedClusterSizes();
    my $sizes = $self->computeClusterSizes();

    foreach my $dicedId (keys %$primaryDicedSizes) {
        foreach my $sizeKey (keys %{ $primaryDicedSizes->{$dicedId} }) {
            if ($primaryDicedSizes->{$dicedId}->{$sizeKey}) {
                $sizes->{$dicedId}->{$sizeKey} = $primaryDicedSizes->{$dicedId}->{$sizeKey};
            }
        }
    }

    $self->insertSizeDataDiced($dicedSizes);
    $self->insertSizeData($sizes);
}


sub computeDicedClusterSizes {
    my $self = shift;

    my $idSql = "SELECT cluster_id, ascore, parent_id FROM diced_network";
    my %ascore;
    my $sth = $self->{dbh}->prepare($idSql);
    warn "Unable to prepare $idSql; ignoring" and return if not $sth;
    
    my %primaryAscore;
    $sth->execute;
    while (my $row = $sth->fetchrow_hashref) {
        my $pid = $row->{parent_id};
        push @{$ascore{"$pid-$row->{ascore}"}}, $row->{cluster_id};
        $primaryAscore{$pid} = $row->{ascore} if not exists $primaryAscore{$pid};
    }

    my $tableBase = "diced_id_mapping";
    my $data = {};
    my $primaryData = {}; # What is the size of Mega-#-# when it's not diced?
    foreach my $type ("uniprot", "uniref50", "uniref90") {
        my $table = $type eq "uniprot" ? $tableBase : "${tableBase}_$type";
        foreach my $ascoreKey (keys %ascore) {
            (my $parent = $ascoreKey) =~ s/\-\d+$//;
            (my $ascore = $ascoreKey) =~ s/^.*-(\d+)$/$1/;
            my $total = 0;
            foreach my $cluster (@{$ascore{$ascoreKey}}) {
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
                $primaryData->{$parent}->{$type} = $total;
            }
        }
    }

    return ($data, $primaryData);
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

    foreach my $clusterId (sort keys %$data) {
        my $net = $data->{$clusterId};

        my $valStr = join(", ", getStrVal($clusterId), getStrVal($net->{title}), getStrVal($net->{name}), getStrVal($net->{desc}));
        my $parentCol = "";
        if ($net->{parent}) {
            $parentCol = ", parent_id";
            $valStr .= ", " . getStrVal($net->{parent});
        }
        my $sql = "INSERT INTO network (cluster_id, title, name, desc $parentCol) VALUES ($valStr)";
        $self->batchInsert($sql);

        if ($net->{regions}) {
            $self->insertRegionData($clusterId, $net->{regions});
        }
        if ($net->{tigr_families}) {
            $self->insertTigrDataForCluster($clusterId, $net->{tigr_families});
        }
    }
}


sub netinfoToSqlite {
    my $self = shift;
    my $file = shift;

    $self->createTable("network");
    my %data;

    open my $fh, "<", $file or die "Unable to read netinfo file $file: $!";
    while (<$fh>) {
        chomp;
        my ($clusterId, $name, $title, $desc) = split(m/\t/, $_, -1);
        $data{$clusterId} = {title => $title, name => $name, desc => $desc};
    }
    close $fh;

    foreach my $clusterId (keys %data) {
        my @p = split(m/-/, $clusterId);
        next if scalar @p < 3;
        my $parent = join("-", @p[0..($#p-1)]);
        if ($data{$parent}) {
            $data{$clusterId}->{parent} = $parent;
        }
    }

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
        my ($parentId, $ascore, $clusterId) = split(m/\t/, $_, -1);
        my $valStr = join(", ", getStrVal($clusterId), getStrVal($ascore), getStrVal($parentId));
        my $sql = "INSERT INTO diced_network (cluster_id, ascore, parent_id) VALUES ($valStr)";
        $self->batchInsert($sql);
    }
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

    print <<SQL;
.separator "\\t"
.import "$file" uniref_map
SQL
}


# FAMILY ###########################################################################################

sub readTigrData {
    my $self = shift;
    my $file = shift;

    my $data = {};
    open my $fh, "<", $file or die "Unable to read TIGR data file $file: $!";
    while (<$fh>) {
        chomp;
        my ($clusterId, $famId, $famDesc) = split(m/\t/, $_, -1);
        push @{$data->{$clusterId}}, {id => $famId, description => $famDesc};
    }
    close $fh;

    return $data;
}


sub insertTigrDataForCluster {
    my $self = shift;
    my $clusterId = shift;
    my $data = shift;

    foreach my $famInfo (@$data) {
        $self->insertTigrDataRow($clusterId, $famInfo->{id});
    }
}


sub insertTigrDataRow {
    my $self = shift;
    my $clusterId = shift;
    my $data = shift;

    my $valStr = join(", ", getStrVal($clusterId), getStrVal($data->{id}), getStrVal("TIGR"));
    my $sql = "INSERT INTO families (cluster_id, family, family_type) VALUES ($valStr)";
    $self->batchInsert($sql);
    if ($data->{description}) {
        $valStr = join(", ", getStrVal($data->{id}), getStrVal($data->{description}));
        $sql = "INSERT OR IGNORE INTO family_info (family, description) VALUES ($valStr)";
        $self->batchInsert($sql);
    }
}


sub insertTigrData {
    my $self = shift;
    my $data = shift;

    $self->createTable("families");
    $self->createTable("family_info");

    foreach my $clusterId (sort keys %$data) {
        foreach my $row (@{$data->{$clusterId}}) {
            $self->insertTigrDataRow($clusterId, $row);
        }
    }
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
        "network" => ["cluster_id TEXT", "title TEXT", "name TEXT", "desc TEXT", "parent_id TEXT"],
        "region" => ["cluster_id TEXT", "region_id TEXT", "region_index INT", "name TEXT", "number TEXT", "coords TEXT"],
        "id_mapping" => ["cluster_id TEXT", "uniprot_id TEXT"],
        "id_mapping_uniref50" => ["cluster_id TEXT", "uniref50_id TEXT"],
        "id_mapping_uniref90" => ["cluster_id TEXT", "uniref90_id TEXT"],
        "enzymecode" => ["code_id TEXT", "desc TEXT"],
        "swissprot" => ["uniprot_id TEXT", "function TEXT", "UNIQUE(uniprot_id, function)"],
        "kegg" => ["uniprot_id TEXT", "kegg TEXT"],
        "pdb" => ["uniprot_id TEXT", "pdb TEXT"],
        "taxonomy" => ["uniprot_id TEXT", "tax_id TEXT", "domain TEXT", "kingdom TEXT", "phylum TEXT", "class TEXT", "taxorder TEXT", "family TEXT", "genus TEXT", "species TEXT"],
        "sfld_desc" => ["sfld_id TEXT", "sfld_desc TEXT", "sfld_color TEXT"],
        "sfld_map" => ["cluster_id TEXT", "sfld_id TEXT"],
        "families" => ["cluster_id TEXT", "family TEXT", "family_type TEXT"],
        "family_info" => ["family TEXT PRIMARY KEY", "description TEXT"],
        "uniref_map" => ["uniprot_id TEXT", "uniref90_id TEXT", "uniref50_id TEXT"],
        "conv_ratio" => ["cluster_id TEXT", "conv_ratio REAL", "num_ids INT", "num_blast INT", "node_conv_ratio REAL", "num_nodes INT", "num_edges INT"],
        "cons_res" => ["cluster_id TEXT", "percent INT", "num_res INT"],
        "annotations" => ["uniprot_id TEXT", "doi TEXT"],
        
        "diced_size" => ["cluster_id TEXT", "ascore INT", "uniprot INT DEFAULT 0", "uniref90 INT DEFAULT 0", "uniref50 INT DEFAULT 0"],
        "diced_network" => ["cluster_id TEXT", "ascore INT", "parent_id TEXT"],
        "diced_id_mapping" => ["cluster_id TEXT", "ascore INT", "uniprot_id TEXT"],
        "diced_id_mapping_uniref50" => ["cluster_id TEXT", "ascore INT", "uniref50_id TEXT"],
        "diced_id_mapping_uniref90" => ["cluster_id TEXT", "ascore INT", "uniref90_id TEXT"],
#        "diced_ssn" => ["cluster_id TEXT", "ascore TEXT"],
        "diced_conv_ratio" => ["cluster_id TEXT", "ascore INT", "conv_ratio REAL", "num_ids INT", "num_blast INT", "node_conv_ratio REAL", "num_nodes INT", "num_edges INT"],
        "diced_cons_res" => ["cluster_id TEXT", "ascore INT", "percent INT", "num_res INT"],
    };

    my $indexes = {
        "size" => [{name => "size_cluster_id_idx", cols => "cluster_id"}],
        "network" => [{name => "network_cluster_id_idx", cols => "cluster_id"}],
        "id_mapping" => [{name => "id_mapping_uniprot_id_idx", cols => "uniprot_id"}, {name => "id_mapping_cluster_id_idx", cols => "cluster_id"}],
        "id_mapping_uniref50" => [{name => "id_mapping_uniref50_id_idx", cols => "uniref50_id"}, {name => "id_mapping_uniref50_cluster_id_idx", cols => "cluster_id"}],
        "id_mapping_uniref90" => [{name => "id_mapping_uniref90_id_idx", cols => "uniref90_id"}, {name => "id_mapping_uniref90_cluster_id_idx", cols => "cluster_id"}],
        "swissprot" => [{name => "swissprot_uniprot_id_idx", cols => "uniprot_id"}],
        "kegg" => [{name => "kegg_uniprot_id_idx", cols => "uniprot_id"}],
        "pdb" => [{name => "pdb_uniprot_id_idx", cols => "uniprot_id"}],
        "taxonomy" => [{name => "taxonomy_tax_uniprot_idx", cols => "uniprot_id"}, {name => "taxonomy_tax_id_idx", cols => "tax_id"}, {name => "taxonomy_domain_idx", cols => "domain"}, {name => "taxonomy_kingdom_idx", cols => "kingdom"}, {name => "taxonomy_phylum_idx", cols => "phylum"}, {name => "taxonomy_class_idx", cols => "class"}, {name => "taxonomy_taxorder_idx", cols => "taxorder"}, {name => "taxonomy_family_idx", cols => "family"}, {name => "taxonomy_genus_idx", cols => "genus"}, {name => "taxonomy_species_idx", cols => "species"}, {name => "taxonomy_uniprot_id_idx", cols => "uniprot_id"}],
        "families" => [{name => "families_cluster_id_idx", cols => "cluster_id"}],
        "family_info" => [{name => "family_info_name_idx", cols => "family"}],
        "uniref_map" => [{name => "uniref_map_up_idx", cols => "uniprot_id"}, {name => "uniref_map_ur90_idx", cols => "uniref90_id"}, {name => "uniref_map_ur50_idx", cols => "uniref50_id"}],
        "conv_ratio" => [{name => "conv_ratio_cluster_id", cols => "cluster_id"}],
        "cons_res" => [{name => "cons_res_cluster_id", cols => "cluster_id, num_res"}],
        "annotations" => [{name => "uniprot_id", cols => "uniprot_id"}],
        
        "diced_size" => [{name => "diced_size_cluster_id_idx", cols => "cluster_id, ascore"}],
        "diced_network" => [{name => "diced_cluster_id", cols => "cluster_id, ascore"}, {name => "diced_parent_id", cols => "parent_id"}],
        "diced_id_mapping" => [{name => "diced_id_mapping_uniprot_id_idx", cols => "uniprot_id"}, {name => "id_mapping_diced_cluster_id_idx", cols => "cluster_id, ascore"}, {name => "id_mapping_diced_uniprot_ascore_idx", cols => "uniprot_id, ascore"}],
        "diced_id_mapping_uniref50" => [{name => "id_mapping_diced_uniref50_id_idx", cols => "uniref50_id"}, {name => "diced_id_mapping_uniref50_cluster_id_idx", cols => "cluster_id, ascore"}],
        "diced_id_mapping_uniref90" => [{name => "id_mapping_diced_uniref90_id_idx", cols => "uniref90_id"}, {name => "diced_id_mapping_uniref90_cluster_id_idx", cols => "cluster_id, ascore"}],
#        "diced_ssn" => [{name => "diced_ssn_cluster_id", cols => "cluster_id"}],
        "diced_conv_ratio" => [{name => "diced_conv_ratio_cluster_id", cols => "cluster_id, ascore"}],
        "diced_cons_res" => [{name => "diced_cons_res_cluster_id", cols => "cluster_id, ascore, num_res"}],
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

