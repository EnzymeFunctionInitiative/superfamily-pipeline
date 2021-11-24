#!/bin/env perl

use strict;
use warnings;


use FindBin;
use Data::Dumper;
use File::Slurp;

use lib "$FindBin::Bin/../lib";

use EFI::HMM::Database qw(validateInputs);
use IdListParser;


my $parms = validateInputs(\&getUsage);

die getUsage("require --data-dir argument") if not -d $parms->{data_dir} and ($parms->{mode} =~ m/swissprot|id\-list/);
die getUsage("require --load-ec-file") if $parms->{mode} =~ m/enzymecode/ and not -f $parms->{load_ec_file};
die getUsage("require --load-kegg-file $parms->{load_kegg_file}") if $parms->{mode} =~ m/kegg/ and not -f $parms->{load_kegg_file};
die getUsage("require --load-sfld-desc-file") if $parms->{mode} =~ m/sfld\-desc/ and not -f $parms->{load_sfld_desc_file};
die getUsage("require --load-sfld-map-file") if $parms->{mode} =~ m/sfld\-map/ and not -f $parms->{load_sfld_map_file};
die getUsage("require --load-id-list-script") if $parms->{mode} =~ m/id\-list/ and not $parms->{load_id_list_script};
die getUsage("require --load-tigr-file") if $parms->{mode} =~ m/tigr/ and not $parms->{load_tigr_file};
die getUsage("require --load-region-file") if $parms->{mode} =~ m/region/ and not $parms->{load_region_file};
die getUsage("require --load-netinfo-file") if $parms->{mode} =~ m/netinfo/ and not $parms->{load_netinfo_file};
die getUsage("require --load-dicing-file") if $parms->{mode} =~ m/dicing/ and not $parms->{load_dicing_file};
#die getUsage("require --load-ssn-file") if $parms->{mode} =~ m/ssn/ and not $parms->{load_ssn_file};
die getUsage("require --load-uniref-file") if $parms->{mode} =~ m/uniref\-?map/ and not $parms->{load_uniref_file};
die getUsage("require --load-conv-ratio-script") if $parms->{mode} =~ m/conv\-ratio/ and not $parms->{load_conv_ratio_script};
die getUsage("require --load-cons-res-script") if $parms->{mode} =~ m/cons\-res/ and not $parms->{load_cons_res_script};
die getUsage("require --load-anno-file") if $parms->{mode} =~ m/load-anno/ and not $parms->{load_anno_file};

my $db = new EFI::HMM::Database($parms);


if ($parms->{mode} =~ m/swissprot/) {
    $db->swissprotsToSqlite($parms->{load_swissprot_file});
}
if ($parms->{mode} =~ m/enzymecode/) {
    $db->ecToSqlite($parms->{load_ec_file});
}
if ($parms->{mode} =~ m/kegg/) {
    $db->keggToSqlite($parms->{load_kegg_file});
}
if ($parms->{mode} =~ m/pdb/) {
    $db->pdbToSqlite($parms->{load_pdb_file});
}
if ($parms->{mode} =~ m/taxonomy/) {
    $db->taxonomyToSqlite($parms->{load_taxonomy_file});
}
if ($parms->{mode} =~ m/sfld\-map/) {
    $db->sfldMapToSqlite($parms->{load_sfld_map_file});
}
if ($parms->{mode} =~ m/sfld\-desc/) {
    $db->sfldDescToSqlite($parms->{load_sfld_desc_file});
}
if ($parms->{mode} =~ m/id\-list/) {
    $db->idListsToSqlite($parms->{data_dir}, $parms->{load_id_list_script}, $parms->{load_diced}, $parms->{job_id_file});
}
if ($parms->{mode} =~ m/sizes/) {
    $db->networkSizeToSqlite();
}
if ($parms->{mode} =~ m/tigr/) {
    my $tigrData = $db->readTigrData($parms->{load_tigr_file});
    $db->insertTigrData($tigrData);
}
if ($parms->{mode} =~ m/netinfo/) {
    $db->netinfoToSqlite($parms->{load_netinfo_file});
}
if ($parms->{mode} =~ m/region/) {
    $db->regionToSqlite($parms->{load_region_file});
}
if ($parms->{mode} =~ m/dicing/) {
    $db->dicingToSqlite($parms->{load_dicing_file});
}
#if ($parms->{mode} =~ m/ssn/) {
#    $db->ssnListToSqlite($parms->{load_ssn_file});
#}
if ($parms->{mode} =~ m/uniref\-?map/) {
    $db->unirefMappingToSqlite($parms->{load_uniref_file});
}
if ($parms->{mode} =~ m/evo\-tree/) {
    $db->evoTreeToSqlite();
}
if ($parms->{mode} =~ m/conv\-ratio/) {
    $db->convRatioToSqlite($parms->{data_dir}, $parms->{load_conv_ratio_script}, $parms->{load_diced}, $parms->{job_id_file});
}
if ($parms->{mode} =~ m/cons\-res/) {
    $db->consResToSqlite($parms->{data_dir}, $parms->{load_cons_res_script}, $parms->{load_diced});
}
if ($parms->{mode} =~ m/load\-anno/) {
    $db->annoToSqlite($parms->{load_anno_file});
}

# commit database
$db->finish;




sub getUsage {
    my $msg = shift || "";

    $msg = "ERROR: $msg\n\n" if $msg;
    return <<USAGE;
${msg}usage: $0 --sqlite-file <OUTPUT_SQLITE_FILE_PATH>
    [--mode swissprot --data-dir <CLUSTER_DATA_DIR_PATH>]
    [--mode enzymecode --load-ec-file <EC_DESC_FILE_PATH>]
    [--mode sfld-desc --load-sfld-desc-file <SFLD_DESC_FILE_PATH>]
    [--mode sfld-map --load-sfld-map-file <SFLD_MAP_FILE_PATH>]
    [--mode pdb --load-pdb-file <PDB_FILE_PATH>]
    [--mode id-list --data-dir <CLUSTER_DATA_DIR_PATH> --load-id-list-script <OUTPUT_SCRIPT_FOR_LOAD_COMMANDS>]
    [--mode kegg]
    [--mode tigr --load-tigr-file <TIGR_FILE_PATH>]
    [--mode sizes]
    [--mode netinfo --load-netinfo-file <NETINFO_FILE>]
    [--mode region --load-region-file <REGION_FILE>]
    [--mode dicing --load-dicing-file <DICING_FILE>]
    [--mode ssn --load-ssn-file <DICING_FILE>]
    [--mode conv-ratio --load-conv-ratio-script <OUTPUT_SCRIPT_FOR_LOAD_COMMANDS>]
    [--mode evo-tree]

    [--load-dicing]

USAGE
}

