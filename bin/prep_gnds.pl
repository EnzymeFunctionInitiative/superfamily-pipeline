#!/bin/env perl

use strict;
use warnings;

use Data::Dumper;
use File::Find;


my $dir = $ARGV[0];


find(\&wanted, $dir);


sub wanted {
    my $name = $File::Find::name;
    my $dir = $File::Find::dir;
    my $as = "";
    my $cname = "";
    if ($name =~ m%dicing-(\d+)/(cluster\-\d+\-\d+\-\d+)/uniprot.txt$%) {
        $as = $1;
        $cname = $2;
    } elsif ($name =~ m%(cluster\-\d+\-\d+[\-\d]*)/uniprot.txt$%) {
        $cname = $1;
    }
    if ($cname) {
        my $title = ucfirst($cname) . ($as ? "-AS$as" : "");
        my $mapfile = "$dir/unirefmapping.txt.load";
        my $dbfile = "$dir/gnd.sqlite";
        print "\$GNTAPPDIR/get_uniref_ids.pl --uniprot-ids $name --uniref-mapping $mapfile\n";
        print "\$GNTAPPDIR/create_diagram_db.pl --id-file $mapfile --db-file $dbfile --uniref 50 --job-type ID_LOOKUP --title \"$title\" --nb-size 20\n";
        print "rm $mapfile\n\n";
    }
}


