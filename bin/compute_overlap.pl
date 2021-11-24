#!/bin/env perl

use strict;
use warnings;

use Data::Dumper;
use FindBin;
use lib "/home/n-z/noberg/dev/EFITools/lib";
use EFI::Database;
use Getopt::Long;
use List::MoreUtils qw{uniq};

$| = 1;

my ($configFile, $family, $noFrag, $treeFile, $outputSimpleFile, $outputFamFamFile, $outputTreeFile, $masterListFile);
my $result = GetOptions(
    "config=s"          => \$configFile,
    "family=s"          => \$family,
    "no-frag"           => \$noFrag,
    "master=s"          => \$masterListFile,
    "family-tree=s"     => \$treeFile,
    "output-simple=s"   => \$outputSimpleFile,
    "output-fam-fam=s"  => \$outputFamFamFile,
    "output-tree=s"     => \$outputTreeFile,
);

$family = "" if not defined $family;

if (not $configFile or not -f $configFile and exists $ENV{EFI_CONFIG}) {
    $configFile = $ENV{EFI_CONFIG};
}

die "Invalid arguments given: no config file.\n" . help() unless (defined $configFile and -f $configFile);
die "Invalid arguments given: no family.\n" . help() if not $family;


if ($family =~ m%/% and -f $family) {
    open my $fh, "<", $family;
    my @fam;
    while (<$fh>) {
        chomp;
        push @fam, $_;
    }
    close $fh;
    $family = join(",", @fam);
}


my ($famFamFh, $simpleFh, $treeFh) = (\*STDOUT, \*STDOUT, \*STDOUT);
openOutputFiles();


my $db = new EFI::Database(config_file_path => $configFile);
my $dbh = $db->getHandle();

my $tree = loadFamilyTree($treeFile) if defined $treeFile and -f $treeFile;

my @families = split(/,/, $family);


my %idfMap;
my %fidMap;
my @accIds;
if ($masterListFile and -s $masterListFile) {
    print "Loading IDs from master file\n";
    getFileIds();
} else {
    print "Loading IDs from database\n";
    getDbIds();
    saveDbIds() if $masterListFile;
}

$dbh->disconnect();

print "Total IDs\t", scalar @accIds, "\n";


my %simpleOverlap;
my %famFamOverlap;
my %treeOverlap;
foreach my $fam (keys %fidMap) {
    my $numOver = 0;
    my $numId = scalar @{ $fidMap{$fam} };
    foreach my $id (@{ $fidMap{$fam} }) {
        my @idFams = @{$idfMap{$id}};
        $numOver++ if scalar @idFams > 1;
        foreach my $idFam (@idFams) {
            $famFamOverlap{$fam}->{$idFam}++;
        }
#        print join("\t", $fam, $id, join(",", @{$idfMap{$id}})), "\n";
    }
    #$simpleOverlap{$fam} = $numOver / $numId;
    $simpleOverlap{$fam} = compOverlap($numOver, $numId);
}

$famFamFh->print(join("\t", "Family", "Shared Overlap Family", "Percent Overlap"), "\n");
foreach my $fam (sort keys %famFamOverlap) {
    my $numId = scalar @{ $fidMap{$fam} };
    foreach my $idFam (sort keys %{$famFamOverlap{$fam}}) {
        my $numOver = $famFamOverlap{$fam}->{$idFam};
        my $overlap = compOverlap($numOver, $numId);
        #my $overlap = int(100000 * $numOver / $numId + 0.5) / 1000;
        $famFamFh->print(join("\t", $fam, $idFam, $overlap), "\n") if $fam ne $idFam;
    }
}

$simpleFh->print(join("\t", "Family", "Percent Overlap"), "\n");
foreach my $fam (sort keys %simpleOverlap) {
    $simpleFh->print(join("\t", $fam, $simpleOverlap{$fam}), "\n");
}




my %orphans;
my %orphanChildren;
my %treeOrphans;
my %other;

foreach my $fam (sort keys %fidMap) {
    if (not $tree->{$fam}) {
        $orphans{$fam} = 1;
    } elsif (not $tree->{$fam}->{parent}) {
        computeTreeOverlap($fam);
    } else {
        $other{$fam} = 1;
    }
}
foreach my $fam (sort keys %treeOverlap) {
    next if $orphans{$fam};
    next if $treeOverlap{$fam}->{parent};
    outputTreeNode($fam, 0);
}
my @orphans = sort keys %orphans;
if (scalar @orphans) {
    $treeFh->print("\n\nNot in Tree:\n");
    foreach my $fam (@orphans) {
        $treeFh->print("$fam\n");
    }
}
#@orphans = sort keys %orphanChildren;
#if (scalar @orphans) {
#    $treeFh->print("\n\nChild of Tree family, but not in rSAM dataset\n");
#    foreach my $fam (@orphans) {
#        $treeFh->print("$fam\n");
#    }
#}
@orphans = sort keys %treeOrphans;
if (scalar @orphans) {
    $treeFh->print("\n\nrSAM dataset families with children in the Tree, that don't have children in the rSAM dataset\n");
    foreach my $fam (@orphans) {
        $treeFh->print("$fam  ($tree->{$fam}->{info})\n");
        foreach my $kid (@{ $treeOrphans{$fam} }) {
            $treeFh->print("--$kid  ($tree->{$fam}->{info})\n");
        }
    }
}
@orphans = grep { not $treeOverlap{$_} } sort keys %other;
if (scalar @orphans) {
    $treeFh->print("\n\nrSAM dataset families that are children in the Tree, but the parent isn't in the rSAM dataset\n");
    foreach my $fam (@orphans) {
        $treeFh->print("$fam  ($tree->{$fam}->{info})\n");
    }
}








sub outputTreeNode {
    my $thisFam = shift;
    my $level = shift;
    if (not $treeOverlap{$thisFam}) {
        $orphanChildren{$thisFam} = 1;
        return;
    }
    my $line = "--" x $level;
    my $off = 16 - length($line);
    $line .= sprintf("%-${off}s", $thisFam);

    my $str = $treeOverlap{$thisFam}->{child_overlap} > -1 ? "(children overlap: $treeOverlap{$thisFam}->{child_overlap}%)" : "";
    $line .= sprintf("%-28s", $str);
    
    #$off = $treeOverlap{$thisFam}->{child_overlap} > -1 ? 28 : 56;
    $str = $treeOverlap{$thisFam}->{parent_overlap} > -1 ? "  (parent overlap: $treeOverlap{$thisFam}->{parent_overlap}%)" : "";
    $line .= sprintf("%-28s", $str);

    #$line .= " " x (length($line) - 72);
    $line .= "($treeOverlap{$thisFam}->{info})";
    #$off = 0;# 94;
    #$line .= sprintf("%-${off}s", "  ($treeOverlap{$thisFam}->{info})");

    $treeFh->print("$line\n");
    foreach my $nextFam (@{ $tree->{$thisFam}->{children} }) {
        outputTreeNode($nextFam, $level + 1);
    }
}


sub computeTreeOverlap {
    my $thisFam = shift;
    my $S = {parent => "", children => [], child_overlap => -1, parent_overlap => -1};
    if ($tree->{$thisFam}->{parent} and $fidMap{$tree->{$thisFam}->{parent}}) {
        $S->{parent} = $tree->{$thisFam}->{parent};
    }
    $S->{info} = $tree->{$thisFam}->{info};
    delete $other{$thisFam} if $other{$thisFam};
    my $ov = findChildOverlap($thisFam);
    $S->{child_overlap} = $ov;
    foreach my $nextFam (@{ $tree->{$thisFam}->{children} }) {
        next if not $fidMap{$nextFam};
        #print "Looking at $thisFam/$nextFam\n";
        computeTreeOverlap($nextFam);
        my $ov = findParentOverlap($nextFam);
        $treeOverlap{$nextFam}->{parent_overlap} = $ov;
    }
    $treeOverlap{$thisFam} = $S;
}





# Find the overlap of a parent with ALL of it's child families
sub findParentOverlap {
    my $family = shift;
    my $listRef = $tree->{$family}->{parent};
    return -1 if not $listRef;
    return findFamilyOverlap($family, $listRef);
}
sub findChildOverlap { 
    my $family = shift;

    my $listRef = $tree->{$family}->{children};
    return -1 if not $listRef;

    my @fams;

    my @kids = @$listRef;
    while (scalar @kids) {
        my $kid = shift @kids;
        if (not $fidMap{$kid}) {
            push @{$treeOrphans{$family}}, $kid;
            next;
        }
        push @kids, @{ $tree->{$kid}->{children} } if $tree->{$kid};
        push @fams, $kid;
    }

    return findFamilyOverlap($family, \@fams);
}
sub findFamilyOverlap {
    my $family = shift;
    my $listRef = shift;

    my @famList = ref($listRef) ? @$listRef : $listRef;

    my %ids = map { $_ => 1 } @{ $fidMap{$family} };
    my %matchIds;
    foreach my $fam (@famList) {
        map { $matchIds{$_} = 1; } @{ $fidMap{$fam} };
    }
    my @mk = keys %matchIds;
    return -1 if scalar(@mk) == 0;

    my $numIds = scalar keys %ids;
    my $numMatchIds = scalar keys %matchIds;
    my $numOverlap = 0;
    foreach my $matchId (keys %matchIds) {
        $numOverlap++ if $ids{$matchId};
    }
    die "Help! We shouldn't be here ($family $numOverlap/$numIds)" if $numIds == 0;
    return compOverlap($numOverlap, $numIds);
}




sub compOverlap {
    my ($numOver, $numId) = @_;
    my $pct = int(100000 * $numOver / $numId + 0.5) / 1000;
    return $pct;
}


sub retrieveForFamily {
    my ($family, $table) = @_;

    my $fragJoin = $noFrag ? "JOIN annotations ON $table.accession = annotations.accession" : "";
    my $fragWhere = $noFrag ? "AND annotations.Fragment = 0" : "";

    my $sql = "select $table.accession from $table $fragJoin where $table.id = '$family' $fragWhere";
    my $sth = $dbh->prepare($sql);
    $sth->execute;

    my @ids;
    while (my $row = $sth->fetchrow_arrayref) {
        #print $row->[0], "\n";
        push @ids, $row->[0];
    }

    return @ids;
}


sub help {
    return <<HELP;
Usage: $0 --family=family_list [--config=config_file_path]

    --family        one or more comma-separated families or Pfam clans

HELP
    ;
}


sub loadFamilyTree {
    my $file = shift;

    my %tree;

    open FILE, $file;

    my @hierarchy;
    my $curDepth = 0;
    my $lastFam = "";

    while (<FILE>) {
        chomp;
        (my $fam = $_) =~ s/^\-*(IPR\d+)::(.*)$/$1/;
        my $info = $2;
        (my $depthDash = $_) =~ s/^(\-*)IPR.*$/$1/;
        my $depth = length $depthDash;
        if ($depth > $curDepth) {
            push @hierarchy, $lastFam;
        } elsif ($depth < $curDepth) {
            for (my $i = 0; $i < ($curDepth - $depth) / 2; $i++) {
                pop @hierarchy;
            }
        }

        my $parent = scalar @hierarchy ? $hierarchy[$#hierarchy] : "";

        $tree{$fam}->{parent} = $parent;
        $tree{$fam}->{children} = [];
        $tree{$fam}->{info} = $info;
        if ($parent) {
            push @{$tree{$parent}->{children}}, $fam;
        }

        $curDepth = $depth;
        $lastFam = $fam;
    }

    close FILE;

    return \%tree;
}


sub openOutputFiles {
    if ($outputFamFamFile) {
        open my $fh, ">", $outputFamFamFile;
        $famFamFh = $fh;
    }
    if ($outputSimpleFile) {
        open my $fh, ">", $outputSimpleFile;
        $simpleFh = $fh;
    }
    if ($outputTreeFile) {
        open my $fh, ">", $outputTreeFile;
        $treeFh = $fh;
    }
}


sub getDbIds {
    foreach my $fam (@families) {
        my $table = "";
        $table = "PFAM" if $fam =~ /^pf/i;
        $table = "INTERPRO" if $fam =~ /^ip/i;
        warn "Invalid family $fam given" if not $table;
    
        my @ids = retrieveForFamily($fam, $table);
        @ids = uniq @ids; # multiples b/c of domains
        #print join("\t", $fam, scalar @ids), "\n";
        map { push @{$idfMap{$_}}, $fam } @ids;
        $fidMap{$fam} = [@ids];
        push(@accIds, @ids);
    }
}


sub getFileIds {
    open my $fh, "<", $masterListFile;
    my %ids;
    while (<$fh>) {
        chomp;
        my ($id, $fam) = split(m/\t/);
        $ids{$id}++;
        push @{$idfMap{$id}}, $fam;
        push @{$fidMap{$fam}}, $id;
    }
    close $fh;
    @accIds = sort keys %ids;
}


sub saveDbIds {
    open my $fh, ">", $masterListFile;
    foreach my $id (sort { $a cmp $b } uniq @accIds) {
        foreach my $fam (@{$idfMap{$id}}) {
            $fh->print(join("\t", $id, $fam), "\n");
        }
    }
    close $fh;
}


