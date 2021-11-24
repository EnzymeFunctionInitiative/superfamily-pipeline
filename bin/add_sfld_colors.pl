#!/bin/env perl

use strict;
use warnings;

use XML::LibXML::Reader;
use Getopt::Long;
use FindBin;
use Data::Dumper;

use lib $FindBin::Bin . "/../lib";
use EFI::SSN;

my ($ssnIn, $ssnOut, $sfldFile);
my $result = GetOptions(
    "ssn-in=s"          => \$ssnIn,
    "ssn-out=s"         => \$ssnOut,
    "sfld-colors=s"     => \$sfldFile,
);


my $usage = "$0 --ssn-in path_to_ssn --ssn-out path_to_output_ssn --sfld-colors path_to_sfld_mapping_file";

die $usage if not $ssnIn or not -f $ssnIn or not $sfldFile or not -f $sfldFile or not $ssnOut;

my $defaultColor = "#999999";


my %sfldColors;
open my $fh, "<", $sfldFile or die "Unable to read --sfld-colors $sfldFile: $!";
while (<$fh>) {
    chomp;
    next if not m/^IPR\d{6,6}/;
    my ($fam, $color) = split(m/\t/);
    $sfldColors{$fam} = $color;
}
close $fh;



my $ssn = openSsn($ssnIn);
die "Unable to open SSN $ssnIn for reading: $!" if not $ssn;


print "Processing $ssnOut\n";


my %colorMap;

my $nodeReaderFn = sub {
    my ($xmlNode, $params) = @_;
    my $nodeId = $xmlNode->getAttribute("label");

    my $iproFn = sub {
        my $ipro = shift;
        $colorMap{$nodeId} = $sfldColors{$ipro} if $sfldColors{$ipro};
    };

    my @annotations = $xmlNode->findnodes("./*");
    foreach my $annotation (@annotations) {
        my $attrName = $annotation->getAttribute("name");
        if ($attrName =~ m/^InterPro/) {
            if ($annotation->getAttribute("type") eq "list") {
                foreach my $att ($annotation->findnodes("./*")) {
                    &$iproFn($att->getAttribute("value"));
                }
            } else {
                &$iproFn($annotation->getAttribute("value"));
            }
        }
    }
};

my $nodeWriterFn = sub {
    my ($nodeId, $childIds, $fieldWriter, $listWriter) = @_;
    if ($colorMap{$nodeId}) {
        #&$fieldWriter("SFLD_Color", "string", $colorMap{$nodeId});
        &$fieldWriter("node.fillColor", "string", $colorMap{$nodeId});
    } else {
        &$fieldWriter("node.fillColor", "string", $defaultColor);
    }
};

$ssn->registerHandler(NODE_READER, $nodeReaderFn);
$ssn->registerHandler(NODE_WRITER, $nodeWriterFn);

$ssn->parse();

$ssn->write($ssnOut);


