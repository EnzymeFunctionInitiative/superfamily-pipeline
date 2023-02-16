#!/bin/env perl

use strict;
use warnings;

use GD;


my $white = 16777215;


my $inputFile = $ARGV[0];
my $outputFile = $ARGV[1];

if (not -f $inputFile) {
    print "Unable to find $inputFile\n";
    exit(1);
}

GD::Image->trueColor(1);

print "Loading PNG $inputFile\n";
my $input = GD::Image->newFromPng($inputFile);
my ($width, $height) = $input->getBounds();

my $leftStart = -1;
my $rightStart = -1;

foreach my $x (1..$width) {
    foreach my $y (1..$height) {
        my $idx = $input->getPixel($x - 1, $y - 1);
        if ($idx != $white) {
            $leftStart = $x - 1;
            last;
        }
    }
    last if $leftStart > -1;
}

foreach my $x (1..$width) {
    foreach my $y (1..$height) {
        my $rightPx = $width - $x;
        my $idx = $input->getPixel($rightPx, $y - 1);
        if ($idx != $white) {
            $rightStart = $rightPx;
            last;
        }
    }
    last if $rightStart > -1;
}

$leftStart -= 10;
$rightStart += 10;

$leftStart = 0 if $leftStart < 0;
$rightStart = $width - 1 if $rightStart < 0;

if ($rightStart - $leftStart < 20) {
    $leftStart = 0;
    $rightStart = 20;
    $height = 20;
}

my $newWidth = $rightStart - $leftStart + 1;
my $output = GD::Image->new($newWidth, $height);

$output->copyResized($input, 0, 0, $leftStart, 0, $newWidth, $height, $newWidth, $height);

my $pngData = $output->png;


open my $outFh, ">", $outputFile or die "Unable to write to output $outputFile: $!";
binmode $outFh;
print $outFh $pngData;
close $outFh;

print "Finished\n";

