#!perl

use strict;
use warnings;

my $outDir = "py2cytoscape/export";
mkdir "$outDir/cytoscape";
my $APPDIR = "C:\\Users\\noberg\\Documents\\rSAM\\py2cytoscape";
my $VIZPATH = "$APPDIR\\vizmap2.xml";
my $CYTOSCAPEDIR = "C:\\Program Files\\Cytoscape_v3.7.2";

print <<PS;
\@echo off

set APPDIR=$APPDIR
set PYSCRIPT=%APPDIR%\\process_ssn_png2.py

set EXPDIR=%APPDIR%\\export
set OUTDIR=%EXPDIR%\\data
set CSDIR=%EXPDIR%\\cytoscape
set DBG=

%DBG% mkdir %EXPDIR%
%DBG% mkdir %OUTDIR%
%DBG% mkdir %CSDIR%



PS

# Remove headers
scalar <>;
scalar <>;

while (<>) {
    chomp;
    my ($jobId, $clusterId, $file) = split(m/\t/);
    $file =~ s%^/private_stores/gerlt/efi_test/%%;
    
    open CS, ">", "$outDir/cytoscape/$clusterId.txt";
    print CS getCyScript($clusterId, $file);
    close CS;
    
    open BS, ">", "$outDir/cytoscape/$clusterId.bat";
    print BS <<BS;
cd $CYTOSCAPEDIR
set CSDIR=%1
set CSDIR2=%CSDIR:\\=/%
cytoscape.bat -R 8888 -S %CSDIR2%/$clusterId.txt
exit
BS
    close BS;

    print <<PS;
%DBG% mkdir %OUTDIR%\\$clusterId
%DBG% start %CSDIR%\\$clusterId.bat %CSDIR%
%DBG% python %PYSCRIPT% %OUTDIR%\\$clusterId\\$clusterId
%DBG% timeout 30 > nul

PS
}


sub getCyScript {
    my $id = shift;
    my $url = shift;
    (my $vizpath = $VIZPATH) =~ s%\\%/%g;
    return <<SCRIPT;
network load url url="https://efi.igb.illinois.edu/dev/efi-est/$url"
SCRIPT
    my $old = <<SCRIPT;
view create
vizmap load file file="$vizpath"
vizmap apply styles=ssn9
layout force-directed
view export view=CURRENT options=PNG outputFile="C:/Users/noberg/Documents/rSAM/py2cytoscape/export/flagimg/$id.png"
SCRIPT
}


