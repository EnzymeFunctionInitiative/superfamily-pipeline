#!/bin/bash

module load Perl
module load HMMER/3.2.1-IGB-gcc-4.9.4

/igbgroup/n-z/noberg/dev/hmm/superfamily-pipeline/bin/hmmscan_multi.pl "$@"


