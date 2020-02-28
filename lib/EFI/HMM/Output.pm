
package EFI::HMM::Output;

use strict;
use warnings;


sub new {
    my ($class, %args) = @_;

    my $self = {};
    bless $self, $class;

    return $self;
}


sub parse {
    my $self = shift;
    my $file = shift;

    return {} if not $file or not -f $file;

    my $data = {};

    open my $fh, "<", $file or die "Unable to read HMM output table $file: $!";

    while (<$fh>) {
        chomp;
        next if m/^\s*#/;
        next if m/^\s*$/;
        my ($targetName, $targetAcc, $queryName, $queryAcc, $evalue, $score, $bias) = split(m/\s+/);
        $targetName =~ s/^[^\|]+\|([^\|]+)\|.+$/$1/;
        $data->{$targetName} = $evalue;
    }

    close $fh;

    return $data;
}


1;

