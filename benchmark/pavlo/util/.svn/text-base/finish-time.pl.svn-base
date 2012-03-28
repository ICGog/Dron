#!/usr/bin/env perl
#/***************************************************************************
# *   Copyright (C) 2008 by Andy Pavlo, Brown University                    *
# *   http://www.cs.brown.edu/~pavlo/                                       *
# *                                                                         *
# *   Permission is hereby granted, free of charge, to any person obtaining *
# *   a copy of this software and associated documentation files (the       *
# *   "Software"), to deal in the Software without restriction, including   *
# *   without limitation the rights to use, copy, modify, merge, publish,   *
# *   distribute, sublicense, and/or sell copies of the Software, and to    *
# *   permit persons to whom the Software is furnished to do so, subject to *
# *   the following conditions:                                             *
# *                                                                         *
# *   The above copyright notice and this permission notice shall be        *
# *   included in all copies or substantial portions of the Software.       *
# *                                                                         *
# *   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,       *
# *   EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF    *
# *   MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.*
# *   IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR     *
# *   OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, *
# *   ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR *
# *   OTHER DEALINGS IN THE SOFTWARE.                                       *
# ***************************************************************************/
use strict;
use warnings;

my @lines = `cat $ARGV[0]`;
my $target = $ARGV[1];
my $trial = int($ARGV[2]);
for (my $ctr = 0; $ctr <= $#lines; $ctr++) {
    if ($lines[$ctr] =~ m/BenchmarkBase\($target\)/ && --$trial < 0) {
        for ( ; $ctr <= $#lines; $ctr++) {
            if ($lines[$ctr] =~ m/The job took ([\d\.]+) seconds/) {
                #my @parts = ConvertSeconds($1);
                print "$1\n"; #join(":", @parts)."\n";
                exit;
            }
        }
    }
}
die("ERROR: Failed to find completion time for trial $ARGV[2] of '$target' in $ARGV[0]\n");

sub ConvertSeconds {
    my $seconds = shift;
    my ($days, $hours, $minutes) = (0, 0, 0);
    if ($seconds > (60*60*24)){
        $days = int($seconds / (60*60*24));
        $seconds -= $days * 60 * 60 * 24;
    }
    if ($seconds > 60*60) {
        $hours = int($seconds / (60*60));
        $seconds -= $hours * 60 * 60;
    }
    if ($seconds > 60) {
        $minutes = int($seconds / 60);
        $seconds -= $minutes * 60;
    }
    return ($hours, $minutes, $seconds);
}