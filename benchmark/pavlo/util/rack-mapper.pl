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

##
## Maps host machines to racks for Hadoop
## This script is used for the University of Wisconsin-Madison Database Group Cluster
##
## Network topoloy is as follows:
##    Nodes d-01 to d-50:     /rack1
##    Nodes d-51 to d-100:    /rack2
##    Nodes d-101 to d-150:   /rack3
##    Nodes d-151 to d-200:   /rack4
##

# open(DEBUG, ">>/tmp/rack-mapper.debug") || die($!);
for my $host (@ARGV) {
   if ($host =~ m/^[\d]{3,3}\.[\d]{1,3}.*?/) {
      use Socket;
      my $iaddr = inet_aton($host);
      $host  = gethostbyaddr($iaddr, AF_INET);
   } # UNLESS
   chomp($host);
   die("INVALID HOSTNAME: $host\n") unless ($host =~ m/^d-([\d]+)\.*?/);
   my $host_idx = int($1);

   my $rack = undef;
   foreach (0 .. 4) {
      if ($host_idx <= (50 * $_)) {
         $rack = $_;
         last;
      }
   } ## FOREACH
   die("UNABLE TO MAP HOSTNAME TO RACK: $host\n") unless (defined($rack));
   print "/rack$rack ";
#    print DEBUG "$host -> /rack$rack\n";
} # FOR
# close(DEBUG);
print "\n" unless ($#ARGV == -1);
exit;
