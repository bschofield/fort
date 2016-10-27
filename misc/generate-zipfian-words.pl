#!/usr/bin/perl

use strict;
use warnings;
use bytes;

use List::Util qw( shuffle );
use POSIX qw( floor );

use constant A => 1.1;
use constant LINES => 10**6;
use constant WORDS => 3;

# ---- load array ----

my @words;

open(IN, '/usr/share/dict/words') || die($!);

while( <IN> )
{
  chomp;
  push( @words, $_ );
}

# ---- shuffle ----

@words = shuffle(@words);

# ---- generate lines ----

# zipfian variate, \propto x^{-A}
# http://luc.devroye.org/chapter_ten.pdf, p551

my $b = 2**(A-1);

my ($u, $v, $x, $t);

for( my $i = 0; $i < LINES; $i++ )
{
  my $line = '';

  for( my $j = 0; $j < WORDS; $j++ )
  {
    do
    {
      ($u, $v) = (rand(1), rand(1));
      $x = $u**(-1/(A-1));
      $t = (1+1/$x)**(A-1);

    } until( ($v*$x*($t-1)/($b-1)) <= ($t/$b) && $x < scalar(@words) );

    $line .= $words[floor($x)];
  }

  print "$line\n";
}
