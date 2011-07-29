#!/usr/bin/env perl

use strict;
use warnings;

use Cassandra::Lite;
use Test::More;

INIT {
    my $c;
    eval {
        $c = Cassandra::Lite->new;
    };

    plan skip_all => 'You need to run Cassandra first' if $@;
}

done_testing;
