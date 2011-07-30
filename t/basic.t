#!/usr/bin/env perl

use strict;
use warnings;

use lib 'lib';

use Cassandra::Lite;
use Cassandra::Types;
use Test::More;

my $c;
eval {
    $c = Cassandra::Lite->new;
};

plan skip_all => 'You need to run Cassandra first' if $@;

my $ks = "TestKeyspace_$$";
eval {
    $c->client->system_drop_keyspace($ks);
};

my $cfObj = Cassandra::CfDef->new({keyspace => $ks, name => 'Std', comparator_type => 'UTF8Type', default_validation_class => 'UTF8Type'});
my $ksObj = Cassandra::KsDef->new({name => $ks, strategy_class => 'NetworkTopologyStrategy', cf_defs => [$cfObj]});
$c->client->system_add_keyspace($ksObj);
$c->keyspace($ks);

plan skip_all => 'Done';

END {
    if (defined $c and defined $c->client) {
        $c->client->system_drop_keyspace($ks);
    }
}

done_testing;
