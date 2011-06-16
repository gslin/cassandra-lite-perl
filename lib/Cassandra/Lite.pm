# ABSTRACT: Simple way to access Cassandra 0.7
package Cassandra::Lite;
BEGIN {
  $Cassandra::Lite::VERSION = '0.0.4';
}
use strict;
use warnings;

=head1 NAME

Cassandra::Lite - Simple way to access Cassandra 0.7

=head1 VERSION

version 0.0.4

=head1 DESCRIPTION

This module will offer you a simple way to access Cassandra 0.7 (maybe later version).  Some parts are not same as standard API document (especially arguments order), it's because I want to keep this module easy to use.

You'll need to install Thrift perl modules first to use Cassandra::Lite.

=head1 SYNOPSIS

    use Cassandra::Lite;

    # All supported options:
    my $c = Cassandra::Lite->new(
                server_name => 'server1',       # optional, default to '127.0.0.1'
                server_port => 9160,            # optional, default to 9160
                username => 'username',         # optional, default to empty string ''
                password => 'xxx',              # optional, default to empty string ''
                consistency_level_read => 'ONE' # optional, default to 'ONE'
                consistency_level_write => 'ONE' # optional, default to 'ONE'
                keyspace => 'Keyspace1',
            );

    # Usually we can use this in dev environment:
    my $c = Cassandra::Lite->new(keyspace => 'Keyspace1');

    # Now just define $columnFamily and $key
    my $columnFamily = 'BlogArticle';
    my $key = 'key12345';

    # Insert it (timestamp is optional)
    $c->insert($columnFamily, $key, {title => 'testing title', body => '...'}, {timestamp => time});

    # Get slice
    my $res1 = $c->get_slice($columnFamily, $key);
    my $res2 = $c->get_slice($columnFamily, $key, {range => ['sliceKeyStart', undef]});
    my $res3 = $c->get_slice($columnFamily, $key, {range => [undef, 'sliceKeyFinish']});
    my $res4 = $c->get_slice($columnFamily, $key, {range => ['sliceKeyStart', 'sliceKeyFinish']});

    # Get a column
    my $v1 = $c->get($columnFamily, $key, 'title');

    # Higher consistency level
    my $v2 = $c->get($columnFamily, $key, 'title', {consistency_level => 'QUORUM'});

    # Remove it
    $c->remove($columnFamily, $key, {timestamp => time});       # You can specify timestamp (optional) and consistency_level (optional)

    # Change keyspace
    $c->keyspace('BlogArticleComment');

    # Get count
    my $num1 = $c->get_count('Foo', 'key1');
    my $num2 = $c->get_count('Foo', 'key2', {consistency_level => 'ALL'});

    ...

=cut

use Any::Moose;
has 'client' => (is => 'rw', isa => 'Cassandra::CassandraClient', lazy_build => 1);
has 'consistency_level_read' => (is => 'rw', isa => 'Str', default => 'ONE');
has 'consistency_level_write' => (is => 'rw', isa => 'Str', default => 'ONE');
has 'keyspace' => (is => 'rw', isa => 'Str', trigger => \&_trigger_keyspace);
has 'password' => (is => 'rw', isa => 'Str', default => '');
has 'protocol' => (is => 'rw', isa => 'Thrift::BinaryProtocol', lazy_build => 1);
has 'server_name' => (is => 'rw', isa => 'Str', default => '127.0.0.1');
has 'server_port' => (is => 'rw', isa => 'Int', default => 9160);
has 'socket' => (is => 'rw', isa => 'Thrift::Socket', lazy_build => 1);
has 'transport' => (is => 'rw', isa => 'Thrift::FramedTransport', lazy_build => 1);
has 'username' => (is => 'rw', isa => 'Str', default => '');

use 5.010;
use Cassandra::Cassandra;
use Cassandra::Types;
use Thrift;
use Thrift::BinaryProtocol;
use Thrift::FramedTransport;
use Thrift::Socket;

sub _build_client {
    my $self = shift;

    my $client = Cassandra::CassandraClient->new($self->protocol);
    $self->transport->open;
    $self->_login($client);

    $client;
}

sub _build_protocol {
    my $self = shift;

    Thrift::BinaryProtocol->new($self->transport);
}

sub _build_socket {
    my $self = shift;

    Thrift::Socket->new($self->server_name, $self->server_port);
}

sub _build_transport {
    my $self = shift;

    Thrift::FramedTransport->new($self->socket, 1024, 1024);
}

sub _consistency_level_read {
    my $self = shift;
    my $opt = shift // {};

    my $level = $opt->{consistency_level} // $self->consistency_level_read;

    eval "\$level = Cassandra::ConsistencyLevel::$level;";
    $level;
}

sub _consistency_level_write {
    my $self = shift;
    my $opt = shift // {};

    my $level = $opt->{consistency_level} // $self->consistency_level_write;

    eval "\$level = Cassandra::ConsistencyLevel::$level;";
    $level;
}

sub _login {
    my $self = shift;
    my $client = shift;

    my $auth = Cassandra::AuthenticationRequest->new;
    $auth->{credentials} = {username => $self->username, password => $self->password};
    $client->login($auth);
}

sub _trigger_keyspace {
    my ($self, $keyspace) = @_;

    $self->client->set_keyspace($keyspace);
}

=head2 get
=cut

sub get {
    my $self = shift;

    my $columnFamily = shift;
    my $key = shift;
    my $column = shift;
    my $opt = shift // {};

    my $columnPath = Cassandra::ColumnPath->new({column_family => $columnFamily, column => $column});
    my $level = $self->_consistency_level_read($opt);

    $self->client->get($key, $columnPath, $level);
}

=head2 get_count
=cut

sub get_count {
    my $self = shift;

    my $columnFamily = shift;
    my $key = shift;
    my $opt = shift;

    # TODO: cache this
    my $columnParent = Cassandra::ColumnParent->new({column_family => $columnFamily});

    my $sliceRange = Cassandra::SliceRange->new($opt);
    if (defined $opt->{range}) {
        $sliceRange->{start} = $opt->{range}->[0] // '';
        $sliceRange->{finish} = $opt->{range}->[1] // '';
    } else {
        $sliceRange->{start} = '';
        $sliceRange->{finish} = '';
    }

    my $predicate = Cassandra::SlicePredicate->new;
    $predicate->{slice_range} = $sliceRange;

    my $level = $self->_consistency_level_read($opt);

    $self->client->get_count($key, $columnParent, $predicate, $level);
}

=head2 get_slice
=cut
sub get_slice {
    my $self = shift;

    my $columnFamily = shift;
    my $key = shift;
    my $opt = shift;

    # TODO: cache this
    my $columnParent = Cassandra::ColumnParent->new({column_family => $columnFamily});

    my $sliceRange = Cassandra::SliceRange->new($opt);
    if (defined $opt->{range}) {
        $sliceRange->{start} = $opt->{range}->[0] // '';
        $sliceRange->{finish} = $opt->{range}->[1] // '';
    } else {
        $sliceRange->{start} = '';
        $sliceRange->{finish} = '';
    }

    my $predicate = Cassandra::SlicePredicate->new;
    $predicate->{slice_range} = $sliceRange;

    my $level = $self->_consistency_level_read($opt);

    $self->client->get_slice($key, $columnParent, $predicate, $level);
}

=head2 multiget_count
=cut

sub multiget_count {
    my $self = shift;

    my $columnFamily = shift;
    my $keys = shift;
    my $opt = shift;

    # TODO: cache this
    my $columnParent = Cassandra::ColumnParent->new({column_family => $columnFamily});

    my $sliceRange = Cassandra::SliceRange->new($opt);
    if (defined $opt->{range}) {
        $sliceRange->{start} = $opt->{range}->[0] // '';
        $sliceRange->{finish} = $opt->{range}->[1] // '';
    } else {
        $sliceRange->{start} = '';
        $sliceRange->{finish} = '';
    }

    my $predicate = Cassandra::SlicePredicate->new;
    $predicate->{slice_range} = $sliceRange;

    my $level = $self->_consistency_level_read($opt);

    $self->client->multiget_count($keys, $columnParent, $predicate, $level);
}

=head2 multiget_slice
=cut

sub multiget_slice {
    my $self = shift;

    my $columnFamily = shift;
    my $keys = shift;
    my $opt = shift;

    # TODO: cache this
    my $columnParent = Cassandra::ColumnParent->new({column_family => $columnFamily});

    my $sliceRange = Cassandra::SliceRange->new($opt);
    if (defined $opt->{range}) {
        $sliceRange->{start} = $opt->{range}->[0] // '';
        $sliceRange->{finish} = $opt->{range}->[1] // '';
    } else {
        $sliceRange->{start} = '';
        $sliceRange->{finish} = '';
    }

    my $predicate = Cassandra::SlicePredicate->new;
    $predicate->{slice_range} = $sliceRange;

    my $level = $self->_consistency_level_read($opt);

    $self->client->multiget_slice($keys,$columnParent, $predicate, $level);
}

=head2 get_range_slices
=cut

sub get_range_slices {
    my $self = shift;

    my $columnFamily = shift;
    my $range = shift;
    my $opt = shift;

    my $predicate_args;
    # TODO: cache this
    my $columnParent = Cassandra::ColumnParent->new({column_family => $columnFamily});

    my $keyRange = Cassandra::KeyRange->new($range);

    if (exists $opt->{columns} )  {
        $predicate_args->{column_names} = $opt->{columns};
    } else {
        my $sliceRange = Cassandra::SliceRange->new($opt);
        if (defined $opt->{range}) {
            $sliceRange->{start} = $opt->{range}->[0] // '';
            $sliceRange->{finish} = $opt->{range}->[1] // '';
        } else {
            $sliceRange->{start} = '';
            $sliceRange->{finish} = '';
        }
        $predicate_args->{slice_range} = $sliceRange;
    }

    my $predicate = Cassandra::SlicePredicate->new($predicate_args);
    my $level = $self->_consistency_level_read($opt);

    $self->client->get_range_slices($columnParent, $predicate, $keyRange, $level);
}



=head2 get_indexed_slices
=cut

#[default@SpikeBL] create column family User with  column_metadata =  [
#...	{column_name: first, validation_class: UTF8Type},
#...	{column_name: last, validation_class: UTF8Type},
#...	{column_name: age, validation_class: UTF8Type, index_type: KEYS}];
#41ed0dfa-96b0-11e0-b73c-f494d99d95ad
#Waiting for schema agreement...
#... schemas agree across the cluster
#[default@SpikeBL] set User['zaphod']['first'] = 'Zaphod';
#Value inserted.
#[default@SpikeBL] set User['zaphod']['last'] = 'Beeblebrox';
#Value inserted.
#[default@SpikeBL] set User['zaphod']['age'] = '42';
# my $res1 = $c->get_indexed_slices($columnFamily,
#                                    { start_key => $key, 
#                                      count     => 5,
#                                      indexes   => [ ['age' => '42'],['second_index' => 'xxx'] ]
#                                    },
#                                    { count     => 1, 
#                                      consistency_level => 'QUORUM'
#                                    });


sub get_indexed_slices {
    my $self = shift;

    my $columnFamily = shift;
    my $indexes = shift;
    my $opt = shift;

    my $expr_list;
    my $predicate_args;
    # TODO: cache this
    my $columnParent = Cassandra::ColumnParent->new({column_family => $columnFamily});

    # indexes must have column_name op value 
    $indexes->{op} = Cassandra::IndexOperator::EQ; # only EQ is supported

    foreach (@{$indexes->{'indexes'}} ) {
	my $index_expr = Cassandra::IndexExpression->new({ column_name =>  $_->[0], op => Cassandra::IndexOperator::EQ, value => $_->[1]});
	push (@{ $expr_list->{expressions} },$index_expr);
    }

    foreach (qw/count start_key/) {
         $expr_list->{$_} = $indexes->{$_} if defined $indexes->{$_};
    }

    # ( expressions start_key count ) 
    my $index_clause = Cassandra::IndexClause->new( $expr_list );

    if (exists $opt->{columns} )  {
        $predicate_args->{column_names} = $opt->{columns};
    } else {
        my $sliceRange = Cassandra::SliceRange->new($opt);
        if (defined $opt->{range}) {
            $sliceRange->{start} = $opt->{range}->[0] // '';
            $sliceRange->{finish} = $opt->{range}->[1] // '';
        } else {
            $sliceRange->{start} = '';
            $sliceRange->{finish} = '';
        }
        $predicate_args->{slice_range} = $sliceRange;
    }

    my $predicate = Cassandra::SlicePredicate->new($predicate_args);
    my $level = $self->_consistency_level_read($opt);

    $self->client->get_indexed_slices($columnParent, $index_clause, $predicate, $level);
}


=head2 insert
=cut

sub insert {
    my $self = shift;

    my $columnFamily = shift;
    my $key = shift;
    my $opt = shift // {};

    # TODO: cache this
    my $columnParent = Cassandra::ColumnParent->new({column_family => $columnFamily});

    my $column = Cassandra::Column->new;

    while (my ($k, $v) = each %$opt) {
        $column->{name} = $k;
        $column->{value} = $v;
        $column->{timestamp} = $opt->{timestamp} // time;
    }

    my $level = $self->_consistency_level_write($opt);

    $self->client->insert($key, $columnParent, $column, $level);
}

=head2 remove
=cut

sub remove {
    my $self = shift;

    my $columnFamily = shift;
    my $key = shift;
    my $column = shift;
    my $opt = shift // {};

    my $columnPath = Cassandra::ColumnPath->new({column_family => $columnFamily});
    my $timestamp = $opt->{timestamp} // time;

    my $level = $self->_consistency_level_write($opt);

    $self->client->remove($key, $columnPath, $timestamp, $level);
}

=head2 batch_insert
=cut

sub batch_insert {
    my $self = shift;

#$client->batch_insert({Key1 => [
#			cf1 => [ foo => bar, bar => foo],
#}]);
    my $data = shift;
    my $opt = shift // {};

    my $columns;
    my $mutation_map;

    # TODO: cache this
    #my $columnParent = Cassandra::ColumnParent->new({column_family => $columnFamily});

   
    foreach my $key (keys %{$data}) {
        $columns = undef;
        foreach my $cf (keys %{$data->{$key}}) {
		foreach my $col (@{$data->{$key}->{$cf}}) {
			my $column = Cassandra::Column->new({
					name => $col->[0], 
					value => $col->[1], 
					timestamp => ( $col->[2] // time )
					});
			push (@{$columns},$column);
		}
		my $super_column = new Cassandra::SuperColumn({ name => 'SuperColumnName', columns => $columns });
		my $sc = Cassandra::ColumnOrSuperColumn->new({ super_column => $super_column});
		my $mutation = new Cassandra::Mutation({ column_or_supercolumn => $sc });
		push(@{$mutation_map->{$key}->{$cf}},$mutation );
        }
    }


    my $level = $self->_consistency_level_write($opt);

    $self->client->batch_mutate($mutation_map, $level);
}

=head2 truncate
=cut

sub truncate {
    my $self = shift;

    my $columnFamily = shift;

    $self->client->truncate($columnFamily);
}

=head2 describe_keyspace
=cut

sub describe_keyspace {
    my $self = shift;

    my $keyspace = shift;

    $self->client->describe_keyspace($keyspace);
}


=head2 describe_keyspaces
=cut

sub describe_keyspaces {
    my $self = shift;

    $self->client->describe_keyspaces();
}


=head2 describe_cluster_name
=cut

sub describe_cluster_name {
    my $self = shift;

    $self->client->describe_cluster_name();
}


=head2 describe_partitioner
=cut

sub describe_partitioner {
    my $self = shift;

    $self->client->describe_partitioner();
}


=head2 describe_ring
=cut

sub describe_ring {
    my $self = shift;

    my $keyspace = shift;

    $self->client->describe_ring($keyspace);
}


=head2 describe_snitch
=cut

sub describe_snitch {
    my $self = shift;

    $self->client->describe_snitch();
}


=head2 describe_version
=cut

sub describe_version {
    my $self = shift;

    $self->client->describe_version();
}


=head1 SEEALSO

=over
=item L<http://wiki.apache.org/cassandra/API>
=item L<http://wiki.apache.org/cassandra/ThriftInterface>
=back

=head1 AUTHOR

Gea-Suan Lin, C<< <gslin at gslin.org> >>

=head1 LICENSE AND COPYRIGHT

Copyright 2011 Gea-Suan Lin.

This software is released under 3-clause BSD license. See
L<http://www.opensource.org/licenses/bsd-license.php> for more
information.

=cut

1;
