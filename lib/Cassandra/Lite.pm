# ABSTRACT: Simple way to access Cassandra 0.7/0.8
package Cassandra::Lite;
use strict;
use warnings;

=head1 NAME

Cassandra::Lite - Simple way to access Cassandra 0.7/0.8

=head1 DESCRIPTION

This module will offer you a simple way to access Cassandra 0.7/0.8 (maybe later version).
Some parts are not same as standard API document (especially arguments order), it's because I want to keep this module easy to use.

You'll need to install L<Thrift> perl modules first to use Cassandra::Lite.

=head1 SYNOPSIS

First to initialize:

    use Cassandra::Lite;

    # Create with default options (C<keyspace> is a mantorary option):
    my $c = Cassandra::Lite->new(keyspace => 'Keyspace1');

    # Now just define $columnFamily and $key
    my $columnFamily = 'BlogArticle';
    my $key = 'key12345';

Then you can insert data:

    # Insert ("insert" is an alias of "put") it. (timestamp is optional)
    $c->put($columnFamily, $key, {title => 'testing title', body => '...'}, {timestamp => time}); # OR
    $c->insert($columnFamily, $key, {title => 'testing title', body => '...'});

And get data:

    # Get slice
    my $res1 = $c->get_slice($columnFamily, $key);
    my $res2 = $c->get_slice($columnFamily, $key, {range => ['sliceKeyStart', undef]});
    my $res3 = $c->get_slice($columnFamily, $key, {range => [undef, 'sliceKeyFinish']});
    my $res4 = $c->get_slice($columnFamily, $key, {range => ['sliceKeyStart', 'sliceKeyFinish']});

    # Get a column
    my $v1 = $c->get($columnFamily, $key, 'title');

    # Now we can search by multi-keys with same function
    my $key2 = 'key56789';
    my $res5 = $c->get($columnFamily, [$key, $key2], 'title');

    # Same reason to get_slice and get_count
    my $res6 = $c->get_slice($columnFamily, [$key, $key2], {range => ['sliceKeyStart', undef]});
    my $num1 = $c->get_count($columnFamily, [$key, $key2]);

    # Higher consistency level
    my $v2 = $c->get($columnFamily, $key, 'title', {consistency_level => 'QUORUM'}); # OR
    my $v3 = $c->get($columnFamily, $key, 'title', {consistency_level => 'ALL'});

More, to delete data:

    # Remove it ("remove" is an alias of "delete")
    $c->delete($columnFamily, $key, {timestamp => time}); # You can specify timestamp (optional) and consistency_level (optional)
    $c->remove($columnFamily, $key);

Others:

    # Change keyspace
    $c->keyspace('BlogArticleComment');

    # Get count
    my $num2 = $c->get_count('Foo', 'key1');
    my $num3 = $c->get_count('Foo', 'key2', {consistency_level => 'ALL'});

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

=head1 FUNCTION
=cut

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

=item
C<new>

All supported options:

    my $c = Cassandra::Lite->new(
                server_name => 'server1',       # optional, default to '127.0.0.1'
                server_port => 9160,            # optional, default to 9160
                username => 'username',         # optional, default to empty string ''
                password => 'xxx',              # optional, default to empty string ''
                consistency_level_read => 'ONE' # optional, default to 'ONE'
                consistency_level_write => 'ONE' # optional, default to 'ONE'
                keyspace => 'Keyspace1',
            );

So, usually we can use this in dev environment:

    my $c = Cassandra::Lite->new(keyspace => 'Keyspace1');

=cut

=item
C<delete>
=cut

sub delete {
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

=item
C<get>
=cut

sub get {
    my $self = shift;

    my $columnFamily = shift;
    my $key = shift;
    my $column = shift;
    my $opt = shift // {};

    my $columnPath = Cassandra::ColumnPath->new({column_family => $columnFamily, column => $column});
    my $level = $self->_consistency_level_read($opt);

    if ('ARRAY' eq ref $key) {
        my $columnParent = Cassandra::ColumnParent->new({column_family => $columnFamily});

        my $sliceRange = Cassandra::SliceRange->new($opt);
        $sliceRange->{start} = '';
        $sliceRange->{finish} = '';

        my $predicate = Cassandra::SlicePredicate->new;
        $predicate->{slice_range} = $sliceRange;

        return $self->client->multiget_slice($key, $columnParent, $predicate, $level);
    }

    $self->client->get($key, $columnPath, $level);
}

=item
C<get_count>
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

    if ('ARRAY' eq ref $key) {
        my $sliceRange = Cassandra::SliceRange->new($opt);
        $sliceRange->{start} = '';
        $sliceRange->{finish} = '';

        my $predicate = Cassandra::SlicePredicate->new;
        $predicate->{slice_range} = $sliceRange;

        return $self->client->multiget_count($key, $columnParent, $predicate, $level);
    }

    $self->client->get_count($key, $columnParent, $predicate, $level);
}

=item
C<get_slice>
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

    if ('ARRAY' eq ref $key) {
        return $self->client->multiget_slice($key, $columnParent, $predicate, $level);
    }

    $self->client->get_slice($key, $columnParent, $predicate, $level);
}

=item
C<insert>
=cut

sub insert {
    my $self = shift;
    $self->put(@_);
}

=item
C<put>
=cut

sub put {
    my $self = shift;

    my $columnFamily = shift;
    my $key = shift;
    my $columns = shift;
    my $opt = shift // {};

    my $level = $self->_consistency_level_write($opt);

    # TODO: cache this
    my $columnParent = Cassandra::ColumnParent->new({column_family => $columnFamily});

    my $column = Cassandra::Column->new;

    while (my ($k, $v) = each %$columns) {
        $column->{name} = $k;
        $column->{value} = $v;
        $column->{timestamp} = $opt->{timestamp} // time;

        $self->client->insert($key, $columnParent, $column, $level);
    }
}

=item
C<remove>
=cut

sub remove {
    my $self = shift;
    $self->delete(@_);
}

=head1 SEE ALSO

=over

=item Cassandra API

L<http://wiki.apache.org/cassandra/API>

=item Cassandra Thrift Interface

L<http://wiki.apache.org/cassandra/ThriftInterface>

=back

=head1 AUTHOR

Gea-Suan Lin, C<< <gslin at gslin.org> >>

=head1 LICENSE AND COPYRIGHT

Copyright 2011 Gea-Suan Lin.

This software is released under 3-clause BSD license.
See L<http://www.opensource.org/licenses/bsd-license.php> for more information.

=cut

1;
