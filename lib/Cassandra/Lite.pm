# ABSTRACT: Simple way to access Cassandra 0.7
package Cassandra::Lite;
BEGIN {
  $Cassandra::Lite::VERSION = '0.0.1';
}
use strict;
use warnings;

=head1 NAME

Cassandra::Lite - Simple way to access Cassandra 0.7

=head1 VERSION

version 0.0.1

=head1 DESCRIPTION

This module will offer a simple way to access Cassandra 0.7 (maybe later version).

=head1 SYNOPSIS

    use Cassandra::Lite;

    my $c = Cassandra::Lite->new(
                server_name => 'server1',       # optional, default to '127.0.0.1'
                server_port => '9160',          # optional, default to 9160
                username => 'username',         # optional, default to empty string ''
                password => 'xxx',              # optional, default to empty string ''
                keyspace => 'Keyspace1',
            );

    my $columnFamily = 'BlogArticle';
    my $key = 'key12345';

    # Insert
    $c->insert($columnFamily, $key, {title => 'testing title', body => '...'});

    # Get slice
    my $res1 = $c->get_slice($columnFamily, $key);
    my $res2 = $c->get_slice($columnFamily, $key, {range => ['sliceKeyStart', undef});
    my $res3 = $c->get_slice($columnFamily, $key, {range => [undef, 'sliceKeyFinish']});
    my $res4 = $c->get_slice($columnFamily, $key, {range => ['sliceKeyStart', 'sliceKeyFinish']});

    # Change keyspace
    $c->keyspace('BlogArticleComment');

    ...

=cut

use Any::Moose;
has 'client' => (is => 'rw', isa => 'Cassandra::CassandraClient', lazy_build => 1);
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
use Cassandra::Constants;
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

=head2 FUNCTION get_slice
=cut

sub get_slice {
    my $self = shift;

    my $columnFamily = shift;
    my $key = shift;
    my $opt = shift;

    # TODO: cache this
    my $columnParent = Cassandra::ColumnParent->new({column_family => $columnFamily});

    my $sliceRange = Cassandra::SliceRange->new;
    if (defined $opt->{range}) {
        $sliceRange->{start} = $opt->{range}->[0] // '';
        $sliceRange->{finish} = $opt->{range}->[1] // '';
    } else {
        $sliceRange->{start} = '';
        $sliceRange->{finish} = '';
    }

    my $predicate = Cassandra::SlicePredicate->new;
    $predicate->{slice_range} = $sliceRange;

    $self->client->get_slice($key, $columnParent, $predicate);
}

=head2 FUNCTION insert
=cut

sub insert {
    my $self = shift;

    my $columnFamily = shift;
    my $key = shift;
    my $opt = shift;

    # TODO: cache this
    my $columnParent = Cassandra::ColumnParent->new({column_family => $columnFamily});

    my $now = time;
    my $column = Cassandra::Column->new;

    while (my ($k, $v) = each %$opt) {
        $column->{name} = $k;
        $column->{value} = $v;
        $column->{timestamp} = $now;
    }

    $self->client->insert($key, $columnParent, $column);
}

=head1 AUTHOR

Gea-Suan Lin, C<< <gslin at gslin.org> >>

=head1 LICENSE AND COPYRIGHT

Copyright 2011 Gea-Suan Lin.

This software is released under 3-clause BSD license. See
L<http://www.opensource.org/licenses/bsd-license.php> for more
information.

=cut

1;
