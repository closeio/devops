#!/usr/bin/env python
"""
Safely scan Redis instance and report key stats.

Can also set the TTL for keys to facilitate removing data.
"""
from __future__ import absolute_import, print_function

import signal
import sys
import time

import click

import redis

loop = True


def signal_handler(signum, frame):
    """Signal handler."""

    global loop
    print('Caught ctrl-c, finishing up.')
    loop = False


def get_size(client, key, key_type):
    """Get size of key."""

    size = -1
    if key_type == b'string':
        size = client.strlen(key)
    elif key_type == b'zset':
        size = client.zcard(key)
    elif key_type == b'set':
        size = client.scard(key)
    elif key_type == b'list':
        size = client.llen(key)
    elif key_type == b'hash':
        size = client.hlen(key)

    return size


@click.command()
@click.option('--file', 'file_name', default='redis-stats.log')
@click.option('--match', default=None)
@click.option(
    '--ttl',
    'set_ttl',
    default=None,
    type=click.INT,
    help="Set TTL if one isn't already set (-1 will remove TTL)",
)
@click.option('--host', required=True)
@click.option('--port', type=click.INT, default=6379)
@click.option('--db', type=click.INT, default=0)
@click.option('--delay', type=click.FLOAT, default=0.1)
@click.option('--print', 'print_it', is_flag=True)
def run(host, port, db, delay, file_name, print_it, match, set_ttl=None):
    """Run scan."""

    if set_ttl is not None and match is None:
        print('You must specify match when setting TTLs!')
        sys.exit(1)

    client = redis.Redis(host=host, port=port, db=db)

    print('Scanning redis keys with match: %s\n' % match)
    cursor = '0'

    log_file = open(file_name, 'w')

    signal.signal(signal.SIGINT, signal_handler)

    while cursor != 0 and loop:

        cursor, data = client.scan(cursor=cursor, match=match)

        for key in data:
            key_type = client.type(key)
            size = get_size(client, key, key_type)

            ttl = client.ttl(key)
            # ttl() returns None in redis 2.x and -1 in redis 3.x for
            # keys that don't have an expiration. Normalize it here.
            if ttl is None:
                ttl = -1

            new_ttl = None
            if set_ttl == -1:
                client.persist(key)
                new_ttl = -1
            elif set_ttl is not None and ttl == -1:
                # Only change TTLs for keys with no TTL
                client.expire(key, set_ttl)
                new_ttl = set_ttl

            line = '%s %s %s %s %d' % (key, key_type, ttl, new_ttl, size,)
            log_file.write(line + '\n')
            if print_it:
                print(line)

        log_file.flush()
        time.sleep(delay)

    log_file.close()


if __name__ == '__main__':
    run()
