"""Safely scan entire Redis instance and report key stats."""
import signal
import time

import redis


HOSTNAME = '127.0.0.1'
PORT = 6379
DB = 0

DELAY = .01  # Sleep a little to not impact redis performance

loop = True


def signal_handler(signum, frame):
    """Signal handler."""

    global loop
    print('Caught ctrl-c, finishing up.')
    loop = False


def get_size(client, key, key_type):
    """Get size of key."""

    size = -1
    if key_type == 'string':
        size = client.strlen(key)
    elif key_type == 'zset':
        size = client.zcard(key)
    elif key_type == 'set':
        size = client.scard(key)
    elif key_type == 'list':
        size = client.llen(key)
    elif key_type == 'hash':
        size = client.hlen(key)

    return size


def run():
    """Run scan."""

    client = redis.Redis(host=HOSTNAME, port=PORT, db=DB)

    print 'Scanning redis keys'
    cursor = '0'
    match = None

    log_file = file('redis-stats.log', 'w')

    signal.signal(signal.SIGINT, signal_handler)

    while cursor != 0 and loop:

        cursor, data = client.scan(cursor=cursor, match=match)

        for key in data:
            key_type = client.type(key)
            size = get_size(client, key, key_type)
            ttl = client.ttl(key)
            log_file.write('%s %s %s %d\n' % (key, key_type, str(ttl), size))
        log_file.flush()
        time.sleep(DELAY)

    log_file.close()


if __name__ == '__main__':
    run()
