"""Safely scan entire Redis instance and report key sizes."""
import itertools
import signal
import time

import redis


HOSTNAME = '127.0.0.1'
PORT = 6379
DB = 0

DELAY = .01  # Sleep a little to not impact redis performance
KEYS_PER_BATCH = 10  # How much keys to query in a single iteration

FETCH_TYPE_TTL = True  # Set this to False to make it fetch less data / go faster

loop = True


def signal_handler(signum, frame):
    """Signal handler."""

    global loop
    print('Caught ctrl-c, finishing up.')
    loop = False


def grouper(iterable, size):
    """Group items from iterable into sequences of fixed size.

    >>> list(grouper([1, 2, 3, 4, 5], 2))
    [[1, 2], [3, 4], [5]]
    """
    it = iter(iterable)
    item = list(itertools.islice(it, size))
    while item:
        yield item
        item = list(itertools.islice(it, size))


def run():
    """Run scan."""

    client = redis.Redis(host=HOSTNAME, port=PORT, db=DB)

    print 'Scanning redis keys'
    match = None
    signal.signal(signal.SIGINT, signal_handler)

    with file('redis-stats.log', 'w') as log_file:
        for keys in grouper(client.scan_iter(match=match), KEYS_PER_BATCH):
            if not loop:
                break

            pipe = client.pipeline()
            for key in keys:
                pipe.debug_object(key)
                if FETCH_TYPE_TTL:
                    pipe.type(key)
                    pipe.ttl(key)
            if FETCH_TYPE_TTL:
                for key, (r, t, ttl) in zip(keys, grouper(pipe.execute(), 3)):
                    log_file.write('%s %s %s %d\n' % (key, t, ttl, r['serializedlength']))
            else:
                for key, r in zip(keys, pipe.execute()):
                    log_file.write('%s %d\n' % (key, r['serializedlength']))
            log_file.flush()
            time.sleep(DELAY)


if __name__ == '__main__':
    run()
