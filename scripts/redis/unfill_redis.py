"""Delete keys from redis instance created by fill_redis.py script."""
import signal
import time

import redis


HOSTNAME = '127.0.0.1'
PORT = 6381
DB = 0
DELAY = .01  # Sleep a little to not impact redis performance

loop = True


def signal_handler(signum, frame):
    """Signal handler."""

    global loop
    print('Caught ctrl-c, finishing up.')
    loop = False


def run():
    """Run purge."""

    client = redis.Redis(host=HOSTNAME, port=PORT, db=DB)

    print 'Scanning redis keys'
    cursor = '0'
    match = 'dummy:*'

    signal.signal(signal.SIGINT, signal_handler)

    while cursor != 0 and loop:

        cursor, data = client.scan(cursor=cursor, match=match)

        for key in data:
            print (key)
            client.delete(key)
        time.sleep(DELAY)


if __name__ == '__main__':
    run()
