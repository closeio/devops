"""
Fill redis instance with 1 meg keys.

This script creates 1 MB keys with a dummy: prefix that expire in 1 hour. It
can either send the whole key value with each SET command which will also stress
the network or use a Lua script to copy a key value to the new dummy key. Copying
the key via the Lua script has negligible network impact.
"""

import signal
import time

import redis


COPY_SOURCE_KEY = 'dummy:v1:1'
TTL = 60 * 60
HOSTNAME = '127.0.0.1'
PORT = 6381
DB = 0
DELAY = .01  # Sleep a little to not impact redis performance
KEY_FORMAT = 'dummy:v1:{}'


loop = True


def signal_handler(signum, frame):
    global loop
    print 'Got signal'
    loop = False


def run():
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    client = redis.Redis(host=HOSTNAME, port=PORT, db=DB)

    copy_key = client.register_script("""local v
                                      v = redis.call("get", KEYS[1])
                                      redis.call("setex", KEYS[2], 3600, v)
                                      """)

    big_string = ''.join('1' for _ in xrange(1000000))

    i = 0
    while loop:
        i += 1
        key = KEY_FORMAT.format(i)
        print (key)
        client.setex(key, big_string, TTL)
        #copy_key(keys=[COPY_SOURCE_KEY, key], args=[])
        time.sleep(.01)


if __name__ == '__main__':
    run()
