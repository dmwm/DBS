from __future__ import print_function
from time import sleep
import signal

class TimeoutError(Exception):
    pass

class Timeout(object):
    def __init__(self, timeout=10):
        self._timeout = timeout
        signal.signal(signal.SIGALRM, self._handle_timeout)
        signal.alarm(self._timeout)

    def __enter__(self):
        return self

    def __exit__(self, ex_type, ex_val, exc_tb):
        signal.alarm(0)
        return False

    def _handle_timeout(self, signum, frame):
        raise TimeoutError("Call did not finish in %s s." % self._timeout)

if __name__ == '__main__':
    print("Testing sleep with 1 s and 2 s timeout")
    with Timeout(2):
        sleep(1)

    print("Testing infinite sleep and 1 s timeout")
    with Timeout(1):
        while True:
            sleep(1)