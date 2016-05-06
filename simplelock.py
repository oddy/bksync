
import os, tempfile, errno
from   os.path import join as OPJ

if os.name == 'nt':
    import msvcrt
else:
    import fcntl

class SimpleLock(object):
    def __init__(self, resName):
        self.resName = resName   
        self.path = OPJ(tempfile.gettempdir(), self.resName)
        self.lockfile = None

    def trylock(self):
        if self.lockfile is None or self.lockfile.closed:
            self.lockfile = open(self.path, 'a')
        try:
            if os.name == 'nt':
                msvcrt.locking(self.lockfile.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                fcntl.lockf(self.lockfile, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except IOError,e:
            if e.errno in (errno.EACCES, errno.EAGAIN):
                return False
            raise

    def release(self):           
        if os.name == 'nt':
            msvcrt.locking(self.lockfile.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            fcntl.lockf(self.lockfile, fcntl.LOCK_EX | fcntl.LOCK_NB)

        if self.lockfile is not None:
            self.lockfile.close()
            self.lockfile = None

import time

def TestMain():
    print 'Creating nadger lock'
    L = SimpleLock('nadger')
    print 'trying '
    i = 0
    while not L.trylock():
        print 'retries %d' % i
        i += 1
        time.sleep(0.1)
    print 'sleeping 10s'
    time.sleep(10)
    print 'releasing'
    L.release()
    print 'done'
    return

if __name__ == '__main__':
    TestMain()