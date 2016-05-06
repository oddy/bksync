
import sys, types, time, datetime
import json, traceback, pickle

import zmq
import tblib.pickling_support
tblib.pickling_support.install()            

from   bksync import GetBackend
from   localfiles import LocalFilesBackend

# =============================================================================================================================
# == Remote server (zmq sockets)
# =============================================================================================================================

# This one is basically an RPC proxy
# For clientside you use a target cfg with backend=zmq, server=blah.com etc,
# For serverside you use a target cfg with e.g. backend=local, path=/path/to etc and run it with bksync *server* targetname

class ZmqTimeout(Exception): pass

class RemoteZMQBackend(object):
    def __init__(self, targname, cfg, isServer=False):        
        self.targname = targname
        self.cfg = cfg
        self.isServer = isServer        
        self.fnCallName = ''

        self.ctx = zmq.Context()
        self.ctx.linger = 100

        if not self.isServer:
            self.sock = self.ctx.socket(zmq.DEALER)
            self.sock.linger = 100
            self.sock.connect('tcp://%s:%s' % (self.cfg['server'],self.cfg.get('port',7677)))  # this times out with EINVAL when no internet 
            self.poller = zmq.Poller()
            self.poller.register(self.sock, zmq.POLLIN)
        else:
            self.sock = self.ctx.socket(zmq.ROUTER)
            self.sock.linger = 100
            self.sock.bind('tcp://*:%s' % (self.cfg.get('port',7677)))
            self.poller = zmq.Poller()
            self.poller.register(self.sock, zmq.POLLIN)
            self.be = GetBackend(self.cfg['backend'])(self.targname, self.cfg)         
            self.inTime = time.time()
            self.inactiveLimit = int(self.cfg.get('inactivelimit',0))
            print 'inactivelimit ',self.inactiveLimit

    def __str__(self):
        if self.isServer:       return 'zmqSrv %s' % (self.targname,)
        else:                   return 'zmqCli %s' % (self.targname,)

    def __repr__(self):
        if self.isServer:       return 'zmqSrv %s @ %s' % (self.targname, self.cfg['path']) 
        else:                   return 'zmqCli %s > %s' % (self.targname, self.cfg['server'],)

    # --- Serverside ---

    def Serve(self):        # server serving mainloop
        while 1:
            if not self.poller.poll(1000):
                if self.inactiveLimit and time.time() - self.inTime > self.inactiveLimit:
                    print 'Server inactive for %2.2fs, shutting down' % self.inactiveLimit
                    return
                continue
            msg = self.sock.recv_multipart()
            if time.time() - self.inTime > 900:   print '\n--- %s ---' % (datetime.datetime.now().isoformat().replace('T',' '))
            self.inTime = time.time()
            try:                
                zmqFrom = msg[0]
                fnName = msg[1]
                # print '\nMessage ',repr(msg)[:120]
                print ' -> ',fnName
                args = StrsToPys(msg[2:])                  # Note no keyword arguments
                fn = getattr(self.be, fnName)
                # --------------------------------------------------------
                ret = fn(*args)
                # --------------------------------------------------------                  
                zOK = [zmqFrom, 'ok']
                print '     <- %s' % (str(ret)[:20])        # todo: make this useful, rather than spamming binary into log like now
                zOK.extend(PysToStrs([ret]))
                self.sock.send_multipart(zOK)
            except Exception,e:
                print 'Failed processing message - error %s\nmsg: %r' % (str(e),msg)
                print traceback.format_exc()
                self.sock.send_multipart([zmqFrom, 'err', pickle.dumps(sys.exc_info())])
                continue

    # --- Clientside ---

    def ClientCall(self, *args):
        # print '\nClientCall called, fnCallName is ',self.fnCallName
        snd = [self.fnCallName]
        snd.extend(PysToStrs(args))

        self.sock.send_multipart(snd)
        r = self.poller.poll(3000 if self.fnCallName=='ListRepos' else 60000)   # for fast detection of no server (aka connect timeout)
        if not r:
            raise ZmqTimeout("Timed out waiting for reply") 
        ret = self.sock.recv_multipart()
        if ret[0] == 'err':
            r1 = pickle.loads(ret[1])
            print '--- SERVER SIDE exception ---'
            raise r1[1], None, r1[2]  

        #print 'ret[1] is %r' % (ret[1:])
        return StrsToPys(ret[1:])[0]            # only ever returning 1 item (which may be a list)

    def __getattr__(self, attName):        
        # Note using LocalFilesBackend here, cant use self.be because it doesnt exist clientside & we'd just get called again hence recursion
        #      The proper soln = qry the server for available callables but that = way more complex, messages etc, so CBF right now.
        if not (hasattr(LocalFilesBackend, attName) and callable(getattr(LocalFilesBackend, attName))):
            raise AttributeError("'%s' object has no attribute '%s'" % ('RemoteZMQBackend', attName))
        # we return the att to the caller, who then presumably calls it straight away
        # Which means we probably have to set the fnName in here for ClientCall to use
        self.fnCallName = attName
        return self.ClientCall

    def LocalPathForChunk(self, n):                             # Deliberate override to
        return ''                                               # tell parent algo we are not a local-disk system  

    # Note if we want to handle this using __getattr__, then we cant have it inheriting from Backend.
    # Because the parent methods will be called before __getattr__ is hit.
        

# --- Conversion functions for 'serialization' ---

def PysToStrs(pys):         # [pyObjs] -> ['I','100','S','hello','L','["1","2"]']
    s = []
    for obj in pys:
        if isinstance(obj, list):   s.extend(['L',json.dumps(obj)])   ; continue
        if isinstance(obj, int):    s.extend(['i', str(obj)])         ; continue
        if isinstance(obj, long):   s.extend(['I', str(obj)])         ; continue
        if isinstance(obj, dict):   s.extend(['D',json.dumps(obj)])   ; continue
        #if isinstance(obj, types.NoneType): s.extend(['n', ''])      ; continue
        s.extend(['s',str(obj)])
    return s

def StrsToPys(strs):        # and vice versa
    o = []
    for typ,dat in zip(strs[::2], strs[1::2]):   # is there a better way than this?
        if typ == 'L':  o.append(json.loads(dat))
        if typ == 'I':  o.append(long(dat))  
        if typ == 'i':  o.append(int(dat))                           
        if typ == 's':  o.append(str(dat)) 
        if typ == 'D':  o.append(json.loads(dat))
        #if typ == 'n':  o.append(None)
    return o                  
