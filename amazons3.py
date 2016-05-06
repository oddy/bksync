
import sys, os, random, ConfigParser
from pprint import pprint

import boto3

import simplelock 

# =============================================================================================================================
# == S3 block storage
# =============================================================================================================================

# Policy: buckets must be named blah.randomshit.bbackup
#         randomshit are discarded and ignored for the purposes of matching backup repo names
#         In the interests of KISS that's all we're doing to get a lock on a repo
# Policy: buckets are flat, there is no data 'dir'. Just named corefiles and numbered chunkfiles. 
#         see how we go with that


class AmazonS3Backend(object):
    def __init__(self, targname, cfg):        
        self.targname = targname
        self.cfg = cfg
        self.allrepos = {}
        self.repo = None
        self.repoDir = None
        self.session = boto3.session.Session( 
                aws_access_key_id       = self.cfg['access_key_id'],
                aws_secret_access_key   = self.cfg['access_key'],
                region_name             = self.cfg['region']
            )
        self.s3 = self.session.resource('s3')
        self.bkt = None
        self.lock = None

    def __str__(self):
        return 'S3 %s'  % (self.targname,)
    def __repr__(self):
        return 'S3 %s @ %s'  % (self.targname, self.cfg['region'])

    def ListRepos(self):
        self.allrepos  = {}                 # bucket names: blah.randomshit.bbackup
        for bkt in self.s3.buckets.all():
            if bkt.name.endswith('.bbackup'):
                repoName = bkt.name.split('.',1)[0]
                self.allrepos[repoName] = bkt.name
        return self.allrepos.keys()

    def SetRepo(self, repo):
        if not self.allrepos:    self.ListRepos()
        if not repo in self.allrepos:  raise ValueError('repo "%s" not found' % (repo,))

        if self.lock:   
            try:                self.lock.release()
            except IOError,e:   pass
        lockRes = '%s_%s' % (self.targname, repo)
        self.lock = simplelock.SimpleLock(lockRes)
        if not self.lock.trylock():
            raise IOError("Repo %s is in use, bailing" % (lockRes,))

        self.repo = repo  
        self.bkt = self.s3.Bucket(self.allrepos[repo])     

    def DoneRepo(self):             # don't rely on this being called
        if self.lock:               # it might also get called when it's not supposed to (e.g. by finally)
            try:
                self.lock.release()
            except IOError,e:
                pass                # best effort
            self.lock = None

    def GetIndexNumber(self):
        try:
            idxObjs = self.bkt.objects.filter(Prefix='index.')
            idx = reduce(max, [int(i.key.split('.')[1]) for i in idxObjs])
        except Exception,e:
            idx = 0                 # e.g. empty repo etc. This is the Right Thing, because algo will copy all repofiles over if necessary.
        return idx

    def GetChunkNames(self):
        return [i.key for i in self.bkt.objects.all() if i.key.isdigit()]

    def GetChunkNamesSizes(self):
        return dict([(i.key, i.size) for i in self.bkt.objects.all() if i.key.isdigit()])

    def GetChunkData(self, n):
        return self.bkt.Object(n).get()['Body'].read()

    def PutChunkData(self, n, data):
        self.bkt.Object(n).put(Body=data)

    def DelChunk(self, n):
        try:
            self.bkt.Object(n).delete()
        except Exception,e:
            pass            # best effort

    def GetCoreFile(self, fname):
        return self.bkt.Object(fname).get()['Body'].read()        

    def PutCoreFile(self, fname, data):
        self.bkt.Object(fname).put(Body=data)        

    def DelCoreFile(self, fname):
        try:
            self.bkt.Object(fname).delete()
        except Exception,e:
            pass            # best effort

    def LocalPathForChunk(self, n):                                 # returns '' if this isn't a valid thing
        return ''                                                   # This isn't a local files backend


def TestMain2(cfg):
    be = AmazonS3Backend('test',cfg)
    print be.ListRepos()
    be.SetRepo('bktest')
    print be.GetIndexNumber()
    print be.GetChunkNames()
    print
    print be.GetChunkNamesSizes()
    be.PutChunkData(999, 'hello world2')
    print be.GetChunkData(999)
    be.DelChunk(999)
    be.PutCoreFile('foo.bar', 'gudday mate')
    print be.GetCoreFile('foo.bar')
    be.DelCoreFile('foo.bar')

# Mostly me learning how to boto3

def TestMain(cfg):
    # boto3.set_stream_logger(name='botocore')
    # os.environ["HTTP_PROXY"] = "http://localhost:8080"    
    # os.environ["HTTPS_PROXY"] = "https://localhost:8999"

    sess = boto3.session.Session( 
            aws_access_key_id       = cfg['access_key_id'],
            aws_secret_access_key   = cfg['access_key'],
            region_name             = cfg['region']
        )

    s3 = sess.resource('s3')

    print '\n--- List Buckets ---'

    for bucket in s3.buckets.all():
        print bucket.name

    bkt = s3.Bucket('fred4')

    # print '\n--- Key exists ---'
    # print bkt.get_key('zuxisaki')     # i dont think this works
    
    print '\n--- List objects in bucket fred4 ---'

    # len(list(bkt.objects.filter(Prefix='index')))     # One request, doesnt return tons of data!
    for i,obj in enumerate(bkt.objects.all()):          # 1348 items - 2 requests, and boto does it for me. 
        print i, obj.key, obj.size                      # does not fetch the actual object, score
        if i > 10000:  break

    print '\n--- Body reads ---'

    print obj.get()['Body'].read()                          # from iteration
    print bkt.Object('zuxisaki').get()['Body'].read()       # by name

    print '\n--- Body write ---'

    data = 'A'*128
    nm = RandChars(8)
    bkt.Object(nm).put(Body=data)
    print 'Wrote key=',nm

# Is Best rand chars
def RandChars(n):   return ''.join([ random.choice('bdfgjklmnprstvwxz')+random.choice('aiueo') for i in range(n/2) ])

if __name__ == '__main__':
    allcfg = ConfigParser.SafeConfigParser() #dict_type=AttrDict)
    allcfg.read('bk.ini')
    cfg = dict(allcfg.items('targs3'))

    # TestMain(cfg)
    TestMain2(cfg)
