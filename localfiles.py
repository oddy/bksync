
import os, errno, ConfigParser
from   os.path import join as OPJ

import simplelock

# =============================================================================================================================
# == Local Files 
# =============================================================================================================================

# We assume that the path exists, and that it has one or more dirs in it that have a dir named 'data' inside them.


# --- Can specify drives by name on Windows ---

def DriveNameToPath(drive):
    if os.name=='nt':
        import win32api
        namePaths = { win32api.GetVolumeInformation(i)[0].lower():i for i in win32api.GetLogicalDriveStrings().split('\0') if i }
        drive = drive.lower()
        if drive not in namePaths:  raise ValueError('Drive name "%s" not found' % drive)
        return namePaths[drive]
    else:
        raise NotImplementedError       # todo: do something with /media/XXX here for ubuntu


class LocalFilesBackend(object):
    def __init__(self, targname, cfg):        
        self.targname = targname
        self.cfg = cfg
        self.allrepos = []
        self.repo = None
        self.repoDir = None
        self.lock = None

        # --- build actual path ---
        pathParts = []        
        if 'drive' in cfg and os.name=='nt':    pathParts.append( DriveNameToPath(self.cfg['drive']) )
        if 'path' in cfg:                       pathParts.append( cfg['path'] )
        if not pathParts:                       raise ValueError('No drive/path params in config')
        self.rootPath = OPJ(*pathParts)
        if not os.path.exists(self.rootPath):   raise ValueError('Path %s not found' % (self.rootPath))
        
    def __str__(self):
        return 'local %s' % (self.targname,)
    def __repr__(self):
        return 'local %s @ %s' % (self.targname, self.rootPath,) 

    def ListRepos(self):
        self.allrepos  = [i for i in os.listdir(self.rootPath) if os.path.isdir(OPJ(self.rootPath, i)) and os.path.isdir(OPJ(self.rootPath, i, 'data')) ]
        return self.allrepos

    def SetRepo(self, repo):
        if not self.allrepos:           self.ListRepos()
        if not repo in self.allrepos:   raise ValueError('repo "%s" not found' % (repo,))

        if self.lock: 
            try:                self.lock.release()
            except IOError,e:   pass
        lockRes = '%s_%s' % (self.targname, repo)
        self.lock = simplelock.SimpleLock(lockRes)
        if not self.lock.trylock():
            raise IOError("Repo %s is in use, bailing" % (lockRes,))

        self.repo = repo        
        self.repoDir = OPJ(self.rootPath, self.repo)

    def DoneRepo(self):             
        if self.lock:
            try:
                self.lock.release()
            except IOError,e:
                pass                # best effort. 
            self.lock = None

    def GetIndexNumber(self):
        try:
            idxFiles = [ i for i in os.listdir(self.repoDir) if i.startswith('index.') ]    # /path/to/repo/index.17  
            idx = reduce(max, [int(i.split('.')[1]) for i in idxFiles])                     # get highest if more than one. Shouldn't Happen, but might if a previous sync was interrupted   
        except Exception,e:
            idx = 0                 # e.g. empty repo etc. This is the Right Thing, because algo will copy all repofiles over if necessary.
        return idx

    def GetChunkNames(self):
        return self.GetChunkNamesSizes().keys()

    def GetChunkNamesSizes(self):
        N = {}
        for rdir, dirs, files in os.walk(unicode( OPJ(self.repoDir, 'data') )):
            for f in files:
                try:
                    N[f] = os.path.getsize(OPJ(rdir,f))
                except TypeError:
                    continue
        return N

    def GetChunkData(self, n):
        subdir = OPJ(self.repoDir, 'data', str(int(n) / 10000))
        return open(OPJ(subdir, n), 'rb').read()

    def PutChunkData(self, n, data):
        subdir = OPJ(self.repoDir, 'data', str(int(n) / 10000))
        self.EnsureDirExists(subdir)
        with open(OPJ(subdir, n), 'wb') as f:
            f.write(data)

    def DelChunk(self, n):
        try:
            os.remove(OPJ(self.repoDir, 'data', str(int(n) / 10000), n))
        except Exception,e:
            pass            # best effort

    def GetCoreFile(self, fname):
        return open(OPJ(self.repoDir, fname),'rb').read()

    def PutCoreFile(self, fname, data):
        with open(OPJ(self.repoDir, fname), 'wb') as f:
            f.write(data)

    def DelCoreFile(self, fname):
        try:
            os.remove(OPJ(self.repoDir, fname))
        except Exception,e:
            pass            # best effort

    def LocalPathForChunk(self, n):                                 # returns '' if this isn't a valid thing
        # return ''  # for testing
        subdir = OPJ(self.repoDir, 'data', str(int(n) / 10000))
        return OPJ(subdir, n)

    def EnsureDirExists(self, wanted):
        if not os.path.isdir(wanted):
            try:
                os.makedirs(wanted)
            except OSError as e:
                if e.errno != errno.EEXIST:         # this will be an upper level perms error that the user will have to sort out.
                    raise



def TestMain(cfg):
    print 'drivename for EDriveIntel', DriveNameToPath('edriveintel')
    print
    be = LocalFilesBackend('targ3',cfg)
   
if __name__ == '__main__':
    allcfg = ConfigParser.SafeConfigParser() #dict_type=AttrDict)
    allcfg.read('bk.ini')
    cfg = dict(allcfg.items('targ3'))

    TestMain(cfg)

