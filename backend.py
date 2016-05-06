
# This is more of an API document now, as we're not inheriting from it any more.

class Backend(object):
    def __init__(self, targname, cfg):
        self.targname = targname
        self.cfg = cfg
        self.allrepos = []
        self.repo = None
        return

    def __str__(self):
        if isinstance(self.cfg, dict) and 'type' in self.cfg:
            return 'generic (type %s)' % (self.cfg['type'],)
        else:
            return 'generic (no cfg)'

    def ListRepos(self):                          # list the BKs this backend has access to
        self.allrepos = []
        return self.allrepos

    def SetRepo(self, repo):                        # Set BK for all operations
        if not self.allrepos:    self.ListRepos()
        if not repo in self.allrepos:  raise ValueError('repo "%s" not found' % (repo,))
        self.repo = repo

    def DoneRepo(self):
        return

    def GetIndexNumber(self):                   
        raise NotImplementedError

    def GetChunkNames(self):                    # returns data chunk names 
        return []

    def GetChunkNamesSizes(self):
        return {}

    def GetChunkData(self, n):                      
        return ''

    def PutChunkData(self, n, data):
        return

    def DelChunk(self, n):
        return

    def GetCoreFile(self, fname):
        return

    def PutCoreFile(self, fname, data):
        return

    def DelCoreFile(self, fname):
        os.remove(OPJ(self.repoDir, fname))

    def LocalPathForChunk(self, n):              # only for local-drive storage - enables shutil.copyfile optimisation
        return ''
