
import sys, os, shutil, ConfigParser, errno, traceback, json, signal, datetime
from os.path import join as OPJ

# Assumptions:
# - The index numbers appear to monotonically increase, no matter what I do (input data, pruning, etc)
# - the block files only ever seem to be written to and replaced, never appended or modified

# Targets, Backends, Repos
# a Target contains Repos and is accessed using a Backend

# python bksync.py target1 target2
# Targets are sections in bk.ini, backend= specifies backend to use, config is passed to backend (path, creds, etc), E.g:
#   [target1]
#   backend=local
#   path=d:\tmp\targ1

# For lifecycle considerations, a repo can be an empty dir / named bucket / etc, if necessary.
# It minimises creation-effort. Such an 'empty' repo can return an index number of 0, which will result in the algo here, copying everything into it.
# User actually has to create these empty 'repos' though. We dont do any lifecycle stuff here except Read/Update

# Typical usage: cron every 20 minutes. The backends should maintain locks on a per-repo basis, and fail to create if a lock is in use.


# --- Backends ---

def GetBackend(name):                                           # only-if-needed imports
    if name == 'local':                                         # so e.g. people who just want local-drive copy and dont have 
        import localfiles                                       # or care about zmq or boto3 deps, dont get screwed on startup
        return localfiles.LocalFilesBackend
    if name == 'zmq':                                           
        import remotezmq                                        
        return remotezmq.RemoteZMQBackend                       
    if name == 's3':                                            
        import amazons3
        return amazons3.AmazonS3Backend
    raise NotImplementedError('Unknown backend name %r' % (name,))

# --- Helpers ---

def CheckMakeDirsFor(path):
    wanted = os.path.dirname(path)
    if not os.path.isdir(wanted):
        try:
            os.makedirs(wanted)
        except OSError as e:
            if e.errno != errno.EEXIST:         # this will be an upper level perms error that the user will have to sort out.
                raise

def LocalCheckAndCopy(src, dst, cn, hdr):
    # --- OPTIMIZATION: if both are local files, use shutil to copy ---
    sPath = src.LocalPathForChunk(cn) ; dPath = dst.LocalPathForChunk(cn)
    if sPath and dPath:     
        print '%s  copying  #%s' % (hdr, cn)        
        CheckMakeDirsFor(dPath)
        shutil.copyfile(sPath, dPath)
        return True
    return False

def ChunksToCopy(repo, srcCS, dstCS):                       # in: chunk names & sizes. If there is a size mismatch, we make sure that one gets copied again
    # --- whichever is in src but not in dst ---
    chunksToCopy = set(srcCS) - set(dstCS)
    # --- if its in src and in dst BUT the sizes are different, copy it again --- 
    for cn in set(srcCS) & set(dstCS):
        if srcCS[cn] != dstCS[cn]:
            print '[%s] Warning: chunk size for #%s different - %s %s' % (repo, cn, srcCS[cn], dstCS[cn])
            chunksToCopy.add(cn)
    return chunksToCopy


# --- Algo ----
# get list of repos. propagate the ones we have in common.
# foreach repo:
#   - figure out who is To and From using index filename numbers. Greater is newer, ie From.
#   - copy needed data chunks
#   - delete unnneded data chunks
#   - copy new index files
#   - delete old index files

def main():
    signal.signal(signal.SIGINT, signal.SIG_DFL)

    # --- Read Cfg ---
    allCfg = ConfigParser.SafeConfigParser() #dict_type=AttrDict)
    allCfg.read('bk.ini')
    cfgTargets = allCfg.sections()
    print '\n-------- %s --------' % (datetime.datetime.now().isoformat().replace('T',' '))

    # --- Special case: 'server blahblah' ---
    if sys.argv[1] == 'server':
        serverBE = GetBackend('zmq')(sys.argv[2], dict(allCfg.items(sys.argv[2])), isServer=True)
        print '\nServer running.'
        serverBE.Serve()              # runs 'forever'
        return

    # --- Commandline Targets ---
    targ1 = sys.argv[1]
    if targ1 not in cfgTargets:     
        print 'Error: Target "%s" not found in bk.ini' % targ1
        return

    targ2 = sys.argv[2]
    if targ2 not in cfgTargets:   
        print 'Error: Target "%s" not found in bk.ini' % targ2
        return

    # --- Init Backends ---
    cfg1 = dict(allCfg.items(targ1))
    be1  = GetBackend(cfg1['backend'])(targ1, cfg1)
        
    cfg2 = dict(allCfg.items(targ2))
    be2  = GetBackend(cfg2['backend'])(targ2, cfg2)

    # --- Get Common Repos ---
    repos1 = be1.ListRepos()
    print '%40s : %s' % ("Repos at '%s'" % (be1,), ', '.join(repos1))
    repos2 = be2.ListRepos()
    print '%40s : %s' % ("Repos at '%s'" % (be2,), ', '.join(repos2))
    print

    repos = set(repos1) & set(repos2)
    if not repos:
        print 'Error - no common Repos (bk* folders) to sync'
        return

    for repo in repos:
        try:
            be1.SetRepo(repo)               
            be2.SetRepo(repo)

            # --- Figure out which way to sync ---      
            idx1 = be1.GetIndexNumber() 
            idx2 = be2.GetIndexNumber() 

            if idx1 == idx2:
                print '[%s] up to date, skipping' % (repo)
                continue

            if idx1 > idx2:
                src = be1 ; dst = be2 ; idxNew = str(idx1) ; idxOld = str(idx2)
            else:
                src = be2 ; dst = be1 ; idxNew = str(idx2) ; idxOld = str(idx1)

            print '[%s]  %s  (%s)  -->  %s  (%s)' % (repo, src, idxNew, dst, idxOld)

            # --- Data Chunks ---
            srcChunks = src.GetChunkNamesSizes() 
            dstChunks = dst.GetChunkNamesSizes() 

            # --- Copy new chunks src->dest ---
            chunksToCopy = ChunksToCopy(repo, srcChunks, dstChunks) 

            numc = len(chunksToCopy) ; n = 0
            for n,cn in enumerate(chunksToCopy):
                hdr = '[%s]  %5d/%-5d' % (repo, n+1, numc)
                if LocalCheckAndCopy(src, dst, cn, hdr):     continue       # Local-drives optimization

                # --- Otherwise copy the get/put way ---
                print '%s  getting  #%s' % (hdr, cn)        
                data = src.GetChunkData(cn)
                print '%s  putting  #%s' % (hdr, cn)        
                dst.PutChunkData(cn, data)

            # --- Copy Core files ---
            for fn in ['config','lock.roster','README']:
                print '[%s]               copying  %s' % (repo, fn)        
                dst.PutCoreFile(fn,      src.GetCoreFile(fn))               # Overwrite existing

            for fn in ['hints.', 'index.']:
                print '[%s]               copying  %s' % (repo, fn+idxNew)        
                dst.PutCoreFile(fn+idxNew, src.GetCoreFile(fn+idxNew))      # Copy new

            # --- Delete stuff dst ---
            chunksToDel = set(dstChunks) - set(srcChunks)
            numc = len(chunksToDel) ; n = 0
            for n,cn in enumerate(chunksToDel):
                print '[%s]  %5d/%-5d  deleting #%s' % (repo, n+1,numc, cn)
                dst.DelChunk(cn)

            for fn in ['hints.', 'index.']:
                print '[%s]               deleting %s%s' % (repo, fn, idxOld)
                dst.DelCoreFile(fn+idxOld)                                  # Delete old

        except Exception,e:
            print '[%s] ERROR: %s' % (repo, str(e))                         # yolo
            # print traceback.format_exc()
        finally:
            be1.DoneRepo()
            be2.DoneRepo()



if __name__ == '__main__':
    main()
