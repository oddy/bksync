# bksync
Syncer for Borg backup / Attic blockfiles

## Terms

* **Repo** : your standard borg/attic repo folder, with the index files & numbered data block files.
* **Backend** : a bksync module that handles access to repos. e.g. `local` for local files on a drive, `s3` for blobs in s3 buckets, etc
* **Target** : a 'location' holding one or more borg repos, accessed using a backend. (e.g. s3 account, local folder with one or more repos in subfolders.

## Getting Started

Syncing is an operation performed between two targets, given on the command line, e.g:

Run `python bksync.py target1 target2` 

`bk.ini` controls how backends are used to access targets containing repos. 

Target names are section names in the bk.ini, e.g:

```
[target1]
backend=local
path=/path/to/repos

[target2]
backend=local
path=/path/to/other/repos
```





