# bksync
Syncer for Borg backup / Attic blockfiles

## Terms

* **Repo** : your standard borg/attic repo folder, with the index files & numbered data block files.
* **Backend** : a bksync module that handles access to repos. e.g. `local` for local files on a drive, `s3` for blobs in s3 buckets, etc
* **Target** : a 'location' holding one or more borg repos, accessed using a backend. (e.g. s3 account, local folder with one or more repos in subfolders.

## Getting Started



Run `python bksync.py target1 target2` on the commandline (or from cron, or whatever),
`target1` and `target2` refer to section headings in the `bk.ini`


