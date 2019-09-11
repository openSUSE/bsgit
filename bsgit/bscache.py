"""On-disk cache for mapping from the build service to git

  Copyright (C) 2009  Andreas Gruenbacher <agruen@suse.de>

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation; either version 2 of the License, or (at
  your option) any later version.

  This program is distributed in the hope that it will be useful, but
  WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
  General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this library; if not, write to the Free Software Foundation,
  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
"""

import logging
import sys
import subprocess
import hashlib
import dbm
import getopt
import re

log = logging.getLogger('bscache')

#-----------------------------------------------------------------------

def compute_srcmd5(files):
    """Return the srcmd5 checksum of a ist of files."""
    hasher = hashlib.md5()
    for file in sorted(files, key=lambda a: a["name"]):
        hasher.update("%s  %s\n" % (file["md5"], file["name"]))
    return hasher.hexdigest()


def check_proc(proc, cmd):
    """Check the status of a subprocess and raise an exception on failure."""
    log.debug('cmd = %s (%s)', cmd, type(cmd))
    status = proc.wait()
    if status != 0:
        if proc.stderr is not None:
            print('stderr:\n{}'.format(proc.stderr.read()), file=sys.stderr)
        raise subprocess.CalledProcessError(status, cmd)


class BuildServiceCache:
    """On-disk cache for mapping between the MD5 hashes of various build
    service objects (files, directory listings, commits) and the corresponding
    git SHA1 hashes.
    """

    def __init__(self, name, opt_git):
        self.database_name = name
        self.opt_git = opt_git
        self.hash = dbm.open(name, 'c')

    def has_key(self, key):
        return key in self.hash

    def keys(self):
        return self.hash.keys()

    def __getitem__(self, key):
        return self.hash[key]

    def __setitem__(self, key, value):
        self.hash[key] = value

    def __delitem__(self, key):
        del self.hash[key]

    def add_blob(self, blob_sha1):
        """Add an existing git blob (file) to the cache."""
        md5 = self.add_new_blob(blob_sha1)
        self.hash["blob " + md5] = blob_sha1

    def add_new_blob(self, blob_sha1):
        hasher = hashlib.md5()
        cmd = [self.opt_git, "cat-file", "blob", blob_sha1]
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
        while True:
            data = proc.stdout.read(16384)
            if len(data) == 0:
                break
            hasher.update(data)
        proc.stdout.close()
        check_proc(proc, cmd)
        md5 = hasher.hexdigest()
        return md5

    def add_tree(self, tree_sha1, commit_sha1):
        """Add an existing git tree (directory) to the cache."""
        md5 = self.add_new_tree(tree_sha1, commit_sha1)
        self.hash["tree " + md5] = tree_sha1

    def add_new_tree(self, tree_sha1, commit_sha1):
        # FIXME: newlines in filenames would need NUL-terminated format (-z)
        # here.
        cmd = [self.opt_git, "ls-tree", tree_sha1]
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
        files = []
        for line in proc.stdout:
            mode, type, sha1, name = re.match(
                r"^(\d{6}) ([^ ]+) ([0-9a-f]{40})\t(.*)", line
            ).groups()
            if type == "blob":
                md5 = self.add_blob(sha1)
                files.append({"name": name, "md5": md5})
            elif type == "tree":
                raise IOError(
                    "Commit %s: subdirectories not supported" % commit_sha1
                )
            else:
                raise IOError(
                    "Commit %s: unexpected %s object" % (commit_sha1, type)
                )
        check_proc(proc, cmd)

        return compute_srcmd5(files)

    def add_commit(self, commit_sha1):
        """Add an existing git commit and all objects reachable from there
        to the cache.

        Note that the key for lookup here is the commit's SHA1 checksum.
        We use this to detech which commits (including all their children)
        are already in the cache.
        """
        if ("commit " + commit_sha1) not in self.hash:
            print("Caching commit " + commit_sha1)
            tree_sha1 = self.add_new_commit(commit_sha1)
            if tree_sha1 is not None:
                self.hash["commit " + commit_sha1] = tree_sha1

    def add_new_commit(self, commit_sha1):
        cmd = [self.opt_git, "cat-file", "commit", commit_sha1]
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
        tree_sha1 = None
        for line in proc.stdout:
            try:
                tag, sha1 = line.rstrip("\n").split(" ", 1)
            except ValueError:
                pass
            if tag == "tree":
                self.add_tree(sha1, commit_sha1)
                if tree_sha1 is not None:
                    raise IOError("bad commit " + sha1)
                tree_sha1 = sha1
            elif tag == "parent":
                self.add_commit(sha1)
            elif tag == "":
                proc.stdout.close()
                break
        check_proc(proc, cmd)
        return tree_sha1

    def update(self, obj):
        """Update the cache by adding all new objects reachable from obj."""
        # FIXME: instead of following a specific object reference,
        # hash the entire repository.
        cmd = [self.opt_git, "rev-parse", obj]
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
        sha1 = proc.stdout.read().rstrip("\n")
        check_proc(proc, cmd)

        cmd = [self.opt_git, "cat-file", "-t", sha1]
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
        type = proc.stdout.read().rstrip("\n")
        check_proc(proc, cmd)

        if type == "commit":
            self.add_commit(sha1)
        else:
            raise IOError("%s is not a commit object" % obj)
