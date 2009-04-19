#!/usr/bin/python

"""Import packages from the build service into git.

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

import sys
from sys import stderr
import hashlib
import re
import getopt
import subprocess
from subprocess import PIPE
from os import (environ, mkdir, chdir, makedirs, unlink)
from os.path import (dirname, basename)
from errno import ENOENT
from urllib2 import HTTPError
import osc.conf
import osc.core
try:
    from xml.etree import cElementTree
except ImportError:
    import cElementTree
from bsgit.bscache import BuildServiceCache, compute_srcmd5, check_proc

import pdb  # Python Debugger
#pdb.set_trace()

#=======================================================================

opt_depth = sys.maxint
opt_git = 'git'
opt_force = False
opt_verbose = False
opt_apiurl = None

#-----------------------------------------------------------------------

bscache = None

#=======================================================================

def git(*args):
    """Run a simple git command (without little standard input and output)."""
    cmd = [opt_git]
    cmd.extend(args)
    proc = subprocess.Popen(cmd, stdout=PIPE, stderr=PIPE)
    result = proc.stdout.read()
    message = proc.stderr.read()
    status = proc.wait()
    if status != 0:
	if (result):
	    message = result + message
	raise IOError(message.rstrip('\n'))
    return result.rstrip('\n')

def get_rev_info(rev):
    """Figure out which branches etc. belong to a given revision."""
    try:
	branch = git('rev-parse', '--verify', '--symbolic-full-name', rev)
	branch = re.sub('^refs/heads/', '', branch)
	remote_branch = git('config', '--get', 'branch.%s.merge' % branch)
	server, project, package = \
	    re.match('^refs/remotes/([^/]+)/(.*)/(.*)',
		     remote_branch).groups()
	project = project.replace('/', ':')
	if opt_apiurl:
	    apiurl = opt_apiurl
	else:
	    for url in ('https://' + server, 'http://' + server):
		if url in osc.conf.config['api_host_options']:
		    apiurl = url
		    break
	return apiurl, project, package, branch, remote_branch
    except:
	raise IOError('Cannot determine the project and package of ' + rev)

def git_get_sha1(branch):
    """Get the SHA1 hash of the head of the specified branch."""
    try:
	commit_sha1 = git('rev-parse', branch)
	return commit_sha1
    except EnvironmentError:
	return None

#-----------------------------------------------------------------------

def get_xml_root(apiurl, rel, query=None):
    """Run a build service query and return the XML root element
    of the result.
    """
    url = osc.core.makeurl(apiurl, rel, query)
    if opt_verbose:
	print "-- GET " + url
    file = osc.core.http_GET(url)
    return cElementTree.parse(file).getroot()

#-----------------------------------------------------------------------

def map_login_to_user(apiurl, login):
    """Map a build service account name to the user's real name and email."""
    if login == 'unknown':
	return 'unknown <UNKNOWN>'
    try:
	email = bscache['email ' + login]
	name = bscache['realname ' + login]
    except KeyError:
	user_info = get_user_info(apiurl, login)
	email = user_info['email']
	bscache['email ' + login] = email
	bscache['login ' + email] = login
	try:
	    name = user_info['realname']
	    bscache['realname' + login] = name
	except KeyError:
	    name = login
    return name, email

def map_email_to_login(apiurl, email):
    """Map an email address to a build service account name."""
    if email == 'UNKNOWN':
	return 'unknown';
    try:
	login = bscache['login ' + email]
	return login
    except KeyError:
	raise IOError("Cannot map email '%s' to a build service acount name. "
		      "Please use the usermap command." % email)

def get_user_info(apiurl, login):
    """Retrieve a build service user's details (email and realname).

    https://api.opensuse.org/person/LOGIN
      <person>
	<login>...</login>
	<email>...</email>
	<realname>...</realname>
      </person>

    Returns:
    {'email': ..., 'realname': ...}
    """
    if login == 'unknown':
	return {}
    try:
	return get_user_info.info[login]
    except KeyError:
	info = get_new_user_info(apiurl, login)
	get_user_info.info[login] = info
	try:
	    email = info['email']
	    stored_login = None
	    try:
		stored_login = bscache['login ' + email]
	    except KeyError:
	        pass
	    if login != stored_login:
		bscache['login ' + email] = login
	except KeyError:
	    pass
	return info
get_user_info.info = {}

def get_new_user_info(apiurl, login):
    root = get_xml_root(apiurl, ['person', login])
    info = {}
    for name in ('email', 'realname'):
	try:
	    value = root.find(name).text
	    if value:
		info[name] = value
	except AttributeError:
	    pass
    return info

#-----------------------------------------------------------------------

def get_package_status(apiurl, project, package, rev=None):
    """Retrieve the status of a package (optionally, of a given revision).

    https://api.opensuse.org/source/PROJECT/PACKAGE
      <directory name="PACKAGE" srcmd5="..." ...>
	<entry name="..." md5="..." size="..." mtime="..." />
	...
      </directory>

    Returns:
    {'rev': ..., 'project': ..., 'package': ..., 'srcmd5': ..., ...,
     'files': [{'name': ..., 'md5': ...}, ...]}
    ...
    """
    server = re.sub('.*://', '', apiurl)
    key = server + '/' + project + '/' + package
    if rev != None:
	key = key + '/' + rev
    try:
	return get_package_status.status[key]
    except KeyError:
	status = get_new_package_status(apiurl, project, package, rev)
	get_package_status.status[key] = status
	if rev == None:
	    rev = status['rev']
	    key = key + '/' + rev
	    get_package_status.status[key] = status
	return status
get_package_status.status = {}

def get_new_package_status(apiurl, project, package, rev):
    query = None
    if rev != None:
	query = 'rev=' + rev
    root = get_xml_root(apiurl, ['source', project, package], query)
    status = {'project' : project, 'package' : package}
    for name in ('rev', 'srcmd5', 'tproject', 'tpackage'):
	try:
	    value = root.get(name)
	    if value:
		status[name] = value
	except AttributeError:
	    pass
    files = []
    for node in root.findall('entry'):
	file = {}
	file['name'] = node.get('name')
	file['md5'] = node.get('md5')
	files.append(file)
    status['files'] = files
    return status

#-----------------------------------------------------------------------

def get_revision(apiurl, project, package, rev=None):
    """Retrieve the history of a package (optionally, until a given revision).

    https://api.opensuse.org/source/PROJECT/PACKAGE/_history
      <revisionlist>
	<revision rev="..." vrev="...">
	  <srcmd5>...</srcmd5>
	  <version>...</version>
	  <time>...</time>
	  <user>...</user>
	  <comment>... checkin.</comment>
	</revision>
	...
      </revisionlist>

    Returns:
    {'rev': ..., 'srcmd5': ..., 'time': ..., 'user': ..., 'comment': ...,
     'parent': {'srcmd5': ..., ...}}
    """
    server = re.sub('.*://', '', apiurl)
    key = server + '/' + project + '/' + package
    try:
	history = get_revision.history[key]
    except KeyError:
	history = get_revisions(apiurl, project, package)
	get_revision.history[key] = history
    return history[rev]
get_revision.history = {}

def get_revision_key(apiurl, project, package, rev):
    """Return the key under which a given revision is stored in bscache."""
    server = re.sub('.*://', '', apiurl)
    return 'revision ' + server + '/' + project + '/' + package + '/' + rev

def get_revisions(apiurl, project, package):
    root = get_xml_root(apiurl, ['source', project, package, '_history'])

    head = None
    history = {}
    need_to_fetch = False
    for node in root.findall('revision'):
	revision = {}
	revision['rev'] = node.get('rev')
	for name in ('srcmd5', 'time', 'user', 'comment'):
	    try:
		value = node.find(name).text
	        revision[name] = value
	    except AttributeError:
		pass

	if head:
	    revision['parent'] = head
	head = revision
	rev = revision['rev']
	history[rev] = head

	# Figure out if this revision is known already.  If it is, we need
	# to connect to it when fetching descendants.
	if need_to_fetch:
	    head['need_to_fetch'] = True
	revision_key = get_revision_key(apiurl, project, package, rev)
	try:
	    if not opt_force:
		commit_sha1 = bscache[revision_key]
		head['commit_sha1'] = commit_sha1
		need_to_fetch = True
	except KeyError:
	    pass

    if head:
	history[None] = head
    return history

#=======================================================================

def guess_link_target(apiurl, tproject, tpackage, revision):
    """Guess which revision (i.e., srcmd5) the given source link refers to.

    This is pretty primitive, and not guaranteed to do the right thing.  The
    problem here is that the build service does not record which revision of
    a package a link was generated against.
    """
    # FIXME: Check for rev=... tags and use them if present !!!
    try:
	time = revision['time']
	trevision = get_revision(apiurl, tproject, tpackage)
	while time < trevision['time']:
	    trevision = trevision['parent']
	return trevision
    except KeyError:
	return None

def parent_links_to_same_target(revision, trevision):
    """Check if the parent of the given revision links to the same target
    revision (in which case the target revision is not a parent of the given
    revision).
    """
    return 'parent' in revision and \
	   revision['parent']['time'] >= trevision['time']

#-----------------------------------------------------------------------

def fetch_files(apiurl, project, package, rev, files):
    """Fetch a list of files from the specified project and revision."""
    for file in files:
	sha1 = fetch_file(apiurl, project, package, rev,
			  file['name'], file['md5'])
	file['sha1'] = sha1

def fetch_file(apiurl, project, package, rev, name, md5):
    """Fetch a file (unless it is already known)."""
    try:
	sha1 = bscache['blob ' + md5]
    except KeyError:
	sha1 = fetch_new_file(apiurl, project, package, rev, name, md5)
	bscache['blob ' + md5] = sha1
    return sha1

def fetch_new_file(apiurl, project, package, rev, name, md5):
    """Fetch a file.

    https://api.opensuse.org/source/PROJECT/PACKAGE/FILE&rev=REV
    """
    query = 'rev=' + rev
    url = osc.core.makeurl(apiurl,
			   ['source', project, package, name],
			   query=query)
    if opt_verbose:
	print "-- GET " + url
    file = osc.core.http_GET(url)
    cmd = [opt_git, 'hash-object', '-w', '--stdin']
    proc = subprocess.Popen(cmd, stdin=PIPE, stdout=PIPE)
    hasher = hashlib.md5()
    while True:
	data = file.read(16384)
	if len(data) == 0:
	    break
	proc.stdin.write(data)
	hasher.update(data)
    if hasher.hexdigest() != md5:
	proc.kill()
	raise IOError('MD5 checksum mismatch')
    proc.stdin.close()
    sha1 = proc.stdout.read().rstrip('\n')
    check_proc(proc, cmd)
    return sha1

def list_tree(commit_sha1, tree_sha1):
    cmd = [opt_git, 'ls-tree', tree_sha1]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    files = []
    for line in proc.stdout:
	mode, type, sha1, name = \
	    re.match('^(\d{6}) ([^ ]+) ([0-9a-f]{40})\t(.*)', line).groups()
	if type == 'blob':
	    files.append({'name': name, 'sha1': sha1})
	    # We won't need the MD5 hash ...
	elif type == 'tree':
	    raise IOError('Commit %s: subdirectories not supported' %
			  commit_sha1)
	else:
	    raise IOError('Commit %s: unexpected %s object' %
			  (commit_sha1, type))
    return files

def apply_patch_to_index(apiurl, project, package, rev, name):
    """Apply a patch in a source link to the git index."""
    url = osc.core.makeurl(apiurl, ['source', project, package, name],
			   query='rev=' + rev)
    if opt_verbose:
	print "-- GET " + url
    file = osc.core.http_GET(url)
    cmd = [opt_git, 'apply', '-p0', '--cached', '--whitespace=nowarn']
    proc = subprocess.Popen(cmd, stdin=PIPE)
    while True:
	data = file.read(16384)
	if len(data) == 0:
	    break
	proc.stdin.write(data)
    proc.stdin.close()
    status = proc.wait()
    if status != 0:
	raise IOError('Failed to apply %s/%s/%s (%s); giving up.' % \
		      (project, package, name, rev))
    check_proc(proc, cmd)

def expand_link(apiurl, project, package, revision, trevision):
    """Expand a source link and create a git tree object from the result."""
    status = revision['status']
    tproject = status['tproject']
    tpackage = status['tpackage']
    trev = trevision['rev']

    rev = status['rev']
    if opt_verbose:
	print "Expanding %s/%s (%s) against %s/%s (%s)" % \
	    (project, package, rev, tproject, tpackage, trev)

    query = 'rev=' + rev
    root = get_xml_root(apiurl, ['source', project, package, '_link'], query)

    tree_sha1 = bscache['tree ' + trevision['srcmd5']]
    old_files = list_tree(trevision['commit_sha1'], tree_sha1)
    patches = root.find('patches')
    if patches:
	patches_apply = patches.findall('apply')
	for node in patches.findall('delete'):
	    name = node.get('name')
	    old_files = [f for f in old_files if f['name'] != name]
    else:
	patches_apply = []

    new_files = [f for f in status['files'] if f['name'] != '_link']
    for patch in patches_apply:
	name = patch.get('name')
	new_files = [f for f in new_files if f['name'] != name]
    fetch_files(apiurl, project, package, rev, new_files)

    tree_sha1 = create_tree(old_files + new_files)

    if patches_apply:
	# Make git use a temporary index file.
	environ['GIT_INDEX_FILE'] = '.osgit'

	git('read-tree', tree_sha1)
	# FIXME: Is there a way to garbage collect tree_sha1 right away?
	for patch in patches_apply:
	    name = patch.get('name')
	    apply_patch_to_index(apiurl, project, package, rev, name)
	tree_sha1 = git(['write-tree'])

	unlink('.osgit')
    return tree_sha1

def create_tree(files):
    """Create a git tree object from a list of files."""
    cmd = [opt_git, 'mktree']
    proc = subprocess.Popen(cmd, stdin=PIPE, stdout=PIPE)
    for file in sorted(files, cmp=lambda a,b: cmp(a['name'], b['name'])):
	line = '100644 blob %s\t%s\n' % (file['sha1'], file['name'])
        proc.stdin.write(line)
    proc.stdin.close()
    tree_sha1 = proc.stdout.read().rstrip('\n')
    check_proc(proc, cmd)
    return tree_sha1

def create_commit(apiurl, tree_sha1, revision):
    """Create a git commit from a tree object and a build service revision."""
    cmd = [opt_git, 'commit-tree', tree_sha1]
    for parent in revision['parents']:
	cmd.extend(['-p', parent['commit_sha1']])

    name, email = map_login_to_user(apiurl, revision['user'])
    time = revision['time']
    environ['GIT_AUTHOR_NAME'] = name
    environ['GIT_COMMITTER_NAME'] = name
    environ['GIT_AUTHOR_EMAIL'] = email
    environ['GIT_COMMITTER_EMAIL'] = email
    environ['GIT_AUTHOR_DATE'] = time
    environ['GIT_COMMITTER_DATE'] = time
    proc = subprocess.Popen(cmd, stdin=PIPE, stdout=PIPE)
    if 'comment' in revision:
	proc.stdin.write(revision['comment'])
    proc.stdin.close()
    commit_sha1 = proc.stdout.read().rstrip('\n')
    check_proc(proc, cmd)
    return commit_sha1

def fetch_revision(apiurl, project, package, revision):
    """Fetch one revision, including the files in it."""
    revision_key = get_revision_key(apiurl, project, package, revision['rev'])
    try:
	commit_sha1 = bscache[revision_key]
    except KeyError:
	rev = revision['rev']
	print "Fetching %s/%s (%s)" % (project, package, rev)
	srcmd5 = revision['srcmd5']
	try:
	    tree_sha1 = bscache['tree ' + srcmd5]
	except KeyError:
	    if 'target' in revision:
		trevision = revision['target']
		tree_sha1 = expand_link(apiurl, project, package,
					revision, trevision)
	    else:
		files = revision['status']['files']
		if compute_srcmd5(files) != srcmd5:
		    raise IOError('MD5 checksum mismatch')
		fetch_files(apiurl, project, package, rev, files)
		tree_sha1 = create_tree(files)
	    bscache['tree ' + srcmd5] = tree_sha1
	commit_sha1 = create_commit(apiurl, tree_sha1, revision)
	bscache[revision_key] = commit_sha1

	# Add a sentinel which tells us that the MD5 hashes of the objects
	# in this commit are in bscache.
	bscache['commit ' + commit_sha1] = tree_sha1
    revision['commit_sha1'] = commit_sha1
    if opt_verbose:
	print "Storing %s/%s (%s) as %s" % (project, package, revision['rev'],
					    commit_sha1)
    return commit_sha1

def fetch_revision_rec(apiurl, project, package, revision, depth):
    """Fetch a revision and its children, up to the defined maximum depth.
    Reconnect to parents further up the tree if they are already known.
    """
    parents = []
    if 'parent' in revision and (depth > 1 or 'need_to_fetch' in revision):
	parent = revision['parent']
	fetch_revision_rec(apiurl, project, package, parent, depth - 1)
	parents.append(parent)

    try:
	commit_sha1 = revision['commit_sha1']
	# Apparently, we have this revision already.
	return commit_sha1
    except KeyError:
	pass

    rev = revision['rev']
    status = get_package_status(apiurl, project, package, rev)
    revision['status'] = status
    if 'tproject' in status:
	tproject = status['tproject']
	tpackage = status['tpackage']
	trevision = guess_link_target(apiurl, tproject, tpackage, revision)
	if not trevision:
	    print >> stderr, 'Warning: cannot expand revision %s of source ' \
			     'link %s/%s: no suitable link target found.' % \
		(rev, project, package)
    else:
	trevision = None
    if trevision:
	revision['target'] = trevision
	trev = trevision['rev']
	#status['trev'] = trev
	fetch_package(apiurl, tproject, tpackage, depth - 1, trev)

	if not parent_links_to_same_target(revision, trevision):
	    parents.append(trevision)
    revision['parents'] = parents
    commit_sha1 = fetch_revision(apiurl, project, package, revision)

def mark_as_needed_rec(rev, revision):
    """Mark all revisions of to rev as needed."""
    # FIXME: If we end up further back in the history than any known revisions,
    # we need to refetch all the revisions.  (Unset all the 'commit-sha1's in
    # that case!)
    if revision['rev'] == rev or \
       ('parent' in revision and
        mark_as_needed_rec(rev, revision['parent'])):
	revision['need_to_fetch'] = True
	return True
    return False

def fetch_package(apiurl, project, package, depth, need_rev=None):
    """Fetch a package, up to the defined maximum depth, but at least including
    the revision with the specified rev.
    """
    status = get_package_status(apiurl, project, package)
    rev = status['rev']
    if rev == 'upload':
	# We are in the middle of an upload, or an upload has not finished:
	# get the latest revision from the package history and ignore the
	# uploaded files.
	revision = get_revision(apiurl, project, package)
	rev = revision['rev']

    if opt_force:
	commit_sha1 = None
    else:
	try:
	    revision_key = get_revision_key(apiurl, project, package, rev)
	    commit_sha1 = bscache[revision_key]
	except KeyError:
	    commit_sha1 = None

    if not commit_sha1:
	revision = get_revision(apiurl, project, package, rev);
	if need_rev:
	    mark_as_needed_rec(need_rev, revision)
	fetch_revision_rec(apiurl, project, package, revision, depth)
	commit_sha1 = revision['commit_sha1']

    remote_branch = remote_branch_name(apiurl, project, package)
    sha1 = git_get_sha1(remote_branch)
    if commit_sha1 != sha1:
	update_branch(remote_branch, commit_sha1)

    return commit_sha1

def remote_branch_name(apiurl, project, package):
    """Return the branch name we create for keeping track of the package's
    state in build service."""
    url = osc.core.makeurl(apiurl, [project.replace(':', '/'), package])
    return 'refs/remotes/' + re.sub('^.*://', '', url)

def update_branch(branch, commit_sha1):
    """Update a branch to point to the given commit.

    Note: we would usually do this with 'git branch BRANCH COMMIT_SHA1', but
    this always creates branches under refs/heads/, and we don't want this
    for the remote branches.
    """

    git_dir = git('rev-parse', '--git-dir')
    path = git_dir + '/' + branch
    try:
	file = open(path, "w")
    except IOError, error:
	if error.errno == ENOENT:
	    makedirs(dirname(path))
	    file = open(path, "w")
	else:
	    raise
    file.write(commit_sha1 + '\n')
    return branch

def fetch_command(args):
    """The fetch command."""
    git('rev-parse', '--is-inside-work-tree')
    if len(args) <= 1:
	if len(args) == 0:
	    branch = 'HEAD'
	else:
	    branch = args[0]
	apiurl, project, package, branch, remote_branch = \
	    get_rev_info(branch)
    else:
	if opt_apiurl:
	    apiurl = opt_apiurl
	else:
	    apiurl = osc.conf.config['apiurl']
	project, package = args
	branch = package

    # Add any objects added to bscache in the meantime.
    if git_get_sha1(branch):
	bscache.update(branch)

    commit_sha1 = fetch_package(apiurl, project, package, opt_depth)

    remote_branch = remote_branch_name(apiurl, project, package)
    sha1 = git_get_sha1(branch)
    if sha1 == None:
	git('branch', '--track', branch, remote_branch)
	print "Branch '%s' created." % branch
    elif sha1 == commit_sha1:
	print "Already up-to-date."
    else:
	print "Branch '%s' differs from the remote branch." % branch
    try:
	git('rev-parse', '--verify', 'HEAD')
    except IOError:
	git('checkout', '-f', branch)
    return branch

def pull_command(args):
    """The pull command."""
    apiurl, project, package, branch, remote_branch = \
	get_rev_info('HEAD')

    # Add any objects added to bscache in the meantime.
    bscache.update(branch)

    commit_sha1 = fetch_package(apiurl, project, package, opt_depth)

    sha1 = git_get_sha1(branch)
    git('rebase', remote_branch)
    new_sha1 = git_get_sha1(branch)
    if sha1 == new_sha1:
	print "Already up-to-date."
    else:
	print "Branch '%s' updated." % branch

def usermap_command(args):
    """The usermap command."""
    if len(args) == 0:
	logins = []
	for key in bscache.keys():
	    if key[0:6] == 'email ':
		logins.append(key[6:])
	for login in sorted(logins):
	    usermap_command([login])
	return

    login = args[0]
    if len(args) == 1:
	try:
	    email = bscache['email ' + login]
	    try:
		realname = bscache['realname ' + login]
	    except KeyError:
		realname = None
		pass
	except KeyError:
	    email = None
	aliases = []
	for key in bscache.keys():
	    if key[0:6] == 'login ' and key[6:] != email and \
	       bscache[key] == login:
		aliases.append(key[6:])
	if email == None:
	    if len(aliases) == 0:
		return
	    else:
		email='?'
	if realname:
		email = '"' + realname + ' <' + email + '>"'
	print login + ' ' + email + ' ' + ' '.join(aliases)
    else:
	first_email = True
	for email in args[1:]:
	    realname = None
	    match = re.match('^([^<>]+) <([^<>]+@[^<>]+)>$', email)
	    if match:
		realname, email = match.groups()
	    else:
		match = re.match('^<([^<>]+@[^<>]+)>$', email)
		if match:
		    email = match.groups()[0]
		else:
		    match = re.match('^([^<>]+@[^<>]+)$', email)
		    if match:
			email = match.groups()[0]
		    else:
			raise IOError("Cannot parse '%s'" % email)
	    if first_email:
		bscache['email ' + login] = email
		if realname:
		    bscache['realname ' + login] = realname
		else:
		    del bscache['realname ' + login]
		first_email = False
	    bscache['login ' + email] = login

def dump_command(args):
    """The dump command."""
    for key in bscache.keys():
	print "%s %s" % (key, bscache[key])

def usage(status):
    print """Usage: %s [options] <command> [args]

Import build service packages into git.

Commands are:
    fetch, fetch <branch>, fetch <project> <package>
	Update the remote branch tracking the specified <project> and
	<package>.  If no project and package is specified, the default
	is to fetch the remote branch that the current branch tracks
	(refs/remotes/<server>/<project>/<package>).

	When a branch point is hit (i.e., a revision that creates a new link
	or updates an existing link), the target package is fetched as well.

    pull
	Do a fetch of the remote branch that the current branch is tracking,
	followed by a rebase of the current branch.

    usermap <login> [<email> ...]
	Show or define which email addresses map to a build service account.
	The first address is used for mapping from account name to email
	address.  Any additional email addresses will map to the same build
	service account.  Instead of an email address, a full name plus email
	address can be given in the form "Full Name <email>".

    dump
	Dump the build service cache (for debugging).

Options are:
    --apiurl=<apiurl>, -A <apiurl>
	Use the specified protocol/server instead of the default from .oscrc.

    --depth=<depth>
	Create a shallow clone with a history truncated to the specified
	number of revisions.  (Note that the --force option is required for
	later increasing the depth.)

    -f, --force
	Recreate all commits even if they appear to be present already.  Files
	still remain cached: we trust the MD5 algorithm to be free enough of
	colisions for our purposes.  (Remove .git/bscache to recompute the
	MD5 checksums.)

    -t, --traceback
	Print a call trace in case of an error (for debugging).

    --verbose
	Be verbose about which requests are being made to the build service.
	""" \
	% basename(sys.argv[0])
    sys.exit(status)

def main():
    opt_traceback = False
    need_bscache = False
    need_osc_config = False

    try:
	opts, args = getopt.gnu_getopt(sys.argv[1:], 'A:tfvh', \
				       ['help', 'depth=', 'git=', 'force',
				        'apiurl=', 'traceback', 'verbose'])
    except getopt.GetoptError, err:
	print err
	usage(2)
    for opt, arg in opts:
	if opt in ('-h', '--help'):
	    usage(0)
	elif opt == '--depth':
	    global opt_depth
	    opt_depth = int(arg)
	elif opt in ('-f', '--force'):
	    global opt_force
	    opt_force = True
	elif opt == '--git':
	    global opt_git
	    opt_git = arg
	elif opt in ('-A', '--apiurl'):
	    global opt_apiurl
	    opt_apiurl = arg
	elif opt in ('-t', '--traceback'):
	    opt_traceback = True
        elif opt in ('-v', '--verbose'):
	    global opt_verbose
	    opt_verbose = True

    command = None
    if len(args) >= 1:
	if args[0] == 'fetch' and len(args) >= 1 and len(args) <= 3:
	    need_osc_config = True
	    need_bscache = True
	    command = fetch_command
	elif args[0] == 'pull' and len(args) == 1:
	    need_osc_config = True
	    need_bscache = True
	    command = pull_command
	elif args[0] == 'dump' and len(args) == 1:
	    need_bscache = True
	    command = dump_command
	elif args[0] == 'usermap':
	    need_bscache = True
	    command = usermap_command
    if command == None:
	usage(2)

    try:
	if need_osc_config:
	    osc.conf.get_config()

	if need_bscache:
	    global bscache
	    git_dir = git('rev-parse', '--git-dir')
	    bscache = BuildServiceCache(git_dir + '/bscache', opt_git)

	command(args[1:])
    except (KeyboardInterrupt, EnvironmentError), error:
	if (opt_traceback):
	    raise
	else:
	    print error
	    exit(1)

if __name__ == "__main__":
    main()
