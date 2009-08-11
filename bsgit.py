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
from locale import getpreferredencoding
import osc.conf
import osc.core
try:
    from xml.etree import cElementTree as ET
except ImportError:
    import cElementTree as ET
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

def git_get_commit(sha1):
    info = {}
    cmd = [opt_git, 'cat-file', 'commit', sha1]
    proc = subprocess.Popen(cmd, stdout=PIPE)
    while True:
	line = proc.stdout.readline()
	if line == '':
	    break
	elif line == '\n':
	    info['message'] = proc.stdout.read().rstrip('\n')
	    break
	else:
	    token, value = line.rstrip('\n').split(' ', 1)
	    if token == 'tree':
		if token in info:
		    raise IOError("Commit %s: parse error in headers" % sha1)
		info[token] = value
	    elif token == 'parent':
		if 'parents' not in info:
		    info['parents'] = []
	        info['parents'].append(value)
	    elif token in ('author', 'committer'):
	        match = re.match('(.*) <([^<>]+)> (\d+) ([-+]\d{4})$', value)
		if not match or token in info:
		    raise IOError("Commit %s: parse error in headers" % sha1)
		name, email, time, timezone = match.groups()
		info[token] = {'name': name, 'email': email, 'time': time,
			       'timezone': timezone}
	    else:
		raise IOError("Commit %s: parse error in headers" % sha1)
    check_proc(proc, cmd)
    return info

def git_abbrev_rev(rev):
    """If rev is a SHA1 hash, return an abbreviated version."""
    if re.match('^[0-9a-f]{40}$', rev):
	return rev[0:7]
    else:
	return rev

def git_list_tree(commit_sha1):
    """Return the list of files in commit_sha1, with their SHA1 hashes."""
    # FIXME: Use NUL-terminated format (-z) for newlines in filenames.
    cmd = [opt_git, 'ls-tree', commit_sha1]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    files = []
    for line in proc.stdout:
	mode, type, sha1, name = \
	    re.match('^(\d{6}) ([^ ]+) ([0-9a-f]{40})\t(.*)', line).groups()
	if type == 'blob':
	    files.append({'mode': mode, 'name': name, 'sha1': sha1})
	elif type == 'tree':
	    raise IOError('Commit %s: subdirectories not supported' %
			  git_abbrev_rev(commit_sha1))
	else:
	    raise IOError('Commit %s: unexpected %s object' %
			  (git_abbrev_rev(commit_sha1), type))
    return files

#-----------------------------------------------------------------------

def get_xml_root(apiurl, rel, query=None):
    """Run a build service query and return the XML root element
    of the result.
    """
    url = osc.core.makeurl(apiurl, rel, query)
    if opt_verbose:
	print "-- GET " + url
    file = osc.core.http_GET(url)
    return ET.parse(file).getroot()

#-----------------------------------------------------------------------

def map_login_to_user(apiurl, login):
    """Map a build service account name to the user's real name and email."""
    if login == 'unknown':
	name = login
	email = 'UNKNOWN'
    elif login == 'buildservice-autocommit':
	name = login
	email = 'BUILDSERVICE-AUTOCOMMIT'
    else:
	login_utf8 = login.encode('UTF-8')
	try:
	    email = bscache['email ' + login_utf8].decode('UTF-8')
	    name = bscache['realname ' + login_utf8].decode('UTF-8')
	except KeyError:
	    user_info = get_user_info(apiurl, login)
	    email = user_info['email']
	    email_utf8 = email.encode('UTF-8')
	    bscache['email ' + login_utf8] = email_utf8
	    bscache['login ' + email_utf8] = login_utf8
	    try:
		name = user_info['realname']
		name_utf8 = name.encode('UTF-8')
		bscache['realname ' + login_utf8] = name_utf8
	    except KeyError:
		name = login
    return name, email

def map_email_to_login(apiurl, email):
    """Map an email address to a build service account name."""
    if email == 'UNKNOWN':
	return 'unknown'
    elif email == 'BUILDSERVICE-AUTOCOMMIT':
	return 'buildservice-autocommit'
    try:
	login = bscache['login ' + email]
	return login
    except KeyError:
	raise IOError("Cannot map email '%s' to a build service account name. "
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
	value = root.find(name)
	if value != None and value.text != None:
	    info[name] = value.text
    return info

#-----------------------------------------------------------------------

def get_package_status(apiurl, project, package, **what):
    """Retrieve the status of a package (optionally, of a given revision).

    REV can at least be 'latest' for the latest revision, a revision number,
    or a srcmd5 hash.  srcmd5 hashes can specify a real revision or an
    "expanded" revision (with patches from links applied).

    https://api.opensuse.org/source/PROJECT/PACKAGE
      <directory name="PACKAGE" srcmd5="..." ...>
	<entry name="..." md5="..." size="..." mtime="..." />
	...
      </directory>

    Returns:
    {'rev': ..., 'srcmd5': ..., ...,
     'files': [{'name': ..., 'md5': ...}, ...]}
    ...
    """

    # NOTES
    #
    # * When no revision is specified, the build service may show
    #   rev="upload" as the latest revision while an upload is in progress
    #   or after a client has failed during an upload).  Specifying
    #   rev="latest" avoids this and returns the latest actual revision.
    #
    # * The mtime attribute is the mtime of the physical file on the server;
    #   it is not part of the per-revision metadata.  The best we can do is
    #   to ignore this attribute.

    server = re.sub('.*://', '', apiurl)
    if what:
	key = server + '/' + project + '/' + package
	try:
	    # (Convert the dict into a tuple -- a tuple is hashable,
	    #  while a dict is not.)
	    return get_package_status.status[key][tuple(what.items())]
	except KeyError:
	    pass
    else:
	what = {'rev': 'latest'}
    status = get_new_package_status(apiurl, project, package, what)
    if 'rev' in status:
	if 'rev' not in what or what['rev'] == 'latest':
	    what['rev'] = status['rev']
	key = server + '/' + project + '/' + package
	if key not in get_package_status.status:
	    get_package_status.status[key] = {}
	get_package_status.status[key][tuple(what.items())] = status
    return status
get_package_status.status = {}

def parse_xml_directory(root):
    status = {}
    for name in ('rev', 'srcmd5', 'xsrcmd5'):
	value = root.get(name)
	if value != None:
	    status[name] = value
    node = root.find('linkinfo')
    if node != None:
	linkinfo = {}
	for name in ('project', 'package', 'baserev', 'srcmd5', 'lsrcmd5', 'rev'):
	    value = node.get(name)
	    if value != None:
		linkinfo[name] = value
	status['linkinfo'] = linkinfo
    files = []
    for node in root.findall('entry'):
	file = {}
	file['name'] = node.get('name')
	file['md5'] = node.get('md5')
	files.append(file)
    status['files'] = files
    return status

def get_new_package_status(apiurl, project, package, what):
    root = get_xml_root(apiurl, ['source', project, package], what)
    status = parse_xml_directory(root)
    return status

#-----------------------------------------------------------------------

def get_revision(apiurl, project, package, rev='latest'):
    """Retrieve the history of a package (optionally, until a given revision).

    REV can be a revision number or the srcmd5 hash of an "unexpanded"
    revision.

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
    try:
	return history[rev]
    except KeyError:
	return None
get_revision.history = {}

def forget_about_latest_revision(apiurl, project, package):
    server = re.sub('.*://', '', apiurl)
    key = server + '/' + project + '/' + package
    if key in get_revision.history:
	get_revision.history.pop(key)
    if key in get_package_status.status:
	statuses = get_package_status.status[key]
	for tuple in statuses.keys():
	    d = dict(tuple)
	    if 'rev' in d and d['rev'] == 'latest':
		statuses.pop(tuple)

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
	    value = node.find(name)
	    if value != None and value.text != None:
		revision[name] = value.text

	if head:
	    revision['parent'] = head
	head = revision
	rev = revision['rev']
	history[rev] = head

	# Index by srcmd5 as well.  (It is possibe that more than one revision
	# has the same srcmd5.  In that case, map from this srcmd5 to the
	# first such revision.)
	srcmd5 = revision['srcmd5']
	if srcmd5 not in history:
	    history[srcmd5] = head

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
	history['latest'] = head
    return history

#=======================================================================

def guess_link_target(apiurl, project, package, rev, linkinfo, time, silent=False):
    """Guess which revision (i.e., srcmd5) the given source link refers to.

    The build service now records which revision a link was generated against
    and reports this as linkinfo basrev=<rev>.  We still need to guess what
    links created before that are based on, and we cannot always get it right.
    """
    # FIXME: See Bug 516795 - <linkinfo baserev> not set in revision 1 of new link
    # FIXME: Check if we have a link of links => what to do then?
    if 'baserev' in linkinfo:
	return linkinfo['baserev']
    else:
	lproject = linkinfo['project']
	lpackage = linkinfo['package']
	if 'rev' in linkinfo:
	    trevision = get_revision(apiurl, lproject, lpackage, rev=linkinfo['rev'])
	    return trevision['srcmd5']
	else:
	    try:
		trevision = get_revision(apiurl, lproject, lpackage)
		while time < trevision['time']:
		    trevision = trevision['parent']
		if not silent:
		    print >>stderr, "Warning: %s/%s (%s): link target " \
				    "guessed as %s(%s) based on timestamps." % \
				    (project, package, rev, lpackage,
				     trevision['srcmd5'])
		return trevision['srcmd5']
	    except KeyError:
		return None

#-----------------------------------------------------------------------

def fetch_files(apiurl, project, package, srcmd5, files):
    """Fetch a list of files from the specified package."""
    for file in files:
	sha1 = fetch_file(apiurl, project, package, srcmd5,
			  file['name'], file['md5'])
	file['sha1'] = sha1

def fetch_file(apiurl, project, package, srcmd5, name, md5):
    """Fetch a file (unless it is already known)."""
    try:
	sha1 = bscache['blob ' + md5]
    except KeyError:
	sha1 = fetch_new_file(apiurl, project, package, srcmd5, name, md5)
	bscache['blob ' + md5] = sha1
    return sha1

def fetch_new_file(apiurl, project, package, srcmd5, name, md5):
    """Fetch a file.

    https://api.opensuse.org/source/PROJECT/PACKAGE/FILE&rev=REV
    """
    query = 'rev=' + srcmd5
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

def create_tree(files):
    """Create a git tree object from a list of files."""
    # FIXME: Use NUL-terminated format (-z) for newlines in filenames.
    cmd = [opt_git, 'mktree']
    proc = subprocess.Popen(cmd, stdin=PIPE, stdout=PIPE)
    for file in sorted(files, cmp=lambda a,b: cmp(a['name'], b['name'])):
	line = '100644 blob %s\t%s\n' % (file['sha1'], file['name'])
        proc.stdin.write(line)
    proc.stdin.close()
    tree_sha1 = proc.stdout.read().rstrip('\n')
    check_proc(proc, cmd)
    return tree_sha1

def create_commit(apiurl, tree_sha1, revision, parents):
    """Create a git commit from a tree object and a build service revision."""
    cmd = [opt_git, 'commit-tree', tree_sha1]
    for commit_sha1 in parents:
	cmd.extend(['-p', commit_sha1])

    try:
	user = revision['user']
    except KeyError:
	user = 'unknown'

    encoding = getpreferredencoding()

    name, email = map_login_to_user(apiurl, user)
    time = revision['time']
    environ['GIT_AUTHOR_NAME'] = name.encode(encoding)
    environ['GIT_COMMITTER_NAME'] = name.encode(encoding)
    environ['GIT_AUTHOR_EMAIL'] = email.encode(encoding)
    environ['GIT_COMMITTER_EMAIL'] = email.encode(encoding)
    environ['GIT_AUTHOR_DATE'] = time
    environ['GIT_COMMITTER_DATE'] = time
    proc = subprocess.Popen(cmd, stdin=PIPE, stdout=PIPE)
    if 'comment' in revision:
	proc.stdin.write(revision['comment'])
    proc.stdin.close()
    commit_sha1 = proc.stdout.read().rstrip('\n')
    check_proc(proc, cmd)
    return commit_sha1

def commit_is_a_parent(base_sha1, sha1):
    info = git_get_commit(sha1)
    if 'parents' in info:
	for parent in info['parents']:
	    if base_sha1 == parent or commit_is_a_parent(base_sha1, parent):
		return True
    return False

def fetch_revision(apiurl, project, package, revision, status):
    """Fetch one revision, including the files in it.

    Also used to fetch expanded / merged versions of packages; in this case,
    revision['rev'] is unset, and revision['srcmd5'] defines which files to
    fetch.
    """
    try:
	rev_or_srcmd5 = revision['rev']
    except KeyError:
	rev_or_srcmd5 = revision['srcmd5']
    revision_key = get_revision_key(apiurl, project, package, rev_or_srcmd5)
    try:
	commit_sha1 = bscache[revision_key]
    except KeyError:
	print "Fetching %s/%s (%s)" % (project, package, rev_or_srcmd5)
	srcmd5 = status['srcmd5']
	try:
	    tree_sha1 = bscache['tree ' + srcmd5]
	except KeyError:
	    files = status['files']
	    # Note: for links, the srcmd5 hash we get does not match the
	    # file list, so we cannot verify the srcmd5 here.
	    #if compute_srcmd5(files) != srcmd5:
	    #	raise IOError('MD5 checksum mismatch')
	    fetch_files(apiurl, project, package, srcmd5, files)
	    tree_sha1 = create_tree(files)
	    bscache['tree ' + srcmd5] = tree_sha1

	parents = []
	if 'parent' in revision:
	    parent = revision['parent']
	    if 'commit_sha1' in parent:
		parents.append(parent['commit_sha1'])
	if 'base_sha1' in revision:
	    base_sha1 = revision['base_sha1']
	    if len(parents) == 0 or \
	       not commit_is_a_parent(base_sha1, parents[0]):
		parents.append(base_sha1)

	commit_sha1 = create_commit(apiurl, tree_sha1, revision, parents)
	bscache[revision_key] = commit_sha1

	# Add a sentinel which tells us that the MD5 hashes of the objects
	# in this commit are in bscache.  This stops bscache.update() from
	# re-hashing this commit.
	bscache['commit ' + commit_sha1] = tree_sha1
    revision['commit_sha1'] = commit_sha1
    if opt_verbose:
	print "Storing %s/%s (%s) as %s" % (project, package, rev_or_srcmd5,
					    git_abbrev_rev(commit_sha1))
    return commit_sha1

def refers_to_parents_only(apiurl, project, package, srcmd5, child_sha1):
    revision = get_revision(apiurl, project, package, srcmd5)
    if revision != None and 'commit_sha1' in revision:
	return commit_is_a_parent(revision['commit_sha1'], child_sha1)

    status = get_package_status(apiurl, project, package, rev=srcmd5)
    if 'linkinfo' in status:
	linkinfo = status['linkinfo']
	if 'lsrcmd5' in linkinfo and not refers_to_parents_only(apiurl,
		project, package, linkinfo['lsrcmd5'], child_sha1):
	    return False
	lproject = linkinfo['project']
	lpackage = linkinfo['package']
	if 'srcmd5' in linkinfo and not refers_to_parents_only(apiurl,
		lproject, lpackage, linkinfo['srcmd5'], child_sha1):
	    return False
	elif 'baserev' in linkinfo and not refers_to_parents_only(apiurl,
		lproject, lpackage, linkinfo['baserev'], child_sha1):
	    return False
    return True

def fetch_base_rec(apiurl, project, package, srcmd5, depth):
    """Fetch the version of a package that a link is based on. (The project
    and package referred to here is the target package.)

    The base version of a simple link will be a proper revision of the target
    package.  The base version of a link of a link (or more deeply nested)
    will be the "expanded" version of the package, i.e., a kind of merge:
    the revisions of links are stored as patches; the "expanded" version is
    that patch applied to the most recent revision of the parent package for
    ordinary packages, or to the most recent "expanded" version of the parent
    link.
    """
    try:
	revision = get_revision(apiurl, project, package, srcmd5)
    except KeyError:
	revision = None

    if revision != None:
	rev = revision['rev']
	commit_sha1 = fetch_revision_rec(apiurl, project, package, revision,
					 depth - 1)
    else:
	status = get_package_status(apiurl, project, package, rev=srcmd5)
	linkinfo = status['linkinfo']
	lproject = linkinfo['project']
	lpackage = linkinfo['package']
	lsrcmd5 = linkinfo['lsrcmd5']
	parent = get_revision(apiurl, project, package, lsrcmd5)
	fetch_revision_rec(apiurl, lproject, lpackage, parent, depth - 1)
	base_sha1 = fetch_base_rec(apiurl, lproject, lpackage,
				   linkinfo['srcmd5'], depth - 1)
	revision = {
	    'srcmd5': srcmd5,
	    'parent': parent,
	    'base_sha1': base_sha1,
	    'time': parent['time'],
	    'comment': 'Expanded %s(%s)' % (package, parent['rev']),
	    }
	for name in ('user', 'time'):
	    if name in parent:
		revision[name] = parent[name]
	commit_sha1 = fetch_revision(apiurl, project, package, revision, status)

    # Make sure we also have the most recent revisions of the link package
    fetch_package(apiurl, project, package)
    return commit_sha1

def get_base_status(apiurl, project, package, rev='latest'):
    try:
	status = get_package_status(apiurl, project, package, rev=rev,
				    linkrev='base', expand='1')
	expanded = True
    except HTTPError, error:
	if error.code == 404:
	    # Most likely, this is an old revision that does not have the
	    # baserev attribute.  Query the unexpanded status; we will try
	    # our best below.
	    status = get_package_status(apiurl, project, package, rev=rev)
	    expanded = False
	else:
	    raise
    if 'linkinfo' in status:
	linkinfo = status['linkinfo']
	lproject = linkinfo['project']
	lpackage = linkinfo['package']
	revision = get_revision(apiurl, project, package, rev)
	baserev = guess_link_target(apiurl, project, package, rev, linkinfo,
				    revision['time'])
	if baserev != None:
	    if not expanded:
		# This revisision hasn't been expanded against linkrev='base'
		# (probably because it doesn't have a baserev tag), and we have
		# guessed a baserev now.
		try:
		    status = get_package_status(apiurl, project, package,
						rev=rev, linkrev=baserev,
						expand='1')
		except HTTPError, error:
		    if error.code == 404:
			print >>stderr, "Warning: %s/%s (%s): cannot expand" % \
					(project, package, rev)
		    else:
			raise
	    if 'baserev' not in linkinfo:
		linkinfo['baserev'] = baserev
    return status

def fetch_revision_rec(apiurl, project, package, revision, depth):
    """Fetch a revision and its children, up to the defined maximum depth.
    Reconnect to parents further up the tree if they are already known.
    """
    if 'parent' in revision and (depth > 1 or 'need_to_fetch' in revision):
	parent = revision['parent']
	commit_sha1 = fetch_revision_rec(apiurl, project, package, parent,
					 depth - 1)
	parent['commit_sha1'] = commit_sha1

    try:
	commit_sha1 = revision['commit_sha1']
	# Apparently, we have this revision already.
	return commit_sha1
    except KeyError:
	pass

    base_status = get_base_status(apiurl, project, package, revision['rev'])
    if 'linkinfo' in base_status:
	linkinfo = base_status['linkinfo']
	if 'baserev' in linkinfo:
	    lproject = linkinfo['project']
	    lpackage = linkinfo['package']
	    baserev = linkinfo['baserev']
	    try:
		parent = revision['parent']
	    except KeyError:
		parent = None

	    if parent == None or 'commit_sha1' not in parent or \
	       not refers_to_parents_only(apiurl, lproject, lpackage,  baserev,
					  parent['commit_sha1']):
		base_sha1 = fetch_base_rec(apiurl, lproject, lpackage, baserev,
					   depth - 1)
		revision['base_sha1'] = base_sha1

    commit_sha1 = fetch_revision(apiurl, project, package, revision, base_status)
    return commit_sha1

def mark_as_needed_rec(rev, revision):
    """Mark all revisions up the rev as needed."""
    # FIXME: If we end up further back in the history than any known revisions,
    # we need to refetch all the revisions.  (Unset all the 'commit-sha1's in
    # that case!)
    if revision['rev'] == rev or \
       ('parent' in revision and
        mark_as_needed_rec(rev, revision['parent'])):
	revision['need_to_fetch'] = True
	return True
    return False

def fetch_package(apiurl, project, package, depth=sys.maxint, need_rev=None,
		  check_uptodate=True):
    """Fetch a package, up to the defined maximum depth, but at least including
    the revision with the specified rev.
    """
    revision = get_revision(apiurl, project, package)
    try:
	rev = revision['rev']
    except KeyError:
	return None

    if opt_force:
	commit_sha1 = None
    else:
	try:
	    revision_key = get_revision_key(apiurl, project, package, rev)
	    commit_sha1 = bscache[revision_key]
	except KeyError:
	    commit_sha1 = None

    if not commit_sha1:
	if need_rev:
	    mark_as_needed_rec(need_rev, revision)
	commit_sha1 = fetch_revision_rec(apiurl, project, package, revision,
					 depth)
	revision['commit_sha1'] = commit_sha1

    remote_branch = remote_branch_name(apiurl, project, package)
    sha1 = git_get_sha1(remote_branch)
    if commit_sha1 != sha1:
	update_branch(remote_branch, commit_sha1)
    if check_uptodate:
	check_link_uptodate(apiurl, project, package, depth)
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

def check_link_uptodate(apiurl, project, package, depth, silent=False):
    """Check if a link is based on the most recent version of its target
    package, and tell the user to perform a merge if not.
    """

    # Make sure we don't check/report the same package more than once.
    key = project + '/' + package
    if key in check_link_uptodate.cached:
	return check_link_uptodate.cached[key]

    status = get_package_status(apiurl, project, package, rev='latest',
				expand='1')
    if 'linkinfo' not in status:
	return
    linkinfo = status['linkinfo']
    if 'srcmd5' not in linkinfo:
	return
    lsrcmd5 = linkinfo['srcmd5']

    if 'baserev' in linkinfo:
	baserev = linkinfo['baserev']
    else:
	revision = get_revision(apiurl, project, package)
	baserev = guess_link_target(apiurl, project, package,
				    revision['rev'], linkinfo,
				    revision['time'], silent=True)
    if lsrcmd5 == baserev:
	merge_sha1 = None
    else:
	lproject = linkinfo['project']
	lpackage = linkinfo['package']
	merge_sha1 = fetch_base_rec(apiurl, lproject, lpackage, lsrcmd5,
				    depth - 1)
	if not silent:
		print ("Package %s/%s not based on the latest expansion of " +
		       "%s/%s (commit %s); you may want to merge.") % \
		      (project, package, lproject, lpackage,
		       git_abbrev_rev(merge_sha1))
    check_link_uptodate.cached[key] = [lsrcmd5, merge_sha1]
    return check_link_uptodate.cached[key]
check_link_uptodate.cached = {}

def fetch_command(args):
    """The fetch command."""
    git('rev-parse', '--is-inside-work-tree')
    if len(args) == 0:
	branch = 'HEAD'
    else:
	branch = args[0]
    try:
	apiurl, project, package, branch, remote_branch = \
	    get_rev_info(branch)
    except IOError, error:
	if opt_apiurl:
	    apiurl = opt_apiurl
	else:
	    apiurl = osc.conf.config['apiurl']
	try:
	    project, package = branch.split('/', 1)
	except ValueError:
	    raise error
	if package.find('/') != -1:
	    raise error
	branch = package
	remote_branch = remote_branch_name(apiurl, project, package)

    # Add any objects added to bscache in the meantime.
    if git_get_sha1(branch):
	bscache.update(branch)

    commit_sha1 = fetch_package(apiurl, project, package, opt_depth)
    if commit_sha1 == None:
	print "This package is empty."
	return

    sha1 = git_get_sha1(branch)
    if sha1 == None:
	git('branch', '--track', branch, remote_branch)
	print "Branch '%s' created." % branch
    elif sha1 == commit_sha1:
	print "Branch %s already up-to-date." % branch
    else:
	print "Branch '%s' differs from the remote branch." % branch
    try:
	git('rev-parse', '--verify', 'HEAD')
    except IOError:
	git('checkout', '-f', branch)
    return

def pull_command(args):
    """The pull command."""
    if len(args) == 0:
	branch = 'HEAD'
    else:
	branch = args[0]
    try:
	apiurl, project, package, branch, remote_branch = \
	    get_rev_info(branch)
    except IOError, error:
	if opt_apiurl:
	    apiurl = opt_apiurl
	else:
	    apiurl = osc.conf.config['apiurl']
	try:
	    project, package = branch.split('/', 1)
	except ValueError:
	    raise error
	if package.find('/') != -1:
	    raise error
	branch = package
	remote_branch = remote_branch_name(apiurl, project, package)

    # Add any objects added to bscache in the meantime.
    if git_get_sha1(branch):
	bscache.update(branch)

    commit_sha1 = fetch_package(apiurl, project, package, opt_depth)
    if commit_sha1 == None:
	print "This package is empty."
	return

    sha1 = git_get_sha1(branch)
    git('rebase', remote_branch, branch)
    new_sha1 = git_get_sha1(branch)
    if sha1 == new_sha1:
	print "Branch %s already up-to-date." % branch
    else:
	print "Branch '%s' updated." % branch

def push_file(apiurl, project, package, name, blob_sha1):
    cmd = [opt_git, 'cat-file', 'blob', blob_sha1]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    # FIXME: should do this in batches and not in one go ...
    data = proc.stdout.read()
    md5 = hashlib.md5(data).hexdigest()

    query = {'rev': 'repository'}
    url = osc.core.makeurl(apiurl, ['source', project, package, name], query)
    if opt_verbose:
	print "-- PUT " + url
    osc.core.http_PUT(url, data=data)
    return md5

def push_commit(apiurl, project, package, message, sha1, old_status, committer,
		baserev=None):
    """Push a commit.

    The old status is used to identify files which the server definitely knows about
    already, and which we don't need to upload."""
    old_files = {}
    for file in old_status['files']:
	name = file['name']
	md5 = file['md5']
	old_files[name] = md5

    new_files = git_list_tree(sha1)
    for file in new_files:
	name = file['name']
	mode = file['mode']
	if mode[0:3] != '100':
	    raise IOError("Commit %s: '%s' is not a regular file" %
			  (git_abbrev_rev(sha1), name))
	if mode[3:] != '644':
	    print >>stderr, "Warning: commit %s, '%s': cannot preserve file " \
			    "mode %s; falling back to 644." % \
			    (git_abbrev_rev(sha1), name, mode[3:])
	try:
	    md5 = old_files[name]
	    sha1 = bscache['blob ' + md5]
	    if sha1 == file['sha1']:
	        file['md5'] = md5
		continue
        except KeyError:
	    pass
	md5 = push_file(apiurl, project, package, name, file['sha1'])
	file['md5'] = md5

    directory = ET.Element('directory')
    for file in new_files:
	name=file['name']
	md5=file['md5']
	directory.append(ET.Element('entry', name=name, md5=md5))

    query = {'cmd': 'commitfilelist',
	     'rev': 'repository',
	     'user': committer,
	     'comment': message}

    if baserev != None:  # Create a revision that is a source link
	# FIXME: is this also correct if the parent is not a link?
	query['linkrev'] = baserev
	query['keeplink'] = '1'

    url = osc.core.makeurl(apiurl, ['source', project, package], query=query)
    if opt_verbose:
	print "-- POST " + url
    file = osc.core.http_POST(url, data=ET.tostring(directory))
    root = ET.parse(file).getroot()
    new_status = parse_xml_directory(root)
    return new_status

def push_command(args):
    """The push command."""
    if len(args) == 0:
	branch = 'HEAD'
    else:
	branch = args[0]
    try:
	apiurl, project, package, branch, remote_branch = \
	    get_rev_info(branch)
    except IOError, error:
	if opt_apiurl:
	    apiurl = opt_apiurl
	else:
	    apiurl = osc.conf.config['apiurl']
	try:
	    project, package = branch.split('/', 1)
	except ValueError:
	    raise error
	if package.find('/') != -1:
	    raise error
	branch = package
	remote_branch = remote_branch_name(apiurl, project, package)

    remote_sha1 = fetch_package(apiurl, project, package, opt_depth,
				check_uptodate=False)
    sha1 = git_get_sha1(branch)

    if remote_sha1 == sha1:
	raise IOError("Nothing to push on branch '%s'." % branch)

    base_status = get_base_status(apiurl, project, package)

    if remote_sha1 != None:
	try:
	    merge_base = git('merge-base', remote_branch, sha1)
	except IOError:
	    merge_base = None
	if merge_base != remote_sha1:
	    raise IOError("Branch '%s' is not a child of the remote branch. "
			  "Please rebase first." % branch)

    # Require a clean index: otherwise, we would lose local chages when doing
    # a hard reset below.merge-base'
    git('update-index', '--refresh')

    # Login name of the user who will show up as the creator of a commit.
    committer = osc.conf.get_apiurl_usr(apiurl)

    path = []
    while sha1 != remote_sha1:
	info = git_get_commit(sha1)
	try:
	    parents = info['parents']
	except KeyError:
	    parents = []
	if len(parents) == 0:
	    parent = None
	elif len(parents) == 1:
	    parent = parents[0]
	    baserev = None
	elif len(parents) == 2:
	    # Assume that this is a merge of an "expanded revision". Try to
	    # figure out which parent is the linkrev (baserev), and which
	    # parent is the previous revision of the package.
	    (baserev, merge_sha1) = check_link_uptodate(apiurl, project, package,
							opt_depth, silent=True)
	    if parents[0] == merge_sha1:
		parent = parents[1]
	    elif parents[1] == merge_sha1:
		parent = parents[0]
	    else:
		raise IOError("Base commit %s is not a parent of commit %s."
			      % (merge_sha1, sha1))
	else:
	    raise IOError("Commit %s is an n-way merge, cannot push."
			  % git_abbrev_rev(sha1))

	email = info['author']['email']
	login = map_email_to_login(apiurl, email)
	if login != committer:
	    print >>stderr, "Warning: commit %s from %s will appear to be " \
			    "from %s.\n" % (git_abbrev_rev(sha1), login,
					    committer)
	message = info['message']

	path.append([sha1, message, baserev])
	if parent == None:
	    raise IOError("I am confused about the commit hierarchy")
	sha1 = parent

    # Put path into "chronological" order
    path.reverse()

    if 'linkinfo' in base_status:
	linkinfo = base_status['linkinfo']
	if 'baserev' in linkinfo:
	    baserev = linkinfo['baserev']
	else:
	    baserev = None

	# Fill in missing baserevs
	for node in path:
	    if node[2] == None:
		node[2] = baserev
	    else:
		baserev = node[2]

    if len(path) == 1:
	commit_s = "commit"
    else:
	commit_s = "commits"

    print "Pushing %d %s" % (len(path), commit_s)
    revision = get_revision(apiurl, project, package)
    if 'rev' in revision:
	next_rev = str(int(revision['rev']) + 1)
    else:
	next_rev = '1'

    for node in path:
	sha1, message, baserev = node
	base_status = push_commit(apiurl, project, package, message, sha1,
				  base_status, committer, baserev)
	if base_status['rev'] != next_rev:
	    raise IOError("Expected to create revision %s, but ended up with "
			  "revision %s" % (next_rev, base_status['rev']))
	next_rev = str(int(next_rev) + 1)

    forget_about_latest_revision(apiurl, project, package)
    remote_sha1 = fetch_package(apiurl, project, package, opt_depth)
    git('reset', '--hard', '-q', remote_sha1)
    # FIXME: Make sure that branch tracks remote_branch: this will not be
    #        the case for the initial commit.
    print "Branch '%s' rebased from %s to %s." \
	    % (branch, git_abbrev_rev(path[0][0]), git_abbrev_rev(remote_sha1))

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
    login_utf8 = login.encode('UTF-8')
    if len(args) == 1:
	try:
	    email = bscache['email ' + login_utf8].decode('UTF-8')
	    try:
		realname = bscache['realname ' + login_utf8].decode('UTF-8')
	    except KeyError:
		realname = None
		pass
	except KeyError:
	    email = None
	aliases = []
	for key in bscache.keys():
	    if key[0:6] == 'login ' and key[6:] != email and \
	       bscache[key] == login_utf8:
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
	    email_utf8 = email.encode('UTF-8')
	    if first_email:
		bscache['email ' + login_utf8] = email_utf8
		if realname:
		    realname_utf8 = realname.encode('UTF-8')
		    bscache['realname ' + login_utf8] = realname_utf8
		elif bscache.has_key('realname ' + login_utf8):
		    del bscache['realname ' + login_utf8]
		first_email = False
	    bscache['login ' + email_utf8] = login_utf8

def dump_command(args):
    """The dump command."""
    for key in bscache.keys():
	print "%s %s" % (key, bscache[key])

def usage(status):
    print """Usage: %s [options] <command> [args]

Import build service packages into git.

Commands are:
    fetch, fetch <branch>, fetch <project>/<package>
	Update the remote branch tracking the specified <project> and
	<package>.  If no project and package is specified, the default
	is to fetch the remote branch that the current branch tracks
	(refs/remotes/<server>/<project>/<package>).

	When a branch point is hit (i.e., a revision that creates a new link
	or updates an existing link), the target package is fetched as well.

    pull, pull <branch>, pull <project>/<package>
	Do a fetch of the remote branch that the current branch is tracking,
	followed by a rebase of the current branch.

    push, push <branch>, push <project>/<package>
	Export simple changes back to the build service.  Note that the build
	service cannot represent things like authorship, subdirectories,
	symlinks and other non-regular files, file modes, or merges.  Pushing
	to the build service will REWRITE THE GIT HISTORY to what the build
	service can represent; ANY ADDITIONAL INFORMATION WILL BE LOST.
	Source links cannot be pushed, yet.

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
	still remain cached.  (Remove .git/bscache to recompute the MD5 checksums.)

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

    # FIXME: allow to specify the local branch name independent from the
    #        package name.

    command = None
    if len(args) >= 1:
	if args[0] == 'fetch' and len(args) >= 1 and len(args) <= 2:
	    need_osc_config = True
	    need_bscache = True
	    command = fetch_command
	elif args[0] == 'pull' and len(args) >= 1 and len(args) <= 2:
	    need_osc_config = True
	    need_bscache = True
	    command = pull_command
	elif args[0] == 'push' and len(args) >= 1 and len(args) <= 2:
	    need_osc_config = True
	    need_bscache = True
	    command = push_command
	elif args[0] == 'dump' and len(args) == 1:
	    need_bscache = True
	    command = dump_command
	elif args[0] == 'usermap':
	    need_bscache = True
	    command = usermap_command
    if command == None:
	usage(2)

    try:
	try:
	    if need_osc_config:
		osc.conf.get_config()

	    if need_bscache:
		global bscache
		git_dir = git('rev-parse', '--git-dir')
		bscache = BuildServiceCache(git_dir + '/bscache', opt_git)

	    command(args[1:])
	except (KeyboardInterrupt, EnvironmentError), error:
	    if opt_traceback:
		import traceback
		traceback.print_exc(file=stderr)
	    else:
		print >>stderr, error
	    raise
    except HTTPError, error:
	if hasattr(error, 'osc_msg'):
	    print >>stderr, error.osc_msg
	body = error.read()
	match = re.search('<summary>(.*)</summary>', body)
	if match:
	    print >>stderr, match.groups()[0]
	exit(1)
    except (KeyboardInterrupt, EnvironmentError), error:
	exit(1)

if __name__ == "__main__":
    main()

# TODO
#
# * Make checkout and pushing to an empty package work
