#!/usr/bin/python
# -*- coding: utf-8 -*-

""" get open pull requests and apply patches to local git working copy """
__author__ = "Choi, Yongjin"


# requirements
# python 2.6 or higher (not python 3)
# yum install python-pip
# pip install requests  # for rest api
# pip install gitpython # for git control


import git
import requests
import json

import sys, os
import getopt
import time
import logging
import subprocess


# global variables
gTimeStr = None


def failed(str):
    print >> sys.stderr, str
    sys.exit(1)

def getCallResult(cmdARGS, shell=True):
    logging.info("run %s" % cmdARGS)
    try:
        if shell is True:
            p = subprocess.Popen(cmdARGS, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        else:
            p = subprocess.Popen(cmdARGS.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        fd_popen = p.stdout
    except OSError, e:
        failed("%s: `%s`" % (e, cmdARGS))
    else:
        data = fd_popen.read().strip()
        fd_popen.close()
        p.wait()
    logging.info("done %s" % cmdARGS)
    return data, p.returncode


def runCmd(cmdstr):
    (data, rc) = getCallResult(cmdstr, shell=True)
    if rc == 0:
        logging.info("CMD: %s success", cmdstr)
    else:
        logging.critical("CMD: %s failure!!!", cmdstr)
        sys.exit(1)


def git_of(path):
    repo = git.Repo(path)
    assert not repo.bare        # assume 'path' contains cloned working dir
    # print repo.remotes.origin.url
    return repo


def prepare_working_copy(repo):
    print "*** preparing local working copy..."
    if repo.is_dirty():
        raise Exception("repository is not clean")

    repo.git.checkout('master')

    # print repo.untracked_files
    origin = repo.remotes.origin
    origin.pull()
    print " pull from %s done" % (repo.remotes.origin.url)

    branch_name = 'build_' + gTimeStr
    working_branch = repo.create_head(branch_name)
    assert repo.active_branch != working_branch
    assert repo.active_branch.commit == working_branch.commit
    # NOTE: repo.head.reference ==  repo.head.ref ???
    repo.head.ref = working_branch
    assert not repo.head.is_detached
   
    # back to master
    # repo.head.ref = repo.heads.master 
    # assert repo.active_branch != working_branch
    # assert repo.active_branch == repo.heads.master
    # repo.delete_head(working_branch)


def get_open_pull_requests(git_url_str, authors, issues):
    print "\n*** getting pull requests..."
    import urlparse
    # print git_url_str
    git_url = urlparse.urlparse(git_url_str)
    # github.com => api.github.com
    new_netloc = 'api.' + git_url.netloc
    # blah/druid.git => repos/blah/druid/pulls
    new_path = '/repos' + '.'.join(git_url.path.split('.')[:-1]) + '/pulls'
    pull_url = git_url._replace(netloc = new_netloc, path = new_path)
    pull_url_str = urlparse.urlunparse(pull_url)
    logging.debug("request to %s" % pull_url_str)
    response = requests.get(pull_url_str)
    pull_json = response.json()
    # print json.dumps(pull_json, indent=2)
    logging.debug(json.dumps(pull_json, indent=2))

    name_len = max([len(name) for name in authors])
    info = {}
    print "%-5s %-9s %-*s %s" % ("issue", "state", name_len, "creator", "title")
    for pr in pull_json:
        creator = pr['user']['login']
        issue_num = pr['number']
        pr_url = pr['url']
        if issues and not issue_num in issues:
            continue
        if creator in authors:
            # print "%d - %s - %s - %s - %s %s" % (pr['number'], pr['state'], pr['title'], pr['user']['login'], pr['patch_url'], pr['diff_url'])
            pr_detail = requests.get(pr_url).json()
            # print json.dumps(pr_detail, indent=2)
            logging.debug(json.dumps(pr_detail, indent=2))
            # print "\t\t%s %s %s" % (pr_detail['merged'], pr_detail['mergeable'], pr_detail['mergeable_state'])
            if pr_detail['merged']:
                state = 'merged'
            else:
                if pr_detail['mergeable']:
                    state = 'mergeable'
                else:
                    state = 'dirty'

            print "%-5d %-9s %-*s %s" % (issue_num, state, name_len, creator, pr['title'])
            pr_info = {
                    'state': state, 
                    'creator': creator,
                    'patch_url': pr['patch_url'],
                    'diff_url': pr['diff_url'],
                    'title': pr['title'],
                    }
            info[issue_num] = pr_info
    return info


def get_patches(info, use_diff=True):
    print "\n*** getting patches..."
    patch_dir = 'druid_patches_' + gTimeStr
    if not os.path.exists(patch_dir):     # race condition? yes, I don't care - piggy
        os.makedirs(patch_dir)

    patches = {}
    for issue, details in info.items():
        if details['state'] == 'mergeable':
            url = details['diff_url'] if use_diff else details['patch_url']
            patch_resp = requests.get(url)  # redirection handled automatically
            filename = patch_dir + '/' + url.split('/')[-1]
            logging.info('try to get patches from %s' % url)
            with open(filename, 'w') as f: 
                f.write(patch_resp.text)
            print ' patch of %d was copied to %s' % (issue, filename)
    return patch_dir


def apply_patches(git_dir, repo, patch_dir, issues, do_commit=False):
    print "\n*** applying patches from %s..." % patch_dir
    
    patch_files = [os.path.abspath(os.path.join(patch_dir, f)) for f in os.listdir(patch_dir) if os.path.isfile(os.path.join(patch_dir, f))]

    build_cmd = "mvn clean install -DskipTests=true"
    merge_failed = []
    build_failed = []
    success = []

    os.chdir(git_dir)
    for pf in patch_files:
        issue_num = int(os.path.basename(os.path.splitext(pf)[0]))
        print " start patching %d" % issue_num
        out, rc = getCallResult("patch -p1 -f --dry-run < " + pf)
        if rc != 0: # merge failure
            print " merge %d failed" % issue_num
            merge_failed.append(issue_num)
            continue

        # merge
        out, rc = getCallResult("patch -p1 -f < " + pf)
        assert rc == 0

        # try to build
        out, rc = getCallResult(build_cmd)
        if rc != 0: # build failure
            print " build %d failed" % issue_num
            build_failed.append(issue_num)
            continue

        # build success
        success.append(issue_num)
        print " merge & build %d success" % issue_num
        
        # commit
        if do_commit:
            # see https://github.com/gitpython-developers/GitPython/issues/212
            # print repo.is_dirty()
            # print repo.untracked_files
            # for diff in index.diff(None):
                # print diff.a_blob.path
            # index.add(repo.untracked_files)
            repo.git.add(all=True)
            commit_message = "****** merge pull request #%d *****" % issue_num
            repo.git.commit(message=commit_message)

    print "merge failed : %s" % (', '.join(map(str, merge_failed)))
    print "build failed : %s" % (', '.join(map(str, build_failed)))
    print "success      : %s" % (', '.join(map(str, success)))

    # back to master
    # if repo.active_branch != repo.heads.master:
        # print 'not master!!!'
        # # repo.git.checkout('master')
        # repo.head.ref = repo.heads.master 



def usage(exec_name):
    usage = '''Usage: %s [-siapv] [arg] druid_working_copy
    -s, --step   e.g, 2 (only show pull requests info)
    -i, --issue  e.g, 1702,2308
    -a, --author e.g, piggy
    -p, --patch  patch directory (already acquired by step 3)
    -v, --verbose''' % exec_name
    failed(usage)


if __name__ == "__main__":
    gExecName = os.path.basename(sys.argv[0])
    try:
        opts, args = getopt.getopt(sys.argv[1:], "s:i:a:p:v", 
                        ["step=", "issue=", "author=", "patch=", "verbose"])
    except getopt.GetoptError as err:
        # print help information and exit:
        print str(err) # will print something like "option -a not recognized"
        usage(gExecName)

    steps = None
    issues = None
    authors = None
    patch_dir = None
    loglvl = logging.CRITICAL
    for o, a in opts:
        if o in ("-s", "--step"):
            steps = map(int, a.split(','))
        elif o in ("-i", "--issue"):
            issues = map(int, a.split(','))
        elif o in ("-a", "--author"):
            authors = a.split(',')
        elif o in ("-p", "--patch"):
            patch_dir = a
        elif o in ("-v", "--verbose"):
            loglvl = logging.DEBUG
        else:
            assert False, "unhandled option %s" % (o)

    if len(args) < 1:
        usage(gExecName)
        # failed("Usage: %s druid_working_copy" % gExecName)

    git_dir = args[0]
    repo = git_of(git_dir)
    
    logging.basicConfig(level=loglvl, format='%(asctime)s [%(levelname)-5s] %(message)s', datefmt='%m/%d %H:%M:%S')
    gTimeStr = time.strftime('%m%d_%H%M%S')
    if steps is None: steps = range(1,5)

    # 1. prepare local git copy
    if 1 in steps: prepare_working_copy(repo)

    # 2. get open pull requests
    if 2 in steps or 3 in steps:
        if authors is None:
            authors = ['sirpkt', 'navis', 'CHOIJAEHONG1']
        info = get_open_pull_requests(repo.remotes.origin.url, authors, issues)

    # 3. download patches
    if 3 in steps:
        if issues is not None:
            info = {k: info[k] for k in issues}     # retain only given issues
        patch_dir = get_patches(info)

    # 4. apply patches
    if 4 in steps:
        if patch_dir is None:
            failed("patch directory is not given")
        apply_patches(git_dir, repo, patch_dir, issues, do_commit=True)

