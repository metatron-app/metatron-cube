#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" Builder class to make temporal branch from github pull requests and stash """
__author__ = "Choi, Yongjin"


# requirements
# python 2.7 or higher (not python 3)
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
import codecs
import re
from datetime import datetime, timedelta


# global variables
gAuthors = ['sirpkt', 'navis', 'jaehc']

# use github api credential to avoid rate limit
gUseCred = True
# following info were generated by jerryjung
client_id ='d24549a4d54170b80d87'
client_secret ='51b213e4148f80f846580902b0cf3bed9b33c836'

github_url = 'https://github.com/druid-io/druid.git' # git remote repository for github
build_cmd = "mvn clean install -DskipTests=true"


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


class Builder:
    def __init__(self, log_lvl, path, branch_prefix):
        self.path = path
        self.branch_prefix = branch_prefix
        logging.basicConfig(level=log_lvl, format='%(asctime)s [%(levelname)-5s] %(message)s', datefmt='%m/%d %H:%M:%S')
        self.time_str = time.strftime('%m%d_%H%M%S')
        try:
            self.repo = git.Repo(path)
            assert not self.repo.bare        # assume 'path' contains cloned working dir
        except git.InvalidGitRepositoryError as e:
            print "Invalid Git repository : %s" % path
            sys.exit(1)

    def git_clean(self):
        self.repo.head.reset(index=True, working_tree=True)
        self.repo.git.clean('-fdx')


    def git_commit(self, commit_message):
        # see https://github.com/gitpython-developers/GitPython/issues/212
        # print repo.is_dirty()
        # print repo.untracked_files
        # for diff in index.diff(None):
            # print diff.a_blob.path
        # index.add(repo.untracked_files)

        # NOTE: check-in patches only if current branch is a temporal branch for build!!!
        repo = self.repo
        cur_brname = str(repo.active_branch)
        if not cur_brname.startswith(self.branch_prefix):
            print "WARNING: the current branch %s is not for build" % (cur_brname)

        repo.git.add(all=True)
        username = "bdd_builder"
        email = "%s@bdd.com" % username
        author = git.Actor("%s <%s>" % (username, email), email)
        decorated_message = "***** %s (into %s) *****" % (commit_message, cur_brname)
        try:
            repo.git.commit(message=decorated_message, author=author)
        except git.exc.GitCommandError as ge:
            pass


    def prepare_working_copy(self):
        repo = self.repo

        print "*** preparing local working copy..."
        if repo.is_dirty():
            raise Exception("repository is not clean")

        repo.git.checkout('master')

        if 'origin' not in map(str, repo.remotes):
            failed("no remotes/origin")

        # remotes/origin should be 'stash/bdd/druid'
        # m = re.match('https://(?P<id>[a-zA-Z0-9_:!$]+)?@tde\.sktelecom\.com/stash/scm/bdd/(?P<repo_name>\w+)\.git', repo.remotes.origin.url)
        m = re.match('https://((?P<id>[^ ]+)@)?tde\.sktelecom\.com/stash/scm/bdd/(?P<repo_name>\w+)\.git', repo.remotes.origin.url)
        if m is None or m.group('repo_name') != 'druid':
            failed("repository is not bdd-druid")

        # pull origin/master, github/master
        origin = repo.remotes.origin
        origin.pull()   # git pull origin master

        # find github remote branch, and add it if not exist
        github = None
        github_name = None
        for remote in repo.remotes:
            if str(remote) == 'origin': continue
            # if re.match('https://github\.com/druid\-io/druid\.git', remote.url):
            if remote.url.startswith(github_url):
                github = remote
                github_name = str(remote)
                break

        if github is None:  # add remote, github/druid
            github_name = 'github'
            i = 0
            while github_name in map(str, repo.remotes):
                # generate new name
                github_name = 'github%d' % i
                i += 1
            github = repo.create_remote(github_name, github_url)

        github.fetch()      # git pull github master

        # make origin/master up-to-date with github/master
        repo.git.rebase("%s/master" % github_name)
        print " rebase master using %s" % (github_name)
        origin.push(refspec="master:master")    # git push -u origin master

        # make branch for build
        branch_name = self.branch_prefix + self.time_str
        working_branch = repo.create_head(branch_name)
        assert repo.active_branch != working_branch
        assert repo.active_branch.commit == working_branch.commit
        repo.head.ref = working_branch
        assert not repo.head.is_detached
        print " create temporal branch %s" % (branch_name)

        # github_master = repo.remotes.github.refs.master


    def prepare_working_copy_for_github(self):
        repo = self.repo

        print "*** preparing local working copy..."
        if repo.is_dirty():
            raise Exception("repository is not clean")

        repo.git.checkout('master')

        github = None
        github_name = None
        for remote in repo.remotes:
            # if re.match('https://github\.com/druid\-io/druid\.git', remote.url):
            if remote.url.startswith(github_url):
                github = remote
                github_name = str(remote)
                break

        github.fetch()      # git pull github master

        repo.git.rebase("%s/master" % github_name)
        print " rebase master using %s" % (github_name)

        # make branch for build
        branch_name = self.branch_prefix + self.time_str
        working_branch = repo.create_head(branch_name)
        assert repo.active_branch != working_branch
        assert repo.active_branch.commit == working_branch.commit
        repo.head.ref = working_branch
        assert not repo.head.is_detached
        print " create temporal branch %s" % (branch_name)


    def merge_stash(self, git_dir):
        repo = self.repo
        # git merge origin/bdd-dev into temporal branch
        # bd = repo.remotes.origin.refs.__getitem__('bdd-dev')
        # repo.git.merge(bd)

        # merge origin/bdd-xxx branches into temporal working branch
        cur_branch_name = str(repo.active_branch)

        saved_path = os.getcwd()
        try:
            os.chdir(git_dir)
            for br in repo.remotes.origin.refs:
                br_name = str(br).split('/')[1]
                if br_name.startswith('bdd-'):
                    logging.debug("try merge %s into %s" % (str(br), cur_branch_name))
                    try:
                        # kind of dry-run, git merge --no-commit --no-ff br
                        repo.git.merge(br, '--no-commit', '--no-ff')
                        # repo.git.merge(br)
                        # logging.info("success: merge %s into %s" % (str(br), cur_branch_name))
                        print "success: merge %s into %s" % (str(br), cur_branch_name)
                    except git.exc.GitCommandError as ge:
                        # merge failed
                        logging.critical("failure: merge %s into %s" % (str(br), cur_branch_name))
                        print "failure: merge %s into %s" % (str(br), cur_branch_name)
                        self.git_clean()
                        # XXX XXX
                        # failed("merge \'%s\' failed" % br_name)

                    # try to build
                    out, rc = getCallResult(build_cmd)
                    if rc != 0:     # build failure
                        print " %s" % out
                        logging.critical("failure: build %s" % br_name)
                        # NOTE: 'br' was already merged, 
                        #        if we want to avoid this, use another temporal branch and discard it when failed
                        self.git_clean()
                        failed("build \'%s\' failed" % br_name)

                    commit_message = "merge branch %s" % (br_name)
                    self.git_commit(commit_message)
        finally:
            os.chdir(saved_path)


    def rest_get(self, url_str):
        if gUseCred:
            url_str += "?client_id=%s&client_secret=%s" % (client_id, client_secret)
        logging.debug("try to get %s" % url_str)
        return requests.get(url_str)


    def get_open_pull_requests(self, git_url_str, authors, issues):
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
        response = self.rest_get(pull_url_str)
        pull_json = []
        while True:
            one_page_pull_json = response.json()
            pull_json += one_page_pull_json
            link = response.headers['Link']
            logging.debug(" got %d pull requests" % len(one_page_pull_json))
            logging.debug(" paging info: %s" % link)
            #'<https://api.github.com/repositories/6358188/pulls?client_id=d24549a4d54170b80d87&client_secret=51b213e4148f80f846580902b0cf3bed9b33c836&page=2>; rel="next", <https://api.github.com/repositories/6358188/pulls?client_id=d24549a4d54170b80d87&client_secret=51b213e4148f80f846580902b0cf3bed9b33c836&page=3>; rel="last"'
            next_str = link.split(',')[0]
            if next_str.split(';')[1].strip() == 'rel="next"':
                pull_url_str = next_str.split(';')[0][1:-1]
                logging.debug("try to get %s" % pull_url_str)
                response = requests.get(pull_url_str)   # DO NOT call rest_get, credential already included
            else:       # no more page
                break

        # pull_json = response.json()
        # print json.dumps(pull_json, indent=2)
        logging.debug(json.dumps(pull_json, indent=2))

        # validity check
        if len(pull_json) > 0:
            if 'user' not in pull_json[0] or 'login' not in pull_json[0]['user']:
                print json.dumps(pull_json, indent=2)
                failed("github api returns invalid pulls")

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
                pr_detail = self.rest_get(pr_url).json()
                logging.debug(json.dumps(pr_detail, indent=2))
                if 'merged' not in pr_detail:
                    print json.dumps(pr_detail, indent=2)
                    failed("github api returns invalid pull request info")

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


    def get_patches(self, info, use_diff=True):
        print "\n*** getting patches..."
        patch_dir = 'druid_patches_' + self.time_str
        if not os.path.exists(patch_dir):     # race condition? yes, I don't care - piggy
            os.makedirs(patch_dir)

        patches = {}
        for issue, details in info.items():
            # if details['state'] == 'mergeable' and not details['title'].lstrip().startswith('[WIP]'):
            if details['state'] == 'mergeable':
                url = details['diff_url'] if use_diff else details['patch_url']
                logging.info('try to get patches from %s' % url)
                patch_resp = self.rest_get(url)  # redirection handled automatically
                filename = patch_dir + '/' + url.split('/')[-1]
                with codecs.open(filename, 'w', encoding='utf-8') as f: 
                    f.write(patch_resp.text)
                print ' patch of %d was copied to %s' % (issue, filename)
        return patch_dir


    def apply_patches(self, git_dir, patch_dir, issue_nums, info, do_commit=False):
        repo = self.repo
        print "\n*** applying patches from %s..." % patch_dir

        patch_files = [os.path.abspath(os.path.join(patch_dir, f)) for f in os.listdir(patch_dir) if os.path.isfile(os.path.join(patch_dir, f))]

        merge_failed = []
        build_failed = []
        success = []

        saved_path = os.getcwd()
        try:
            os.chdir(git_dir)
            for pf in patch_files:
                issue_num = int(os.path.basename(os.path.splitext(pf)[0]))
                if issue_nums and not issue_num in issue_nums:
                    continue
                print " start patching %d" % issue_num
                out, rc = getCallResult("patch -p1 -f --dry-run < " + pf)
                if rc != 0: # merge failure
                    print " merge %d failed" % issue_num
                    print " %s" % out
                    merge_failed.append(issue_num)
                    continue

                # merge
                out, rc = getCallResult("patch -p1 -f < " + pf)
                assert rc == 0

                # try to build
                out, rc = getCallResult(build_cmd)
                if rc != 0: # build failure
                    print " build %d failed" % issue_num
                    # when build failed, 
                    print " %s" % out
                    build_failed.append(issue_num)
                    # restore state
                    self.git_clean()
                    continue

                # build success
                success.append(issue_num)
                print " merge & build %d success" % issue_num

                # commit
                if do_commit:
                    title = ''
                    if info and issue_num in info and 'title' in info[issue_num]:
                        title = info[issue_num]['title']
                    commit_message = "merge pull request #%d: %s" % (issue_num, title)
                    self.git_commit(commit_message)

            print "merge failed : %s" % (', '.join(map(str, merge_failed)))
            print "build failed : %s" % (', '.join(map(str, build_failed)))
            print "success      : %s" % (', '.join(map(str, success)))

            # back to master
            # if repo.active_branch != repo.heads.master:
                # print 'not master!!!'
                # # repo.git.checkout('master')
                # repo.head.ref = repo.heads.master 
        finally:
            os.chdir(saved_path)


    def push_tag_and_branch(self):
        repo = self.repo

        # push branch
        origin = repo.remotes.origin
        cur_brname = str(repo.active_branch)

        # check current branche name, if it does not start with self.branch_prefix, do NOT push it
        if cur_brname.startswith(self.branch_prefix):
            logging.info("push branch %s" % cur_brname)
            refs_mapping = "%s:%s" % (cur_brname, cur_brname)
            origin.push(refspec=refs_mapping)   # git push -u origin build_xxx

            tag_name = 'latest'
            # http://stackoverflow.com/questions/8044583/how-can-i-move-a-tag-on-a-git-branch-to-a-different-commit
            # delete the tag on remote
            origin.push(':refs/tags/%s' % tag_name)
            # overwrite local tag
            repo.create_tag(tag_name, ref=repo.active_branch, force=True, message='%s_release' % tag_name)
            # push tag
            origin.push('refs/tags/%s' % tag_name)
        else:
            print "WARNING: current branch %s is not for build" % (cur_brname)


    def remove_old_branches(self, threshold):
        repo = self.repo
        origin = repo.remotes.origin

        for br in repo.branches:
            br_name = str(br)
            if br_name.startswith(self.branch_prefix):
                br_ts = br_name[6:]
                fmt = '%m%d_%H%M%S'
                td = datatime.strptime(self.time_str, fmt) - datetime.strptime(br_ts, fmt)
                if td > timedelta(days=threshold):  # old enough, remote it!
                    origin.push(':%s' % br_name)
                    repo.delete_head(br, '-D')  # delete force

