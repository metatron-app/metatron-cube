#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" get open pull requests and apply patches to local git working copy """
__author__ = "Choi, Yongjin"


# requirements
# python 2.7 or higher (not python 3)
# pip install requests  # for rest api
# pip install gitpython # for git control


from builder import *

import sys, os
import getopt
import logging


def usage(exec_name):
    usage = '''Usage: %s [-siapv] [arg] druid_working_copy
    -s, --step   e.g, 2 (only show pull requests info)
    -i, --issue  e.g, 1702,2308
    -a, --author e.g, piggy
    -p, --patch  patch directory (already acquired by step 3)
    -v, --verbose''' % exec_name
    failed(usage)


if __name__ == "__main__":
    execName = os.path.basename(sys.argv[0])
    try:
        opts, args = getopt.getopt(sys.argv[1:], "s:i:a:p:v", 
                        ["step=", "issue=", "author=", "patch=", "verbose"])
    except getopt.GetoptError as err:
        # print help information and exit:
        print str(err) # will print something like "option -a not recognized"
        usage(execName)

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
        usage(execName)

    git_dir = args[0]

    if steps is None: steps = range(1,5)
    
    builder = Builder(loglvl, git_dir, 'build_github_')

    # 1. prepare local git copy
    if 1 in steps: builder.prepare_working_copy_for_github()

    # 2. get open pull requests
    if 2 in steps or 3 in steps:
        if authors is None:
            authors = gAuthors
        info = builder.get_open_pull_requests(github_url, authors, issues)

    # 3. download patches
    if 3 in steps:
        if issues is not None:
            # info = {k: info[k] for k in issues}     # retain only given issues
            info = dict( (k, v) for (k, v) in info.iteritems() if k in issues )
        patch_dir = builder.get_patches(info)

    # 4. apply patches
    if 4 in steps:
        if patch_dir is None:
            failed("patch directory is not given")
        builder.apply_patches(git_dir, patch_dir, issues, info, do_commit=True)

