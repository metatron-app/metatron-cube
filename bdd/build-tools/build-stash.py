#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" merge working branches in stash """
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
    -s, --step       e.g, 2 (only show pull requests info)
    -f, --force-push push build branch
    -c, --clean      e.g, 7 (days, defaults: 7, clean build branches)
    -v, --verbose
 NOTE: it'd be better to add druid-github repository using
 $ git remote add github https://github.com/druid-io/druid.git
 $ git fetch github''' % exec_name
    failed(usage)


if __name__ == "__main__":
    execName = os.path.basename(sys.argv[0])
    try:
        opts, args = getopt.getopt(sys.argv[1:], "c:s:vf",
                        ["clean=", "step=", "verbose", "force-push"])
    except getopt.GetoptError as err:
        # print help information and exit:
        print str(err) # will print something like "option -a not recognized"
        usage(execName)

    steps = None
    issues = None
    authors = None
    patch_dir = None
    force_push = False
    clean_branch = None     # in days, None: do not clean
    loglvl = logging.CRITICAL
    for o, a in opts:
        if o in ("-c", "--clean"):
            clean_branch = int(a)
        if o in ("-s", "--step"):
            steps = map(int, a.split(','))
        elif o in ("-v", "--verbose"):
            loglvl = logging.DEBUG
        elif o in ("-f", "--force-push"):
            force_push = True
        else:
            assert False, "unhandled option %s" % (o)

    if len(args) < 1:
        usage(execName)
        # failed("Usage: %s druid_working_copy" % execName)

    git_dir = args[0]
    
    if steps is None: steps = range(1,4)

    builder = Builder(loglvl, git_dir, 'build_stash_')

    # 1. prepare local git copy
    if 1 in steps: builder.prepare_working_copy()

    # 2. merge bdd stash
    if 2 in steps:
        builder.merge_stash(git_dir)

    # 3. finalize result
    if 3 in steps and force_push:
        builder.push_tag_and_branch()

    if clean_branch:
        builder.remove_old_branches(clean_branch)
