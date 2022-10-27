#!/bin/bash

BASEDIR=$1

git describe --tags --exact-match > $BASEDIR/git.tag 2> /dev/null
git rev-parse --short HEAD > $BASEDIR/git.version
git log --oneline -30 > $BASEDIR/git.history
