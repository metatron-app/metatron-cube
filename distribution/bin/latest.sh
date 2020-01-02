#!/bin/bash

BASEDIR=$1

git rev-parse HEAD > $BASEDIR/git.version
git log --oneline -30 > $BASEDIR/git.history
