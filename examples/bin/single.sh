#!/bin/bash -eu

java `cat conf/druid/single/jvm.config | xargs` -cp conf/druid/_common:conf/druid:conf/druid/single:lib/* io.druid.cli.ServerRunnable $@ > log/single.log &
