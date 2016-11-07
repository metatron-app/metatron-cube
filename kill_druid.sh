#!/bin/sh

ps -ef | grep io.druid.cli.Main | grep -v grep | awk '{print $2}' | xargs kill -9
