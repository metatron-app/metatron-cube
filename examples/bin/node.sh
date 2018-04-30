#!/bin/bash -eu

## Initializtion script for druid nodes
## Runs druid nodes as a daemon and pipes logs to log/ directory

usage="Usage: node.sh nodeType (start|stop|status|tools)"

if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

mkdir -p var/druid/pids

IFS=':' read -ra ARRAY <<< "$1"
nodeType=$1
conf=${ARRAY[1]:-${ARRAY[0]}}

shift

startStop=$1
pid=var/druid/pids/$nodeType.pid

case $startStop in

  (start)

    if [ -f $pid ]; then
      if kill -0 `cat $pid` > /dev/null 2>&1; then
        echo $nodeType node running as process `cat $pid`.  Stop it first.
        exit 1
      fi
    fi

    if [[ -z $1 ]] ; then
      log='log/$nodeType.log'
    else
      log=$1
    fi

    nohup java `cat conf/druid/$conf/jvm.config | xargs` -cp conf/druid:conf/druid/$conf:lib/* io.druid.cli.Main server $nodeType > $log 2>&1 &
    nodeType_PID=$!
    echo $nodeType_PID > $pid
    echo "Started $nodeType node ($nodeType_PID)"
    ;;

  (stop)

    if [ -f $pid ]; then
      TARGET_PID=`cat $pid`
      if kill -0 $TARGET_PID > /dev/null 2>&1; then
        echo Stopping process `cat $pid`...
        kill $TARGET_PID
      else
        echo No $nodeType node to stop
      fi
      rm -f $pid
    else
      echo No $nodeType node to stop
    fi
    ;;

  (status)
    if [ -f $pid ]; then
      if kill -0 `cat $pid` > /dev/null 2>&1; then
        echo RUNNING
        exit 0
      else
        echo STOPPED
      fi
    else
      echo STOPPED
    fi
    ;;

  (*)
    case $nodeType in
      (tools)
        echo Running tool $1...
        java `cat conf/druid/_common/jvm.config | xargs` -cp conf/druid:lib/* io.druid.cli.Main $nodeType $@
        ;;
    esac
esac