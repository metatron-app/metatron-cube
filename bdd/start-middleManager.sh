nohup java `cat conf/druid/middleManager/jvm.config | xargs` -cp conf/druid/_common:conf/druid/middleManager:lib/* io.druid.cli.Main server middleManager > var/middleManager.out 2>&1 &
