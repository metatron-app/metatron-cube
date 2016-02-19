nohup java `cat conf/druid/broker/jvm.config | xargs` -cp conf/druid/_common:conf/druid/broker:lib/* io.druid.cli.Main server broker > var/broker.out 2>&1 &
