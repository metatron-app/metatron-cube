nohup java `cat conf/druid/overlord/jvm.config | xargs` -cp conf/druid/_common:conf/druid/overlord:lib/* io.druid.cli.Main server overlord > var/overlord.out 2>&1 &
