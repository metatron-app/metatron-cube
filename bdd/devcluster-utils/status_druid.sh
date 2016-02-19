for i in `cat slaves`
do
  ssh $i ps -ef | grep io.druid.cli.Main | grep -v grep
done

echo "coordinator : emn-g04-03:8081"
echo "overlord : emn-g04-03:8090"
echo "broker : ear-g04-05:8082"
echo "historical : ear-g04-01:8083"
echo "historical : ear-g04-02:8083"
echo "middlemanager: ear-g04-03:8091"
echo "middlemanager: ear-g04-04:8091"
