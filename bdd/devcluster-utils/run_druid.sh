#!/bin/sh
# see https://tde.sktelecom.com/wiki/display/BDD/Druid

ssh emn-G04-03 "cd /home/hadoop/server/druid; ./run-coordinator.sh"
ssh emn-G04-03 "cd /home/hadoop/server/druid; ./run-overlord.sh"
ssh ear-G04-01 "cd /home/hadoop/server/druid; ./run-historical.sh"
ssh ear-G04-02 "cd /home/hadoop/server/druid; ./run-historical.sh"
ssh ear-G04-03 "cd /home/hadoop/server/druid; ./run-middleManager.sh"
ssh ear-G04-04 "cd /home/hadoop/server/druid; ./run-middleManager.sh"
ssh ear-G04-05 "cd /home/hadoop/server/druid; ./run-broker.sh"

./status_druid.sh
