#!/bin/sh

if [ $# -ne 1 ]; then
    echo "Usage: $0 file_to_distribute"
    exit -1
fi

TARGET=$1
BASEDIR=/home/hadoop/server
BASENAME=${TARGET##*/}


for i in `cat slaves`
do
    echo scp -r $TARGET $i:$BASEDIR/
    scp -r $TARGET $i:$BASEDIR/
    if [[ $TARGET == *.tar.gz ]]; then
        DIR=${BASENAME%-bin.tar.gz}
    ssh $i "cd $BASEDIR ; rm druid; tar zxvf $BASENAME ; ln -s $BASEDIR/$DIR druid"
    #ssh $i rm -rf $DIR.tar.gz
        #ssh $i tar zxvf $BASENAME
    fi
done
