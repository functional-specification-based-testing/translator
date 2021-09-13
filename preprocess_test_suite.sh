#!/usr/bin/env bash

if [ $# -ne 1 ]; then
    echo -e "usage:\t$0 <mmtp_output_dir>"
    exit 2
fi
DIR=$1

rm -r res/*
for i in feed/*; do
    f=$(basename $i)
    name=${f/.mmtp/}
    echo $name
    mkdir -p res/$name/actual
    ./preprocess_msgs.sh $DIR/$f res/$name/actual
    mkdir -p res/$name/oracle
    ./preprocess_msgs.sh oracle/$f res/$name/oracle
    sleep 0.01
    diff -u res/$name/oracle res/$name/actual | colordiff
    # break
done
