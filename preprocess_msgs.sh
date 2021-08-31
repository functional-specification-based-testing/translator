#!/usr/bin/env bash

if [ $# -ne 2 ]; then
    echo -e "usage:\t$0 <input.mmtp> <dir>"
    exit 2
fi


SRC=$1
DIR=$2

rm -r -f $DIR
mkdir -p $DIR
cat $SRC | \
    tee \
        >(grep -E "^.{16}0105" | sed -E "s/^(.{100}).{8}/\1        /" | sed -E "s/^(.{108}).{6}/\1      /" | sed -E "s/^(.{121}).{8}/\1        /" | sed -E "s/^(.{174}).{14}/\1              /" | sed -E "s/^(.{199}).{20}/\1                    /" >$DIR/SLE-0105.mmtp) \
        >(grep -E "^.{16}0144" | sed -E "s/^(.{24}).{77}/\1                                                                             /" >$DIR/SLE-0144.mmtp) \
        >(grep -E "^.{16}0138" | sed -E "s/^(.{95}).{6}/\1      /" | sed -E "s/^(.{138}).{14}/\1              /" >$DIR/SLE-0138.mmtp) \
        >(grep -E "^.{16}0172" | sed -E "s/^(.{84}).{6}/\1      /" | sed -E "s/^(.{218}).{14}/\1              /" | sed -E "s/^(.{255}.{14}).{20}/\1                    /" >$DIR/SLE-0172.mmtp) \
        > /dev/null
