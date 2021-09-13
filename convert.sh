#!/usr/bin/env bash


for i in raw/*; do
    f=$(basename $i)
    echo $f
    ./haskell2mmtp.py raw/$f feed/$f.mmtp oracle/$f.mmtp
done
