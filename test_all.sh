#!/usr/bin/env bash

export GOPATH=${PWD}

test_dir=("mapreduce" "raft" "kvraft" "shardmaster" "shardkv")

for dir_name in ${test_dir[@]};
do
    echo "Test in "${dir_name}
    current_dir=${GOPATH}/src/${dir_name}
    cd ${current_dir}
    go test
    echo
done

