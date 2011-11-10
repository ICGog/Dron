#!/bin/bash

for node in "$@"
do
    if [ "$node" != "$1" ]
    then
        ssh ${node} $1    
    fi
done
