#!/bin/bash

count=1
for mem in $(ls memory-*)
do
    echo $mem
    cat $mem | awk '{print $24 " " $26 " " $27}' | tail -n +"$1" | head -n "$2" >> "mem-$count.in"
    count=$((count+1))
done