#!/bin/bash

count=1
for cpu in $(ls cpu-ip*)
do
    echo $cpu
    cat $cpu | tail -n +"$1" | head -n "$2" | grep "20120525 " | awk '{print $34 " " $35}' >> "dsk-$count.in"
    count=$((count+1))
done