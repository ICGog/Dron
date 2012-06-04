#!/bin/bash

for mem in $(ls memory-ip*)
do
    echo $mem
    cat $mem | awk '{print $24 " " $26 " " $27}' | tail -n +"$1" | head -n "$2" >> "dron-mem.in"
done