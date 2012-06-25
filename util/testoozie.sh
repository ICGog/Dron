#!/bin/bash

for run in `seq 1 $1`;
do
    echo Registering job $run
    oozie job -oozie http://localhost:11000/oozie -config examples/apps/no-op/job.properties -run
done
