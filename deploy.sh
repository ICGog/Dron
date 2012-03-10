#!/bin/bash
# Expects DRON_NODES and DRON_BRANCH
# If DRON_NEW is empty then the Dron directory is created as well.

if [ -z "$DRON_NEW" ]; then
    for instance in $DRON_NODES; do
        ssh ubuntu@$instance "cd Dron; make clean; git pull origin $DRON_BRANCH; ./rebar get-deps; make compile"
    done
else
    for instance in $DRON_NODES; do
        ssh ubuntu@$instance "git clone git://github.com/ICGog/Dron.git; cd Dron; git pull origin $DRON_BRANCH; ./rebar get-deps; make compile"
    done
fi