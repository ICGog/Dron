#!/bin/bash

source "~/code/Dron/dron_exports"

echo "$ADDR_NODES"

for instance in $ADDR_NODES
do
    ssh ubuntu@$instance "$DRON_EXEC_CMD"
done
