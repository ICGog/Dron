#!/bin/bash

source "~/code/Dron/dron_exports"

for instance in $ADDR_NODES
do
    scp -r whirr@$instance:"$DRON_SRC_DIR" "$DRON_DEST_DIR"
done
