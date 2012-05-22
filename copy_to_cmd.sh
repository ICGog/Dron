#!/bin/bash

source "~/code/Dron/dron_exports"

for instance in $ADDR_NODES
do
    scp "$DRON_SRC_DIR" whirr@$instance:"$DRON_DEST_DIR"
done

