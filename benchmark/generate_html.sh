#!/bin/bash

hadoop jar jars/dataloader.jar rankings "$DATA_DIR/Rankings.dat" /input/rankings/Rankings.dat

hadoop jar jars/dataloader.jar uservisits "$DATA_DIR/UserVisits.dat" /input/uservisits/UserVisits.dat