#!/bin/bash

hadoop jar jars/dataloader.jar rankings "$DATA_DIR/rankings/Rankings.dat" /input/rankings/Rankings.dat

hadoop jar jars/dataloader.jar uservisits "$DATA_DIR/uservisits/UserVisits.dat" /input/uservisits/UserVisits.dat
