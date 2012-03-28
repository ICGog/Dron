#!/bin/sh
#/***************************************************************************
# *   Copyright (C) 2009 by Andy Pavlo, Brown University                    *
# *   http://www.cs.brown.edu/~pavlo/                                       *
# *                                                                         *
# *   Permission is hereby granted, free of charge, to any person obtaining *
# *   a copy of this software and associated documentation files (the       *
# *   "Software"), to deal in the Software without restriction, including   *
# *   without limitation the rights to use, copy, modify, merge, publish,   *
# *   distribute, sublicense, and/or sell copies of the Software, and to    *
# *   permit persons to whom the Software is furnished to do so, subject to *
# *   the following conditions:                                             *
# *                                                                         *
# *   The above copyright notice and this permission notice shall be        *
# *   included in all copies or substantial portions of the Software.       *
# *                                                                         *
# *   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,       *
# *   EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF    *
# *   MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.*
# *   IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR     *
# *   OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, *
# *   ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR *
# *   OTHER DEALINGS IN THE SOFTWARE.                                       *
# ***************************************************************************/

##
## Configuration & Utility Functions
##
source ./runner.conf
source ./runner-utils.sh

## =====================================================
## BENCHMARK EXECUTION
## =====================================================
for BENCHMARK_SET in ${BENCHMARK_SETS[@]}; do
   eval INPUT_FILES=( "\${${BENCHMARK_SET}_INPUT_FILES[@]}" )
   eval BENCHMARK_TYPES=( "\${${BENCHMARK_SET}_BENCHMARK_TYPES[@]}" )
   eval DATA_SETS=( "\${${BENCHMARK_SET}_DATA_SETS[@]}" )
   eval LOCAL_DATA_DIR="\${${BENCHMARK_SET}_INPUT_DIR}"
   
   for DATA_SET in ${DATA_SETS[@]}; do
      eval CLUSTER_CONFIGURATIONS=( "\${${BENCHMARK_SET}_${DATA_SET}_CLUSTER_CONFIGURATIONS[@]}" )
      for CLUSTER_CONFIG in ${CLUSTER_CONFIGURATIONS[@]}; do
         ##
         ## Figure out the size of each file for the 1TB data set
         ##
         if [ $BENCHMARK_SET = "SORTGREP" ]; then
            DATA_SET_SIZE="535MB"
            if [ $DATA_SET != "535MB" ]; then
               if [ $CLUSTER_CONFIG = "slaves-025" ]; then
                  DATA_SET_SIZE="40GB"
               elif [ $CLUSTER_CONFIG = "slaves-050" ]; then
                  DATA_SET_SIZE="20GB"
               elif [ $CLUSTER_CONFIG = "slaves-100" ]; then
                  DATA_SET_SIZE="10GB"
               else
                  error "Invalid cluster configuration type '$CLUSTER_CONFIG'"
                  exit 1
               fi
            fi
            INPUT_FILES=( "SortGrep${DATA_SET_SIZE}" )
            DATA_SET=$DATA_SET_SIZE
         fi
      
         SLAVE_FILE="$SLAVES_CONF_DIR/$CLUSTER_CONFIG"
         NUM_OF_MAPPERS=`wc -l $SLAVE_FILE | awk '{print $1}'`
         NUM_OF_REDUCERS=$NUM_OF_MAPPERS
         
         ##
         ## Console Output Log File
         ##
         cd $HADOOP_CONF_DIR
         CONSOLE_OUTPUT="$BASE_CONSOLE_OUTPUT.$BENCHMARK_SET.$CLUSTER_CONFIG"
         if [ -e "$CONSOLE_OUTPUT" ]; then
            moveOutputFile $CONSOLE_OUTPUT "$CONSOLE_OUTPUT.last"
         fi
         cd -
         
         ##
         ## Data Loader Arguments
         ##
         DATALOADER_ARGS=""
         if [ "$TUPLE_DATA" = 1 ]; then
            SEQUENCE_FILE="1"
            DATALOADER_ARGS="$DATALOADER_ARGS -tuple"
         fi
         if [ "$COMPRESS_DATA" = 1 ]; then
            if [ "$SEQUENCE_FILE" = 1 ]; then
               DATALOADER_ARGS="$DATALOADER_ARGS -compress -sequence"
            else
               DATALOADER_ARGS="$DATALOADER_ARGS -local"
            fi
         else
            if [ "$SEQUENCE_FILE" = 1 ]; then
               DATALOADER_ARGS="$DATALOADER_ARGS -sequence"
            fi
         fi
         if [ "$INPUT_LIMIT" -gt 0 ]; then
            DATALOADER_ARGS="$DATALOADER_ARGS -limit $INPUT_LIMIT"
         fi
         if [ "$DEBUG" = 1 ]; then
            DATALOADER_ARGS="$DATALOADER_ARGS -debug"
         fi
         
         ##
         ## Output Banner
         ##
         banner "$BENCHMARK_SET $DATA_SET"
      
         echo $INNER_LINE 
         echo "Initializing..."
         
         ##
         ## Change the symbolic link for the slaves file to the one
         ## we need for the current cluster configuration
         ##
         rm -f $HADOOP_CONF_DIR/slaves
         ln -s $SLAVE_FILE $HADOOP_CONF_DIR/slaves
         
         ##
         ## Reformat HDFS
         ##
         if [ $REFORMAT_HDFS = 1 ]; then
            hadoopReformat $SLAVE_FILE
         fi
         
         ##
         ## HDFS Master
         ##
         if [ $START_HDFS_MASTER = 1 ]; then
            hdfsMaster "start"
         fi
      
         ##
         ## Copy in all of our data
         ## The data is split on multiple machines, so we have go to each one
         ## and go insert it into HDFS. The input data is not formatted the way Hadoop needs it,
         ## so we have to run our cleaner program
         ##
         if [ $LOAD_INPUT_DATA = 1 ]; then
            ##
            ## First clear out input directory
            ##
            printf "$PRINTF_FORMAT" "Input Directory:"
            doesDirExist $BASE_HDFS_INPUT_DIR
            if [ $? = 0 ]; then
               hadoopExec "fs -rmr $BASE_HDFS_INPUT_DIR"
            fi
         
            hadoopExec "fs -mkdir $BASE_HDFS_INPUT_DIR"
            success $SUCCESS_MSG
      
            ##
            ## Drop Cache
            ##
            if [ "$FLUSH_CACHE_BEFORE_LOAD" = "1" ]; then
               flushCache $SLAVE_FILE
            fi
         
            ##
            ## Input Data Files
            ##
            cleanup_files=""
            for datafile in ${INPUT_FILES[@]}; do
               ##
               ## Get the type name that we pass to the dataloader
               ##
               input_type=`echo -n $datafile | perl -ane '$_ =~ m/([A-Za-z]+)[\d]*(?:\.dat|)/; print lc($1);'`
               if [ -z "$input_type" ]; then
                  error "Failed to get input type from '$datafile'"
                  shutdown
               fi
               hadoopExec "fs -mkdir $BASE_HDFS_INPUT_DIR/$input_type"
               
               ##
               ## HACK! We have to make a copy of the docs directory *before* we start keeping
               ## time, since that shouldn't really count against Hadoop
               ##
               if [ $input_type = "docs" -a $COMPRESS_DATA = 1 ]; then
                  printf "$PRINTF_FORMAT" "Preparing $input_type:"
                  data_dirname=`dirname $ORIG_DOCS_DATA_DIR`
                  data_basename=`basename $ORIG_DOCS_DATA_DIR`
                  zip_basename="${data_basename}_zip"
                  cmd="sh -c 'if [ ! -d ${data_dirname}/${zip_basename} ]; then cd $data_dirname && cp -R $data_basename $zip_basename ; fi '"
                  cleanup_files="$cleanup_files ${data_dirname}/${zip_basename}"
                  for host in `cat $SLAVE_FILE`; do
                     remoteExec $host "$cmd"
                  done
                  wait
                  clearChars $NUM_OF_MAPPERS
                  success $SUCCESS_MSG
               fi
               
               ##
               ## Next loop through our list of hosts and copy their version of the file 
               ## into this new directory. We will pipe their file into the convertor running
               ## here and create a new file with their hostname appended to it
               ##
               printf "$PRINTF_FORMAT" "Input[$input_type]:"
               START=$(date +%s%N)
               for host in `cat $SLAVE_FILE`; do
                  cmd=""
                  ##
                  ## HTML Documents
                  ##
                  if [ $input_type = "docs" ]; then
                     ##
                     ## If they want to compress the input files, then we use our extra directory that we zip up
                     ## 
                     if [ $COMPRESS_DATA = 1 ]; then
                        data_dirname=`dirname $ORIG_DOCS_DATA_DIR`
                        data_basename=`basename $ORIG_DOCS_DATA_DIR`
                        zip_basename="${data_basename}_zip"
                        cmd="cd $data_dirname/$zip_basename && sh -c 'if [ \`ls -l docs.*/0.html.gz | wc -l\` != 6 ]; then $CMD_COMPRESS */*.html ; fi' && "
                        input_file="${data_dirname}/${zip_basename}/$input_type.*"
                     else
                        input_file="$ORIG_DOCS_DATA_DIR/$input_type.*"
                     fi
                     output_file="$BASE_HDFS_INPUT_DIR/$input_type/$input_type.$host"
                     cmd="$cmd $CMD_HADOOP fs -put $input_file $output_file"
                     hadoopExec "fs -mkdir $output_file"

                  ##
                  ## SortGrep/UserVisits/Rankings Files
                  ##
                  else
                     input_file="$LOCAL_DATA_DIR/$datafile"
                     output_file="$BASE_HDFS_INPUT_DIR/$input_type/$input_type.$host"
                     
                     ##
                     ## Split the data files into separate, smaller files
                     ##
                     if [ $SPLIT_DATA = 1 ]; then
                        temp_file=`dirname $INTERNAL_HADOOP_DIR`"/$input_type.$host"
                        
                        if [ $input_type = "sortgrep" ]; then
                           SPLIT_SIZE="314572800"
                        else
                           SPLIT_SIZE="419430400"
                        fi
                        
                        cmd="mkdir $temp_file && cd $temp_file && $CMD_FILE_SPLIT --line-bytes=$SPLIT_SIZE $input_file $input_type"
                        input_file=$temp_file
                        cleanup_files="$cleanup_files $temp_file"
                        
                        ##
                        ## Assume compression for now...
                        ##
                        if [ $input_type != "sortgrep" ]; then
                           cmd="$cmd && find $input_file -type f -exec $CMD_HADOOP jar $DATALOADER_JAR -local -xargs $input_type '{}' \; && find $input_file -type f -a ! -name '*.dl' -delete"
                        fi
                        
                        cmd="sh -c \"if [ ! -d $temp_file ]; then $cmd && $CMD_COMPRESS $input_file/$input_type* ; fi\" && $CMD_HADOOP fs -put $input_file $output_file/.. "
                        echo $cmd >> $CONSOLE_OUTPUT
      
                     ##
                     ## If we're not using SequenceFiles but we *are* using Compression, then
                     ## we need to tell the DataLoader to write the data out to a local file, which we
                     ## will then zip up and push into HDFS
                     ##
                     elif [ $COMPRESS_DATA = 1 -a $SEQUENCE_FILE != 1 ]; then
                        temp_file=`dirname $INTERNAL_HADOOP_DIR`"/${input_type}.tmp"
                        zip_file="${temp_file}.gz"
                        output_file="${output_file}.gz"
                        ##
                        ## Just copy the original SortGrep data
                        ##
                        if [ $input_type = "sortgrep" ]; then
                           cmd="$cmd cp $input_file $temp_file"
                        ##
                        ## All other data must go through the DataLoader
                        ##
                        else
                           cmd="$cmd $CMD_HADOOP jar $DATALOADER_JAR $DATALOADER_ARGS $input_type $input_file $temp_file"
                        fi
                        cmd="$cmd && $CMD_COMPRESS $temp_file && $CMD_HADOOP fs -put $zip_file $output_file"
                        cleanup_files="$cleanup_files $zip_file"
                     ##
                     ## SortGrep can just use put if it's going in raw
                     ##
                     elif [ $input_type = "sortgrep" -a $SEQUENCE_FILE != 1 ]; then
                        cmd="$CMD_HADOOP fs -put $input_file $output_file"
                     ##
                     ## Otherwise use the DataLoader to write directly into HDFS
                     ##
                     else
                        cmd="$CMD_HADOOP jar $DATALOADER_JAR $DATALOADER_ARGS $input_type $input_file $output_file"
                     fi
                  fi
                  echo $cmd >> $CONSOLE_OUTPUT
                  remoteExec $host "$cmd"
               done
               wait
               END=$(date +%s%N)
               DIFF=`perl -e "printf(\"%0.3f sec\n\", ($END - $START) / 1000000000);"`
               clearChars $NUM_OF_MAPPERS
               success $DIFF
            done
            HDFS_DATA_LOADED=1
            ##
            ## Cleanup an temporary files that were made when loading the input data
            ##
            if [ "$CLEANUP_INPUT_DATA" = 1 ]; then
               printf "$PRINTF_FORMAT" "Remove Temporary Files:"
               $CMD_EXEC_PUSHER "rm -rf $cleanup_files" --hosts=$SLAVE_FILE >> $CONSOLE_OUTPUT
               success $SUCCESS_MSG
            fi
         fi
         
         ##
         ## Job Tracker
         ##
         if [ $START_JOB_TRACKER = 1 ]; then
            jobTracker "start"
         fi
         
         echo "DONE!"
         echo $INNER_LINE
      
         ##
         ## Combine Results Configuration
         ## We will want to run this multiple times using the loaded data
         ## 
         TRIAL_COMBINE_RESULTS=$COMBINE_RESULTS
         if [ $NUM_OF_REDUCERS -le 1 ]; then
            TRIAL_COMBINE_RESULTS=0
         fi
            
         ## ----------------------------
         ## Trials
         ## ----------------------------
         trial=0
         while [ $trial -lt $NUM_OF_TRIALS ]; do
            echo "Executing Trial #$trial..."
         
            ##
            ## Remove previous results
            ## This is necessary otherwise Hadoop will always fail
            ##
            removeOutput
      
            ##
            ## Drop Cache
            ##
            if [ "$FLUSH_CACHE_BEFORE_TRIAL" = "1" ]; then
               flushCache $SLAVE_FILE
            fi
         
            ##
            ## BenchmarkBase Arguments
            ##
            BASE_ARGS=""
            if [ $TRIAL_COMBINE_RESULTS = 1 ]; then
               BASE_ARGS="$BASE_ARGS -combine"
            fi
            if [ $SPLIT_DATA = 1 ]; then
               BASE_ARGS="$BASE_ARGS -recursive-dirs"
            fi
            if [ $COMPRESS_DATA = 1 ]; then
               BASE_ARGS="$BASE_ARGS -compress"
            fi
            if [ $SEQUENCE_FILE = 1 ]; then
               BASE_ARGS="$BASE_ARGS -sequence"
            fi
            if [ $TUPLE_DATA = 1 ]; then
               BASE_ARGS="$BASE_ARGS -tuple -sequence"
            fi
            if [ $DEBUG = 1 ]; then
               BASE_ARGS="$BASE_ARGS -debug"
            fi
         
            ## ----------------------------
            ## Benchmark Types
            ## ----------------------------
            for BENCHMARK in ${BENCHMARK_TYPES[@]}; do
               BENCHMARK_CLASS=$BENCHMARK
               OUTPUT_TARGET="$BASE_HDFS_OUTPUT_DIR/$CLUSTER_CONFIG/$BENCHMARK"
               INPUT_TARGET=""
               ARGS=$BASE_ARGS
      
               ##
               ## Benchmark Parameters
               ##
               case $BENCHMARK in
                  Benchmark1)
                     INPUT_TARGET="$BASE_HDFS_INPUT_DIR/rankings"
                     ARGS="$ARGS -Dmapreduce.minpagerank=10"
                     ;;
                  Benchmark2)
                     INPUT_TARGET="$BASE_HDFS_INPUT_DIR/uservisits"
                     # BENCHMARK_CLASS="DummyJob"
                     ;;
                  Benchmark2a)
                     INPUT_TARGET="$BASE_HDFS_INPUT_DIR/uservisits"
                     BENCHMARK_CLASS="Benchmark2"
                     ARGS="$ARGS -basename $BENCHMARK -Dmapreduce.benchmarks2.substring=true"
                     ;;
                  Benchmark3)
                     INPUT_TARGET="$BASE_HDFS_INPUT_DIR/uservisits $BASE_HDFS_INPUT_DIR/rankings"
                     ARGS="$ARGS -Dmapreduce.startdate=2008-01-15 -Dmapreduce.stopdate=2008-01-22"
                     ;;
                  Benchmark4)
                     for host in `cat $SLAVE_FILE`; do
                        INPUT_TARGET="${INPUT_TARGET}${BASE_HDFS_INPUT_DIR}/docs/docs.$host/ "
                     done
                     ARGS="$ARGS -recursive-dirs"
                     ;;
                  Grep)
                     INPUT_TARGET="$BASE_HDFS_INPUT_DIR/sortgrep"
                     GREP_ARGS=( \
                        "-Dmapreduce.grep.textfind=true" \
                        #"-Dmapreduce.grep.indexof=true" \
                        #"-Dmapreduce.grep.sigmod=true" \
                        "-Dmapreduce.grep.pattern=XYZ" \
                        "-Dmapreduce.grep.match_group=-1" \
                     )
                     ARGS="$ARGS ${GREP_ARGS[@]}"
                     NUM_OF_REDUCERS=0
                     BENCHMARK_CLASS="Grep"
                     ;;
                  Sort)
                     INPUT_TARGET="$BASE_HDFS_INPUT_DIR/sortgrep"
                     BENCHMARK_CLASS="Sort"
                     ;;
                  DummyJob)
                     INPUT_TARGET="$BASE_HDFS_INPUT_DIR/uservisits"
                     #INPUT_TARGET="$BASE_HDFS_INPUT_DIR/sortgrep"
                     NUM_OF_REDUCERS=0
                     ;;
                  *)
                     error "Invalid Benchmark Type: $BENCHMARK"
                     exit
                     ;;
               esac
               ##
               ## Drop Cache
               ##
               if [ "$FLUSH_CACHE_BEFORE_BENCHMARK" = "1" ]; then
                  flushCache $SLAVE_FILE
               fi
               ##
               ## Execute
               ## We always have to delete the output target
               ##
               printf "$PRINTF_FORMAT" "$BENCHMARK:"
               TIME=`$CMD_TIME $CMD_HADOOP jar $BENCHMARK_JAR $BENCHMARK_CLASS $INPUT_TARGET $OUTPUT_TARGET -m $NUM_OF_MAPPERS -r $NUM_OF_REDUCERS $ARGS >> $CONSOLE_OUTPUT`
               echo -n $TIME
               
               if [ $TRIAL_COMBINE_RESULTS = 1 -a $BENCHMARK != "Benchmark3" ]; then
                  printf "$PRINTF_FORMAT" "Combine:"
                  $CMD_FINISH_TIME $CONSOLE_OUTPUT "$BENCHMARK.combine" $trial
               fi
               echo
            done
            trial=`expr $trial + 1`
         done # TRIAL
         echo "DONE!"
         cleanup
         echo ""
      done # CLUSTER CONFIGURATION
   done # DATA SET
done # BENCHMARK SET
exit
