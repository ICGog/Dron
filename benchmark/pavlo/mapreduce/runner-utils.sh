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

## =====================================================
## BASIC SETUP
## =====================================================

##
## Colors & Other Niceties
##
if [ "$TERM" != "screen" ]; then
   COLOR_GREEN='\E[32m'
   COLOR_RED='\E[31m'
   alias reset="tput sgr0"
else
   COLOR_GREEN=''
   COLOR_RED=''
   alias reset=""
fi
OUTER_LINE="====================================================================="
INNER_LINE="---------------------------------------------------------------------"

## =====================================================
## OUTPUT UTILITY FUNCTIONS
## =====================================================

## -----------------------------------------------------
## Print Success Message
## -----------------------------------------------------
success() {
   args=( $@ )
   echo -ne $COLOR_GREEN
   echo -n ${args[0]}
   reset
   echo " "${args[@]:1:${#args[@]}}
}
## -----------------------------------------------------
## Print Error Message
## -----------------------------------------------------
error() {
   args=( $@ )
   echo -ne $COLOR_RED
   echo -n ${args[0]}
   reset
   echo " "${args[@]:1:${#args[@]}}
}
## -----------------------------------------------------
## Clear Characters
## -----------------------------------------------------
clearChars() {
   if [ "$TERM" != "screen" ]; then
      ctr=0
      while [ "$ctr" -lt "$1" ]; do
         echo -ne "\b"
         ctr=`expr $ctr + 1`
      done
      ctr=0
      while [ "$ctr" -lt "$1" ]; do
         echo -n " "
         ctr=`expr $ctr + 1`
      done
      ctr=0
      while [ "$ctr" -lt "$1" ]; do
         echo -ne "\b"
         ctr=`expr $ctr + 1`
      done
   fi
}
## -----------------------------------------------------
## Benchmark Run Banner
## -----------------------------------------------------
banner() {
   JAVA_VERSION=`$JAVA_HOME/bin/java -version 2>&1 | perl -ane '$_ =~ m/"(.*?)"/; print $1; exit;'`
   echo $OUTER_LINE
   printf "$PRINTF_FORMAT%s\n" "Cluster:"        "$CLUSTER_CONFIG - Data Set '$1'"
   printf "$PRINTF_FORMAT%s\n" "Benchmark Conf:" $CONFIGURATION_FILE
   printf "$PRINTF_FORMAT%s\n" "Hadoop Home:"    `readlink -f -n $HADOOP_HOME`
   printf "$PRINTF_FORMAT%s\n" "Java Home:"      `readlink -f $JAVA_HOME`" ($JAVA_VERSION)"
   printf "$PRINTF_FORMAT%s\n" "Slave List:"     $SLAVE_FILE
   printf "$PRINTF_FORMAT%s\n" "Console Output:" $CONSOLE_OUTPUT
   printf "$PRINTF_FORMAT%s\n" "HDFS Master:"    $HDFS_MASTER_HOST
   printf "$PRINTF_FORMAT%s\n" "Job Tracker:"    $JOB_TRACKER_HOST
   printf "$PRINTF_FORMAT%s\n" "Execution Host:" `hostname --long`
   printf "$PRINTF_FORMAT%s\n" "# of Mappers:"   $NUM_OF_MAPPERS
   printf "$PRINTF_FORMAT%s\n" "# of Reducers:"  $NUM_OF_REDUCERS
   printf "$PRINTF_FORMAT%s\n" "# of Trials:"    $NUM_OF_TRIALS
   printf "$PRINTF_FORMAT%s\n" "Combine Results:" `yesno $COMBINE_RESULTS`
   printf "$PRINTF_FORMAT%s\n" "Compress Data:"   `yesno $COMPRESS_DATA`
   printf "$PRINTF_FORMAT%s\n" "Split Data:"      `yesno $SPLIT_DATA`
   printf "$PRINTF_FORMAT%s\n" "Sequence Files:"  `yesno $SEQUENCE_FILE`
   printf "$PRINTF_FORMAT%s\n" "Tuple Data:"      `yesno $TUPLE_DATA`
   printf "$PRINTF_FORMAT" "Input Limit:"
   if [ $INPUT_LIMIT -gt 0 ]; then
      echo $INPUT_LIMIT
   else
      echo "None"
   fi
   printf "$PRINTF_FORMAT" "Start Time:"
   date
}
## -----------------------------------------------------
## Move an output file by appending its mtime date to it
## -----------------------------------------------------
moveOutputFile() {
   timestamp=`date "+%F-%R" --reference=$1`
   newFile="$1.$timestamp"
   mv $1 $newFile
   gzip --best $newFile
}
## -----------------------------------------------------
## Hack for ucfirst
## -----------------------------------------------------
ucfirst() {
   echo $1 | perl -ane 'print ucfirst($_);'
}
## -----------------------------------------------------
## Hack for toUpper
## -----------------------------------------------------
toUpper() {
   echo $1 | tr "[:lower:]" "[:upper:]"
}
## -----------------------------------------------------
## Print yes or no
## -----------------------------------------------------
yesno() {
   if [ "$1" = 1 ]; then
      echo "Yes"
   else
      echo "No"
   fi
}

## =====================================================
## HADOOP UTILITY FUNCTIONS
## =====================================================

## -----------------------------------------------------
## Remote command execution on a host
## -----------------------------------------------------
remoteExec() {
   remoteExec_host=$1
   remoteExec_cmd=$2
   remoteExec_noblock=$3
   remoteExec_tries=10
   remoteExec_ok=0
   while [ $remoteExec_tries -gt "0" ]; do
      if [ -z "$remoteExec_noblock" ]; then
         ssh $SSH_OPTIONS $remoteExec_host "$remoteExec_cmd" >> $CONSOLE_OUTPUT && echo -n $WAIT_CHARACTER &
      else
         ssh $SSH_OPTIONS $remoteExec_host "$remoteExec_cmd" >> $CONSOLE_OUTPUT && echo -n $WAIT_CHARACTER
      fi
      if [ "$?" -eq "0" ]; then
         remoteExec_ok=1
         break
      fi
      echo -n $FAIL_CHARACTER
      echo "Failed to remotely execute '$remoteExec_cmd' on '$remoteExec_host'. Will try $remoteExec_tries more times" >> $CONSOLE_OUTPUT
      echo "Sleeping for $HADOOP_EXEC_SLEEP seconds" >> $CONSOLE_OUTPUT
      sleep $HADOOP_EXEC_SLEEP
      remoteExec_tries=`expr $remoteExec_tries - 1`
   done
   if [ "$remoteExec_ok" -ne "1" ]; then
      error $FAIL_MSG
      shutdown
   fi
}

## -----------------------------------------------------
## Cleanup and quit
## -----------------------------------------------------
trap "shutdown" SIGINT
shutdown() {
   SHUTDOWN_CALLED=1
   cleanup
   exit
}
## -----------------------------------------------------
## This function attempts to take down the Hadoop services (if they were started)
## -----------------------------------------------------
cleanup() {
   if [ "$SHUTDOWN_CALLED" = "1" ]; then
      echo
   fi
   echo $INNER_LINE
   echo "Cleanup..."
   
   if [ $JOB_TRACKER_ISRUNNING = 1 ]; then
      jobTracker "stop"
   fi

   if [ $HDFS_MASTER_ISRUNNING = 1 ]; then
      ##
      ## Remove Input/Output Directories in HDFS before we stop the DFS server
      ##
      removeInput
      if [ $UNLOAD_OUTPUT_DATA = 1 ]; then
         removeOutput
      fi
      hdfsMaster "stop"
   fi
   echo "DONE!"
}
## -----------------------------------------------------
## Remove output directories from HDFS
## -----------------------------------------------------
removeOutput() {
   doesDirExist "${BASE_HDFS_OUTPUT_DIR}/${CLUSTER_CONFIG}"
   if [ $? = 0 -o $UNLOAD_OUTPUT_DATA = 1 ]; then
      printf "$PRINTF_FORMAT" "Remove Output:"
      hadoopExec "fs -rmr $BASE_HDFS_OUTPUT_DIR/$CLUSTER_CONFIG"
      if [ $? -ne "0" ]; then
         error $FAIL_MSG
      else
         success $SUCCESS_MSG
      fi
      reset
   fi
}
## -----------------------------------------------------
## Remove Input Directories from HDFS
## -----------------------------------------------------
removeInput() {
   if [ $UNLOAD_INPUT_DATA = 1 ]; then
      doesDirExist $BASE_HDFS_INPUT_DIR
      if [ $? = 0 -a $HDFS_DATA_LOADED = 1 ]; then
         printf "$PRINTF_FORMAT" "Remove Input:"
         hadoopExec "fs -rmr $BASE_HDFS_INPUT_DIR"
         HDFS_DATA_LOADED=0
         success $SUCCESS_MSG
      fi
   fi
   reset
}
## -----------------------------------------------------
## Execute a Hadoop command
## We will attempt to execute the operation multiple times
## in order to handle things like services that have not
## fully started yet
## -----------------------------------------------------
hadoopExec() {
   hadoopExec_cmd=$@
   hadoopExec_tries=0
   hadoopExec_ok=0
   while [ $hadoopExec_tries -lt $HADOOP_EXEC_ATTEMPTS ]; do
#       echo "Execute: $CMD_HADOOP $hadoopExec_cmd" >> $CONSOLE_OUTPUT
      $CMD_HADOOP $hadoopExec_cmd 2>> $CONSOLE_OUTPUT 1>> $CONSOLE_OUTPUT
      result=$?
#       echo "Result: $result" >> $CONSOLE_OUTPUT
      if [ "$result" -eq "0" ]; then
         hadoopExec_ok=1
         break
      fi
      echo -n $FAIL_CHARACTER
      echo "Failed to execute '$hadoopExec_cmd'. Attempt $hadoopExec_tries out of $HADOOP_EXEC_ATTEMPTS" >> $CONSOLE_OUTPUT
      echo "Sleeping for $HADOOP_EXEC_SLEEP seconds" >> $CONSOLE_OUTPUT
      sleep $HADOOP_EXEC_SLEEP
      hadoopExec_tries=`expr $hadoopExec_tries + 1`
   done
   if [ $hadoopExec_ok -ne "1" ]; then
      error "FAILED TO EXECUTE '$hadoopExec_cmd'"
      shutdown
   fi
   clearChars $hadoopExec_tries
   return 0
}
## -----------------------------------------------------
## Reformat HDFS
## -----------------------------------------------------
hadoopReformat() {
   printf "$PRINTF_FORMAT" "Reformat HDFS:"
   $CMD_EXEC_PUSHER "rm -rf $INTERNAL_HADOOP_DIR ; mkdir -p $INTERNAL_HADOOP_DIR/logs" --hosts=$1 >> $CONSOLE_OUTPUT
   echo "Y" | hadoopExec "namenode -format" 2>&1 | cat >> $CONSOLE_OUTPUT
   success $SUCCESS_MSG
}
## -----------------------------------------------------
## Flush Disk Cache on all Nodes
## -----------------------------------------------------
flushCache() {
   printf "$PRINTF_FORMAT" "Flushing Cache:"
   $CMD_EXEC_PUSHER $CMD_FLUSH_CACHE --hosts=$1 >> $CONSOLE_OUTPUT
   success $SUCCESS_MSG
}
## -----------------------------------------------------
## Toggle HDFS Master On/Off
## -----------------------------------------------------
hdfsMaster() {
   action=$1
   printf "$PRINTF_FORMAT" `ucfirst $action`" HDFS Master:"
   ssh $SSH_OPTIONS $HDFS_MASTER_HOST "$HADOOP_HOME/bin/$action-dfs.sh" >> $CONSOLE_OUTPUT
   if [ "$?" -ne 0 ]; then
      error $FAIL_MSG
      cleanup
      exit
   fi
   success $SUCCESS_MSG
   HDFS_MASTER_ISRUNNING=1
   sleep 5
}
## -----------------------------------------------------
## Toggle JobTracker On/Off
## -----------------------------------------------------
jobTracker() {
   action=$1
   printf "$PRINTF_FORMAT" `ucfirst $action`" Job Tracker:"
   ssh $SSH_OPTIONS $JOB_TRACKER_HOST "$HADOOP_HOME/bin/$action-mapred.sh" >> $CONSOLE_OUTPUT
   if [ "$?" -ne 0 ]; then
      error $FAIL_MSG
      cleanup
      exit
   fi
   success $SUCCESS_MSG
   JOB_TRACKER_ISRUNNING=1
}
## -----------------------------------------------------
## Check whether a dir exists in HDFS
## -----------------------------------------------------
doesDirExist() {
   ${HADOOP_HOME}/bin/hadoop fs -ls $1 2>&1 | grep -v "No such file or directory" | grep --silent "$1"
   return $?
}

## =====================================================
## Internal flags (don't change)
## =====================================================
HDFS_MASTER_ISRUNNING=0
JOB_TRACKER_ISRUNNING=0
HDFS_DATA_LOADED=0
SHUTDOWN_CALLED=0

## =====================================================
## LOAD BENCHMARKS CONFIGURATION
## =====================================================
if [ $# = "0" ]; then
   echo "USAGE: $0 </path/to/benchmarks.conf>"
   exit 1
elif [ ! -f "$1" ]; then
   error "ERROR:" "The benchmarks configuration file '$1' does not exist!"
   exit 1
fi
source $1
CONFIGURATION_FILE=`readlink -f $1`

## =====================================================
## ENVIRONMENT CHECK
## =====================================================

if [ -z "$HADOOP_CONF_DIR" ]; then
   error "ERROR:" "The environment variable 'HADOOP_CONF_DIR' is empty"
   exit 1
fi

if [ ! -f $BENCHMARK_JAR ]; then
   error "ERROR:" "Missing benchmarks jar file '$BENCHMARK_JAR'"
   exit 1
elif [ ! -f $DATALOADER_JAR ]; then
   error "ERROR:" "Missing dataloader jar file '$DATALOADER_JAR'"
   exit 1
fi

##
## Log Output Dir
##
if [ ! -d $BASE_CONSOLE_OUTPUT_DIR ]; then
   mkdir -p $BASE_CONSOLE_OUTPUT_DIR
fi
