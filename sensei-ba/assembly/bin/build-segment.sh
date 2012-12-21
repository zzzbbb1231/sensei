#!/usr/bin/env bash

#usage="Usage: build-segment.sh "<segmentname> <file path>  [--exclude=<<comma separated list of excluded columns>>] [--sort=<<comma separated list of  columns to sort>>] [output directory]"

# if no args specified, show usage
#if [ $# -le 2 ]; then
#  echo $usage
#  exit 1
#fi

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

OS=`uname`
IP="" # store IP
case $OS in
   Linux) IP=`/sbin/ifconfig  | grep 'inet addr:'| grep -v '127.0.0.1' | cut -d: -f2 | awk '{ print $1}' | head -n 1`;;
   FreeBSD|OpenBSD|Darwin) IP=`ifconfig | grep -E '^en[0-9]:' -A 4 | grep -E 'inet.[0-9]' | grep -v '127.0.0.1' | awk '{ print $2}' | head -n 1` ;;
   SunOS) IP=`ifconfig -a | grep inet | grep -v '127.0.0.1' | awk '{ print $2} ' | head -n 1` ;;
   *) IP="Unknown";;
esac


lib=$bin/../lib
resources=$bin/../config
logs=$bin/../logs


# HEAP_OPTS="-Xmx4096m -Xms2048m -XX:NewSize=1024m" # -d64 for 64-bit awesomeness
HEAP_OPTS="-Xmx2g -Xms1g -XX:NewSize=256m  -d64 -XX:MaxDirectMemorySize=-1"
# HEAP_OPTS="-Xmx1024m -Xms512m -XX:NewSize=128m"
# HEAP_OPTS="-Xmx512m -Xms256m -XX:NewSize=64m"
GC_OPTS=" -XX:+UseConcMarkSweepGC -XX:+UseParNewGC"
#JAVA_DEBUG="-Xdebug -Xrunjdwp:transport=dt_socket,address=1044,server=y,suspend=y"
#GC_OPTS="-XX:+UseConcMarkSweepGC -XX:+UseParNewGC"
JAVA_OPTS="-server -d64"

MAIN_CLASS="com.senseidb.ba.util.IndexConverter"


CLASSPATH=$lib/*:$dist/*




  
   java $JAVA_OPTS  $HEAP_OPTS $GC_OPTS $JAVA_DEBUG -classpath $CLASSPATH   $MAIN_CLASS $1 $2 $3 $4 $5 $6 

