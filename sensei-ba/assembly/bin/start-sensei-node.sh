#!/usr/bin/env bash

#usage="Usage: start-sensei-node.sh <id> <port> <partitions> <conf-dir>"

# if no args specified, show usage
#if [ $# -le 3 ]; then
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

if [[ ! -d $logs ]]; then
  echo "Log file does not exists, creating one..."
  mkdir $logs
fi

# HEAP_OPTS="-Xmx4096m -Xms2048m -XX:NewSize=1024m" # -d64 for 64-bit awesomeness
HEAP_OPTS="-Xmx40g -Xms10g -XX:NewSize=256m  -d64 -XX:MaxDirectMemorySize=-1"
# HEAP_OPTS="-Xmx1024m -Xms512m -XX:NewSize=128m"
# HEAP_OPTS="-Xmx512m -Xms256m -XX:NewSize=64m"
GC_OPTS=" -XX:+UseConcMarkSweepGC -XX:+UseParNewGC"
#JAVA_DEBUG="-Xdebug -Xrunjdwp:transport=dt_socket,address=1044,server=y,suspend=y"
#GC_OPTS="-XX:+UseConcMarkSweepGC -XX:+UseParNewGC"
JAVA_OPTS="-server -d64"
JMX_OPTS="-Djava.rmi.server.hostname=$IP -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=18889 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false"

MAIN_CLASS="com.senseidb.search.node.SenseiServer"


CLASSPATH=$resources/:$lib/*:$dist/*:$1/ext/*

PIDFILE=/tmp/sensei-search-node.pid

if [ -f $PIDFILE ]; then
  echo "File $PIDFILE exists shutdown may not be proper"
  echo "Please check PID" `cat $PIDFILE`
  echo "Make sure the node is shutdown and the file" $PIDFILE "is removed before stating the node"
 else
  
   java $JAVA_OPTS $JMX_OPTS $HEAP_OPTS $GC_OPTS $JAVA_DEBUG -classpath $CLASSPATH  -Dlog.home=$logs $MAIN_CLASS $1  &
  echo $! > ${PIDFILE}
  echo "Sensei node started successfully! Logs are at $logs"
 fi
