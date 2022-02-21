#!/bin/bash

# Get the relative path to this file.
DIR=$(dirname "$0")

CLASSPATH=".:${DIR}:${DIR}/bin/*:${DIR}/bin:${DIR}/lib/*:${DIR}/cfg/*:${DIR}/.."

JVM_SETTINGS="-Dfile.encoding=UTF-8"

JAVACMD=java
if [ $JAVA_HOME ];
then
	JAVACMD=$JAVA_HOME/bin/java
fi
   ${JAVACMD} ${JVM_SETTINGS} -cp ${CLASSPATH} com.zhouhc.readLog.ReadWriteLog "$@"



