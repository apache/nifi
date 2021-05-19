#!/bin/sh
#
#    Licensed to the Apache Software Foundation (ASF) under one or more
#    contributor license agreements.  See the NOTICE file distributed with
#    this work for additional information regarding copyright ownership.
#    The ASF licenses this file to You under the Apache License, Version 2.0
#    (the "License"); you may not use this file except in compliance with
#    the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

# Script structure inspired from Apache Karaf and other Apache projects with similar startup approaches

# Discover the path of the file


# Since MacOS X, FreeBSD and some other systems lack gnu readlink, we use a more portable
# approach based on following StackOverflow comment http://stackoverflow.com/a/1116890/888876

TARGET_FILE=$0

cd $(dirname $TARGET_FILE)
TARGET_FILE=$(basename $TARGET_FILE)

# Iterate down a (possible) chain of symlinks
while [ -L "$TARGET_FILE" ]
do
    TARGET_FILE=$(readlink $TARGET_FILE)
    cd $(dirname $TARGET_FILE)
    TARGET_FILE=$(basename $TARGET_FILE)
done

# Compute the canonicalized name by finding the physical path
# for the directory we're in and appending the target file.
PHYS_DIR=$(pwd -P)

SCRIPT_DIR=$PHYS_DIR
PROGNAME=$(basename "$0")

. "${SCRIPT_DIR}/nifi-stateless-env.sh"

detectOS() {
    # OS specific support (must be 'true' or 'false').
    cygwin=false;
    aix=false;
    os400=false;
    darwin=false;
    case "$(uname)" in
        CYGWIN*)
            cygwin=true
            ;;
        AIX*)
            aix=true
            ;;
        OS400*)
            os400=true
            ;;
        Darwin)
            darwin=true
            ;;
    esac
    # For AIX, set an environment variable
    if ${aix}; then
         export LDR_CNTRL=MAXDATA=0xB0000000@DSA
         echo ${LDR_CNTRL}
    fi
    # In addition to those, go around the linux space and query the widely
    # adopted /etc/os-release to detect linux variants
    if [ -f /etc/os-release ]; then
        . /etc/os-release
    fi
}

locateJava() {
    # Setup the Java Virtual Machine
    if $cygwin ; then
        [ -n "${JAVA}" ] && JAVA=$(cygpath --unix "${JAVA}")
        [ -n "${JAVA_HOME}" ] && JAVA_HOME=$(cygpath --unix "${JAVA_HOME}")
    fi

    if [ "x${JAVA}" = "x" ] && [ -r /etc/gentoo-release ] ; then
        JAVA_HOME=$(java-config --jre-home)
    fi
    if [ "x${JAVA}" = "x" ]; then
        if [ "x${JAVA_HOME}" != "x" ]; then
            if [ ! -d "${JAVA_HOME}" ]; then
                die "JAVA_HOME is not valid: ${JAVA_HOME}"
            fi
            JAVA="${JAVA_HOME}/bin/java"
        else
            warn "JAVA_HOME not set; results may vary"
            JAVA=$(type java)
            JAVA=$(expr "${JAVA}" : '.* \(/.*\)$')
            if [ "x${JAVA}" = "x" ]; then
                die "java command not found"
            fi
        fi
    fi
    # if command is env, attempt to add more to the classpath
    if [ "$1" = "env" ]; then
        [ "x${TOOLS_JAR}" =  "x" ] && [ -n "${JAVA_HOME}" ] && TOOLS_JAR=$(find -H "${JAVA_HOME}" -name "tools.jar")
        [ "x${TOOLS_JAR}" =  "x" ] && [ -n "${JAVA_HOME}" ] && TOOLS_JAR=$(find -H "${JAVA_HOME}" -name "classes.jar")
        if [ "x${TOOLS_JAR}" =  "x" ]; then
             warn "Could not locate tools.jar or classes.jar. Please set manually to avail all command features."
        fi
    fi

}

init() {
    # Determine if there is special OS handling we must perform
    detectOS

    # Locate the Java VM to execute
    locateJava "$1"
}


run() {
    echo
    echo "Java home: ${JAVA_HOME}"
    echo "NiFi home: ${NIFI_HOME}"
    echo

    LOG_PARAMS="-Dorg.apache.nifi.bootstrap.config.log.dir='${NIFI_LOG_DIR}'"

    # uncomment to allow debugging of the bootstrap process
    # DEBUG_PARAMS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=8000"

    LAUNCH_PARAMS="${LOG_PARAMS} ${DEBUG_PARAMS}"


    # default to 1 GB heap
    STATELESS_JAVA_OPTS="${STATELESS_JAVA_OPTS:=-Xms1024m -Xmx1024m}"
    KILL_ON_OOME_OPTS="-XX:OnOutOfMemoryError='kill -TERM %p'"

    echo
    echo "Note: Use of this command is considered experimental. The commands and approach used may change from time to time."
    echo
    echo "Java home (JAVA_HOME): ${JAVA_HOME}"
    echo "NiFi home (NIFI_HOME): ${NIFI_HOME}"
    echo "Java options (STATELESS_JAVA_OPTS): ${STATELESS_JAVA_OPTS}"
    echo
    run_nifi_cmd="'${JAVA}' '-Dlogback.configurationFile=${NIFI_HOME}/conf/stateless-logback.xml' -cp '${NIFI_HOME}/lib/*:${NIFI_HOME}/conf' ${LAUNCH_PARAMS} ${KILL_ON_OOME_OPTS} ${STATELESS_JAVA_OPTS} 'org.apache.nifi.stateless.bootstrap.RunStatelessFlow'"

    eval "cd ${NIFI_HOME}"
    # Our arguments may have spaces (especially for passing parameters). The eval command will strip those out. To avoid that, we need to use "$@" to get the quotes in the arguments, and then
    # surround that by single-ticks in order to prevent eval from stripping the quotes out.
    eval "${run_nifi_cmd}" '"$@"'
    EXIT_STATUS=$?
    echo
    return;
}

init "$1"
run "$@"
