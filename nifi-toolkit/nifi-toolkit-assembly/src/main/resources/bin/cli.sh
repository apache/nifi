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
#
#

# Script structure inspired from Apache Karaf and other Apache projects with similar startup approaches

SCRIPT_DIR=$(dirname "$0")
SCRIPT_NAME=$(basename "$0")
NIFI_TOOLKIT_HOME=$(cd "${SCRIPT_DIR}" && cd .. && pwd)
PROGNAME=$(basename "$0")


warn() {
    echo "${PROGNAME}: $*"
}

die() {
    warn "$*"
    exit 1
}

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
}

apply_java_compatibility() {
    compatibility_arg=""
    compatibility_lib=""
    java_version="$("${JAVA}" -version 2>&1 | head -n 1 | awk -F '"' '{print $2}')"

    case "$java_version" in
        9*|10*)
            compatibility_arg="--add-modules=java.xml.bind"
            ;;
        [1-9][1-9]*)
            # java versions 11-99
            compatibility_lib="${NIFI_TOOLKIT_HOME}/lib/java11/*"
            ;;
        1.*)
            ;;
    esac

    JAVA_OPTS="${JAVA_OPTS:--Xms128m -Xmx256m}"
    if [ "x${compatibility_arg}" != "x" ]; then
        JAVA_OPTS="${JAVA_OPTS} $compatibility_arg"
    fi

    if [ "x${compatibility_lib}" != "x" ]; then
        CLASSPATH="$CLASSPATH$classpath_separator$compatibility_lib"
    fi
}

init() {
    # Determine if there is special OS handling we must perform
    detectOS

    # Locate the Java VM to execute
    locateJava
}

run() {
    LIBS="${NIFI_TOOLKIT_HOME}/lib/*"

    sudo_cmd_prefix=""
    if $cygwin; then
        classpath_separator=";"
        NIFI_TOOLKIT_HOME=$(cygpath --path --windows "${NIFI_TOOLKIT_HOME}")
        CLASSPATH="$NIFI_TOOLKIT_HOME/classpath;$(cygpath --path --windows "${LIBS}")"
    else
        classpath_separator=":"
        CLASSPATH="$NIFI_TOOLKIT_HOME/classpath:${LIBS}"
    fi

   export JAVA_HOME="$JAVA_HOME"
   export NIFI_TOOLKIT_HOME="$NIFI_TOOLKIT_HOME"
   apply_java_compatibility

   umask 0077
   exec "${JAVA}" -cp "${CLASSPATH}" ${JAVA_OPTS} org.apache.nifi.toolkit.cli.CLIMain "$@"
}


init
run "$@"
