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
export C2_SERVER_HOME=$(cd "${SCRIPT_DIR}" && cd .. && pwd)
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

init() {
    # Determine if there is special OS handling we must perform
    detectOS

    # Locate the Java VM to execute
    locateJava
}

run() {
    LIBS="${C2_SERVER_HOME}/lib/*"

    sudo_cmd_prefix=""
    if $cygwin; then
        C2_SERVER_HOME=$(cygpath --path --windows "${C2_SERVER_HOME}")
        CLASSPATH="$C2_SERVER_HOME/conf;$(cygpath --path --windows "${LIBS}")"
    else
        CLASSPATH="$C2_SERVER_HOME/conf:${LIBS}"
    fi

    echo
    echo "Java home: ${JAVA_HOME}"
    echo "C2 Server home: ${C2_SERVER_HOME}"
    echo
    echo

  if [ "$1" = "debug" ]; then
    "${JAVA}" -cp "${CLASSPATH}"  -Xms12m -Xmx24m -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005 -Djava.net.preferIPv4Stack=true org.apache.nifi.minifi.c2.jetty.JettyServer $@
  else
    "${JAVA}" -cp "${CLASSPATH}" -Xms12m -Xmx24m -Djava.net.preferIPv4Stack=true org.apache.nifi.minifi.c2.jetty.JettyServer $@
  fi
   return $?
}

init
run "$@"
