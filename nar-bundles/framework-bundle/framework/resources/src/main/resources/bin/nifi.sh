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

# Script structure inspired from Apache Karaf and other Apache projects with similar startup approaches

DIRNAME=`dirname "$0"`
PROGNAME=`basename "$0"`

#
# Sourcing environment settings for NIFI similar to tomcats setenv
#
NIFI_SCRIPT="nifi.sh"
export NIFI_SCRIPT
if [ -f "$DIRNAME/setenv.sh" ]; then
  . "$DIRNAME/setenv.sh"
fi

#
# Check/Set up some easily accessible MIN/MAX params for JVM mem usage
#
if [ "x$JAVA_MIN_MEM" = "x" ]; then
    JAVA_MIN_MEM=512M
    export JAVA_MIN_MEM
fi
if [ "x$JAVA_MAX_MEM" = "x" ]; then
    JAVA_MAX_MEM=512M
    export JAVA_MAX_MEM
fi
if [ "x$JAVA_PERMSIZE" = "x" ]; then
    JAVA_PERMSIZE=128M
    export JAVA_PERMSIZE
fi
if [ "x$JAVA_MAX_PERMSIZE" = "x" ]; then
    JAVA_MAX_PERMSIZE=128M
    export JAVA_MAX_PERMSIZE
fi

#
#Readlink is not available on all systems. Change variable to appropriate alternative as part of OS detection
#

READLINK="readlink"

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
    case "`uname`" in
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
    if $aix; then
         export LDR_CNTRL=MAXDATA=0xB0000000@DSA
         echo $LDR_CNTRL
    fi
    if $darwin; then
    	READLINK="greadlink"
    fi
}

unlimitFD() {
    # Use the maximum available, or set MAX_FD != -1 to use that
    if [ "x$MAX_FD" = "x" ]; then
        MAX_FD="maximum"
    fi

    # Increase the maximum file descriptors if we can
    if [ "$os400" = "false" ] && [ "$cygwin" = "false" ]; then
        MAX_FD_LIMIT=`ulimit -H -n`
        if [ "$MAX_FD_LIMIT" != 'unlimited' ]; then
            if [ $? -eq 0 ]; then
                if [ "$MAX_FD" = "maximum" -o "$MAX_FD" = "max" ]; then
                    # use the system max
                    MAX_FD="$MAX_FD_LIMIT"
                fi

                ulimit -n $MAX_FD > /dev/null
                # echo "ulimit -n" `ulimit -n`
                if [ $? -ne 0 ]; then
                    warn "Could not set maximum file descriptor limit: $MAX_FD"
                fi
            else
                warn "Could not query system maximum file descriptor limit: $MAX_FD_LIMIT"
            fi
        fi
    fi
}

locateHome() {
    if [ "x$NIFI_HOME" != "x" ]; then
        warn "Ignoring predefined value for NIFI_HOME"
    fi

    # In POSIX shells, CDPATH may cause cd to write to stdout
    (unset CDPATH) >/dev/null 2>&1 && unset CDPATH
    NIFI_HOME=$(dirname $($READLINK -f $0))/../
    NIFI_HOME=$($READLINK -f $NIFI_HOME)
    cd $NIFI_HOME
    echo "Directory changed to NIFI_HOME of '$NIFI_HOME'"
    if [ ! -d "$NIFI_HOME" ]; then
        die "NIFI_HOME is not valid: $NIFI_HOME"
    fi

}

locateBase() {
    if [ "x$NIFI_BASE" != "x" ]; then
        if [ ! -d "$NIFI_BASE" ]; then
            die "NIFI_BASE is not valid: $NIFI_BASE"
        fi
    else
        NIFI_BASE=$NIFI_HOME
    fi
}


locateConf() {
    if [ "x$NIFI_CONF" != "x" ]; then
        if [ ! -d "$NIFI_CONF" ]; then
            die "NIFI_CONF is not valid: $NIFI_CONF"
        fi
    else
        NIFI_CONF=$NIFI_BASE/conf
    fi
}

setupNativePath() {
    # Support for loading native libraries
    LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:$NIFI_BASE/lib:$NIFI_HOME/lib"

    # For Cygwin, set PATH from LD_LIBRARY_PATH
    if $cygwin; then
        LD_LIBRARY_PATH=`cygpath --path --windows "$LD_LIBRARY_PATH"`
        PATH="$PATH;$LD_LIBRARY_PATH"
        export PATH
    fi
    export LD_LIBRARY_PATH
}

pathCanonical() {
    local dst="${1}"
    while [ -h "${dst}" ] ; do
        ls=`ls -ld "${dst}"`
        link=`expr "$ls" : '.*-> \(.*\)$'`
        if expr "$link" : '/.*' > /dev/null; then
            dst="$link"
        else
            dst="`dirname "${dst}"`/$link"
        fi
    done
    local bas=`basename "${dst}"`
    local dir=`dirname "${dst}"`
    if [ "$bas" != "$dir" ]; then
        dst="`pathCanonical "$dir"`/$bas"
    fi
    echo "${dst}" | sed -e 's#//#/#g' -e 's#/./#/#g' -e 's#/[^/]*/../#/#g'
}

locateJava() {
    # Setup the Java Virtual Machine
    if $cygwin ; then
        [ -n "$JAVA" ] && JAVA=`cygpath --unix "$JAVA"`
        [ -n "$JAVA_HOME" ] && JAVA_HOME=`cygpath --unix "$JAVA_HOME"`
    fi

    if [ "x$JAVA" = "x" ] && [ -r /etc/gentoo-release ] ; then
        JAVA_HOME=`java-config --jre-home`
    fi
    if [ "x$JAVA" = "x" ]; then
        if [ "x$JAVA_HOME" != "x" ]; then
            if [ ! -d "$JAVA_HOME" ]; then
                die "JAVA_HOME is not valid: $JAVA_HOME"
            fi
            JAVA="$JAVA_HOME/bin/java"
        else
            warn "JAVA_HOME not set; results may vary"
            JAVA=`type java`
            JAVA=`expr "$JAVA" : '.* \(/.*\)$'`
            if [ "x$JAVA" = "x" ]; then
                die "java command not found"
            fi
        fi
    fi
    if [ "x$JAVA_HOME" = "x" ]; then
        JAVA_HOME="$(dirname $(dirname $(pathCanonical "$JAVA")))"
    fi
}

detectJVM() {
   #echo "`$JAVA -version`"
   # This service should call `java -version`,
   # read stdout, and look for hints
   if $JAVA -version 2>&1 | grep "^IBM" ; then
       JVM_VENDOR="IBM"
   # on OS/400, java -version does not contain IBM explicitly
   elif $os400; then
       JVM_VENDOR="IBM"
   else
       JVM_VENDOR="SUN"
   fi
   # echo "JVM vendor is $JVM_VENDOR"
}

setupDebugOptions() {
    if [ "x$JAVA_OPTS" = "x" ]; then
        JAVA_OPTS="$DEFAULT_JAVA_OPTS"
    fi
    export JAVA_OPTS

    if [ "x$EXTRA_JAVA_OPTS" != "x" ]; then
        JAVA_OPTS="$JAVA_OPTS $EXTRA_JAVA_OPTS"
    fi

    # Set Debug options if enabled
    if [ "x$NIFI_DEBUG" != "x" ]; then
        # Use the defaults if JAVA_DEBUG_OPTS was not set
        if [ "x$JAVA_DEBUG_OPTS" = "x" ]; then
            JAVA_DEBUG_OPTS="$DEFAULT_JAVA_DEBUG_OPTS"
        fi

        JAVA_OPTS="$JAVA_DEBUG_OPTS $JAVA_OPTS"
        warn "Enabling Java debug options: $JAVA_DEBUG_OPTS"
    fi
}

setupDefaults() {
    DEFAULT_JAVA_OPTS="-Xms$JAVA_MIN_MEM -Xmx$JAVA_MAX_MEM -XX:PermSize=$JAVA_PERMSIZE -XX:MaxPermSize=$JAVA_MAX_PERMSIZE"

    #Set the JVM_VENDOR specific JVM flags
    if [ "$JVM_VENDOR" = "SUN" ]; then
        #
        # Check some easily accessible MIN/MAX params for JVM mem usage
        #
        if [ "x$JAVA_PERM_MEM" != "x" ]; then
            DEFAULT_JAVA_OPTS="$DEFAULT_JAVA_OPTS -XX:PermSize=$JAVA_PERM_MEM"
        fi
        if [ "x$JAVA_MAX_PERM_MEM" != "x" ]; then
            DEFAULT_JAVA_OPTS="$DEFAULT_JAVA_OPTS -XX:MaxPermSize=$JAVA_MAX_PERM_MEM"
        fi
        DEFAULT_JAVA_OPTS="-server $DEFAULT_JAVA_OPTS -Dcom.sun.management.jmxremote"
    elif [ "$JVM_VENDOR" = "IBM" ]; then
        if $os400; then
            DEFAULT_JAVA_OPTS="$DEFAULT_JAVA_OPTS"
        elif $aix; then
            DEFAULT_JAVA_OPTS="-Xverify:none -Xdump:heap -Xlp $DEFAULT_JAVA_OPTS"
        else
            DEFAULT_JAVA_OPTS="-Xverify:none $DEFAULT_JAVA_OPTS"
        fi
    fi

    DEFAULT_JAVA_OPTS="$DEFAULT_JAVA_OPTS  -Djava.net.preferIPv4Stack=true -Dsun.net.http.allowRestrictedHeaders=true -Djava.protocol.handler.pkgs=sun.net.www.protocol -Dorg.apache.jasper.compiler.disablejsr199=true -XX:ReservedCodeCacheSize=128m -XX:+UseCodeCacheFlushing"
    
    # Setup classpath
    CLASSPATH="$NIFI_HOME"/conf
    for f in "$NIFI_HOME"/lib/*
    do
      CLASSPATH="${CLASSPATH}":"${f}"
    done
    
    
    DEFAULT_JAVA_DEBUG_OPTS="-Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5005"

}

init() {
    # Determine if there is special OS handling we must perform
    detectOS

    # Unlimit the number of file descriptors if possible
    unlimitFD

    # Locate the NiFi home directory
    locateHome

    # Locate the NiFi base directory
    locateBase

    # Locate the NiFi conf directory
    locateConf

    # Setup the native library path
    setupNativePath

    # Locate the Java VM to execute
    locateJava

    # Determine the JVM vendor
    detectJVM

    # Setup default options
    setupDefaults

    # Install debug options
    setupDebugOptions

}

run() {

    if $cygwin; then
        NIFI_HOME=`cygpath --path --windows "$NIFI_HOME"`
        NIFI_BASE=`cygpath --path --windows "$NIFI_BASE"`
        NIFI_CONF=`cygpath --path --windows "$NIFI_CONF"`
        CLASSPATH=`cygpath --path --windows "$CLASSPATH"`
    fi
    # export CLASSPATH to the java process. Could also pass in via -cp
    export CLASSPATH
    echo 
    echo "Classpath: $CLASSPATH"
    echo
    echo "Java home: $JAVA_HOME"
    echo "NiFi home: $NIFI_HOME"
    echo "Java Options: $JAVA_OPTS"
    echo
    echo "Launching NiFi.  See logs..."
    exec "$JAVA" -Dapp=nifi $JAVA_OPTS -Dnifi.properties.file.path="$NIFI_HOME"/conf/nifi.properties org.apache.nifi.NiFi

}

main() {
    init
    run "$@"
}

main "$@"
