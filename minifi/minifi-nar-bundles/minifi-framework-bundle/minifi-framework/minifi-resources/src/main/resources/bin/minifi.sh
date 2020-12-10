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
PHYS_DIR=`pwd -P`

SCRIPT_DIR=$PHYS_DIR
SCRIPT_NAME=$(basename "$0")
PROGNAME=$(basename "$0")

. "$SCRIPT_DIR"/minifi-env.sh


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

unlimitFD() {
    # Use the maximum available, or set MAX_FD != -1 to use that
    if [ "x${MAX_FD}" = "x" ]; then
        MAX_FD="maximum"
    fi

    # Increase the maximum file descriptors if we can
    if [ "${os400}" = "false" ] && [ "${cygwin}" = "false" ]; then
        MAX_FD_LIMIT=$(ulimit -H -n)
        if [ "${MAX_FD_LIMIT}" != 'unlimited' ]; then
            if [ $? -eq 0 ]; then
                if [ "${MAX_FD}" = "maximum" -o "${MAX_FD}" = "max" ]; then
                    # use the system max
                    MAX_FD="${MAX_FD_LIMIT}"
                fi

                ulimit -n ${MAX_FD} > /dev/null
                # echo "ulimit -n" `ulimit -n`
                if [ $? -ne 0 ]; then
                    warn "Could not set maximum file descriptor limit: ${MAX_FD}"
                fi
            else
                warn "Could not query system maximum file descriptor limit: ${MAX_FD_LIMIT}"
            fi
        fi
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

    # Unlimit the number of file descriptors if possible
    unlimitFD

    # Locate the Java VM to execute
    locateJava "$1"
}


install() {
   detectOS

    if [ "${darwin}" = "true"  ] || [ "${cygwin}" = "true" ]; then
        echo 'Installing Apache MiNiFi as a service is not supported on OS X or Cygwin.'
        exit 1
    fi

    SVC_NAME=minifi
    if [ "x$2" != "x" ] ; then
        SVC_NAME=$2
    fi

    initd_dir='/etc/init.d'
    SVC_FILE="${initd_dir}/${SVC_NAME}"

    if [ ! -w  "${initd_dir}" ]; then
        echo "Current user does not have write permissions to ${initd_dir}. Cannot install MiNiFi as a service."
        exit 1
    fi

# Create the init script, overwriting anything currently present
cat <<SERVICEDESCRIPTOR > ${SVC_FILE}
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
# chkconfig: 2345 20 80
# description: Apache NiFi is a dataflow system based on the principles of Flow-Based Programming.
#

# Make use of the configured NIFI_HOME directory and pass service requests to the nifi.sh executable
MINIFI_HOME=${MINIFI_HOME}
bin_dir=\${MINIFI_HOME}/bin
minifi_executable=\${bin_dir}/minifi.sh

\${minifi_executable} "\$@"
SERVICEDESCRIPTOR

    if [ ! -f "${SVC_FILE}" ]; then
        echo "Could not create service file ${SVC_FILE}"
        exit 1
    fi

    # Provide the user execute access on the file
    chmod u+x ${SVC_FILE}

    rm -f "/etc/rc2.d/S65${SVC_NAME}"
    ln -s "/etc/init.d/${SVC_NAME}" "/etc/rc2.d/S65${SVC_NAME}" || { echo "Could not create link /etc/rc2.d/S65${SVC_NAME}"; exit 1; }
    rm -f "/etc/rc2.d/K65${SVC_NAME}"
    ln -s "/etc/init.d/${SVC_NAME}" "/etc/rc2.d/K65${SVC_NAME}" || { echo "Could not create link /etc/rc2.d/K65${SVC_NAME}"; exit 1; }
    echo "Service ${SVC_NAME} installed"
}

run() {
    BOOTSTRAP_CONF_DIR="${MINIFI_HOME}/conf"
    BOOTSTRAP_CONF="${BOOTSTRAP_CONF_DIR}/bootstrap.conf";
    MINIFI_LIBS="${MINIFI_HOME}/lib/*"
    BOOTSTRAP_LIBS="${MINIFI_HOME}/lib/bootstrap/*"

    run_as=$(grep '^\s*run.as' "${BOOTSTRAP_CONF}" | cut -d'=' -f2)
    # If the run as user is the same as that starting the process, ignore this configuration
    if [ "$run_as" = "$(whoami)" ]; then
        unset run_as
    fi

    sudo_cmd_prefix=""
    if $cygwin; then
        if [ -n "${run_as}" ]; then
            echo "The run.as option is not supported in a Cygwin environment. Exiting."
            exit 1
        fi;

        MINIFI_HOME=$(cygpath --path --windows "${MINIFI_HOME}")
        MINIFI_LOG_DIR=$(cygpath --path --windows "${MINIFI_LOG_DIR}")
        MINIFI_PID_DIR=$(cygpath --path --windows "${MINIFI_PID_DIR}")
        BOOTSTRAP_CONF=$(cygpath --path --windows "${BOOTSTRAP_CONF}")
        BOOTSTRAP_CONF_DIR=$(cygpath --path --windows "${BOOTSTRAP_CONF_DIR}")
        BOOTSTRAP_LIBS=$(cygpath --path --windows "${BOOTSTRAP_LIBS}")
        MINIFI_LIBS=$(cygpath --path --windows "${MINIFI_LIBS}")
        BOOTSTRAP_CLASSPATH="${BOOTSTRAP_CONF_DIR};${BOOTSTRAP_LIBS};${MINIFI_LIBS}"
        if [ -n "${TOOLS_JAR}" ]; then
            TOOLS_JAR=$(cygpath --path --windows "${TOOLS_JAR}")
            BOOTSTRAP_CLASSPATH="${TOOLS_JAR};${BOOTSTRAP_CLASSPATH};${MINIFI_LIBS}"
        fi
    else
        if [ -n "${run_as}" ]; then
            if id -u "${run_as}" >/dev/null 2>&1; then
                sudo_cmd_prefix="sudo -u ${run_as}"
            else
                echo "The specified run.as user ${run_as} does not exist. Exiting."
                exit 1
            fi
        fi;
        BOOTSTRAP_CLASSPATH="${BOOTSTRAP_CONF_DIR}:${BOOTSTRAP_LIBS}:${MINIFI_LIBS}"
        if [ -n "${TOOLS_JAR}" ]; then
            BOOTSTRAP_CLASSPATH="${TOOLS_JAR}:${BOOTSTRAP_CLASSPATH}:${MINIFI_LIBS}"
        fi
    fi

    echo
    echo "Bootstrap Classpath: ${BOOTSTRAP_CLASSPATH}"
    echo "Java home: ${JAVA_HOME}"
    echo "MiNiFi home: ${MINIFI_HOME}"
    echo
    echo "Bootstrap Config File: ${BOOTSTRAP_CONF}"
    echo


    #setup directory parameters
    BOOTSTRAP_LOG_PARAMS="-Dorg.apache.nifi.minifi.bootstrap.config.log.dir="\""${MINIFI_LOG_DIR}"\"""
    BOOTSTRAP_PID_PARAMS="-Dorg.apache.nifi.minifi.bootstrap.config.pid.dir="\""${MINIFI_PID_DIR}"\"""
    BOOTSTRAP_CONF_PARAMS="-Dorg.apache.nifi.minifi.bootstrap.config.file="\""${BOOTSTRAP_CONF}"\"""

    BOOTSTRAP_DIR_PARAMS="${BOOTSTRAP_LOG_PARAMS} ${BOOTSTRAP_PID_PARAMS} ${BOOTSTRAP_CONF_PARAMS}"

    RUN_MINIFI_CMD="cd "\""${MINIFI_HOME}"\"" && ${sudo_cmd_prefix} "\""${JAVA}"\"" -cp "\""${BOOTSTRAP_CLASSPATH}"\"" -Xms12m -Xmx24m ${BOOTSTRAP_DIR_PARAMS}  org.apache.nifi.minifi.bootstrap.RunMiNiFi"

    # run 'start' in the background because the process will continue to run, monitoring MiNiFi.
    # all other commands will terminate quickly so want to just wait for them
    if [ "$1" = "start" ]; then
        (eval $RUN_MINIFI_CMD $@ &)
    else
        (eval $RUN_MINIFI_CMD $@)
    fi
    EXIT_STATUS=$?

    # Wait just a bit (3 secs) to wait for the logging to finish and then echo a new-line.
    # We do this to avoid having logs spewed on the console after running the command and then not giving
    # control back to the user
    sleep 3
    echo
}

main() {
    init "$1"
    run "$@"
}


case "$1" in
    install)
        install "$@"
        ;;
    start|stop|run|status|flowStatus|dump|env)
        main "$@"
        exit $EXIT_STATUS
        ;;
    restart)
        init
        run "stop"
        run "start"
        exit $EXIT_STATUS
    ;;
    *)
        echo "Usage minifi {start|stop|run|restart|status|flowStatus|dump|install}"
        ;;
esac
