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

. "${SCRIPT_DIR}/nifi-env.sh"



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
    # In addition to those, go around the linux space and query the widely
    # adopted /etc/os-release to detect linux variants
    if [ -f /etc/os-release ]; then
        . /etc/os-release
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
        echo 'Installing Apache NiFi as a service is not supported on OS X or Cygwin.'
        exit 1
    fi

    SVC_NAME=nifi
    if [ "x$2" != "x" ] ; then
        SVC_NAME=$2
    fi

    # since systemd seems to honour /etc/init.d we don't still create native systemd services
    # yet...
    initd_dir='/etc/init.d'
    SVC_FILE="${initd_dir}/${SVC_NAME}"

    if [ ! -w  "${initd_dir}" ]; then
        echo "Current user does not have write permissions to ${initd_dir}. Cannot install NiFi as a service."
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
NIFI_HOME=${NIFI_HOME}
bin_dir=\${NIFI_HOME}/bin
nifi_executable=\${bin_dir}/nifi.sh

\${nifi_executable} "\$@"
SERVICEDESCRIPTOR

    if [ ! -f "${SVC_FILE}" ]; then
        echo "Could not create service file ${SVC_FILE}"
        exit 1
    fi

    # Provide the user execute access on the file
    chmod u+x ${SVC_FILE}


    # If SLES or OpenSuse...
    if [ "${ID}" = "opensuse" ] || [ "${ID}" = "sles" ]; then
        rm -f "/etc/rc.d/rc2.d/S65${SVC_NAME}"
        ln -s "/etc/init.d/${SVC_NAME}" "/etc/rc.d/rc2.d/S65${SVC_NAME}" || { echo "Could not create link /etc/rc.d/rc2.d/S65${SVC_NAME}"; exit 1; }
        rm -f "/etc/rc.d/rc2.d/K65${SVC_NAME}"
        ln -s "/etc/init.d/${SVC_NAME}" "/etc/rc.d/rc2.d/K65${SVC_NAME}" || { echo "Could not create link /etc/rc.d/rc2.d/K65${SVC_NAME}"; exit 1; }
        echo "Service ${SVC_NAME} installed"
    # Anything other fallback to the old approach
    else
        rm -f "/etc/rc2.d/S65${SVC_NAME}"
        ln -s "/etc/init.d/${SVC_NAME}" "/etc/rc2.d/S65${SVC_NAME}" || { echo "Could not create link /etc/rc2.d/S65${SVC_NAME}"; exit 1; }
        rm -f "/etc/rc2.d/K65${SVC_NAME}"
        ln -s "/etc/init.d/${SVC_NAME}" "/etc/rc2.d/K65${SVC_NAME}" || { echo "Could not create link /etc/rc2.d/K65${SVC_NAME}"; exit 1; }
        echo "Service ${SVC_NAME} installed"
    fi

    # systemd: generate nifi.service from init.d
    if [ -d "/run/systemd/system/" ] || [ ! -z "$(pidof systemd 2>/dev/null)" ]; then
        systemctl daemon-reload
    fi
}

is_nonzero_integer() {

    if [ "$1" -gt 0 ] 2>/dev/null; then
        return 0
    else
        return 1
    fi
}

run() {
    BOOTSTRAP_CONF_DIR="${NIFI_HOME}/conf"
    BOOTSTRAP_CONF="${BOOTSTRAP_CONF_DIR}/bootstrap.conf";
    BOOTSTRAP_LIBS="${NIFI_HOME}/lib/bootstrap/*"

    WAIT_FOR_INIT_DEFAULT_TIMEOUT=900
    WAIT_FOR_INIT_SLEEP_TIME=2
    WAIT_FOR_INIT_FEEDBACK_INTERVAL=10

    run_as_user=$(grep '^\s*run.as' "${BOOTSTRAP_CONF}" | cut -d'=' -f2)
    # If the run as user is the same as that starting the process, ignore this configuration
    if [ "${run_as_user}" = "$(whoami)" ]; then
        unset run_as_user
    fi

    if $cygwin; then
        if [ -n "${run_as_user}" ]; then
            echo "The run.as option is not supported in a Cygwin environment. Exiting."
            exit 1
        fi;

        NIFI_HOME=$(cygpath --path --windows "${NIFI_HOME}")
        NIFI_LOG_DIR=$(cygpath --path --windows "${NIFI_LOG_DIR}")
        NIFI_PID_DIR=$(cygpath --path --windows "${NIFI_PID_DIR}")
        BOOTSTRAP_CONF=$(cygpath --path --windows "${BOOTSTRAP_CONF}")
        BOOTSTRAP_CONF_DIR=$(cygpath --path --windows "${BOOTSTRAP_CONF_DIR}")
        BOOTSTRAP_LIBS=$(cygpath --path --windows "${BOOTSTRAP_LIBS}")
        BOOTSTRAP_CLASSPATH="${BOOTSTRAP_CONF_DIR};${BOOTSTRAP_LIBS}"
        if [ -n "${TOOLS_JAR}" ]; then
            TOOLS_JAR=$(cygpath --path --windows "${TOOLS_JAR}")
            BOOTSTRAP_CLASSPATH="${TOOLS_JAR};${BOOTSTRAP_CLASSPATH}"
        fi
    else
        if [ -n "${run_as_user}" ]; then
            if ! id -u "${run_as_user}" >/dev/null 2>&1; then
                echo "The specified run.as user ${run_as_user} does not exist. Exiting."
                exit 1
            fi
        fi;
        BOOTSTRAP_CLASSPATH="${BOOTSTRAP_CONF_DIR}:${BOOTSTRAP_LIBS}"
        if [ -n "${TOOLS_JAR}" ]; then
            BOOTSTRAP_CLASSPATH="${TOOLS_JAR}:${BOOTSTRAP_CLASSPATH}"
        fi
    fi

    echo
    echo "Java home: ${JAVA_HOME}"
    echo "NiFi home: ${NIFI_HOME}"
    echo
    echo "Bootstrap Config File: ${BOOTSTRAP_CONF}"
    echo

    # run 'start' in the background because the process will continue to run, monitoring NiFi.
    # all other commands will terminate quickly so want to just wait for them

    #setup directory parameters
    BOOTSTRAP_LOG_PARAMS="-Dorg.apache.nifi.bootstrap.config.log.dir='${NIFI_LOG_DIR}'"
    BOOTSTRAP_PID_PARAMS="-Dorg.apache.nifi.bootstrap.config.pid.dir='${NIFI_PID_DIR}'"
    BOOTSTRAP_CONF_PARAMS="-Dorg.apache.nifi.bootstrap.config.file='${BOOTSTRAP_CONF}'"

    # uncomment to allow debugging of the bootstrap process
    #BOOTSTRAP_DEBUG_PARAMS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=8000"

    BOOTSTRAP_DIR_PARAMS="${BOOTSTRAP_LOG_PARAMS} ${BOOTSTRAP_PID_PARAMS} ${BOOTSTRAP_CONF_PARAMS}"

    run_bootstrap_cmd="'${JAVA}' -cp '${BOOTSTRAP_CLASSPATH}' -Xms48m -Xmx48m ${BOOTSTRAP_DIR_PARAMS} ${BOOTSTRAP_DEBUG_PARAMS} ${BOOTSTRAP_JAVA_OPTS} org.apache.nifi.bootstrap.RunNiFi"
    run_nifi_cmd="${run_bootstrap_cmd} $@"

    if [ -n "${run_as_user}" ]; then
      preserve_environment=$(grep '^\s*preserve.environment' "${BOOTSTRAP_CONF}" | cut -d'=' -f2 | tr '[:upper:]' '[:lower:]')
      SUDO="sudo"
      if [ "$preserve_environment" = "true" ]; then
        SUDO="sudo -E"
      fi
      # Provide SCRIPT_DIR and execute nifi-env for the run.as user command
      run_nifi_cmd="${SUDO} -u ${run_as_user} sh -c \"SCRIPT_DIR='${SCRIPT_DIR}' && . '${SCRIPT_DIR}/nifi-env.sh' && ${run_nifi_cmd}\""
    fi

    if [ "$1" = "run" ]; then
      # Use exec to handover PID to RunNiFi java process, instead of foking it as a child process
      run_nifi_cmd="exec ${run_nifi_cmd}"
    fi

    if [ "$1" = "set-sensitive-properties-algorithm" ]; then
        run_command="'${JAVA}' -cp '${BOOTSTRAP_CLASSPATH}' '-Dnifi.properties.file.path=${NIFI_HOME}/conf/nifi.properties' 'org.apache.nifi.flow.encryptor.command.SetSensitivePropertiesAlgorithm'"
        eval "cd ${NIFI_HOME}"
        shift
        eval "${run_command}" '"$@"'
        EXIT_STATUS=$?
        echo
        return;
    fi

    if [ "$1" = "set-sensitive-properties-key" ]; then
        run_command="'${JAVA}' -cp '${BOOTSTRAP_CLASSPATH}' '-Dnifi.properties.file.path=${NIFI_HOME}/conf/nifi.properties' 'org.apache.nifi.flow.encryptor.command.SetSensitivePropertiesKey'"
        eval "cd ${NIFI_HOME}"
        shift
        eval "${run_command}" '"$@"'
        EXIT_STATUS=$?
        echo
        return;
    fi

    if [ "$1" = "set-single-user-credentials" ]; then
        run_command="'${JAVA}' -cp '${BOOTSTRAP_CLASSPATH}' '-Dnifi.properties.file.path=${NIFI_HOME}/conf/nifi.properties' 'org.apache.nifi.authentication.single.user.command.SetSingleUserCredentials'"
        eval "cd ${NIFI_HOME}"
        shift
        eval "${run_command}" '"$@"'
        EXIT_STATUS=$?
        echo
        return;
    fi

    if [ "$1" = "start" ]; then
        ( eval "cd ${NIFI_HOME} && ${run_nifi_cmd}" & )> /dev/null 1>&-

        if [ "$2" = "--wait-for-init" ]; then

            if is_nonzero_integer "$3" ; then
                wait_timeout="$3"
            else
                wait_timeout=$WAIT_FOR_INIT_DEFAULT_TIMEOUT
            fi

            starttime=$(date +%s)
            endtime=$(($starttime+$wait_timeout))
            current_time=$starttime
            time_since_feedback=0
            not_running_counter=0

            is_nifi_loaded="false" # 3 possible values: "true", "false", "not_running". "not_running" means NiFi has not been started.
            while [ "$is_nifi_loaded" != "true" ]; do
                time_at_previous_loop=$current_time

                current_time=$(date +%s)
                if [ "$current_time" -ge "$endtime" ]; then
                  echo "Exited the script due to --wait-for-init timeout"
                  break;
                fi

                time_since_feedback=$(($time_since_feedback+($current_time-$time_at_previous_loop)))
                if [ "$time_since_feedback" -ge "$WAIT_FOR_INIT_FEEDBACK_INTERVAL" ]; then
                  time_since_feedback=0
                  echo "NiFi has not fully initialized yet..."
                fi

                is_nifi_loaded=$( eval "cd ${NIFI_HOME} && ${run_bootstrap_cmd} is_loaded" )

                if [ "$is_nifi_loaded" = "not_running" ]; then
                  not_running_counter=$(($not_running_counter+1))
                  if [ "$not_running_counter" -ge 3 ]; then
                    echo "NiFi is not running. Stopped waiting for it to initialize."
                    break;
                  fi
                fi

                sleep $WAIT_FOR_INIT_SLEEP_TIME
            done
            if [ "$is_nifi_loaded" = "true" ]; then
              echo "NiFi initialized."
              echo "Exiting startup script..."
            fi
        fi
    else
        eval "cd ${NIFI_HOME} && ${run_nifi_cmd}"
    fi
    EXIT_STATUS=$?

    # Wait just a bit (3 secs) to wait for the logging to finish and then echo a new-line.
    # We do this to avoid having logs spewed on the console after running the command and then not giving
    # control back to the user
    sleep 1
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

    start|stop|decommission|run|status|is_loaded|dump|diagnostics|status-history|env|set-sensitive-properties-algorithm|set-sensitive-properties-key|set-single-user-credentials|cluster-status)
        main "$@"
        ;;

    restart)
        init
        run "stop"
        run "start"
        ;;
    *)
        echo "Usage nifi {start|stop|decommission|run|restart|status|dump|diagnostics|status-history|install|set-sensitive-properties-algorithm|set-sensitive-properties-key|set-single-user-credentials|cluster-status}"
        ;;
esac
