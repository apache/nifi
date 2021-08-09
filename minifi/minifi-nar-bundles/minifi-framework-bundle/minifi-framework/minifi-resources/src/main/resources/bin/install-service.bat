@echo off
rem
rem    Licensed to the Apache Software Foundation (ASF) under one or more
rem    contributor license agreements.  See the NOTICE file distributed with
rem    this work for additional information regarding copyright ownership.
rem    The ASF licenses this file to You under the Apache License, Version 2.0
rem    (the "License"); you may not use this file except in compliance with
rem    the License.  You may obtain a copy of the License at
rem
rem       http://www.apache.org/licenses/LICENSE-2.0
rem
rem    Unless required by applicable law or agreed to in writing, software
rem    distributed under the License is distributed on an "AS IS" BASIS,
rem    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
rem    See the License for the specific language governing permissions and
rem    limitations under the License.
rem

call minifi-env.bat

set CONF_DIR=%MINIFI_ROOT%conf

set BOOTSTRAP_CONF_FILE=%CONF_DIR%\bootstrap.conf
set SRV_BIN=%cd%\minifi.exe
set SVC_NAME=minifi
set SVC_DISPLAY="Apache MiNiFi"
set SVC_DESCRIPTION="Apache MiNiFi"
set JVM=auto
set PR_JVMMS=12
set PR_JVMMX=24
set PR_JVMSS=4000
set START_MODE=jvm
set STOP_MODE=jvm
set STOP_TIMEOUT=10
set STARTUP=auto
set JAVA_ARGS=-Dorg.apache.nifi.minifi.bootstrap.config.log.dir="%MINIFI_LOG_DIR%";-Dorg.apache.nifi.minifi.bootstrap.config.pid.dir="%MINIFI_PID_DIR%";-Dorg.apache.nifi.minifi.bootstrap.config.file="%BOOTSTRAP_CONF_FILE%"
set START_CLASS=org.apache.nifi.minifi.bootstrap.WindowsService
set START_METHOD=start
set STOP_CLASS=org.apache.nifi.minifi.bootstrap.WindowsService
set STOP_METHOD=stop
REM START_PARAMS=-verbose
set START_PARAMS=
REM STOP_PARAMS=-verbose
set STOP_PARAMS=
set CLASS_PATH="%CONF_DIR%";"%MINIFI_ROOT%lib\*";"%MINIFI_ROOT%lib\bootstrap\*"
set LOG_PATH="%MINIFI_ROOT%\logs"
set LOG_PREFIX=minifi.log

"%SRV_BIN%" //IS//%SVC_NAME% ^
--DisplayName=%SVC_DISPLAY% ^
--Description=%SVC_DESCRIPTION% ^
--Install="%SRV_BIN%" ^
--Jvm="%JVM%" ^
--JvmMs="%PR_JVMMS%" ^
--JvmMx="%PR_JVMMX%" ^
--JvmSs="%PR_JVMSS%" ^
--StartMode="%START_MODE%" ^
--StopMode="%STOP_MODE%" ^
--Startup="%STARTUP%" ^
--StartClass="%START_CLASS%" ^
--StopClass="%STOP_CLASS%" ^
--StartParams="%START_PARAMS%" ^
--StopParams="%STOP_PARAMS%" ^
--StartMethod="%START_METHOD%" ^
--StopMethod="%STOP_METHOD%" ^
--StopTimeout="%STOP_TIMEOUT%" ^
--Classpath="%CLASS_PATH%" ^
--JvmOptions="%JAVA_ARGS%" ^
--LogLevel=ERROR ^
--LogPath="%LOG_PATH%" ^
--LogPrefix="%LOG_PREFIX%" ^
--StdOutput=auto ^
--StdError=auto

REM Start the service after installation
"%SRV_BIN%" //ES//%SVC_NAME%
