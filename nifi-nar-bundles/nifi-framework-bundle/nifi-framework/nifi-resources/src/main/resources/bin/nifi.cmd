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

call %~dp0\nifi-env.cmd

if exist "%JAVA_HOME%\bin\java.exe" (
  set JAVA_EXE=%JAVA_HOME%\bin\java.exe
) else (
  echo JAVA_HOME not defined: using PATH for java command
  set JAVA_EXE=java
)

set BOOTSTRAP_LIB_DIR=%NIFI_HOME%\lib\bootstrap
set CONF_DIR=%NIFI_HOME%\conf

set LOG_DIR_PROPERTY=-Dorg.apache.nifi.bootstrap.config.log.dir=%NIFI_LOG_DIR%
set PID_DIR_PROPERTY=-Dorg.apache.nifi.bootstrap.config.pid.dir=%NIFI_PID_DIR%
set CONFIG_FILE_PROPERTY=-Dorg.apache.nifi.bootstrap.config.file=%CONF_DIR%\bootstrap.conf
set PROPERTIES_FILE_PROPERTY=-Dnifi.properties.file.path=%CONF_DIR%\nifi.properties
set BOOTSTRAP_HEAP_SIZE=48m

set JAVA_ARGS=%LOG_DIR_PROPERTY% %PID_DIR_PROPERTY% %CONFIG_FILE_PROPERTY% %PROPERTIES_FILE_PROPERTY%
set JAVA_PARAMS=-cp %BOOTSTRAP_LIB_DIR%\*;%CONF_DIR% %JAVA_ARGS%
set JAVA_MEMORY=-Xms%BOOTSTRAP_HEAP_SIZE% -Xmx%BOOTSTRAP_HEAP_SIZE%

echo JAVA_HOME: %JAVA_HOME%
echo NIFI_HOME: %NIFI_HOME%
echo.

pushd %NIFI_HOME%

set RUN_COMMAND="%~1"
if %RUN_COMMAND% == "set-single-user-credentials" (
  rem Set credentials with quoted arguments passed to Java command
  set "CREDENTIALS=^"%~2^" ^"%~3^""
  call "%JAVA_EXE%" %JAVA_PARAMS% org.apache.nifi.authentication.single.user.command.SetSingleUserCredentials %CREDENTIALS%
) else if %RUN_COMMAND% == "set-sensitive-properties-key" (
  call "%JAVA_EXE%" %JAVA_PARAMS% org.apache.nifi.flow.encryptor.command.SetSensitivePropertiesKey %~2
) else if %RUN_COMMAND% == "set-sensitive-properties-algorithm" (
  call "%JAVA_EXE%" %JAVA_PARAMS% org.apache.nifi.flow.encryptor.command.SetSensitivePropertiesAlgorithm %~2
) else (
  call "%JAVA_EXE%" %JAVA_MEMORY% %JAVA_PARAMS% org.apache.nifi.bootstrap.RunNiFi %RUN_COMMAND%
)

popd
