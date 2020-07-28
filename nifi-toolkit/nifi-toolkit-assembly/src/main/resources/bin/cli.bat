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
rem Use JAVA_HOME if it's set; otherwise, just use java
if "%JAVA_HOME%" == "" goto noJavaHome
if not exist "%JAVA_HOME%\bin\java.exe" goto noJavaHome
set JAVA_EXE=%JAVA_HOME%\bin\java.exe
goto startConfig
:noJavaHome
echo The JAVA_HOME environment variable is not defined correctly.
echo Instead the PATH will be used to find the java executable.
echo.
set JAVA_EXE=java
goto startConfig
:startConfig
set LIB_DIR=%~dp0..\classpath;%~dp0..\lib\
if "%NIFI_TOOLKIT_HOME%" == "" (
    set NIFI_TOOLKIT_HOME=%~dp0..
) else (
    SET trailing_char=%NIFI_TOOLKIT_HOME:~-1%
    if "%trailing_char%" == "\" (set NIFI_TOOLKIT_HOME=%NIFI_TOOLKIT_HOME:~0,-1%)
)
if "%JAVA_OPTS%" == "" set JAVA_OPTS=-Xms128m -Xmx256m
for /f tokens^=2-5^ delims^=.-+_^" %%j in ('"%JAVA_EXE%" -fullversion 2^>^&1') do (
    set "java_version=%%j%%k%%l%%m"
    set "major=%%j"
    set "minor=%%k"
    set "micro=%%l"
    set "build=%%m"
)
set compatibility_arg=
set compatibility_lib=
if %major% EQU 9 (
    set compatibility_arg=--add-modules=java.xml.bind
    echo Detected Java 9 runtime version
) else if %major% EQU 10 (
    set compatibility_arg=--add-modules=java.xml.bind
    echo Detected Java 10 runtime version
) else if %major% GEQ 11 (
    set compatibility_lib=%NIFI_TOOLKIT_HOME%\lib\java11\
    echo Detected java 11 or later runtime version: %major%
)
if not "%compatibility_arg%" == "" (set JAVA_OPTS=%JAVA_OPTS% %compatibility_arg%)
if not "%compatibility_lib%" == "" (
   set LIB_DIR="%LIB_DIR%*;%compatibility_lib%*"
) else (
   set LIB_DIR="%LIB_DIR%*"
)

rem remove surrounding quotes
SET LIB_DIR=%LIB_DIR:"=%
SET JAVA_OPTS=%JAVA_OPTS:"=%

SET JAVA_PARAMS=-cp %LIB_DIR% %JAVA_OPTS% org.apache.nifi.toolkit.cli.CLIMain
cmd.exe /C ""%JAVA_EXE%" %JAVA_PARAMS% %* "