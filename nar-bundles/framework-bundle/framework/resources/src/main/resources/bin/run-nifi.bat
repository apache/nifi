@echo off

rem Use JAVA_HOME if it's set; otherwise, just use java
IF "%JAVA_HOME%"=="" (SET JAVA_EXE=java) ELSE (SET JAVA_EXE=%JAVA_HOME%\bin\java.exe)

SET LIB_DIR=%~dp0..\lib
SET CONF_DIR=%~dp0..\conf

SET BOOTSTRAP_CONF_FILE=%CONF_DIR%\bootstrap.conf
SET JAVA_ARGS=-Dorg.apache.nifi.boostrap.config.file=%BOOTSTRAP_CONF_FILE%

SET JAVA_PARAMS=-cp %LIB_DIR%\nifi-bootstrap*.jar -Xms12m -Xmx24m %JAVA_ARGS% org.apache.nifi.bootstrap.RunNiFi
SET BOOTSTRAP_ACTION=run

cmd.exe /C "%JAVA_EXE%" %JAVA_PARAMS% %BOOTSTRAP_ACTION%
