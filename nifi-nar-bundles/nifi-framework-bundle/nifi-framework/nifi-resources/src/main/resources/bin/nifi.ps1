#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

$myDir = $PSScriptRoot
. "${myDir}/nifi-env.ps1"

$runCommand = $Args[0]

If (Test-Path -Path "${env:JAVA_HOME}\bin\java.exe" -PathType Leaf) {
  $javaExe = "${env:JAVA_HOME}\bin\java.exe"
} Else {
  echo JAVA_HOME not defined: using PATH for java command
  $javaExe = (Get-Command java).Source
}

$bootstrapLibDir = "${env:NIFI_HOME}\lib\bootstrap"
$confDir = "${env:NIFI_HOME}\conf"

$logDirProp = "-Dorg.apache.nifi.bootstrap.config.log.dir=${env:NIFI_LOG_DIR}"
$pidDirProp = "-Dorg.apache.nifi.bootstrap.config.pid.dir=${env:NIFI_PID_DIR}"
$configFileProp = "-Dorg.apache.nifi.bootstrap.config.file=${confDir}\bootstrap.conf"
$propFileProp = "-Dnifi.properties.file.path=${confDir}\nifi.properties"
$bootstrapHeapSize = "48m"

$javaProps = @($logDirProp
               $pidDirProp
			   $configFileProp
			   $propFileProp
			   )
			   
$javaParams = @("-classpath"
                "${bootstrapLibDir}\*;${confDir}")
$javaMemory = @("-Xms${bootstrapHeapSize}"
                "-Xmx${bootstrapHeapSize}")

echo "JAVA_HOME: ${env:JAVA_HOME}"
echo "NIFI_HOME: ${env:NIFI_HOME}"
echo "Run: $runCommand"

pushd ${env:NIFI_HOME}

Switch ($runCommand)
{
	"help" {
		Write-Host "help"
		Write-Host "set-single-user-credentials <user> <password>"
		Write-Host "set-sensitive-properties-key <key>"
		Write-Host "set-sensitive-properties-algorithm <algorithm>"
	}
	"set-single-user-credentials" {
	  echo "Set credentials with quoted arguments passed to Java command"
	  $userName = $Args[1]
	  $passWord = $Args[2]
	  $args = $javaParams + $javaProps +
	      @("org.apache.nifi.authentication.single.user.command.SetSingleUserCredentials"
	        "${userName}"
			"${passWord}")
	  & "$javaExe" $args
	}
	"set-sensitive-properties-key" {
	  echo "Set sensitive properties key with Java command"		
	  $spKey = $Args[1]
	  $args = $javaParams + $javaProps +
	      @("org.apache.nifi.flow.encryptor.command.SetSensitivePropertiesKey"
	        "${spKey}")
	  & "$javaExe" $args
	}
	"set-sensitive-properties-algorithm" {
	  echo "Set sensitive properties key with Java command"		
	  $spAlgorithm = $Args[1]
	  $args = $javaParams + $javaProps +
	      @("org.apache.nifi.flow.encryptor.command.SetSensitivePropertiesAlgorithm"
	        "${spAlgorithm}")
	  & "$javaExe" $args
	}
	default {
	  echo "Run NiFi with Java command"		
	  $javaMain = 
	  # Start-Process -Wait -FilePath $javaExe -ArgumentList "$javaMemory $javaParams $javaProps $runCommand"
	  $args = $javaParams + $javaProps +
	      @("org.apache.nifi.bootstrap.RunNiFi")
	  & "$javaExe" $args
	}
}

popd
