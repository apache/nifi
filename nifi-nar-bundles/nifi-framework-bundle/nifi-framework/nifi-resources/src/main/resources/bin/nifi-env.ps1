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

#  Set Java version
#  $env:JAVA_HOME = "C:\Program Files\Java\jdk"

#  Set application home directory based on parent directory of script location
$myHome = $PSScriptRoot
$env:NIFI_HOME = (Split-Path -Path $myHome -Parent)

#  Set run directory for process identifier tracking
$env:NIFI_PID_DIR = "${env:NIFI_HOME}\run"

#  Set application log directory
$env:NIFI_LOG_DIR = "${env:NIFI_HOME}\logs"
