#!/bin/bash
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


# Replace variables seems to be the only option to pass proper values to surefire
# Note: The reason the sed is done as part of script is to ensure the pom hack 
# won't affect the 'clean install' above
set -x 
if [ "$USER_LANGUAGE" != "default" ]; then
    sed -i.bak -e "s|<maven\.surefire\.arguments/>|<maven\.surefire\.arguments>-Duser.language=${USER_LANGUAGE} -Duser.region=${USER_REGION}</maven\.surefire\.arguments>|" pom.xml
    diff pom.xml pom.xml.bak
    rm pom.xml.bak
fi
