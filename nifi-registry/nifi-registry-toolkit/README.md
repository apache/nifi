<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at
      http://www.apache.org/licenses/LICENSE-2.0
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->
# Apache NiFi Registry Toolkit

This submodule is a landing zone for command line utilities that can be used for maintenance/automation of registry actions.

It currently only contains a migration tool for changing flow persistence providers.

## Build

```
mvn clean install
```

## Flow Persistence Provider migrator usage

1. Shutdown registry
1. (Optional but recommended) Backup your registry by zipping/tarring the directory up
1. Copy providers.xml -> providers-to.xml
1. Edit providers-to.xml to reflect what you'd like to migrate to (e.g. git)
1. In registry home as working directory, run persistence-toolkit.sh -t providers-to.xml
1. Rename providers-to.xml -> providers.xml
1. Start registry back up