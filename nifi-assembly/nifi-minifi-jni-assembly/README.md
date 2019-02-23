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
# Apache NiFi JNI Assembly

This package creates a deliverable for Apache NiFi MiNiFi C++ JNI integration. This integration
allows NiFi Processors to be run from within MiNiFi C++. This package and the target assemble-minifi-jni
create archives of the directory minifi-jni/lib and minifi-jni/nars. The lib directory is a bootstrap
for the JniClassloader. A slightly different bootstrap than NiFi, the JniClassloader loads the NARs into
the context of MiNiFi C++.

When the CMAKE JNI extension is used, the minifi-jni directory will be placed within the archive of that project
that is produced during the `make package` process. 
