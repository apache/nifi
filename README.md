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
# Apache NiFi

Apache NiFi is an easy to use, powerful, and reliable system to process and distribute data.

## Table of Contents

- [Getting Started](#getting-started)
- [License](#license)
- [Disclaimer](#disclaimer)

## Getting Started

- Read through the [quickstart guide for development](http://nifi.incubator.apache.org/quickstart.html).
  It will include information on getting a local copy of the source, give pointers on issue
  tracking, and provide some warnings about common problems with development environments.
- For a more comprehensive guide to development and information about contributing to the project
  read through the [NiFi Developer's Guide](http://nifi.incubator.apache.org/developer-guide.html).
- Optional: Build supporting modules. This should only be needed if the current 'nifi' module is in
  the process of updating to a new version of either the 'nifi-parent' or 'nifi-nar-maven-plugin'
  artifacts.

    If in doubt, just skip to building the main nifi project. If the build fails, come back here and
    figure out which optional step you are missing; each entry below will give an example of the
    errors you'll receive if that step needs to be followed. The version numbers may change but the
    error text should still look very familiar.
    - Install the nifi-parent pom. Change directory to 'nifi-parent' and follow the directions found
      there in [README.md](nifi-parent/README.md).

    If you don't build the nifi-parent pom and the main nifi code relies on an unreleased version
    you'll see an erorr like the following:

        [ERROR]     Non-resolvable parent POM: Could not find artifact
            org.apache.nifi:nifi-parent:pom:1.0.0-incubating-SNAPSHOT in example.snapshots.repo
            (https://repository.example.com/content/repositories/snapshots) and
            'parent.relativePath' points at no local POM @ line 18, column 13 -> [Help 2]
    - Build the nifi-nar-maven-plugin.  Change directory to 'nifi-nar-maven-plugin' and
      follow the directions found there in [README.md](nifi-nar-maven-plugin/README.md).

    If you don't build the nifi-nar-maven-plugin and the main nifi code relies on an unreleased
    version you'll see an error like the following:

        [ERROR]     Unresolveable build extension: Plugin
            org.apache.nifi:nifi-nar-maven-plugin:1.0.1-incubating-SNAPSHOT or one of its
            dependencies could not be resolved: Could not find artifact
            org.apache.nifi:nifi-nar-maven-plugin:jar:1.0.1-incubating-SNAPSHOT -> [Help 2]
- Build nifi.  Change directory to 'nifi' and follow the directions found there in
  [README.md](nifi/README.md).
- Run NiFi. The directions found in the [README.md](nifi/README.md) file within the 'nifi' module
  will also include how to run an instance of NiFi. For help on how to build your first data flow,
  see the [NiFi User Guide](http://nifi.incubator.apache.org/docs/nifi-docs/user-guide.html).

## Documentation

See http://nifi.incubator.apache.org/ for the latest documentation.

## License

Except as otherwise noted this software is licensed under the
[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.html)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

## Disclaimer

Apache NiFi is an effort undergoing incubation at the Apache Software
Foundation (ASF), sponsored by the Apache Incubator PMC.

Incubation is required of all newly accepted projects until a further review
indicates that the infrastructure, communications, and decision making process
have stabilized in a manner consistent with other successful ASF projects.

While incubation status is not necessarily a reflection of the completeness
or stability of the code, it does indicate that the project has yet to be
fully endorsed by the ASF.

