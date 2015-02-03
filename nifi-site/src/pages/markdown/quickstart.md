---
title:     Apache NiFi Development Quickstart
---

This documentation is in progress, but should get many started at building Apache NiFi.

## Source Code

Apache NiFi source code is version controlled using [Git][git] version control ([browse][gitbrowse]|[checkout][gitrepo]).  

The code is also mirrored to [Github][githubrepo]

The code as it was initially contributed and entered the incubator is on the 'master' branch.

To view the lastest codebase as we work toward an initial release checkout the 'develop' branch.

All guidance that follows assumes you are working on the 'develop' branch.

## Issue Tracking

Track issues on the "NIFI" Project on the Apache Jira ([browse][jira]).

## Building

#### Checking out from Git

To check out the code:

    git clone http://git-wip-us.apache.org/repos/asf/incubator-nifi.git

Then checkout the 'develop' branch

    git checkout develop

### Build steps

1. You need a recent Java 7 (or newer) JDK.
2. You need Apache [Maven 3.X][maven]. We've successfully used 3.2.3 and as far back as 3.0.5
3. Build the maven plugins.  In the root dir of the source tree cd to `nifi-nar-maven-plugin`.
   Run `mvn clean install`
4. Build the entire code base.  In the root dir of the source tree cd to `nifi` and run `mvn -T C2.0 clean install`
   You can tweak the maven build settings as you like but the previous command will execute with 2 threads per core.

Now you should have a fully functioning build off the latest code in the develop branch.

## Running the application

#### ** WARNING **

Without any configuration, the application will run on port 8080 and does not require any credentials to modify
the flow. This means of running Apache NiFi should be used only for development/testing and in an environment where only
connections from trusted computers and users can connect to port 8080. Using iptables to allow only localhost connections
to 8080 is a good start, but on systems with multiple (potentially untrusted) users, also not a sufficient protection.

#### Decompress and launch

Running the above build will create a tar.gz (and zip) file in `nifi/nifi-assembly/target`. This tar.gz should
contain the full application. Decompressing the tar.gz should make a directory for you containing several other
directories. `conf` contains application configuration, `bin` contains scripts
for launching the application. On linux and OSX, NiFi can be run using `bin/nifi.sh <command>` where
`<command>` is one of:

+ start: starts NiFi in the background
+ stop: stops NiFi that is running in the background
+ status: provides the current status of NiFi
+ run: runs NiFi in the foreground and waits to receive a Ctrl-C, which then shuts down NiFi.
+ install: (available in Linux only, not OSX): installs NiFi as a service that can then be controlled
via `service nifi start`, `service nifi stop`, `service nifi status`.


For Windows users, there exist several scripts in the `bin` directory that are analogous to those above:
`start-nifi.bat`, `stop-nifi.bat`, `nifi-status.bat`, and `run-nifi.bat`.

The configuration that is to be used when launching NiFi, such as Java heap size, the user
to run as, which Java command to use, etc. are configurable via the `conf/bootstrap.conf` file.

The entire concept of how the application will integrate to a given OS and run as an
enduring service is something we're working hard on and would appreciate ideas for.  The user experience needs to
be excellent.

With the default settings you can point a web browser at `http://localhost:8080/nifi/`

Logging is configured by default to log to `./logs/nifi-app.log`. The following log message should indicate the web ui
is ready for use:

    2014-12-09 00:42:03,540 INFO [main] org.apache.nifi.web.server.JettyServer NiFi has started. The UI is available at the following URLs:


[maven]: http://maven.apache.org/
[jira]: https://issues.apache.org/jira/browse/NIFI
[git]: http://git-scm.com/
[gitbrowse]: https://git-wip-us.apache.org/repos/asf?p=incubator-nifi.git;a=summary
[gitrepo]: http://git-wip-us.apache.org/repos/asf/incubator-nifi.git
[githubrepo]: https://github.com/apache/incubator-nifi

