### NiFi IDE Integration.

_The goal of this project is to provide necessary integration with variety of IDEs primarily to facilitate interactive debugging_

The project uses [Gradle](http://gradle.org/) for dependency management and build. You don't have to have Gradle pre-installed. It will self-provision if you don't have it therefore below directions shoudl work without pre-requisites other then knowing your NiFi home directory

##### Clone NIFi project

```git clone https://github.com/apache/nifi.git```


##### Generate necessary configuration files to import project into your IDE. 

To do that execute the following:

```cd nifi-external/nifi-ide-integration/```

For Eclipse:

```./gradlew clean eclipse``` 

For IntelliJ:

```./gradlew clean idea```

This will generate all necessary Eclipse/Idea configuration files. Now you can import the project into your workspace as _regular project_ (not Maven, not Gradle).

For Eclipse follow these instructions:

```File -> Import -> General -> Existing project into workspace```


##### Configure _Run Configuration_ for ```org.apache.nifi.NiFi```

For Eclipse follow these instructions:

```Click on the little black arrow to the right of the Run button -> Run Configurations -> Java Application -> New```

**Main tab:**
* Name: _NiFi Startup_
* Project: _nifi-ide-integration_
* Main class: _org.apache.nifi.NiFi_

**Arguments Tab:**
* VM Arguments: -Dnifi.properties.file.path=&lt;NIFI_HOME&gt;/./conf/nifi.properties
* Working directory: &lt;NIFI_HOME&gt;

> NOTE: You are not required to build NiFi project yourself. _NIFI_HOME_ in the context of this guide simply implies that you have NiFi distribution on your machine regardless if you built it yourself or installed binary distribution.

##### Add sources:

Switch to _Source_ tab ```Add -> External System Directory``` and browse to the root of your NiFi clone.

That is it. You can "Run" or "Debug" it. 

===



