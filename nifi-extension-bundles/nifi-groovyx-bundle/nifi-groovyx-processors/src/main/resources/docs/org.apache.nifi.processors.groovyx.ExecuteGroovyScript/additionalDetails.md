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

# Groovy

## Summary

This is the grooviest groovy script :)

## Script Bindings:

| variable                     | type                                                                                                                                                                                                                                                | description                                                                                                                                                                                                                                 |
|------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| session                      | org.apache.nifi.processor.ProcessSession                                                                                                                                                                                                            | the session that is used to get, change, and transfer input files                                                                                                                                                                           |
| context                      | org.apache.nifi.processor.ProcessContext                                                                                                                                                                                                            | the context (almost unuseful)                                                                                                                                                                                                               |
| log                          | org.apache.nifi.logging.ComponentLog                                                                                                                                                                                                                | the logger for this processor instance                                                                                                                                                                                                      |
| REL\_SUCCESS                 | org.apache.nifi.processor.Relationship                                                                                                                                                                                                              | the success relationship                                                                                                                                                                                                                    |
| REL\_FAILURE                 | org.apache.nifi.processor.Relationship                                                                                                                                                                                                              | the failure relationship                                                                                                                                                                                                                    |
| CTL                          | java.util.HashMap<String,[ControllerService](https://github.com/apache/nifi/blob/main/nifi-api/src/main/java/org/apache/nifi/controller/ControllerService.java)\>                                                                                   | Map populated with controller services defined with \`CTL.\*\` processor properties.  <br>The \`CTL.\` prefixed properties could be linked to controller service and provides access to this service from a script without additional code. |
| SQL                          | java.util.HashMap<String, [groovy.sql.Sql](http://docs.groovy-lang.org/latest/html/api/groovy/sql/Sql.html)\>                                                                                                                                       | Map populated with \`groovy.sql.Sql\` objects connected to corresponding database defined with \`SQL.\*\` processor properties.  <br>The \`SQL.\` prefixed properties could be linked only to DBCPSercice.                                  |
| RecordReader                 | java.util.HashMap<String,[RecordReaderFactory](https://github.com/apache/nifi/blob/main/nifi-nar-bundles/nifi-standard-services/nifi-record-serialization-service-api/src/main/java/org/apache/nifi/serialization/RecordReaderFactory.java)\>       | Map populated with controller services defined with \`RecordReader.\*\` processor properties.  <br>The \`RecordReader.\` prefixed properties are to be linked to RecordReaderFactory controller service instances.                          |
| RecordWriter                 | java.util.HashMap<String,[RecordSetWriterFactory](https://github.com/apache/nifi/blob/main/nifi-nar-bundles/nifi-standard-services/nifi-record-serialization-service-api/src/main/java/org/apache/nifi/serialization/RecordSetWriterFactory.java)\> | Map populated with controller services defined with \`RecordWriter.\*\` processor properties.  <br>The \`RecordWriter.\` prefixed properties are to be linked to RecordSetWriterFactory controller service instances.                       |
| Dynamic processor properties | org.apache.nifi.components.PropertyDescriptor                                                                                                                                                                                                       | All processor properties not started with \`CTL.\` or \`SQL.\` are bound to script variables                                                                                                                                                |

## SQL map details

**Example:** if you defined property `` `SQL.mydb` `` and linked it to any DBCPService, then you can access it from code
`SQL.mydb.rows('select * from mytable')`

The processor automatically takes connection from dbcp service before executing script and tries to handle
transaction:  
database transactions automatically rolled back on script exception and committed on success.  
Or you can manage transaction manually.  
NOTE: Script must not disconnect connection.

## SessionFile - flow file extension

The (org.apache.nifi.processors.groovyx.flow.SessionFile) is an actual object returned by session in Extended Groovy
processor.  
This flow file is a container that references session and the real flow file.  
This allows to use simplified syntax to work with file attributes and content:

_set new attribute value_

```groovy
flowFile.ATTRIBUTE_NAME = ATTRIBUTE_VALUE
flowFile.'mime.type' = 'text/xml'
flowFile.putAttribute("ATTRIBUTE_NAME", ATTRIBUTE_VALUE)
// the same as
flowFile = session.putAttribute(flowFile, "ATTRIBUTE_NAME", ATTRIBUTE_VALUE)
```

_remove attribute_

```groovy
flowFile.ATTRIBUTE_NAME = null
// equals to
flowFile = session.removeAttribute(flowFile, "ATTRIBUTE_NAME")
```

_get attribute value_

```groovy
String a = flowFile.ATTRIBUTE_NAME
```

_write content_

```groovy
flowFile.write("UTF-8", "THE CharSequence to write into flow file replacing current content")
flowFile.write("UTF-8") { writer ->
    // do something with java.io.Writer...
}
flowFile.write { outStream ->
    // do something with output stream...
}
flowFile.write { inStream, outStream ->
    // do something with input and output streams...
}
```

_get content_

```groovy
InputStream i = flowFile.read()
def json = new groovy.json.JsonSlurper().parse(flowFile.read())
String text = flowFile.read().getText("UTF-8")
```

_transfer flow file to success relation_

```groovy
REL_SUCCESS << flowFile
flowFile.transfer(REL_SUCCESS)
// the same as:
session.transfer(flowFile, REL_SUCCESS)
```

_work with dbcp_

```groovy
import groovy.sql.Sql

// define property named `SQL.db` connected to a DBCPConnectionPool controller service
// for this case it's an H2 database example

// read value from the database with prepared statement
// and assign into flowfile attribute `db.yesterday`
def daysAdd = -1
def row = SQL.db.firstRow("select dateadd('DAY', ${daysAdd}, sysdate) as DB_DATE from dual")
flowFile.'db.yesterday' = row.DB_DATE

// to work with BLOBs and CLOBs in the database
// use parameter casting using groovy.sql.Sql.BLOB(Stream) and groovy.sql.Sql.CLOB(Reader)

// write content of the flow file into database blob
flowFile.read { rawIn ->
    def parms = [
            p_id  : flowFile.ID as Long, // get flow file attribute named \`ID\`
            p_data: Sql.BLOB(rawIn),   // use input stream as BLOB sql parameter
    ]
    SQL.db.executeUpdate(parms, "update mytable set data = :p_data where id = :p_id")
}
```

## Handling processor start & stop

In the extended groovy processor you can catch \`start\` and \`stop\` and \`unscheduled\` events by providing
corresponding static methods:

```groovy
import org.apache.nifi.processor.ProcessContext
import java.util.concurrent.atomic.AtomicLong

class Const {
    static Date startTime = null;
    static AtomicLong triggerCount = null;
}

static onStart(ProcessContext context) {
    Const.startTime = new Date()
    Const.triggerCount = new AtomicLong(0)
    println "onStart $context ${Const.startTime}"
}

static onStop(ProcessContext context) {
    def alive = (System.currentTimeMillis() - Const.startTime.getTime()) / 1000
    println "onStop $context executed ${Const.triggerCount} times during ${alive} seconds"
}

static onUnscheduled(ProcessContext context) {
    def alive = (System.currentTimeMillis() - Const.startTime.getTime()) / 1000
    println "onUnscheduled $context executed ${Const.triggerCount} times during ${alive} seconds"
}

flowFile.'trigger.count' = Const.triggerCount.incrementAndGet()
REL_SUCCESS << flowFile
```
