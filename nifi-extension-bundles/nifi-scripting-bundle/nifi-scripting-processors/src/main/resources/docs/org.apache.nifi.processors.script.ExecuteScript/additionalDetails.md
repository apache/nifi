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

# ExecuteScript

### Description

The ExecuteScript Processor provides the ability to use a scripting language in order to leverage the NiFi API to
perform tasks such as the following:

* Read content and/or attributes from an incoming FlowFile
* Create a new FlowFile (with or without a parent)
* Write content and/or attributes to an outgoing FlowFile
* Interact with the ProcessSession to transfer FlowFiles to relationships
* Read/write to the State Manager to keep track of variables across executions of the processor

**Notes:**

* ExecuteScript uses the JSR-223 Script Engine API to evaluate scripts, so the use of idiomatic language structure is
  sometimes limited. For example, in the case of Groovy, there is a separate ExecuteGroovyScript processor that allows
  you to do many more idiomatic Groovy tasks. For example, it's easier to interact with Controller Services via
  ExecuteGroovyScript vs. ExecuteScript (see the ExecuteGroovyScript documentation for more details)

### Variable Bindings

The Processor expects a user defined script that is evaluated when the processor is triggered. The following variables
are available to the scripts:

| Variable Name        | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      | Variable Class                                                                                                             |
|----------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------|
| **session**          | This is a reference to the ProcessSession assigned to the processor. The session allows you to perform operations on FlowFiles such as create(), putAttribute(), and transfer(), as well as read() and write()                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   | [ProcessSession](https://www.javadoc.io/doc/org.apache.nifi/nifi-api/latest/org/apache/nifi/processor/ProcessSession.html) |
| **context**          | This is a reference to the ProcessContext for the processor. It can be used to retrieve processor properties, relationships, Controller Services, and the State Manager.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         | [ProcessContext](https://www.javadoc.io/doc/org.apache.nifi/nifi-api/latest/org/apache/nifi/processor/ProcessContext.html) |
| **log**              | This is a reference to the ComponentLog for the processor. Use it to log messages to NiFi, such as log.info('Hello world!')                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      | [ComponentLog](https://www.javadoc.io/doc/org.apache.nifi/nifi-api/latest/org/apache/nifi/logging/ComponentLog.html)       |
| **REL\_SUCCESS**     | This is a reference to the "success" relationship defined for the processor. It could also be inherited by referencing the static member of the parent class (ExecuteScript), this is a convenience variable. It also saves having to use the fully-qualified name for the relationship.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         | [Relationship](https://www.javadoc.io/doc/org.apache.nifi/nifi-api/latest/org/apache/nifi/processor/Relationship.html)     |
| **REL\_FAILURE**     | This is a reference to the "failure" relationship defined for the processor. As with REL\_SUCCESS, it could also be inherited by referencing the static member of the parent class (ExecuteScript), this is a convenience variable. It also saves having to use the fully-qualified name for the relationship.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   | [Relationship](https://www.javadoc.io/doc/org.apache.nifi/nifi-api/latest/org/apache/nifi/processor/Relationship.html)     |
| _Dynamic Properties_ | Any dynamic (user-defined) properties defined in ExecuteScript are passed to the script engine as variables set to the PropertyValue object corresponding to the dynamic property. This allows you to get the String value of the property, but also to evaluate the property with respect to NiFi Expression Language, cast the value as an appropriate data type (e.g., Boolean), etc. Because the dynamic property name becomes the variable name for the script, you must be aware of the variable naming properties for the chosen script engine. For example, Groovy does not allow periods (.) in variable names, so an error will occur if "my.property" was a dynamic property name. Interaction with these variables is done via the NiFi Java API. The 'Dynamic Properties' section below will discuss the relevant API calls as they are introduced. | [PropertyValue](https://www.javadoc.io/doc/org.apache.nifi/nifi-api/latest/org/apache/nifi/components/PropertyValue.html)  |

## Example Scripts

**Get an incoming FlowFile from the session**

**Use Case**: You have incoming connection(s) to ExecuteScript and want to retrieve one FlowFile from the queue(s) for
processing.

**Approach**: Use the get() method from the session object. This method returns the FlowFile that is next highest
priority FlowFile to process. If there is no FlowFile to process, the method will return null. NOTE: It is possible to
have null returned even if there is a steady flow of FlowFiles into the processor. This can happen if there are multiple
concurrent tasks for the processor, and the other task(s) have already retrieved the FlowFiles. If the script requires a
FlowFile to continue processing, then it should immediately return if null is returned from session.get()

_Groovy_

```groovy
flowFile = session.get()
if (!flowFile) return
```

**Get multiple incoming FlowFiles from the session**:

**Use Case**: You have incoming connection(s) to ExecuteScript and want to retrieve multiple FlowFiles from the queue(s)
for processing.

**Approach**: Use the get(_maxResults_) method from the session object. This method returns up to _maxResults_ FlowFiles
from the work queue. If no FlowFiles are available, an empty list is returned (the method does not return null). NOTE:
If multiple incoming queues are present, the behavior is unspecified in terms of whether all queues or only a single
queue will be polled in a single call. Having said that, the observed behavior (for both NiFi 1.1.0+ and before) is
described [here](https://issues.apache.org/jira/browse/NIFI-2751).

**Examples**:

_Groovy_

```groovy
flowFileList = session.get(100)
if (!flowFileList.isEmpty()) {
    flowFileList.each { flowFile ->
// Process each FlowFile here
    }
}
```

**Create a new FlowFile**

**Use Case**: You want to generate a new FlowFile to send to the next processor.

**Approach**: Use the create() method from the session object. This method returns a new FlowFile object, which you can
perform further processing on

**Examples**:

_Groovy_

```groovy
flowFile = session.create()
// Additional processing here
```

**Create a new FlowFile from a parent FlowFile**

**Use Case**: You want to generate new FlowFile(s) based on an incoming FlowFile.

**Approach**: Use the create(_parentFlowFile_) method from the session object. This method takes a parent FlowFile
reference and returns a new child FlowFile object. The newly created FlowFile will inherit all the parent's attributes
except for the UUID. This method will automatically generate a Provenance FORK event or a Provenance JOIN event,
depending on whether other FlowFiles are generated from the same parent before the ProcessSession is committed.

**Examples**:

_Groovy_

```groovy
flowFile = session.get()
if (!flowFile) return
newFlowFile = session.create(flowFile)
// Additional processing here
```

**Add an attribute to a FlowFile**

**Use Case**: You have a FlowFile to which you'd like to add a custom attribute.

**Approach**: Use the putAttribute(_flowFile_, _attributeKey_, _attributeValue_) method from the session object. This
method updates the given FlowFile's attributes with the given key/value pair. NOTE: The "uuid" attribute is fixed for a
FlowFile and cannot be modified; if the key is named "uuid", it will be ignored.

Also this is a good point to mention that FlowFile objects are immutable; this means that if you update a FlowFile's
attributes (or otherwise alter it) via the API, you will get a new reference to the new version of the FlowFile. This is
very important when it comes to transferring FlowFiles to relationships. You must keep a reference to the latest version
of a FlowFile, and you must transfer or remove the latest version of all FlowFiles retrieved from or created by the
session, otherwise you will get an error when executing. Most often, the variable used to store a FlowFile reference
will be overwritten with the latest version returned from a method that alters the FlowFile (intermediate FlowFile
references will be automatically discarded). In these examples you will see this technique of reusing a FlowFile
reference when adding attributes. Note that the current reference to the FlowFile is passed into the putAttribute()
method. The resulting FlowFile has an attribute named 'myAttr' with a value of 'myValue'. Also note that the method
takes a String for the value; if you have an Object you will have to serialize it to a String. Finally, please note that
if you are adding multiple attributes, it is better to create a Map and use putAllAttributes() instead (see next recipe
for details).

**Examples**:

_Groovy_

```groovy
flowFile = session.get()
if (!flowFile) return
flowFile = session.putAttribute(flowFile, 'myAttr', 'myValue')
```

**Add multiple attributes to a FlowFile**

**Use Case**: You have a FlowFile to which you'd like to add custom attributes.

**Approach**: Use the putAllAttributes(_flowFile_, _attributeMap_) method from the session object. This method updates
the given FlowFile's attributes with the key/value pairs from the given Map. NOTE: The "uuid" attribute is fixed for a
FlowFile and cannot be modified; if the key is named "uuid", it will be ignored.

**Examples**:

_Groovy_

```groovy
attrMap = ['myAttr1': '1', 'myAttr2': Integer.toString(2)]
flowFile = session.get()
if (!flowFile) return
flowFile = session.putAllAttributes(flowFile, attrMap)
```

**Get an attribute from a FlowFile**

**Use Case**: You have a FlowFile from which you'd like to inspect an attribute.

**Approach**: Use the getAttribute(_attributeKey_) method from the FlowFile object. This method returns the String value
for the given attributeKey, or null if the attributeKey is not found. The examples show the retrieval of the value for
the "filename" attribute.

**Examples**:

_Groovy_

```groovy
flowFile = session.get()
if (!flowFile) return
myAttr = flowFile.getAttribute('filename')
```

**Get all attributes from a FlowFile**

**Use Case**: You have a FlowFile from which you'd like to retrieve its attributes.

**Approach**: Use the getAttributes() method from the FlowFile object. This method returns a Map with String keys and
String values, representing the key/value pairs of attributes for the FlowFile. The examples show an iteration over the
Map of all attributes for a FlowFile.

**Examples**:

_Groovy_

```groovy
flowFile = session.get()
if (!flowFile) return
flowFile.getAttributes().each { key, value ->
// Do something with the key/value pair
}
```

**Transfer a FlowFile to a relationship**

**Use Case**: After processing a FlowFile (new or incoming), you want to transfer the FlowFile to a relationship ("
success" or "failure"). In this simple case let us assume there is a variable called "errorOccurred" that indicates
which relationship to which the FlowFile should be transferred. Additional error handling techniques will be discussed
in part 2 of this series.

**Approach**: Use the transfer(_flowFile_, _relationship_) method from the session object. From the documentation: this
method transfers the given FlowFile to the appropriate destination processor work queue(s) based on the given
relationship. If the relationship leads to more than one destination the state of the FlowFile is replicated such that
each destination receives an exact copy of the FlowFile though each will have its own unique identity.

NOTE: ExecuteScript will perform a session.commit() at the end of each execution to ensure the operations have been
committed. You do not need to (and should not) perform a session.commit() within the script.

**Examples**:

_Groovy_

```groovy
flowFile = session.get()
if (!flowFile) return
// Processing occurs here
if (errorOccurred) {
    session.transfer(flowFile, REL \ _FAILURE)
} else {
    session.transfer(flowFile, REL \ _SUCCESS)
}
```

**Send a message to the log at a specified logging level**

**Use Case**: You want to report some event that has occurred during processing to the logging framework.

**Approach**: Use the log variable with the warn(), trace(), debug(), info(), or error() methods. These methods can take
a single String, or a String followed by an array of Objects, or a String followed by an array of Objects followed by a
Throwable. The first one is used for simple messages. The second is used when you have some dynamic objects/values that
you want to log. To refer to these in the message string use "{}" in the message. These are evaluated against the Object
array in order of appearance, so if the message reads "Found these things: {} {} {}" and the Object array is \['Hello'
,1,true\], then the logged message will be "Found these things: Hello 1 true". The third form of these logging methods
also takes a Throwable parameter, and is useful when an exception is caught and you want to log it.

**Examples**:

_Groovy_

```groovy
log.info('Found these things: {} {} {}', \ ['Hello', 1, true \] as Object \ [\])
```

**Read the contents of an incoming FlowFile using a callback**

**Use Case**: You have incoming connection(s) to ExecuteScript and want to retrieve the contents of a FlowFile from the
queue(s) for processing.

**Approach**: Use the read(_flowFile_, _inputStreamCallback_) method from the session object. An InputStreamCallback
object is needed to pass into the read() method. Note that because InputStreamCallback is an object, the contents are
only visible to that object by default. If you need to use the data outside the read() method, use a more
globally-scoped variable. The examples will store the full contents of the incoming FlowFile into a String (using Apache
Commons' IOUtils class). NOTE: For large FlowFiles, this is not the best technique; rather you should read in only as
much data as you need, and process that as appropriate. For something like SplitText, you could read in a line at a time
and process it within the InputStreamCallback, or use the session.read(flowFile) approach mentioned earlier to get an
InputStream reference to use outside the callback.

**Examples**:

_Groovy_

```groovy
import org.apache.commons.io.IOUtils
import java.nio.charset.StandardCharsets

flowFile = session.get()
if (!flowFile) return
def text = ''
// Cast a closure with an inputStream parameter to InputStreamCallback
session.read(flowFile, { inputStream ->
    text = IOUtils.toString(inputStream, StandardCharsets.UTF \ _8)
// Do something with text here
} as InputStreamCallback)
```

**Write content to an outgoing FlowFile using a callback**

**Use Case**: You want to generate content for an outgoing FlowFile.

**Approach**: Use the write(_flowFile_, _outputStreamCallback_) method from the session object. An OutputStreamCallback
object is needed to pass into the write() method. Note that because OutputStreamCallback is an object, the contents are
only visible to that object by default. If you need to use the data outside the write() method, use a more
globally-scoped variable. The examples will write a sample String to a FlowFile.

**Examples**:

_Groovy_

```groovy
import org.apache.commons.io.IOUtils
import java.nio.charset.StandardCharsets

flowFile = session.get()
if (!flowFile) return
def text = 'Hello world!'
// Cast a closure with an outputStream parameter to OutputStreamCallback
flowFile = session.write(flowFile, { outputStream ->
    outputStream.write(text.getBytes(StandardCharsets.UTF \ _8))
} as OutputStreamCallback)
```

**Overwrite an incoming FlowFile with updated content using a callback**

**Use Case**: You want to reuse the incoming FlowFile but want to modify its content for the outgoing FlowFile.

**Approach**: Use the write(_flowFile_, _streamCallback_) method from the session object. An StreamCallback object is
needed to pass into the write() method. StreamCallback provides both an InputStream (from the incoming FlowFile) and an
outputStream (for the next version of that FlowFile), so you can use the InputStream to get the current contents of the
FlowFile, then modify them and write them back out to the FlowFile. This overwrites the contents of the FlowFile, so for
append you'd have to handle that by appending to the read-in contents, or use a different approach (with
session.append() rather than session.write() ). Note that because StreamCallback is an object, the contents are only
visible to that object by default. If you need to use the data outside the write() method, use a more globally-scoped
variable. The examples will reverse the contents of the incoming flowFile (assumed to be a String) and write out the
reversed string to a new version of the FlowFile.

**Examples**:

_Groovy_

```groovy
import org.apache.commons.io.IOUtils
import java.nio.charset.StandardCharsets

flowFile = session.get()
if (!flowFile) return
def text = 'Hello world!'
// Cast a closure with an inputStream and outputStream parameter to StreamCallback
flowFile = session.write(flowFile, { inputStream, outputStream ->
    text = IOUtils.toString(inputStream, StandardCharsets.UTF \ _8)
    outputStream.write(text.reverse().getBytes(StandardCharsets.UTF \ _8))
} as StreamCallback)
session.transfer(flowFile, REL \ _SUCCESS)
```

**Handle errors during script processing**

**Use Case**: An error occurs in the script (either by data validation or a thrown exception), and you want the script
to handle it gracefully.

**Approach**: For exceptions, use the exception-handling mechanism for the scripting language (often they are try/catch
block(s)). For data validation, you can use a similar approach, but define a boolean variable like "valid" and an
if/else clause rather than a try/catch clause. ExecuteScript defines "success" and "failure" relationships; often your
processing will transfer "good" FlowFiles to success and "bad" FlowFiles to failure (logging an error in the latter
case).

**Examples**:

_Groovy_

```groovy
flowFile = session.get()
if (!flowFile) return
try {
// Something that might throw an exception here

// Last operation is transfer to success (failures handled in the catch block)
    session.transfer(flowFile, REL \ _SUCCESS)
} catch (e) {
    log.error('Something went wrong', e)
    session.transfer(flowFile, REL \ _FAILURE)
}
```
