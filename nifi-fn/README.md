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
# NiFi-Fn

### Build:
`mvn package`

Docker image will be tagged nifi-fn:1.10.0-SNAPSHOT

### Usage:
After building, the Program can be run from the `target` directory:
`java -cp "lib/*" org.apache.nifi.fn.NiFiFn <lib dir> <nar working directory> <arguments>`

Where the arguments dictate the runtime to use:
```
1) RunFromRegistry [Once|Continuous] <NiFi registry URL> <Bucket ID> <Flow ID> <Input Variables> [<Failure Output Ports>] [<Input FlowFile>]
   RunFromRegistry [Once|Continuous] --json <JSON>
   RunFromRegistry [Once|Continuous] --file <File Name>   # Filename of JSON file that matches the examples below.

2) RunYARNServiceFromRegistry        <YARN RM URL> <Docker Image Name> <Service Name> <# of Containers> \
                                           <NiFi registry URL> <Bucket ID> <Flow ID> <Input Variables> [<Failure Output Ports>] [<Input FlowFile>]
   RunYARNServiceFromRegistry        <YARN RM URL> <Docker Image Name> <Service Name> <# of Containers> --json <JSON>
   RunYARNServiceFromRegistry        <YARN RM URL> <Docker Image Name> <Service Name> <# of Containers> --file <File Name>

3) RunOpenwhiskActionServer          <Port>
```

### Examples:
```
1) java -cp "lib/*" org.apache.nifi.fn.NiFiFn lib/ work/ \
    RunFromRegistry Once http://172.0.0.1:61080 e53b8a0d-5c85-4fcd-912a-1c549a586c83 6cf8277a-c402-4957-8623-0fa9890dd45d \
    "DestinationDirectory-/tmp/nififn/output2/" "" "absolute.path-/tmp/nififn/input/;filename-test.txt" "absolute.path-/tmp/nififn/input/;filename-test2.txt"
2) java -cp "lib/*" org.apache.nifi.fn.NiFiFn lib/ work/ \
    RunFromRegistry Once --file /Users/nifi/nifi-fn-configs/flow-abc.json
3) java -cp "lib/*" org.apache.nifi.fn.NiFiFn lib/ work/ \
    RunYARNServiceFromRegistry http://127.0.0.1:8088 nifi-fn:latest kafka-to-solr 3 --file kafka-to-solr.json
4) java -cp "lib/*" org.apache.nifi.fn.NiFiFn lib/ work/ \
    RunOpenwhiskActionServer 8080
```

###Notes:
```
1) <Input Variables> will be split on ';' and '-' then injected into the flow using the variable registry interface.
    2) <Failure Output Ports> will be split on ';'. FlowFiles routed to matching output ports will immediately fail the flow.
    3) <Input FlowFile> will be split on ';' and '-' then injected into the flow using the "nifi_content" field as the FlowFile content.
    4) Multiple <Input FlowFile> arguments can be provided.
    5) The configuration file must be in JSON format.
    6) When providing configurations via JSON, the following attributes must be provided: nifi_registry, nifi_bucket, nifi_flow.
          All other attributes will be passed to the flow using the variable registry interface
```

### JSON Format
The JSON that is provided, either via the `--json` command-line argument or the `--file` command-line argument has the following elements:

- `registryUrl` : The URL of the NiFi Registry that should be used for pulling the Flow
- `bucketId` : The UUID of the Bucket containing the flow
- `flowId` : The UUID of the flow to run
- `flowVersion` : _Optional_ - The Version of the flow to run. If not present or equal to -1, then the latest version of the flow will be used.
- `materializeContent` : _Optional_ - Whether or not the contents of the FlowFile should be stored in Java Heap so that they can be read multiple times. If this value is `false`, the contents of any
input FlowFile will be read as a stream of data and not buffered into heap. However, this means that the contents can be read only one time. This can be useful if transferring large files from HDFS to
 another HDFS instance or directory, for example, and contains a simple flow such as `ListHDFS -> FetchHDFS -> PutHDFS`. In this flow, the contents of the files will be buffered into Java Heap if the
 value of this argument is `true` but will not be if the value of this argument is `false`.
- `failurePortIds`: _Optional_ - An array of Port UUID's, such that if any data is sent to one of the ports with these ID's, the flow is considered "failed" and will stop immediately.
- `ssl`: _Optional_ - If present, provides SSL keystore and truststore information that can be used for interacting with the NiFi Registry and for Site-to-Site communications for Remote Process 
Groups.
- `flowFiles`: _Optional_ - An array of FlowFiles that should be provided to the flow's Input Port. Each element in the array is a JSON object. That JSON object can have multiple keys. If any of those
keys is `nifi_content` then the String value of that element will be the FlowFile's content. Otherwise, the key/value pair is considered an attribute of the FlowFile.
- `variables`: _Optional_ - Key/value pairs that will be passed to the NiFi Flow as variables of the root Process Group.


### Minimal JSON Sample:
    {
      "registryUrl": "http://localhost:18080",
      "bucketId": "3aa885db-30c8-4c87-989c-d32b8ea1d3d8",
      "flowId": "0d219eb8-419b-42ba-a5ee-ce07445c6fc5"
    }


### Full JSON Sample:
    {
      "registryUrl": "https://localhost:9443",
      "bucketId": "3aa885db-30c8-4c87-989c-d32b8ea1d3d8",
      "flowId": "0d219eb8-419b-42ba-a5ee-ce07445c6fc5",
      "flowVersion": 8,
      "materializeContent":true,
      "failurePortIds": ["f25c9204-6c95-3aa9-b0a8-c556f5f61849"],
      "ssl": {
        "keystoreFile": "/etc/security/keystore.jks",
        "keystorePass": "apachenifi",
        "keyPass": "nifiapache",
        "keystoreType": "JKS",
        "truststoreFile": "/etc/security/truststore.jks",
        "truststorePass": "apachenifi",
        "truststoreType": "JKS"
      },
      "flowFiles":[{
          "absolute.path": "/tmp/nififn/input/",
          "filename": "test.txt",

          "nifi_content": "hello"
      },
      {
            "absolute.path": "/tmp/nififn/input/",
            "filename": "test2.txt",

            "nifi_content": "hi"
      }],
      "variables": {
        "DestinationDirectory" : "/tmp/nififn/output2/"
      }
    }



### TODO:
* Provenance is always recorded instead of waiting for commit. Rollback could result in duplicates:
    -FnProvenanceReporter.send force option is not appreciated
    -FnProcessSession.adjustCounter immediate is not appreciated
* Send logs, metrics, and provenance to kafka/solr (configure a flow ID for each?)
* counters
* tests
* Processor and port IDs from the UI do not match IDs in templates or the registry
