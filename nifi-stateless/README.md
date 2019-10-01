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
# Stateless NiFi 
Similar to other stream processing frameworks, receipt of incoming data is not acknowledged until it is written to a destination. In the event of failure, data can be replayed from the source rather than relying on a stateful content repository. This will not work for all cases (e.g. fire-and-forget HTTP/tcp), but a large portion of use cases have a resilient source to retry from.

Note: Provenance, metrics, logs are not extracted at this time. Docker and other container engines can be used for logs and metrics.
### Build:
`mvn package -P docker`

Docker image will be tagged apache/nifi-stateless:1.10.0-SNAPSHOT-dockermaven

### Usage:
After building, the image can be used as follows
`docker run <options> apache/nifi-stateless:1.10.0-SNAPSHOT-dockermaven <arguments>`

Where the arguments dictate the runtime to use:
```
1) RunFromRegistry [Once|Continuous] --json <JSON>
   RunFromRegistry [Once|Continuous] --file <File Name>   # Filename of JSON file that matches the examples below.

2) RunYARNServiceFromRegistry <YARN RM URL> <Docker Image Name> <Service Name> <# of Containers> --json <JSON>
   RunYARNServiceFromRegistry <YARN RM URL> <Docker Image Name> <Service Name> <# of Containers> --file <File Name>

3) RunOpenwhiskActionServer   <Port>
```

### Examples:
```
1) docker run --rm -it nifi-stateless:1.10.0-SNAPSHOT-dockermaven \
    RunFromRegistry Once --file /Users/nifi/nifi-stateless-configs/flow-abc.json
2) docker run --rm -it nifi-stateless:1.10.0-SNAPSHOT-dockermaven \
    RunYARNServiceFromRegistry http://127.0.0.1:8088 nifi-stateless:latest kafka-to-solr 3 --file kafka-to-solr.json
3) docker run -d nifi-stateless:1.10.0-SNAPSHOT-dockermaven \
    RunOpenwhiskActionServer 8080
```

###Notes:
```
1) The configuration file must be in JSON format.
2) When providing configurations via JSON, the following attributes must be provided: nifi_registry, nifi_bucket, nifi_flow.
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
          "absolute.path": "/tmp/nifistateless/input/",
          "filename": "test.txt",

          "nifi_content": "hello"
      },
      {
            "absolute.path": "/tmp/nifistateless/input/",
            "filename": "test2.txt",

            "nifi_content": "hi"
      }],
      "parameters": {
        "DestinationDirectory" : "/tmp/nifistateless/output2/",
        "Username" : "jdoe",
        "Password": { "sensitive": "true", "value": "password" }
      }
    }



### TODO:
* Provenance is always recorded instead of waiting for commit. Rollback could result in duplicates:
    -StatelessProvenanceReporter.send force option is not appreciated
    -StatelessProcessSession.adjustCounter immediate is not appreciated
* Send logs, metrics, and provenance to kafka/solr (configure a flow ID for each?)
* counters
* tests
* Processor and port IDs from the UI do not match IDs in templates or the registry
