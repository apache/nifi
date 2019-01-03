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
```mvn package```

docker image will be tagged nifi-fn:1.9.0-SNAPSHOT

### Usage:
```
1) RunFromRegistry [Once|Continuous] <NiFi registry URL> <Bucket ID> <Flow ID> <Input Variables> [<Failure Output Ports>] [<Input FlowFile>]
   RunFromRegistry [Once|Continuous] --json <JSON>
   RunFromRegistry [Once|Continuous] --file <File Name>

2) RunYARNServiceFromRegistry        <YARN RM URL> <Docker Image Name> <Service Name> <# of Containers> \
                                           <NiFi registry URL> <Bucket ID> <Flow ID> <Input Variables> [<Failure Output Ports>] [<Input FlowFile>]
   RunYARNServiceFromRegistry        <YARN RM URL> <Docker Image Name> <Service Name> <# of Containers> --json <JSON>
   RunYARNServiceFromRegistry        <YARN RM URL> <Docker Image Name> <Service Name> <# of Containers> --file <File Name>

3) RunOpenwhiskActionServer          <Port>
```

### Examples:
```
1) RunFromRegistry Once http://172.0.0.1:61080 e53b8a0d-5c85-4fcd-912a-1c549a586c83 6cf8277a-c402-4957-8623-0fa9890dd45d \
         "DestinationDirectory-/tmp/nififn/output2/" "" "absolute.path-/tmp/nififn/input/;filename-test.txt" "absolute.path-/tmp/nififn/input/;filename-test2.txt"
2) RunFromRegistry Once http://172.0.0.1:61080 e53b8a0d-5c85-4fcd-912a-1c549a586c83 6cf8277a-c402-4957-8623-0fa9890dd45d \
         "DestinationDirectory-/tmp/nififn/output2/" "f25c9204-6c95-3aa9-b0a8-c556f5f61849" "absolute.path-/tmp/nififn/input/;filename-test.txt"
3) RunYARNServiceFromRegistry http://127.0.0.1:8088 nifi-fn:latest kafka-to-solr 3 --file kafka-to-solr.json
4) RunOpenwhiskActionServer 8080
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

###JSON Sample:
```
{
  "nifi_registry": "http://localhost:61080",
  "nifi_bucket": "3aa885db-30c8-4c87-989c-d32b8ea1d3d8",
  "nifi_flow": "0d219eb8-419b-42ba-a5ee-ce07445c6fc5",
  "nifi_flowversion": -1,
  "nifi_materializecontent":true,
  "nifi_failureports": ["f25c9204-6c95-3aa9-b0a8-c556f5f61849"],
  "nifi_flowfiles":[{
      "absolute.path":"/tmp/nififn/input/",
      "filename":"test.txt",

      "nifi_content":"hello"
  },
  {
        "absolute.path":"/tmp/nififn/input/",
        "filename":"test2.txt",

        "nifi_content":"hi"
  }],

  "DestinationDirectory":"/tmp/nififn/output2/"
}
```

### TODO:
* Provenance is always recorded instead of waiting for commit. Rollback could result in duplicates:
    -FnProvenanceReporter.send force option is not appreciated
    -NiFi-FnProcessSession.adjustCounter immediate is not appreciated
* Nar directory is hardcoded
    reflectionUtil uses /usr/share/nifi-1.8.0/lib/ (location inside dockerfile)
* ####Classloader does not work
* Add support for:
    process groups
    funnels
* Send logs, metrics, and provenance to kafka/solr (configure a flow ID for each?)
* counters
* tests
* Processor and port IDs from the UI do not match IDs in templates or the registry
