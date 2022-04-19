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
## Apache NiFi MiNiFi Command and Control (C2) Client
The c2-client-bundle provides implementation for the client aspect of the [C2 Protocol](https://cwiki.apache.org/confluence/display/MINIFI/C2+Design). The essence of the implementation is the heartbeat construction and the communication with the [C2 server](../../../../minifi-c2/README.md) via the C2HttpClient.

Currently, relying on the [C2 Protocol API](../c2-protocol) is limited to sending heartbeats and processing/acknowledging UPDATE configuration operation in the response (if any). When exposed the new configuration will be downloaded and passed back to the system using the C2 protocol.

When C2 is enabled, C2ClientService will be scheduled to send heartbeats periodically, so the C2 Server can notify the client about any operations that is defined by the protocol and needs to be executed on the client side.

Using the client means that configuration changes and other operations can be triggered and controlled centrally via the C2 server making the management of clients more simple and configuring them more flexible. The client supports bidirectional TLS authentication.

### Configuration
To use the client, the parameters coming from `C2ClientConfig` need to be properly set (this configuration class is also used for instantiating `C2HeartbeatFactory` and `C2HttpClient`)

```
    # The C2 Server endpoint where the heartbeat is sent
    private final String c2Url;
    
    # The C2 Server endpoint where the acknowledge is sent
    private final String c2AckUrl;
    
    # The class the agent belongs to (flow definition is tied to agent class on the server side)
    private final String agentClass;
    
    # Unique identifier for the agent if not provided it will be generated
    private final String agentIdentifier;
    
    # Directory where persistent configuration (e.g.: generated agent and device id will be persisted)
    private final String confDirectory;
    
    # Property of RuntimeManifest defined in c2-protocol. A unique identifier for the manifest
    private final String runtimeManifestIdentifier;
    
    # Property of RuntimeManifest defined in c2-protocol. The type of the runtime binary. Usually set when the runtime is built
    private final String runtimeType;
    
    # The frequency of sending the heartbeats. This property is used by the c2-client-bundle user who should schedule the client
    private final Long heartbeatPeriod;
    
    # Security properties for communication with the C2 Server
    private final String keystoreFilename;
    private final String keystorePass;
    private final String keyPass;
    private final KeystoreType keystoreType;
    private final String truststoreFilename;
    private final String truststorePass;
    private final KeystoreType truststoreType;
    private final HostnameVerifier hostnameVerifier;
```