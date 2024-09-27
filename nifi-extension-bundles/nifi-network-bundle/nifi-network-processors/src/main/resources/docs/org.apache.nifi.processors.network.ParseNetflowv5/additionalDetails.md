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

# Netflowv5Parser

Netflowv5Parser processor parses the ingress netflowv5 datagram format and transfers it either as flowfile attributes or JSON object. Netflowv5 format has predefined schema named "template" for parsing the netflowv5 record. More information: [RFC-netflowv5](https://www.cisco.com/c/en/us/td/docs/net_mgmt/netflow_collection_engine/3-6/user/guide/format.html "RFC-netflowv5")

## Netflowv5 JSON Output Schema

```json
{
  "port": int,
  "format": string,
  "header": {
    "version": int,
    "count": int,
    "sys_uptime": long,
    "unix_secs": long,
    "unix_nsecs": long,
    "flow_sequence": long,
    "engine_type": short,
    "engine_id": short,
    "sampling_interval": int
  },
  "record": {
    "srcaddr": string,
    "dstaddr": string,
    "nexthop": string,
    "input": int,
    "output": int,
    "dPkts": long,
    "dOctets": long,
    "first": long,
    "last": long,
    "srcport": int,
    "dstport": int,
    "pad1": short,
    "tcp_flags": short,
    "prot": short,
    "tos": short,
    "src_as": int,
    "dst_as": int,
    "src_mask": short,
    "dst_mask": short,
    "pad2": int
  }
}
```
