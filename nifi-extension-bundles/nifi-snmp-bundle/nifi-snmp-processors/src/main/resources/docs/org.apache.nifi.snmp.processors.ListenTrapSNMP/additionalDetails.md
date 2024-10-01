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

# ListenTrapSNMP

## Summary

This processor listens to SNMP traps and creates a flowfile from the trap PDU. The versions SNMPv1, SNMPv2c and SNMPv3
are supported. The component is based on [SNMP4J](http://www.snmp4j.org/).

SNMPv3 has user-based security. The USM Users Source property allows users to choose between three different ways to
provide the USM user database. An example json file containing two users:

```json
[
  {
    "securityName": "user1",
    "authProtocol": "MD5",
    "authPassphrase": "abc12345",
    "privProtocol": "DES",
    "privPassphrase": "abc12345"
  },
  {
    "securityName": "newUser2",
    "authProtocol": "MD5",
    "authPassphrase": "abc12345",
    "privProtocol": "AES256",
    "privPassphrase": "abc12345"
  }
]

```