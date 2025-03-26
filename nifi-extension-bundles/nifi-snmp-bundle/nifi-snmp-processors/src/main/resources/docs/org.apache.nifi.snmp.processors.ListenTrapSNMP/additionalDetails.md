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

The ListenTrapSNMP processor listens for incoming SNMP traps and generates a FlowFile from the received Protocol Data Unit (PDU). It supports SNMPv1, SNMPv2c, and SNMPv3, utilizing the SNMP4J library.

When configured to use SNMPv3, SNMPv1 and SNMPv2c are automatically disabled. As a result, traps using SNMPv1 or SNMPv2c message models will not be received or processed. This is done to enforce a higher level of security, as SNMPv1 and SNMPv2c transmit community strings in plaintext, making them vulnerable to interception and unauthorized access.

For SNMPv3, security is based on a User-Based Security Model (USM). The 'USM Users Input Method' property allows users to configure the USM user database in different ways. Below is an example JSON file defining two users as "Json Content":

```json
[
  {
    "securityName": "user1",
    "authProtocol": "HMAC384SHA512",
    "authPassphrase": "authPassphrase1",
    "privProtocol": "AES192",
    "privPassphrase": "privPassphrase1"
  },
  {
    "securityName": "user2",
    "authProtocol": "HMAC192SHA256",
    "authPassphrase": "authPassphrase2",
    "privProtocol": "AES256",
    "privPassphrase": "privPassphrase2"
  }
]

```