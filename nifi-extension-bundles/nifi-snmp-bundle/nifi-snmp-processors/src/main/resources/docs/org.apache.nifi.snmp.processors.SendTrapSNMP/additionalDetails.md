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

# SendTrapSNMP

## Summary

This processor generates and transmits SNMP Traps to the specified SNMP manager. Attributes can be given as processor
properties, either predefined or dynamically using Expression Language from flowfiles. Flowfile properties with snmp
prefix (e.g. snmp$1.2.3.4.5 - OID) value can be used to define additional PDU variables.

The allowable Generic Trap Types are:

0. Cold Start
1. Warm Start
2. Link Down
3. Link Up
4. Authentication Failure
5. EGP Neighbor Loss
6. Enterprise Specific

Specific trap type can set in case of Enterprise Specific generic trap type is chosen.