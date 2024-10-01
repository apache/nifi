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

# GetSNMP

## Summary

This processor polls an SNMP agent to get information for a given OID or OIDs (Strategy = GET) or for all the subtree
associated to a given OID or OIDs (Strategy = WALK). This processor supports SNMPv1, SNMPv2c and SNMPv3. The component
is based on [SNMP4J](http://www.snmp4j.org/).

The processor can compile the SNMP Get PDU from the attributes of an input flowfile (multiple OIDs can be specified) or
from a single OID specified in the processor property. In the former case, the processor will only consider the OIDs
specified in the flowfile. The processor is looking for attributes prefixed with _
snmp\$_. If such an attribute is found, the attribute name is split using the \$ character. The second element must
respect the OID format to be considered as a valid OID. The flowfile attribute value can be empty (it will be later
filled with the retrieved value and written into the outgoing flowfile). When the processor is triggered, it sends the
SNMP request and gets the information associated to request OID(s). Once response is received from the SNMP agent, a
FlowFile is constructed. The FlowFile content is empty, all the information is written in the FlowFile attributes. In
case of a single GET request, the properties associated to the received PDU are transferred into the FlowFile as
attributes. In case of a WALK request, only the couples "OID/value" are transferred into the FlowFile as attributes.
SNMP attributes names are prefixed with _snmp\$_ prefix.

Regarding the attributes representing the couples "OID/value", the attribute name has the following format:

* snmp\$_OID_\$_SMI\_Syntax\_Value_

where OID is the request OID, and SMI\_Syntax\_Value is the integer representing the type of the value associated to the
OID. This value is provided to allow the SetSNMP processor setting values in the correct type.

## SNMP Properties

In case of a single SNMP Get request, the following is the list of available standard SNMP properties which may come
with the PDU: _("snmp\$errorIndex", "snmp\$errorStatus", "snmp\$errorStatusText", "snmp\$nonRepeaters", 
"snmp\$requestID", "snmp\$type")_