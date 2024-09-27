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

# ControlRate

This processor throttles throughput of FlowFiles based on a configured rate. The rate can be specified as either a
direct data rate (bytes per time period), or by counting FlowFiles or a specific attribute value. In all cases, the time
period for measurement is specified in the Time Duration property.

The processor operates in one of four available modes. The mode is determined by the Rate Control Criteria property.

| Mode                        | Description                                                                                                                                                                                                                                                                                         |
|-----------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Data Rate                   | The FlowFile content size is accumulated for all FlowFiles passing through this processor. FlowFiles are throttled to ensure a maximum overall data rate (bytes per time period) is not exceeded. The Maximum Rate property specifies the maximum bytes allowed per Time Duration.                  |
| FlowFile Count              | FlowFiles are counted regardless of content size. No more than the specified number of FlowFiles pass through this processor in the given Time Duration. The Maximum Rate property specifies the maximum number of FlowFiles allowed per Time Duration.                                             |
| Attribute Value             | The value of an attribute is accumulated to determine overall rate. The Rate Controlled Attribute property specifies the attribute whose value will be accumulated. The value of the specified attribute is expected to be an integer. This mode is independent of overall FlowFile size and count. |
| Data Rate or FlowFile Count | This mode provides a combination of Data Rate and FlowFile Count. Both rates are accumulated and FlowFiles are throttled if either rate is exceeded. Both Maximum Data Rate and Maximum FlowFile Rate properties must be specified to determine content size and FlowFile count per Time Duration.  |

If the Grouping Attribute property is specified, all rates are accumulated separately for unique values of the specified
attribute. For example, assume Grouping Attribute property is specified and its value is "city". All FlowFiles
containing a "city" attribute with value "Albuquerque" will have an accumulated rate calculated. A separate rate will be
calculated for all FlowFiles containing a "city" attribute with a value "Boston". In other words, separate rate
calculations will be accumulated for all unique values of the Grouping Attribute.