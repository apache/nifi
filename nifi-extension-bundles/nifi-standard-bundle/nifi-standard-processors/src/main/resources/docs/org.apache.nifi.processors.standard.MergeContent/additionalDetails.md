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

# MergeContent

### Introduction

The MergeContent Processor provides the ability to combine many FlowFiles into a single FlowFile. There are many reasons
that a dataflow designer may want to do this. For example, it may be helpful to create batches of data before sending to
a downstream system, because the downstream system is better optimized for large files than for many tiny files. NiFi
itself can also benefit from this, as NiFi operates best on "micro-batches," where each FlowFile is several kilobytes to
several megabytes in size.

The Processor creates several 'bins' to put the FlowFiles in. The maximum number of bins to use is set to 5 by default,
but this can be changed by updating the value of the <Maximum number of Bins> property. The number of bins is bound in
order to avoid running out of Java heap space. Note: while the contents of a FlowFile are stored in the Content
Repository and not in the Java heap space, the Processor must hold the FlowFile objects themselves in memory. As a
result, these FlowFiles with their attributes can potentially take up a great deal of heap space and cause
OutOfMemoryError's to be thrown. In order to avoid this, if you expect to merge many small FlowFiles together, it is
advisable to instead use a MergeContent that merges no more than say 1,000 FlowFiles into a bundle and then use a second
MergeContent to merges these small bundles into larger bundles. For example, to merge 1,000,000 FlowFiles together, use
MergeContent that uses a <Maximum Number of Entries> of 1,000 and route the "merged" Relationship to a second
MergeContent that also sets the <Maximum Number of Entries> to 1,000. The second MergeContent will then merge 1,000
bundles of 1,000, which in effect produces bundles of 1,000,000.

### How FlowFiles are Binned

How the Processor determines which bin to place a FlowFile in depends on a few different configuration options. Firstly,
the Merge Strategy is considered. The Merge Strategy can be set to one of two options: "Bin Packing Algorithm," or "
Defragment". When the goal is to simply combine smaller FlowFiles into one larger FlowFile, the Bin Packing Algorithm
should be used. This algorithm picks a bin based on whether the FlowFile can fit in the bin according to its size and
the <Maximum Bin Size> property and whether the FlowFile is 'like' the other FlowFiles in the bin. What it means for two
FlowFiles to be 'like FlowFiles' is discussed at the end of this section.

The "Defragment" Merge Strategy can be used when FlowFiles need to be explicitly assigned to the same bin. For example,
if data is split apart using the UnpackContent Processor, each unpacked FlowFile can be processed independently and
later merged back together using this Processor with the Merge Strategy set to Defragment. In order for FlowFiles to be
added to the same bin when using this configuration, the FlowFiles must have the same value for the "
fragment.identifier" attribute. Each FlowFile with the same identifier must also have a unique value for the "
fragment.index" attribute so that the FlowFiles can be ordered correctly. For a given "fragment.identifier", at least
one FlowFile must have the "fragment.count" attribute (which indicates how many FlowFiles belong in the bin). Other
FlowFiles with the same identifier must have the same value for the "fragment.count" attribute, or they can omit this
attribute. **NOTE:** while there are valid use cases for breaking apart FlowFiles and later re-merging them, it is an
antipattern to take a larger FlowFile, break it into a million tiny FlowFiles, and then re-merge them. Doing so can
result in using huge amounts of Java heap and can result in Out Of Memory Errors. Additionally, it adds large amounts of
load to the NiFi framework. This can result in increased CPU and disk utilization and often times can be an order of
magnitude lower throughput and an order of magnitude higher latency. As an alternative, whenever possible, dataflows
should be built to make use of Record-oriented processors, such as QueryRecord, PartitionRecord, UpdateRecord,
LookupRecord, PublishKafkaRecord\_2\_6, etc.

In order to be added to the same bin, two FlowFiles must be 'like FlowFiles.' In order for two FlowFiles to be like
FlowFiles, they must have the same schema, and if the <Correlation Attribute Name> property is set, they must have the
same value for the specified attribute. For example, if the <Correlation Attribute Name> is set to "filename", then two
FlowFiles must have the same value for the "filename" attribute in order to be binned together. If more than one
attribute is needed in order to correlate two FlowFiles, it is recommended to use an UpdateAttribute processor before
the MergeContent processor and combine the attributes. For example, if the goal is to bin together two FlowFiles only if
they have the same value for the "abc" attribute and the "xyz" attribute, then we could accomplish this by using
UpdateAttribute and adding a property with name "correlation.attribute" and a value of "abc=${abc},xyz=${xyz}" and then
setting MergeContent's <Correlation Attribute Name> property to "correlation.attribute".

### When a Bin is Merged

Above, we discussed how a bin is chosen for a given FlowFile. Once a bin has been created and FlowFiles added to it, we
must have some way to determine when a bin is "full" so that we can bin those FlowFiles together into a "merged"
FlowFile.

If the <Merge Strategy> property is set to "Bin Packing Algorithm", then the following rules will be evaluated.

MergeContent exposes several different thresholds that can be used to create bins that are of an ideal size. For
example, the user can specify the minimum number of FlowFiles that must be packaged together before merging will be
performed. The minimum number of bytes can also be configured. Additionally, a maximum number of FlowFiles and bytes may
be specified.

There are two other conditions that will result in the contents of a Bin being merged together. The Max Bin Age
property specifies the maximum amount of time that FlowFiles can be binned together before the bin is merged. This
property should almost always be set, as it provides a means to set a timeout on a bin, so that even if data stops
flowing to the Processor for a while (due to a problem with an upstream system, a source processor being stopped, etc.)
the FlowFiles won't remain stuck in the MergeContent processor indefinitely. Additionally, the processor exposes a
property for the maximum number of Bins that should be used. For some use cases, this won't matter much. However, if the
Correlation Attribute property is set, this can be important. When an incoming FlowFile is to be placed in a Bin, the
processor must find an appropriate Bin to place the FlowFile into, or else create a new one. If a Bin must be created,
and the number of Bins that exist is greater than or equal to the value of the <Maximum Number of Bins> property, then
the oldest Bin will be merged together to make room for the new one.

If the <Merge Strategy> property is set to "Defragment", then a bin is full only when the number of FlowFiles in the bin
is equal to the number specified by the "fragment.count" attribute of one of the FlowFiles in the bin. All FlowFiles
that have this attribute must have the same value for this attribute, or else they will be routed to the "failure"
relationship. It is not necessary that all FlowFiles have this value, but at least one FlowFile in the bin must have
this value or the bin will never be complete. If all the necessary FlowFiles are not binned together by the point at
which the bin times amount (as specified by the <Max Bin Age> property), then the FlowFiles will all be routed to the '
failure' relationship instead of being merged together.

Finally, a bin can be merged if the <Bin Termination Check> property is configured and a FlowFile is received that
satisfies the specified condition. The condition is specified as an Expression Language expression. If any FlowFile
result in the expression returning a value of true, then the bin will be merged, regardless of how much data is in
the bin or how old the bin is. This incoming FlowFile that triggers the bin to be merged can either be added as the
last entry in the bin, as the first entry in a new bin, or output as its own bin, depending on the value of the
<FlowFile Insertion Strategy> property.

A bin of FlowFiles, then, is merged when any one of the following conditions is met:
- The bin has reached the maximum number of bytes, as configured by the <Max Group Size> property.
- The bin has reached the maximum number of FlowFiles, as configured by the <Maximum Number of Entries> property.
- The bin has reached both the minimum number of bytes, as configured by the <Min Group Size> property,
  AND the minimum number of FlowFiles, as configured by the <Minimum Number of Entries> property.
- The bin has reached the maximum age, as configured by the <Max Bin Age> property.
- The maximum number of bins has been reached, as configured by the <Maximum number of Bins> property, and a new bin must be created.
- The <Bin Termination Check> property is configured and a FlowFile is received that satisfies the specified condition.


### Reason for Merge

Whenever the contents of a Bin are merged, an attribute with the name "merge.reason" will be added to the merged
FlowFile. The below table provides a listing of all possible values for this attribute with an explanation of each.

| Attribute Value                  | Explanation                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
|----------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| MAX\_BYTES\_THRESHOLD\_REACHED   | The bin has reached the maximum number of bytes, as configured by the <Max Group Size> property. When this threshold is reached, the contents of the Bin will be merged together, even if the Minimum Number of Entries has not yet been reached.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| MAX\_ENTRIES\_THRESHOLD\_REACHED | The bin has reached the maximum number of FlowFiles, as configured by the <Maximum Number of Entries> property. When this threshold is reached, the contents of the Bin will be merged together, even if the minimum number of bytes (Min Group Size) has not yet been reached.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| MIN\_THRESHOLDS\_REACHED         | The bin has reached both the minimum number of bytes, as configured by the <Min Group Size> property, AND the minimum number of FlowFiles, as configured by the <Minimum Number of Entries> property. The bin has not reached the maximum number of bytes (Max Group Size) OR the maximum number of FlowFiles (Maximum Number of Entries).                                                                                                                                                                                                                                                                                                                                                                                                                             |
| TIMEOUT                          | The Bin has reached the maximum age, as configured by the <Max Bin Age> property. If this threshold is reached, the contents of the Bin will be merged together, even if the Bin has not yet reached either of the minimum thresholds. Note that the age here is determined by when the Bin was created, NOT the age of the FlowFiles that reside within those Bins. As a result, if the Processor is stopped until it has 1 million FlowFiles queued, each one being 10 days old, but the Max Bin Age is set to "1 day," the Max Bin Age will not be met for at least one full day, even though the FlowFiles themselves are much older than this threshold. If the Processor is stopped and restarted, all Bins are destroyed and recreated, and the timer is reset. |
| BIN\_MANAGER\_FULL               | If an incoming FlowFile does not fit into any of the existing Bins (either due to the Maximum thresholds set, or due to the Correlation Attribute being used, etc.), then a new Bin must be created for the incoming FlowFiles. If the number of active Bins is already equal to the <Maximum number of Bins> property, the oldest Bin will be merged in order to make room for the new Bin. In that case, the Bin Manager is said to be full, and this value will be used.                                                                                                                                                                                                                                                                                            |
| BIN\_TERMINATION\_SIGNAL         | A FlowFile signaled that the Bin should be terminated by satisfying the configured <Bin Termination Check> property.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |

Note that the attribute value is minimally named, while the textual description is far more verbose. This is done for a
few reasons. Firstly, storing a large value for the attribute can be more costly, utilizing more heap space and
requiring more resources to process. Secondly, it's more succinct, which makes it easier to talk about. Most
importantly, though, it means that a processor such as RouteOnAttribute can be used, if necessary, to route based on the
value of the attribute. In this way, the explanation can be further expanded or updated, without changing the value of
the attribute and without disturbing existing flows.