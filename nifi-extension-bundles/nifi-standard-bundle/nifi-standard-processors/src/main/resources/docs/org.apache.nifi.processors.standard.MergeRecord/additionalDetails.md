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

# MergeRecord

### Introduction

The MergeRecord Processor allows the user to take many FlowFiles that consist of record-oriented data (any data format
for which there is a Record Reader available) and combine the FlowFiles into one larger FlowFile. This may be preferable
before pushing the data to a downstream system that prefers larger batches of data, such as HDFS, or in order to improve
performance of a NiFi flow by reducing the number of FlowFiles that flow through the system (thereby reducing the
contention placed on the FlowFile Repository, Provenance Repository, Content Repository, and FlowFile Queues).

The Processor creates several 'bins' to put the FlowFiles in. The maximum number of bins to use is set to 5 by default,
but this can be changed by updating the value of the <Maximum number of Bins> property. The number of bins is bound in
order to avoid running out of Java heap space. Note: while the contents of a FlowFile are stored in the Content
Repository and not in the Java heap space, the Processor must hold the FlowFile objects themselves in memory. As a
result, these FlowFiles with their attributes can potentially take up a great deal of heap space and cause
OutOfMemoryError's to be thrown. In order to avoid this, if you expect to merge many small FlowFiles together, it is
advisable to instead use a MergeRecord that merges no more than say 1,000 records into a bundle and then use a second
MergeRecord to merges these small bundles into larger bundles. For example, to merge 1,000,000 records together, use
MergeRecord that uses a <Maximum Number of Records> of 1,000 and route the "merged" Relationship to a second MergeRecord
that also sets the <Maximum Number of Records> to 1,000. The second MergeRecord will then merge 1,000 bundles of 1,000,
which in effect produces bundles of 1,000,000.

### How FlowFiles are Binned

How the Processor determines which bin to place a FlowFile in depends on a few different configuration options. Firstly,
the Merge Strategy is considered. The Merge Strategy can be set to one of two options: Bin Packing Algorithm, or
Defragment. When the goal is to simply combine smaller FlowFiles into one larger FlowFiles, the Bin Packing Algorithm
should be used. This algorithm picks a bin based on whether the FlowFile can fit in the bin according to its size and
the <Maximum Bin Size> property and whether the FlowFile is 'like' the other FlowFiles in the bin. What it means for two
FlowFiles to be 'like FlowFiles' is discussed at the end of this section.

The "Defragment" Merge Strategy can be used when records need to be explicitly assigned to the same bin. For example, if
data is split apart using the SplitRecord Processor, each 'split' can be processed independently and later merged back
together using this Processor with the Merge Strategy set to Defragment. In order for FlowFiles to be added to the same
bin when using this configuration, the FlowFiles must have the same value for the "fragment.identifier" attribute. Each
FlowFile with the same identifier must also have the same value for the "fragment.count" attribute (which indicates how
many FlowFiles belong in the bin) and a unique value for the "fragment.index" attribute so that the FlowFiles can be
ordered correctly.

In order to be added to the same bin, two FlowFiles must be 'like FlowFiles.' In order for two FlowFiles to be like
FlowFiles, they must have the same schema, and if the <Correlation Attribute Name> property is set, they must have the
same value for the specified attribute. For example, if the <Correlation Attribute Name> is set to "filename" then two
FlowFiles must have the same value for the "filename" attribute in order to be binned together. If more than one
attribute is needed in order to correlate two FlowFiles, it is recommended to use an UpdateAttribute processor before
the MergeRecord processor and combine the attributes. For example, if the goal is to bin together two FlowFiles only if
they have the same value for the "abc" attribute and the "xyz" attribute, then we could accomplish this by using
UpdateAttribute and adding a property with name "correlation.attribute" and a value of "abc=${abc},xyz=${xyz}" and then
setting MergeRecord's <Correlation Attribute Name> property to "correlation.attribute".

It is often useful to bin together only Records that have the same value for some field. For example, if we have
point-of-sale data, perhaps the desire is to bin together records that belong to the same store, as identified by the '
storeId' field. This can be accomplished by making use of the PartitionRecord Processor ahead of MergeRecord. This
Processor will allow one or more fields to be configured as the partitioning criteria and will create attributes for
those corresponding values. An UpdateAttribute processor could then be used, if necessary, to combine multiple
attributes into a single correlation attribute, as described above. See documentation for those processors for more
details.

### When a Bin is Merged

Above, we discussed how a bin is chosen for a given FlowFile. Once a bin has been created and FlowFiles added to it, we
must have some way to determine when a bin is "full" so that we can bin those FlowFiles together into a "merged"
FlowFile.

If the <Merge Strategy> property is set to "Bin Packing Algorithm" then the following rules will be evaluated. Firstly,
in order for a bin to be full, both of the thresholds specified by the <Minimum Bin Size> and
the <Minimum Number of Records> properties must be satisfied. If one of these properties is not set, then it is ignored.
Secondly, if either the <Maximum Bin Size> or the <Maximum Number of Records> property is reached, then the bin is
merged. That is, both of the minimum values must be reached but only one of the maximum values need be reached. Note
that the <Maximum Number of Records> property is a "soft limit," meaning that all records in a given input FlowFile will
be added to the same bin, and as a result the number of records may exceed the maximum configured number of records.
Once this happens, though, no more Records will be added to that same bin from another FlowFile. If the <Max Bin Age> is
reached for a bin, then the FlowFiles in that bin will be merged, **even if** the minimum bin size and minimum number of
records have not yet been met. Finally, if the maximum number of bins have been created (as specified by
the <Maximum number of Bins> property), and some input FlowFiles cannot fit into any of the existing bins, then the
oldest bin will be merged to make room. This is done because otherwise we would not be able to add any additional
FlowFiles to the existing bins and would have to wait until the Max Bin Age is reached (if ever) in order to merge any
FlowFiles.

If the <Merge Strategy> property is set to "Defragment" then a bin is full only when the number of FlowFiles in the bin
is equal to the number specified by the "fragment.count" attribute of one of the FlowFiles in the bin. All FlowFiles
that have this attribute must have the same value for this attribute, or else they will be routed to the "failure"
relationship. It is not necessary that all FlowFiles have this value, but at least one FlowFile in the bin must have
this value or the bin will never be complete. If all the necessary FlowFiles are not binned together by the point at
which the bin times amount (as specified by the <Max Bin Age> property), then the FlowFiles will all be routed to the '
failure' relationship instead of being merged together.

Once a bin is merged into a single FlowFile, it can sometimes be useful to understand why exactly the bin was merged
when it was. For example, if the maximum number of allowable bins is reached, a merged FlowFile may consist of far fewer
records than expected. In order to help understand the behavior, the Processor will emit a JOIN Provenance Events when
creating the merged FlowFile, and the JOIN event will include in it a "Details" field that explains why the bin was
merged when it was. For example, the event will indicate "Records Merged due to: Bin is full" if the bin reached its
minimum thresholds and no more subsequent FlowFiles were added to it. Or it may indicate "Records Merged due to: Maximum
number of bins has been exceeded" if the bin was merged due to the configured maximum number of bins being filled and
needing to free up space for a new bin.

### When a Failure Occurs

When a bin is filled, the Processor is responsible for merging together all the records in those FlowFiles into a single
FlowFile. If the Processor fails to do so for any reason (for example, a Record cannot be read from an input FlowFile),
then all the FlowFiles in that bin are routed to the 'failure' Relationship. The Processor does not skip the single
problematic FlowFile and merge the others. This behavior was chosen because of two different considerations. Firstly,
without those problematic records, the bin may not truly be full, as the minimum bin size may not be reached without
those records. Secondly, and more importantly, if the problematic FlowFile contains 100 "good" records before the
problematic ones, those 100 records would already have been written to the "merged" FlowFile. We cannot un-write those
records. If we were to then send those 100 records on and route the problematic FlowFile to 'failure' then in a
situation where the "failure" relationship is eventually routed back to MergeRecord, we could end up continually
duplicating those 100 successfully processed records.

## Examples

To better understand how this Processor works, we will lay out a few examples. For the sake of simplicity of these
examples, we will use CSV-formatted data and write the merged data as CSV-formatted data, but the format of the data is
not really relevant, as long as there is a Record Reader that is capable of reading the data and a Record Writer capable
of writing the data in the desired format.

### Example 1 - Batching Together Many Small FlowFiles

When we want to batch together many small FlowFiles in order to create one larger FlowFile, we will accomplish this by
using the "Bin Packing Algorithm" Merge Strategy. The idea here is to bundle together as many FlowFiles as we can within
our minimum and maximum number of records and bin size. Consider that we have the following properties set:

| Property Name             | Property Value        |
|---------------------------|-----------------------|
| Merge Strategy            | Bin Packing Algorithm |
| Minimum Number of Records | 3                     |
| Maximum Number of Records | 5                     |

Also consider that we have the following data on the queue, with the schema indicating a Name and an Age field:

| FlowFile ID | FlowFile Contents      |
|-------------|------------------------|
| 1           | Mark, 33               |
| 2           | John, 45  <br>Jane, 43 |
| 3           | Jake, 3                |
| 4           | Jan, 2                 |

In this, because we have not configured a Correlation Attribute, and because all FlowFiles have the same schema, the
Processor will attempt to add all of these FlowFiles to the same bin. Because the Minimum Number of Records is 3 and the
Maximum Number of Records is 5, all the FlowFiles will be added to the same bin. The output, then, is a single FlowFile
with the following content:

`Mark, 33 John, 45 Jane, 43 Jake, 3 Jan, 2`

When the Processor runs, it will bin all the FlowFiles that it can get from the queue. After that, it will merge any bin
that is "full enough." So if we had only 3 FlowFiles on the queue, those 3 would have been added, and a new bin would
have been created in the next iteration, once the 4th FlowFile showed up. However, if we had 8 FlowFiles queued up, only
5 would have been added to the first bin. The other 3 would have been added to a second bin, and that bin would then be
merged since it reached the minimum threshold of 3 also.