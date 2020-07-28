/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.controller.queue.clustered.partition;

import org.apache.nifi.controller.queue.FlowFileQueueContents;
import org.apache.nifi.controller.queue.LocalQueuePartitionDiagnostics;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.processor.FlowFileFilter;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * An extension of a Queue Partition that contains the methods necessary for Processors, Funnels, and Ports to interact with the Partition
 * as if it were an entire FlowFile Queue itself.
 */
public interface LocalQueuePartition extends QueuePartition {
    /**
     * @return <code>true</code> if the active queue is empty, <code>false</code> otherwise
     */
    boolean isActiveQueueEmpty();

    /**
     * @return <code>true</code> if there is at least one FlowFile that has not yet been acknowledged, <code>false</code> if all FlowFiles have been acknowledged.
     */
    boolean isUnacknowledgedFlowFile();

    /**
     * Returns a single FlowFile with the highest priority that is available in the partition, or <code>null</code> if no FlowFile is available
     *
     * @param expiredRecords a Set of FlowFileRecord's to which any expired records that are encountered should be added
     * @return a single FlowFile with the highest priority that is available in the partition, or <code>null</code> if no FlowFile is available
     */
    FlowFileRecord poll(Set<FlowFileRecord> expiredRecords);

    /**
     * Returns up to <code>maxResults</code> FlowFiles from the queue
     *
     * @param maxResults the maximum number of FlowFiles to return
     * @param expiredRecords a Set of FlowFileRecord's to which any expired records that are encountered should be added
     * @return a List of FlowFiles (possibly empty) with the highest priority FlowFiles that are available in the partition
     */
    List<FlowFileRecord> poll(int maxResults, Set<FlowFileRecord> expiredRecords);

    /**
     * Returns a List of FlowFiles that match the given filter
     *
     * @param filter the filter to determine whether or not a given FlowFile should be returned
     * @param expiredRecords a Set of FlowFileRecord's to which any expired records that are encountered should be added
     * @return a List of FlowFiles (possibly empty) with FlowFiles that matched the given filter
     */
    List<FlowFileRecord> poll(FlowFileFilter filter, Set<FlowFileRecord> expiredRecords);

    /**
     * Acknowledges that the given FlowFile has been accounted for and is no longer the responsibility of this partition
     * @param flowFile the FlowFile that has been accounted for
     */
    void acknowledge(FlowFileRecord flowFile);

    /**
     * Acknowledges that the given FlowFiles have been accounted for and is no longer the responsibility of this partition
     * @param flowFiles the FlowFiles that have been accounted for
     */
    void acknowledge(Collection<FlowFileRecord> flowFiles);

    /**
     * Returns the FlowFile with the given UUID, or <code>null</code> if the FlowFile with that UUID is not found in the partition
     *
     * @param flowFileUuid the UUID of the FlowFile
     * @return the FlowFile with the given UUID or <code>null</code> if the FlowFile cannot be found
     * @throws IOException if unable to read swapped data from a swap file
     */
    FlowFileRecord getFlowFile(final String flowFileUuid) throws IOException;

    /**
     * Returns the FlowFiles that can be provided as the result of as "List FlowFiles" action
     * @return a List of FlowFiles
     */
    List<FlowFileRecord> getListableFlowFiles();

    /**
     * Inherits the contents of another queue/partition
     * @param queueContents the contents to inherit
     */
    void inheritQueueContents(FlowFileQueueContents queueContents);

    /**
     * @return diagnostics information about the queue partition
     */
    LocalQueuePartitionDiagnostics getQueueDiagnostics();
}
