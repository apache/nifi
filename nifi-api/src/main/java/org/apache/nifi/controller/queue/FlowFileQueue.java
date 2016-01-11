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
package org.apache.nifi.controller.queue;

import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.flowfile.FlowFilePrioritizer;
import org.apache.nifi.processor.FlowFileFilter;

public interface FlowFileQueue {

    /**
     * @return the unique identifier for this FlowFileQueue
     */
    String getIdentifier();

    /**
     * @return list of processing priorities for this queue
     */
    List<FlowFilePrioritizer> getPriorities();

    /**
     * Reads any Swap Files that belong to this queue and increments counts so that the size
     * of the queue will reflect the size of all FlowFiles regardless of whether or not they are
     * swapped out. This will be called only during NiFi startup as an initialization step. This
     * method is then responsible for returning the largest ID of any FlowFile that is swapped
     * out, or <code>null</code> if no FlowFiles are swapped out for this queue.
     *
     * @return the largest ID of any FlowFile that is swapped out for this queue, or <code>null</code> if
     *         no FlowFiles are swapped out for this queue.
     */
    Long recoverSwappedFlowFiles();

    /**
     * Destroys any Swap Files that exist for this queue without updating the FlowFile Repository
     * or Provenance Repository. This is done only on startup in the case of non-persistent
     * repositories. In the case of non-persistent repositories, we may still have Swap Files because
     * we may still need to overflow the FlowFiles from heap onto disk, even though we don't want to keep
     * the FlowFiles on restart.
     */
    void purgeSwapFiles();

    /**
     * Resets the comparator used by this queue to maintain order.
     *
     * @param newPriorities the ordered list of prioritizers to use to determine
     *            order within this queue.
     * @throws NullPointerException if arg is null
     */
    void setPriorities(List<FlowFilePrioritizer> newPriorities);

    /**
     * Establishes this queue's preferred maximum work load.
     *
     * @param maxQueueSize the maximum number of flow files this processor
     *            recommends having in its work queue at any one time
     */
    void setBackPressureObjectThreshold(long maxQueueSize);

    /**
     * @return maximum number of flow files that should be queued up at any one
     *         time
     */
    long getBackPressureObjectThreshold();

    /**
     * @param maxDataSize Establishes this queue's preferred maximum data size.
     */
    void setBackPressureDataSizeThreshold(String maxDataSize);

    /**
     * @return maximum data size that should be queued up at any one time
     */
    String getBackPressureDataSizeThreshold();

    QueueSize size();

    /**
     * @return true if no items queue; false otherwise
     */
    boolean isEmpty();

    /**
     * @return <code>true</code> if the queue is empty or contains only FlowFiles that already are being processed
     *         by others, <code>false</code> if the queue contains at least one FlowFile that is available for processing,
     *         regardless of whether that FlowFile(s) is in-memory or swapped out.
     */
    boolean isActiveQueueEmpty();

    /**
     * Returns a QueueSize that represents all FlowFiles that are 'unacknowledged'. A FlowFile
     * is considered to be unacknowledged if it has been pulled from the queue by some component
     * but the session that pulled the FlowFile has not yet been committed or rolled back.
     *
     * @return a QueueSize that represents all FlowFiles that are 'unacknowledged'.
     */
    QueueSize getUnacknowledgedQueueSize();

    void acknowledge(FlowFileRecord flowFile);

    void acknowledge(Collection<FlowFileRecord> flowFiles);

    /**
     * @return true if maximum queue size has been reached or exceeded; false
     *         otherwise
     */
    boolean isFull();

    /**
     * places the given file into the queue
     *
     * @param file to place into queue
     */
    void put(FlowFileRecord file);

    /**
     * places the given files into the queue
     *
     * @param files to place into queue
     */
    void putAll(Collection<FlowFileRecord> files);

    /**
     * @param expiredRecords expired records
     * @return the next flow file on the queue; null if empty
     */
    FlowFileRecord poll(Set<FlowFileRecord> expiredRecords);

    /**
     * @param maxResults limits how many results can be polled
     * @param expiredRecords for expired records
     * @return the next flow files on the queue up to the max results; null if
     *         empty
     */
    List<FlowFileRecord> poll(int maxResults, Set<FlowFileRecord> expiredRecords);

    /**
     * Drains flow files from the given source queue into the given destination
     * list.
     *
     * @param sourceQueue queue to drain from
     * @param destination Collection to drain to
     * @param maxResults max number to drain
     * @param expiredRecords for expired records
     * @return size (bytes) of flow files drained from queue
     */
    long drainQueue(Queue<FlowFileRecord> sourceQueue, List<FlowFileRecord> destination, int maxResults, Set<FlowFileRecord> expiredRecords);

    List<FlowFileRecord> poll(FlowFileFilter filter, Set<FlowFileRecord> expiredRecords);

    String getFlowFileExpiration();

    int getFlowFileExpiration(TimeUnit timeUnit);

    void setFlowFileExpiration(String flowExpirationPeriod);

    /**
     * Initiates a request to drop all FlowFiles in this queue. This method returns
     * a DropFlowFileStatus that can be used to determine the current state of the request.
     * Additionally, the DropFlowFileStatus provides a request identifier that can then be
     * passed to the {@link #getDropFlowFileStatus(String)} and {@link #cancelDropFlowFileStatus(String)}
     * methods in order to obtain the status later or cancel a request
     *
     * @param requestor the entity that is requesting that the FlowFiles be dropped; this will be
     *            included in the Provenance Events that are generated.
     *
     * @return the status of the drop request.
     */
    DropFlowFileStatus dropFlowFiles(String requestIdentifier, String requestor);

    /**
     * Returns the current status of a Drop FlowFile Request that was initiated via the
     * {@link #dropFlowFiles()} method that has the given identifier
     *
     * @param requestIdentifier the identifier of the Drop FlowFile Request
     * @return the status for the request with the given identifier, or <code>null</code> if no
     *         request status exists with that identifier
     */
    DropFlowFileStatus getDropFlowFileStatus(String requestIdentifier);

    /**
     * Cancels the request to drop FlowFiles that has the given identifier. After this method is called, the request
     * will no longer be known by this queue, so subsequent calls to {@link #getDropFlowFileStatus(String)} or
     * {@link #cancelDropFlowFileRequest(String)} will return <code>null</code>
     *
     * @param requestIdentifier the identifier of the Drop FlowFile Request
     * @return the status for the request with the given identifier after it has been canceled, or <code>null</code> if no
     *         request status exists with that identifier
     */
    DropFlowFileStatus cancelDropFlowFileRequest(String requestIdentifier);
}
