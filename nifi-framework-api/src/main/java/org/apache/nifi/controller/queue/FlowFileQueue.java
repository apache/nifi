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

import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.controller.repository.SwapSummary;
import org.apache.nifi.flowfile.FlowFilePrioritizer;
import org.apache.nifi.processor.FlowFileFilter;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

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
     * Reads any Swap Files that belong to this queue and returns a summary of what is swapped out.
     * This will be called only during NiFi startup as an initialization step. This
     * method is then responsible for returning a FlowFileSummary of the FlowFiles that are swapped
     * out, or <code>null</code> if no FlowFiles are swapped out for this queue.
     *
     * @return a SwapSummary that describes the FlowFiles that exist in the queue but are swapped out.
     */
    SwapSummary recoverSwappedFlowFiles();

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

    void acknowledge(FlowFileRecord flowFile);

    void acknowledge(Collection<FlowFileRecord> flowFiles);

    /**
     * @return <code>true</code> if at least one FlowFile is unacknowledged, <code>false</code> if all FlowFiles that have been dequeued have been acknowledged
     */
    boolean isUnacknowledgedFlowFile();

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

    List<FlowFileRecord> poll(FlowFileFilter filter, Set<FlowFileRecord> expiredRecords);

    String getFlowFileExpiration();

    int getFlowFileExpiration(TimeUnit timeUnit);

    void setFlowFileExpiration(String flowExpirationPeriod);

    /**
     * Initiates a request to drop all FlowFiles in this queue. This method returns
     * a DropFlowFileStatus that can be used to determine the current state of the request.
     * Additionally, the DropFlowFileStatus provides a request identifier that can then be
     * passed to the {@link #getDropFlowFileStatus(String)} and {@link #cancelDropFlowFileRequest(String)}
     * methods in order to obtain the status later or cancel a request
     *
     * @param requestIdentifier the identifier of the Drop FlowFile Request
     * @param requestor the entity that is requesting that the FlowFiles be dropped; this will be
     *            included in the Provenance Events that are generated.
     *
     * @return the status of the drop request.
     */
    DropFlowFileStatus dropFlowFiles(String requestIdentifier, String requestor);

    /**
     * Returns the current status of a Drop FlowFile Request that was initiated via the
     * {@link #dropFlowFiles(String, String)} method that has the given identifier
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

    /**
     * <p>
     * Initiates a request to obtain a listing of FlowFiles in this queue. This method returns a
     * ListFlowFileStatus that can be used to obtain information about the FlowFiles that exist
     * within the queue. Additionally, the ListFlowFileStatus provides a request identifier that
     * can then be passed to the {@link #getListFlowFileStatus(String)}. The listing of FlowFiles
     * will be returned ordered by the position of the FlowFile in the queue.
     * </p>
     *
     * <p>
     * Note that if maxResults is larger than the size of the "active queue" (i.e., the un-swapped queued,
     * FlowFiles that are swapped out will not be returned.)
     * </p>
     *
     * @param requestIdentifier the identifier of the List FlowFile Request
     * @param maxResults the maximum number of FlowFileSummary objects to add to the ListFlowFileStatus
     *
     * @return the status for the request
     *
     * @throws IllegalStateException if either the source or the destination of the connection to which this queue belongs
     *             is currently running.
     */
    ListFlowFileStatus listFlowFiles(String requestIdentifier, int maxResults);

    /**
     * Returns the current status of a List FlowFile Request that was initiated via the {@link #listFlowFiles(String, int)}
     * method that has the given identifier
     *
     * @param requestIdentifier the identifier of the Drop FlowFile Request
     * @return the current status of the List FlowFile Request with the given identifier or <code>null</code> if no
     *         request status exists with that identifier
     */
    ListFlowFileStatus getListFlowFileStatus(String requestIdentifier);

    /**
     * Cancels the request to list FlowFiles that has the given identifier. After this method is called, the request
     * will no longer be known by this queue, so subsequent calls to {@link #getListFlowFileStatus(String)} or
     * {@link #cancelListFlowFileRequest(String)} will return <code>null</code>
     *
     * @param requestIdentifier the identifier of the Drop FlowFile Request
     * @return the current status of the List FlowFile Request with the given identifier or <code>null</code> if no
     *         request status exists with that identifier
     */
    ListFlowFileStatus cancelListFlowFileRequest(String requestIdentifier);

    /**
     * Returns the FlowFile with the given UUID or <code>null</code> if no FlowFile can be found in this queue
     * with the given UUID
     *
     * @param flowFileUuid the UUID of the FlowFile to retrieve
     * @return the FlowFile with the given UUID or <code>null</code> if no FlowFile can be found in this queue
     *         with the given UUID
     *
     * @throws IOException if unable to read FlowFiles that are stored on some external device
     */
    FlowFileRecord getFlowFile(String flowFileUuid) throws IOException;

    /**
     * Ensures that a listing can be performed on the queue
     *
     * @throws IllegalStateException if the queue is not in a state in which a listing can be performed
     */
    void verifyCanList() throws IllegalStateException;

    /**
     * Returns diagnostic information about the queue
     */
    QueueDiagnostics getQueueDiagnostics();

    void lock();

    void unlock();

    void setLoadBalanceStrategy(LoadBalanceStrategy strategy, String partitioningAttribute);

    /**
     * Offloads the flowfiles in the queue to other nodes.  This disables the queue from partition flowfiles locally.
     * <p>
     * This operation is a no-op if the node that contains this queue is not in a cluster.
     */
    void offloadQueue();

    /**
     * Resets a queue that has previously been offloaded.  This allows the queue to partition flowfiles locally, and
     * has no other effect on processors or remote process groups.
     * <p>
     * This operation is a no-op if the queue is not currently offloaded or the node that contains this queue is not
     * clustered.
     */
    void resetOffloadedQueue();

    LoadBalanceStrategy getLoadBalanceStrategy();

    void setLoadBalanceCompression(LoadBalanceCompression compression);

    LoadBalanceCompression getLoadBalanceCompression();

    String getPartitioningAttribute();

    void startLoadBalancing();

    void stopLoadBalancing();

    /**
     * @return <code>true</code> if the queue is actively transferring data to another node, <code>false</code> otherwise
     */
    boolean isActivelyLoadBalancing();
}
