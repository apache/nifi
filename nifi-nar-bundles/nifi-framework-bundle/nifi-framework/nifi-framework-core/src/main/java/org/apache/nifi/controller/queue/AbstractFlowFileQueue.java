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

import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.controller.repository.FlowFileRepository;
import org.apache.nifi.controller.repository.RepositoryRecord;
import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaimManager;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.provenance.ProvenanceEventBuilder;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventRepository;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.util.FormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public abstract class AbstractFlowFileQueue implements FlowFileQueue {
    private static final Logger logger = LoggerFactory.getLogger(AbstractFlowFileQueue.class);
    private final String identifier;
    private final FlowFileRepository flowFileRepository;
    private final ProvenanceEventRepository provRepository;
    private final ResourceClaimManager resourceClaimManager;
    private final ProcessScheduler scheduler;

    private final AtomicReference<TimePeriod> expirationPeriod = new AtomicReference<>(new TimePeriod("0 mins", 0L));
    private final AtomicReference<MaxQueueSize> maxQueueSize = new AtomicReference<>(new MaxQueueSize("1 GB", 1024 * 1024 * 1024, 10000));

    private final ConcurrentMap<String, ListFlowFileRequest> listRequestMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, DropFlowFileRequest> dropRequestMap = new ConcurrentHashMap<>();

    private LoadBalanceStrategy loadBalanceStrategy = LoadBalanceStrategy.DO_NOT_LOAD_BALANCE;
    private String partitioningAttribute = null;

    private LoadBalanceCompression compression = LoadBalanceCompression.DO_NOT_COMPRESS;


    public AbstractFlowFileQueue(final String identifier, final ProcessScheduler scheduler,
            final FlowFileRepository flowFileRepo, final ProvenanceEventRepository provRepo, final ResourceClaimManager resourceClaimManager) {
        this.identifier = identifier;
        this.scheduler = scheduler;
        this.flowFileRepository = flowFileRepo;
        this.provRepository = provRepo;
        this.resourceClaimManager = resourceClaimManager;
    }

    @Override
    public String getIdentifier() {
        return identifier;
    }

    protected ProcessScheduler getScheduler() {
        return scheduler;
    }

    @Override
    public String getFlowFileExpiration() {
        return expirationPeriod.get().getPeriod();
    }

    @Override
    public int getFlowFileExpiration(final TimeUnit timeUnit) {
        return (int) timeUnit.convert(expirationPeriod.get().getMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void setFlowFileExpiration(final String flowExpirationPeriod) {
        final long millis = FormatUtils.getTimeDuration(flowExpirationPeriod, TimeUnit.MILLISECONDS);
        if (millis < 0) {
            throw new IllegalArgumentException("FlowFile Expiration Period must be positive");
        }

        expirationPeriod.set(new TimePeriod(flowExpirationPeriod, millis));
    }

    @Override
    public void setBackPressureObjectThreshold(final long threshold) {
        boolean updated = false;
        while (!updated) {
            MaxQueueSize maxSize = getMaxQueueSize();
            final MaxQueueSize updatedSize = new MaxQueueSize(maxSize.getMaxSize(), maxSize.getMaxBytes(), threshold);
            updated = maxQueueSize.compareAndSet(maxSize, updatedSize);
        }
    }

    @Override
    public long getBackPressureObjectThreshold() {
        return getMaxQueueSize().getMaxCount();
    }

    @Override
    public void setBackPressureDataSizeThreshold(final String maxDataSize) {
        final long maxBytes = DataUnit.parseDataSize(maxDataSize, DataUnit.B).longValue();

        boolean updated = false;
        while (!updated) {
            MaxQueueSize maxSize = getMaxQueueSize();
            final MaxQueueSize updatedSize = new MaxQueueSize(maxDataSize, maxBytes, maxSize.getMaxCount());
            updated = maxQueueSize.compareAndSet(maxSize, updatedSize);
        }
    }

    @Override
    public String getBackPressureDataSizeThreshold() {
        return getMaxQueueSize().getMaxSize();
    }

    private MaxQueueSize getMaxQueueSize() {
        return maxQueueSize.get();
    }

    @Override
    public boolean isFull() {
        return isFull(size());
    }

    protected boolean isFull(final QueueSize queueSize) {
        final MaxQueueSize maxSize = getMaxQueueSize();

        // Check if max size is set
        if (maxSize.getMaxBytes() <= 0 && maxSize.getMaxCount() <= 0) {
            return false;
        }

        if (maxSize.getMaxCount() > 0 && queueSize.getObjectCount() >= maxSize.getMaxCount()) {
            return true;
        }

        if (maxSize.getMaxBytes() > 0 && queueSize.getByteCount() >= maxSize.getMaxBytes()) {
            return true;
        }

        return false;
    }


    @Override
    public ListFlowFileStatus listFlowFiles(final String requestIdentifier, final int maxResults) {
        // purge any old requests from the map just to keep it clean. But if there are very few requests, which is usually the case, then don't bother
        if (listRequestMap.size() > 10) {
            final List<String> toDrop = new ArrayList<>();
            for (final Map.Entry<String, ListFlowFileRequest> entry : listRequestMap.entrySet()) {
                final ListFlowFileRequest request = entry.getValue();
                final boolean completed = request.getState() == ListFlowFileState.COMPLETE || request.getState() == ListFlowFileState.FAILURE;

                if (completed && System.currentTimeMillis() - request.getLastUpdated() > TimeUnit.MINUTES.toMillis(5L)) {
                    toDrop.add(entry.getKey());
                }
            }

            for (final String requestId : toDrop) {
                listRequestMap.remove(requestId);
            }
        }

        // numSteps = 1 for each swap location + 1 for active queue + 1 for swap queue.
        final ListFlowFileRequest listRequest = new ListFlowFileRequest(requestIdentifier, maxResults, size());

        final Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                int position = 0;
                final List<FlowFileSummary> summaries = new ArrayList<>();

                // Create an ArrayList that contains all of the contents of the active queue.
                // We do this so that we don't have to hold the lock any longer than absolutely necessary.
                // We cannot simply pull the first 'maxResults' records from the queue, however, because the
                // Iterator provided by PriorityQueue does not return records in order. So we would have to either
                // use a writeLock and 'pop' the first 'maxResults' records off the queue or use a read lock and
                // do a shallow copy of the queue. The shallow copy is generally quicker because it doesn't have to do
                // the sorting to put the records back. So even though this has an expensive of Java Heap to create the
                // extra collection, we are making this trade-off to avoid locking the queue any longer than required.
                final List<FlowFileRecord> allFlowFiles = getListableFlowFiles();
                final QueuePrioritizer prioritizer = new QueuePrioritizer(getPriorities());

                listRequest.setState(ListFlowFileState.CALCULATING_LIST);

                // sort the FlowFileRecords so that we have the list in the same order as on the queue.
                allFlowFiles.sort(prioritizer);

                for (final FlowFileRecord flowFile : allFlowFiles) {
                    summaries.add(summarize(flowFile, ++position));
                    if (summaries.size() >= maxResults) {
                        break;
                    }
                }

                logger.debug("{} Finished listing FlowFiles for active queue with a total of {} results out of {} FlowFiles", this, summaries.size(), allFlowFiles.size());
                listRequest.setFlowFileSummaries(summaries);
                listRequest.setState(ListFlowFileState.COMPLETE);
            }
        }, "List FlowFiles for Connection " + getIdentifier());
        t.setDaemon(true);
        t.start();

        listRequestMap.put(requestIdentifier, listRequest);
        return listRequest;
    }

    @Override
    public ListFlowFileStatus getListFlowFileStatus(final String requestIdentifier) {
        return listRequestMap.get(requestIdentifier);
    }

    @Override
    public ListFlowFileStatus cancelListFlowFileRequest(final String requestIdentifier) {
        logger.info("Canceling ListFlowFile Request with ID {}", requestIdentifier);
        final ListFlowFileRequest request = listRequestMap.remove(requestIdentifier);
        if (request != null) {
            request.cancel();
        }

        return request;
    }

    /**
     * @return all FlowFiles that should be listed in response to a List Queue request
     */
    protected abstract List<FlowFileRecord> getListableFlowFiles();


    @Override
    public DropFlowFileStatus dropFlowFiles(final String requestIdentifier, final String requestor) {
        logger.info("Initiating drop of FlowFiles from {} on behalf of {} (request identifier={})", this, requestor, requestIdentifier);

        // purge any old requests from the map just to keep it clean. But if there are very requests, which is usually the case, then don't bother
        if (dropRequestMap.size() > 10) {
            final List<String> toDrop = new ArrayList<>();
            for (final Map.Entry<String, DropFlowFileRequest> entry : dropRequestMap.entrySet()) {
                final DropFlowFileRequest request = entry.getValue();
                final boolean completed = request.getState() == DropFlowFileState.COMPLETE || request.getState() == DropFlowFileState.FAILURE;

                if (completed && System.currentTimeMillis() - request.getLastUpdated() > TimeUnit.MINUTES.toMillis(5L)) {
                    toDrop.add(entry.getKey());
                }
            }

            for (final String requestId : toDrop) {
                dropRequestMap.remove(requestId);
            }
        }

        final DropFlowFileRequest dropRequest = new DropFlowFileRequest(requestIdentifier);
        final QueueSize originalSize = size();
        dropRequest.setCurrentSize(originalSize);
        dropRequest.setOriginalSize(originalSize);
        if (originalSize.getObjectCount() == 0) {
            dropRequest.setDroppedSize(originalSize);
            dropRequest.setState(DropFlowFileState.COMPLETE);
            dropRequestMap.put(requestIdentifier, dropRequest);
            return dropRequest;
        }

        final Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                dropFlowFiles(dropRequest, requestor);
            }
        }, "Drop FlowFiles for Connection " + getIdentifier());
        t.setDaemon(true);
        t.start();

        dropRequestMap.put(requestIdentifier, dropRequest);

        return dropRequest;
    }


    @Override
    public DropFlowFileRequest cancelDropFlowFileRequest(final String requestIdentifier) {
        final DropFlowFileRequest request = dropRequestMap.remove(requestIdentifier);
        if (request == null) {
            return null;
        }

        request.cancel();
        return request;
    }

    @Override
    public DropFlowFileStatus getDropFlowFileStatus(final String requestIdentifier) {
        return dropRequestMap.get(requestIdentifier);
    }

    /**
     * Synchronously drops all FlowFiles in the queue
     *
     * @param dropRequest the request
     * @param requestor the identity of the user/agent who made the request
     */
    protected abstract void dropFlowFiles(final DropFlowFileRequest dropRequest, final String requestor);

    @Override
    public void verifyCanList() throws IllegalStateException {
    }


    protected FlowFileSummary summarize(final FlowFile flowFile, final int position) {
        // extract all of the information that we care about into new variables rather than just
        // wrapping the FlowFile object with a FlowFileSummary object. We do this because we want to
        // be able to hold many FlowFileSummary objects in memory and if we just wrap the FlowFile object,
        // we will end up holding the entire FlowFile (including all Attributes) in the Java heap as well,
        // which can be problematic if we expect them to be swapped out.
        final String uuid = flowFile.getAttribute(CoreAttributes.UUID.key());
        final String filename = flowFile.getAttribute(CoreAttributes.FILENAME.key());
        final long size = flowFile.getSize();
        final Long lastQueuedTime = flowFile.getLastQueueDate();
        final long lineageStart = flowFile.getLineageStartDate();
        final boolean penalized = flowFile.isPenalized();

        return new FlowFileSummary() {
            @Override
            public String getUuid() {
                return uuid;
            }

            @Override
            public String getFilename() {
                return filename;
            }

            @Override
            public int getPosition() {
                return position;
            }

            @Override
            public long getSize() {
                return size;
            }

            @Override
            public long getLastQueuedTime() {
                return lastQueuedTime == null ? 0L : lastQueuedTime;
            }

            @Override
            public long getLineageStartDate() {
                return lineageStart;
            }

            @Override
            public boolean isPenalized() {
                return penalized;
            }
        };
    }

    protected QueueSize drop(final List<FlowFileRecord> flowFiles, final String requestor) throws IOException {
        // Create a Provenance Event and a FlowFile Repository record for each FlowFile
        final List<ProvenanceEventRecord> provenanceEvents = new ArrayList<>(flowFiles.size());
        final List<RepositoryRecord> flowFileRepoRecords = new ArrayList<>(flowFiles.size());
        for (final FlowFileRecord flowFile : flowFiles) {
            provenanceEvents.add(createDropProvenanceEvent(flowFile, requestor));
            flowFileRepoRecords.add(createDeleteRepositoryRecord(flowFile));
        }

        long dropContentSize = 0L;
        for (final FlowFileRecord flowFile : flowFiles) {
            dropContentSize += flowFile.getSize();
            final ContentClaim contentClaim = flowFile.getContentClaim();
            if (contentClaim == null) {
                continue;
            }

            final ResourceClaim resourceClaim = contentClaim.getResourceClaim();
            if (resourceClaim == null) {
                continue;
            }

            resourceClaimManager.decrementClaimantCount(resourceClaim);
        }

        provRepository.registerEvents(provenanceEvents);
        flowFileRepository.updateRepository(flowFileRepoRecords);
        return new QueueSize(flowFiles.size(), dropContentSize);
    }

    private ProvenanceEventRecord createDropProvenanceEvent(final FlowFileRecord flowFile, final String requestor) {
        final ProvenanceEventBuilder builder = provRepository.eventBuilder();
        builder.fromFlowFile(flowFile);
        builder.setEventType(ProvenanceEventType.DROP);
        builder.setLineageStartDate(flowFile.getLineageStartDate());
        builder.setComponentId(getIdentifier());
        builder.setComponentType("Connection");
        builder.setAttributes(flowFile.getAttributes(), Collections.emptyMap());
        builder.setDetails("FlowFile Queue emptied by " + requestor);
        builder.setSourceQueueIdentifier(getIdentifier());

        final ContentClaim contentClaim = flowFile.getContentClaim();
        if (contentClaim != null) {
            final ResourceClaim resourceClaim = contentClaim.getResourceClaim();
            builder.setPreviousContentClaim(resourceClaim.getContainer(), resourceClaim.getSection(), resourceClaim.getId(), contentClaim.getOffset(), flowFile.getSize());
        }

        return builder.build();
    }

    private RepositoryRecord createDeleteRepositoryRecord(final FlowFileRecord flowFile) {
        return new DropFlowFileRepositoryRecord(this, flowFile);
    }

    @Override
    public synchronized void setLoadBalanceStrategy(final LoadBalanceStrategy strategy, final String partitioningAttribute) {
        if (strategy == LoadBalanceStrategy.PARTITION_BY_ATTRIBUTE && !FlowFile.KeyValidator.isValid(partitioningAttribute)) {
            throw new IllegalArgumentException("Cannot set Load Balance Strategy to " + strategy + " without providing a valid Partitioning Attribute");
        }

        this.loadBalanceStrategy = strategy;
        this.partitioningAttribute = partitioningAttribute;
    }

    @Override
    public synchronized String getPartitioningAttribute() {
        return partitioningAttribute;
    }

    @Override
    public synchronized LoadBalanceStrategy getLoadBalanceStrategy() {
        return loadBalanceStrategy;
    }

    @Override
    public synchronized void setLoadBalanceCompression(final LoadBalanceCompression compression) {
        this.compression = compression;
    }

    @Override
    public synchronized LoadBalanceCompression getLoadBalanceCompression() {
        return compression;
    }
}
