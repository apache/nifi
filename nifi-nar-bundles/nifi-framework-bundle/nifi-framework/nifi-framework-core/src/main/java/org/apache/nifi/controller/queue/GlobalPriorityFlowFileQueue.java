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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.controller.repository.FlowFileRepository;
import org.apache.nifi.controller.repository.FlowFileSwapManager;
import org.apache.nifi.controller.repository.SwapContents;
import org.apache.nifi.controller.repository.SwapSummary;
import org.apache.nifi.controller.repository.claim.ResourceClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaimManager;
import org.apache.nifi.controller.swap.StandardSwapSummary;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.FlowFilePrioritizer;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.prioritizer.BucketPrioritizer;
import org.apache.nifi.priority.Rule;
import org.apache.nifi.priority.RulesManager;
import org.apache.nifi.processor.FlowFileFilter;
import org.apache.nifi.provenance.ProvenanceEventRepository;
import org.apache.nifi.reporting.Severity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/*
 * This connection works by sorting records based on satisfying globally defined priority rules. Those rule evaluations are tracked
 * statically so that they persist as records move from queue to queue unless they are forced to be re-evaluated. When a poll
 * occurs this connection will consider the highest priority record it currently holds and determine whether or not data with a higher
 * priority was recently polled elsewhere. If that is the case there is a chance that this connection will give up its  thread rather
 * than returning a record so as not to thread-starve higher priority data elsewhere in the system.
 */
public class GlobalPriorityFlowFileQueue extends AbstractFlowFileQueue {
    private static final Logger LOGGER = LoggerFactory.getLogger(GlobalPriorityFlowFileQueue.class);
    // Manages the set of rules against which flowfiles are evaluated
    private final RulesManager rulesManager;

    // This cache will track what rule a FlowFileRecord satisfied the last time it was evaluated by any GlobalPriorityFlowFileQueue
    static final AtomicReference<LoadingCache<FlowFileRecord, Rule>> FLOW_FILE_RULE_CACHE = new AtomicReference<>();
    // ruleBuckets has essentially replaced the priority queue from other connection implementations. This is where we will track all
    // FlowFileRecords that this connection is aware of.
    private final AtomicReference<Map<Rule, Queue<FlowFileRecord>>> ruleBuckets = new AtomicReference<>(new ConcurrentHashMap<>());

    private final AtomicLong bytesInBuckets = new AtomicLong(0L);
    private final AtomicInteger objectsInBuckets = new AtomicInteger(0);
    private final AtomicInteger numUnacknowledgedFlowFiles = new AtomicInteger(0);
    private final AtomicLong numUnacknowledgedBytes = new AtomicLong(0L);
    private final AtomicInteger numSwapFlowFiles = new AtomicInteger(0);
    private final AtomicLong numSwapFileBytes = new AtomicLong(0L);

    // Members used to slow processing of lower priority files while higher priority files exist
    private static final Map<Rule, Long> RULE_LAST_POLL_TIME = new ConcurrentHashMap<>();
    private static final Random RANDOM = new Random();

    private volatile List<FlowFilePrioritizer> prioritizers = Collections.emptyList();

    // When set to true all incoming records will be re-evaluated against the global ruleset during a put operation
    volatile boolean alwaysReevaluate = false;

    private final FlowFileSwapManager flowFileSwapManager;

    final static int MAX_EXPIRED_RECORDS = 10_000;
    private static final int SWAP_RECORD_POLL_SIZE = 10_000;
    private static final int SWAP_RECORD_PUSH_SIZE = SWAP_RECORD_POLL_SIZE / 2;
    private final int queueSwapThreshold;
    private final EventReporter eventReporter;

    private final Queue<String> swapLocations = new LinkedBlockingQueue<>();

    public GlobalPriorityFlowFileQueue(String identifier, ProcessScheduler processScheduler, FlowFileRepository flowFileRepository,
                                       ProvenanceEventRepository provenanceEventRepository, ResourceClaimManager resourceClaimManager,
                                       RulesManager rulesManager, long cacheAccessExpiration, long defaultBackPressureObjectThreshold,
                                       final String defaultBackPressureDataSizeThreshold, FlowFileSwapManager flowFileSwapManager,
                                       int queueSwapThreshold, EventReporter eventReporter) {
        super(identifier, processScheduler, flowFileRepository, provenanceEventRepository, resourceClaimManager);
        this.rulesManager = rulesManager;
        this.flowFileSwapManager = flowFileSwapManager;
        this.queueSwapThreshold = queueSwapThreshold;
        this.eventReporter = eventReporter;
        setBackPressureDataSizeThreshold(defaultBackPressureDataSizeThreshold);
        setBackPressureObjectThreshold(defaultBackPressureObjectThreshold);
        if(FLOW_FILE_RULE_CACHE.get() == null) {
            FLOW_FILE_RULE_CACHE.compareAndSet(null, CacheBuilder.newBuilder()
            .expireAfterAccess(cacheAccessExpiration, TimeUnit.MINUTES)
            .build(new CacheLoader<FlowFileRecord, Rule>() {
                @Override
                public Rule load(FlowFileRecord flowFileRecord) throws Exception {
                    return Rule.UNEVALUATED;
                }
            }));
        }
    }

    /**
     * @return A list of FlowFileRecords that this connection contains sorted in order by priority
     */
    @Override
    protected List<FlowFileRecord> getListableFlowFiles() {
        List<FlowFileRecord> listableFlowFiles = new ArrayList<>(objectsInBuckets.get());
        rulesManager.getRuleList().forEach(rule -> {
            if(ruleBuckets.get().containsKey(rule)) {
                listableFlowFiles.addAll(ruleBuckets.get().get(rule));
            }
        });
        return listableFlowFiles;
    }

    @Override
    protected void dropFlowFiles(DropFlowFileRequest dropFlowFileRequest, String requestor) {
        dropFlowFiles(dropFlowFileRequest, requestor, true);
    }

    protected void dropFlowFiles(DropFlowFileRequest dropFlowFileRequest, String requestor, boolean setCompletionState) {
        // Drop files from the system in batches of this size
        int batchSize = 1000;
        Map<Rule, Queue<FlowFileRecord>> currentRuleBuckets;

        currentRuleBuckets = ruleBuckets.getAndSet(new ConcurrentHashMap<>());

        LOGGER.debug("Executing DropFlowFileRequest for requestor {}", requestor);
        try {
            dropFlowFileRequest.setState(DropFlowFileState.DROPPING_FLOWFILES);

            // Used to track how many flowfiles we've batched so far to determine whether or not its time to perform a drop
            long index = 0;
            List<FlowFileRecord> recordsToDrop = new ArrayList<>(batchSize);
            final FlowFileRecord[] flowFileRecord = new FlowFileRecord[1];

            // Start dropping flowfiles from these queues in batches of batchSize
            for(Queue<FlowFileRecord> recordQueue : currentRuleBuckets.values()) {
                while(!dropFlowFileRequest.getState().equals(DropFlowFileState.CANCELED) && (flowFileRecord[0] = recordQueue.poll()) != null) {
                    recordsToDrop.add(flowFileRecord[0]);
                    // We've hit batchSize. Perform a drop.
                    if(++index % batchSize == 0) {
                        dropFlowFileRequest.setDroppedSize(dropFlowFileRequest.getDroppedSize().add(dropFlowFiles(recordsToDrop, requestor)));
                        recordsToDrop.clear();
                    }
                }

                // Drop was cancelled, return these records to the appropriate bucket
                if(dropFlowFileRequest.getState().equals(DropFlowFileState.CANCELED)) {
                    recordsToDrop.forEach(record -> put(record, false));
                    return;
                }
            }

            if(!recordsToDrop.isEmpty()) {
                dropFlowFileRequest.setDroppedSize(dropFlowFileRequest.getDroppedSize().add(dropFlowFiles(recordsToDrop, requestor)));
            }

            if(!dropFlowFileRequest.getState().equals(DropFlowFileState.CANCELED) && swapInFilesIfNecessary()) {
                dropFlowFiles(dropFlowFileRequest, requestor, false);
            }

            // We only want the top level of recursion to set this state
            if(setCompletionState) {
                dropFlowFileRequest.setState(DropFlowFileState.COMPLETE);
            }
        } catch(IOException e) {
            LOGGER.error("Exception during dropRequest {}", dropFlowFileRequest, e);
            dropFlowFileRequest.setState(DropFlowFileState.FAILURE, e.getMessage());
        } finally {
            // If this collection contains any records at this point then the operation was either cancelled
            // or there was an exception. Return the remaining records from whence they came
            currentRuleBuckets.values().stream().flatMap(Queue::stream).forEach(record -> put(record, false));
        }
    }

    private QueueSize dropFlowFiles(List<FlowFileRecord> recordsToDrop, String requestor) throws IOException {
        QueueSize droppedQueueSize = drop(recordsToDrop, requestor);
        LOGGER.debug("objectsInBuckets: {}, bytesInBuckets: {}\nobjectsDropped: {}, bytesDropped: {}",
                objectsInBuckets.get(), bytesInBuckets.get(), droppedQueueSize.getObjectCount(), droppedQueueSize.getByteCount());
        objectsInBuckets.addAndGet(-droppedQueueSize.getObjectCount());
        bytesInBuckets.addAndGet(-droppedQueueSize.getByteCount());

        return droppedQueueSize;
    }

    @Override
    public List<FlowFilePrioritizer> getPriorities() {
        return prioritizers;
    }

    @Override
    public SwapSummary recoverSwappedFlowFiles() {
        try {
            List<String> swapLocations = flowFileSwapManager.recoverSwapLocations(this, null);
            AtomicInteger queueSize = new AtomicInteger();
            AtomicLong totalQueueBytes = new AtomicLong();
            AtomicReference<Long> maxFlowFileId = new AtomicReference<>(null);
            List<ResourceClaim> resourceClaims = new ArrayList<>();
            swapLocations.forEach(swapLocation -> {
                try {
                    SwapSummary swapSummary = flowFileSwapManager.getSwapSummary(swapLocation);
                    LOGGER.info("Located {} flowfiles, {} bytes in swap location {} for queue {}",
                            swapSummary.getQueueSize().getByteCount(), swapSummary.getQueueSize().getByteCount(),
                            swapLocation, getIdentifier());
                    queueSize.addAndGet(swapSummary.getQueueSize().getObjectCount());
                    totalQueueBytes.addAndGet(swapSummary.getQueueSize().getByteCount());
                    final Long maxSwapRecordId = swapSummary.getMaxFlowFileId();
                    if(maxSwapRecordId != null) {
                        if(maxFlowFileId.get() == null || maxSwapRecordId > maxFlowFileId.get()) {
                            maxFlowFileId.set(maxSwapRecordId);
                        }
                    }
                } catch (IOException e) {
                    LOGGER.error("Error getting swap summary for location {}", swapLocation, e);
                    if(eventReporter != null) {
                        eventReporter.reportEvent(Severity.ERROR, "FlowFile Swapping",
                                "Failed to recover FlowFiles from SwapFile " + swapLocation +
                                "; the file appears to be corrupt. See logs for more details.");
                    }
                }
            });

            this.swapLocations.addAll(swapLocations);
            numSwapFlowFiles.addAndGet(queueSize.get());
            numSwapFileBytes.addAndGet(totalQueueBytes.get());
            LOGGER.info("Located {} files, {} bytes swapped for queue {}.", queueSize.get(), totalQueueBytes.get(), getIdentifier());
            return new StandardSwapSummary(new QueueSize(queueSize.get(), totalQueueBytes.get()), maxFlowFileId.get(), resourceClaims);
        } catch (IOException e) {
            LOGGER.error("Error recovering swap locations for queue {}.", getIdentifier(), e);
            if(eventReporter != null) {
                eventReporter.reportEvent(Severity.ERROR, "FlowFile Swapping",
                        "Failed to determine whether or not any Swap Files exist for FlowFile Queue " +
                        this.getIdentifier() + "; see logs for more details.");
            }
            return null;
        }
    }

    @Override
    public void purgeSwapFiles() {
        flowFileSwapManager.purge();
    }

    @Override
    public void setPriorities(List<FlowFilePrioritizer> newPriorities) {
        prioritizers = newPriorities;
        alwaysReevaluate = prioritizers.stream().anyMatch(prioritizer -> prioritizer instanceof BucketPrioritizer);
    }

    @Override
    public QueueSize size() {
        return new QueueSize(objectsInBuckets.get() + numSwapFlowFiles.get(), bytesInBuckets.get() + numSwapFlowFiles.get());
    }

    @Override
    public boolean isEmpty() {
        return objectsInBuckets.get() == 0;
    }

    @Override
    public boolean isActiveQueueEmpty() {
        return isEmpty();
    }

    @Override
    public void acknowledge(FlowFileRecord flowFileRecord) {
        numUnacknowledgedFlowFiles.decrementAndGet();
        numUnacknowledgedBytes.addAndGet(-flowFileRecord.getSize());
    }

    @Override
    public void acknowledge(Collection<FlowFileRecord> flowFileRecords) {
        numUnacknowledgedFlowFiles.addAndGet(-flowFileRecords.size());
        numUnacknowledgedBytes.addAndGet(-flowFileRecords.stream().mapToLong(FlowFile::getSize).sum());
    }

    @Override
    public boolean isUnacknowledgedFlowFile() {
        return numUnacknowledgedFlowFiles.get() > 0;
    }

    @Override
    public void put(FlowFileRecord flowFileRecord) {
        put(flowFileRecord, true);
    }

    /** Adds a FlowFileRecord to this connection's backing collection
     * @param flowFileRecord The record to add
     * @param updateMetrics Whether or not to adjust the object and byte counts of this connection using this flowFileRecord
     */
    public void put(FlowFileRecord flowFileRecord, boolean updateMetrics) {
        Rule rule = FLOW_FILE_RULE_CACHE.get().getUnchecked(flowFileRecord);

        if(alwaysReevaluate || rule.isExpired()) {
            rule = rulesManager.getRule(flowFileRecord);
        }

        FLOW_FILE_RULE_CACHE.get().put(flowFileRecord, rule);

        ruleBuckets.get().computeIfAbsent(rule, r-> new LinkedBlockingQueue<>()).add(flowFileRecord);

        if(updateMetrics) {
            objectsInBuckets.incrementAndGet();
            bytesInBuckets.addAndGet(flowFileRecord.getSize());
        }

        swapOutFilesIfNecessary();
    }

    @Override
    public void putAll(Collection<FlowFileRecord> flowFileRecords) {
        flowFileRecords.forEach(this::put);
    }

    public void putAll(Collection<FlowFileRecord> flowFileRecords, boolean updateMetrics) {
        flowFileRecords.forEach(flowFileRecord -> put(flowFileRecord, updateMetrics));
    }

    /** Will return the highest priority FlowFileRecord from the backing collection that is not expired or penalized
     * @param expiredRecords Up to 10,000 expired records that were encountered, at least in part, as part of this poll operation
     * @param penalizedFlowFiles Any amount of penalized records that were encountered, at least in part, during this poll operation.
     *                           These records MUST be returned to the collection when polling is done.
     * @param stagger Whether or not to consider giving up this thread based on if higher priority data was recently processed
     *                elsewhere. Set this to false in order to always use the available thread.
     * @return An unexpired, un-penalized FlowFileRecord or null if none was found
     */
    public FlowFileRecord poll(Set<FlowFileRecord> expiredRecords, Set<FlowFileRecord> penalizedFlowFiles, boolean stagger) {
        swapInFilesIfNecessary();

        if(objectsInBuckets.get() == 0) {
            return null;
        }

        // Rules in the RulesManager are ordered by priority
        List<Rule> rules = rulesManager.getRuleList();

        // Used to track whether or not higher priority data was recently polled so we know whether or not to reduce the odds
        // of this thread receiving a FlowFileRecord
        boolean higherPriorityDataRecentlyPolled = false;

        // Attempt to poll rules in order of priority
        for(Rule rule: rules) {
            if(ruleBuckets.get().containsKey(rule) && ruleBuckets.get().get(rule).size() > 0) {
                // If higher priority data was recently polled elsewhere, reduce the odds of this thread processing data.
                // If we've already encountered expiredRecords then we already beat the odds. Continue purging expired records
                // until we find a good one or hit the limit.
                // We do not reduce odds if we've already encountered expired or penalized records because that means we have already
                // beaten the odds to earn a thread.
                if(stagger && higherPriorityDataRecentlyPolled && expiredRecords.isEmpty() && penalizedFlowFiles.isEmpty()) {
                    LOGGER.debug("Polling while higher priority files are waiting elsewhere.");

                    if(RANDOM.nextInt(100) >= rule.getRateOfThreadUsage()) {
                        LOGGER.debug("Random int didn't allow this to process, better luck next time.");
                        return null;
                    }
                }

                Queue<FlowFileRecord> queue = ruleBuckets.get().get(rule);
                FlowFileRecord recordToReturn = queue != null ? queue.poll() : null;

                // Aggregate expired and penalized records as we encounter them
                while(recordToReturn != null && (isExpired(recordToReturn) || isPenalized(recordToReturn))) {
                    // Though we are adjusting counts for the penalized records here they will be adjusted again
                    // later when the penalized records are put back in.
                    objectsInBuckets.decrementAndGet();
                    bytesInBuckets.addAndGet(-recordToReturn.getSize());

                    // Expired takes precedence as the record will be removed from the system
                    if(isExpired(recordToReturn)) {
                        expiredRecords.add(recordToReturn);
                        if(expiredRecords.size() >= MAX_EXPIRED_RECORDS) {
                            return null;
                        }
                    } else {
                        // Whether this is part of a single poll or ap oll operation that will return multiple results we will
                        // aggregate a collection of any penalized flowfiles we encounter. These MUST be returned to their buckets when the operation
                        // is complete.
                        penalizedFlowFiles.add(recordToReturn);
                    }

                    recordToReturn = queue.poll();
                }

                if(recordToReturn != null) {
                    objectsInBuckets.decrementAndGet();
                    bytesInBuckets.addAndGet(-recordToReturn.getSize());

                    RULE_LAST_POLL_TIME.put(rule, System.currentTimeMillis());
                    numUnacknowledgedFlowFiles.incrementAndGet();
                    numUnacknowledgedBytes.addAndGet(recordToReturn.getSize());

                    return recordToReturn;
                }
            }

            // For each rule for which we do not have data, track the last time anyone polled a file of that priority. Once we know that any
            // data of higher priority was recently polled then we no longer have to do this check.
            // Do not do this for UNEVALUATED files as we do not want that to cause other files to be staggered.
            if(stagger && rule != Rule.UNEVALUATED && !higherPriorityDataRecentlyPolled && RULE_LAST_POLL_TIME.containsKey(rule)) {
                long ruleLastPollTime = RULE_LAST_POLL_TIME.get(rule);
                long currentTime = System.currentTimeMillis();
                long timeDiff = currentTime - ruleLastPollTime;

                if(timeDiff < TimeUnit.SECONDS.toMillis(5)) {
                    higherPriorityDataRecentlyPolled = true;
                }
            }
        }

        return null;
    }

    private boolean swapInFilesIfNecessary() {
        boolean swappedInFiles = false;
        List<String> failedSwapLocations = new ArrayList<>();
        // Only swap files in while we have files to swap in and we have room to do so. We don't want to create a situation where we swap in 10,000 files and then
        // immediately swap them back out.
        while(objectsInBuckets.get() < queueSwapThreshold && numSwapFlowFiles.get() > 0 && !swapLocations.isEmpty()
                && (objectsInBuckets.get() < Math.max(queueSwapThreshold/2, queueSwapThreshold - SWAP_RECORD_PUSH_SIZE))) {
            String swapLocation = swapLocations.remove();
            try {
                SwapContents swapContents = flowFileSwapManager.swapIn(swapLocation, this);
                LOGGER.info("Swapped in {} flowfiles", swapContents.getFlowFiles().size());
                putAll(swapContents.getFlowFiles());
                QueueSize swapQueueSize = swapContents.getSummary().getQueueSize();
                numSwapFlowFiles.addAndGet(-swapQueueSize.getObjectCount());
                numSwapFileBytes.addAndGet(-swapQueueSize.getByteCount());
                swappedInFiles = true;
            } catch (IOException e) {
                LOGGER.error("Error while attempting to swap in files from location {}. Will try again later as necessary.", swapLocation, e);
                failedSwapLocations.add(swapLocation);
            }
        }

        swapLocations.addAll(failedSwapLocations);

        if(failedSwapLocations.size() > 0) {
            return false;
        } else {
            return swappedInFiles;
        }
    }

    private void swapOutFilesIfNecessary() {
        LOGGER.debug("{} objects in buckets, swapping out if >= {}", objectsInBuckets.get(), (queueSwapThreshold + SWAP_RECORD_POLL_SIZE));
        // Swap until we are no longer at or above the queue swap threshold plus poll size
        while(objectsInBuckets.get() >= (queueSwapThreshold + SWAP_RECORD_POLL_SIZE)) {
            LOGGER.info("Beginning swap out of {} flowfiles.", SWAP_RECORD_POLL_SIZE);
            List<FlowFileRecord> filesToSwap = getFilesToSwap();
            try {
                swapOutFiles(filesToSwap);
                LOGGER.info("Completed swapping out {} flowfiles.", filesToSwap.size());
            } catch(IOException e) {
                LOGGER.error("Error while attempting to swap flowfiles. Returning flowfiles to buckets.", e);
                // Because these were not retrieved via the normal poll method the object and byte counts were
                // not updated. Therefore return them without updating said counts.
                putAll(filesToSwap, false);
                // We don't want to be stuck in an infinite loop. If there's an exception then bail.
                return;
            }
        }
    }

    private List<FlowFileRecord> getFilesToSwap() {
        List<FlowFileRecord> filesToSwap = new ArrayList<>();

        List<Rule> ruleList = rulesManager.getRuleList();
        ListIterator<Rule> ruleListIterator = ruleList.listIterator();
        // Start at the end of the list as we want to swap files in order of least to greatest priority
        while(ruleListIterator.hasNext()) {
            ruleListIterator.next();
        }

        Rule rule;
        while(ruleListIterator.hasPrevious() && filesToSwap.size() < SWAP_RECORD_POLL_SIZE) {
            rule = ruleListIterator.previous();
            Queue<FlowFileRecord> bucket = ruleBuckets.get().get(rule);
            if(bucket != null) {
                FlowFileRecord flowFileToSwap;

                // The order of boolean clauses matters here. We don't want to poll a file to swap but then forget about it due
                // to having already hit our SWAP_RECORD_POLL_SIZE
                while(filesToSwap.size() < SWAP_RECORD_POLL_SIZE && (flowFileToSwap = bucket.poll()) != null) {
                    filesToSwap.add(flowFileToSwap);
                }
            }
        }

        return filesToSwap;
    }

    private void swapOutFiles(List<FlowFileRecord> filesToSwap) throws IOException {
        long bytesToSwap = filesToSwap.stream().mapToLong(FlowFile::getSize).sum();
        String swapLocation = flowFileSwapManager.swapOut(filesToSwap, this, null);
        swapLocations.add(swapLocation);
        objectsInBuckets.addAndGet(-filesToSwap.size());
        numSwapFlowFiles.addAndGet(filesToSwap.size());
        bytesInBuckets.addAndGet(-bytesToSwap);
        numSwapFileBytes.addAndGet(bytesToSwap);
        // As these files are swapped back in later they will once again be considered unacknowledged.
        acknowledge(filesToSwap);
    }

    @Override
    public FlowFileRecord poll(Set<FlowFileRecord> expiredRecords) {
        Set<FlowFileRecord> penalizedFlowFiles = new HashSet<>();

        try {
            return poll(expiredRecords, penalizedFlowFiles, true);
        } finally {
            LOGGER.debug("Encountered {} penalized flowfiles during poll operation.", penalizedFlowFiles.size());
            putAll(penalizedFlowFiles);
        }
    }

    private boolean isExpired(FlowFileRecord flowFileRecord) {
        return getFlowFileExpiration(TimeUnit.MILLISECONDS) != 0 && (System.currentTimeMillis() - flowFileRecord.getEntryDate()) > getFlowFileExpiration(TimeUnit.MILLISECONDS);
    }

    private boolean isPenalized(FlowFileRecord flowFileRecord) {
        long penaltyTime = flowFileRecord.getPenaltyExpirationMillis();
        return penaltyTime > 0 && System.currentTimeMillis() <= penaltyTime;
    }

    @Override
    public List<FlowFileRecord> poll(int maxResults, Set<FlowFileRecord> expiredRecords) {
        if(maxResults == 0) {
            return Collections.emptyList();
        } else {
            Set<FlowFileRecord> penalizedFlowFiles = new HashSet<>();
            boolean stagger = true;
            List<FlowFileRecord> results = new ArrayList<>();
            FlowFileRecord lastPolled;
            while((lastPolled = poll(expiredRecords, penalizedFlowFiles, stagger)) != null && results.size() <= maxResults) {
                results.add(lastPolled);
                // We beat the odds at least once so we've earned a thread
                stagger = false;
            }

            LOGGER.debug("Encountered {} penalized flowfiles during poll operation.", penalizedFlowFiles.size());
            putAll(penalizedFlowFiles);

            return results;
        }
    }

    /** Returns a list of all unexpired, un-penalized FlowFileRecords that this collection contains that pass the provided filter.
     *
     * @param filter The filter to apply
     * @param expiredRecords Up to 10,000 expired records that were encountered, at least in past, as part of this poll operation
     * @return A list of unexpired un-penalized FlowFileRecords that pass the filter
     */
    @Override
    public List<FlowFileRecord> poll(FlowFileFilter filter, Set<FlowFileRecord> expiredRecords) {
        List<FlowFileRecord> acceptedFiles = new ArrayList<>();
        List<FlowFileRecord> rejectedFiles = new ArrayList<>();
        Set<FlowFileRecord> penalizedFlowFiles = new HashSet<>();

        boolean stagger = true;
        FlowFileRecord flowFileRecord;
        while((flowFileRecord = poll(expiredRecords, penalizedFlowFiles, stagger)) != null) {
            if(filter.filter(flowFileRecord).isAccept()) {
                acceptedFiles.add(flowFileRecord);
            } else {
                rejectedFiles.add(flowFileRecord);
            }

            // We beat the odds at least once so we've earned a thread
            stagger = false;
        }

        // Return the files that were not accepted by the filter
        putAll(rejectedFiles);

        LOGGER.debug("Encountered {} penalized flowfiles during poll operation.", penalizedFlowFiles.size());
        putAll(penalizedFlowFiles);

        return acceptedFiles;
    }

    @Override
    public FlowFileRecord getFlowFile(String flowFileUuid) throws IOException {
        if(flowFileUuid == null) {
            return null;
        } else {
            return getListableFlowFiles()
                    .stream()
                    .filter(flowFileRecord -> flowFileUuid.equals(flowFileRecord.getAttribute(CoreAttributes.UUID.key())))
                    .findAny()
                    .orElse(null);
        }
    }

    @Override
    public QueueDiagnostics getQueueDiagnostics() {
        FlowFileQueueSize flowFileQueueSize = new FlowFileQueueSize(objectsInBuckets.get(), bytesInBuckets.get(), 0, 0, 0, numUnacknowledgedFlowFiles.get(), numUnacknowledgedBytes.get());
        LocalQueuePartitionDiagnostics localQueuePartitionDiagnostics = new StandardLocalQueuePartitionDiagnostics(flowFileQueueSize, false, false);
        return new StandardQueueDiagnostics(localQueuePartitionDiagnostics, Collections.emptyList());
    }

    @Override
    public void lock() {
    }

    @Override
    public void unlock() {
    }

    @Override
    public void offloadQueue() {
    }

    @Override
    public void resetOffloadedQueue() {
    }

    @Override
    public void startLoadBalancing() {
    }

    @Override
    public void stopLoadBalancing() {
    }

    @Override
    public boolean isActivelyLoadBalancing() {
        return false;
    }
}
