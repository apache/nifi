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
package org.apache.nifi.controller.repository;

import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.BackoffMechanism;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.lifecycle.TaskTermination;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.queue.PollStrategy;
import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.controller.repository.claim.ContentClaimWriteCache;
import org.apache.nifi.controller.repository.claim.ResourceClaim;
import org.apache.nifi.controller.repository.io.ContentClaimInputStream;
import org.apache.nifi.controller.repository.io.DisableOnCloseInputStream;
import org.apache.nifi.controller.repository.io.DisableOnCloseOutputStream;
import org.apache.nifi.controller.repository.io.FlowFileAccessInputStream;
import org.apache.nifi.controller.repository.io.FlowFileAccessOutputStream;
import org.apache.nifi.controller.repository.io.LimitedInputStream;
import org.apache.nifi.controller.repository.io.TaskTerminationInputStream;
import org.apache.nifi.controller.repository.io.TaskTerminationOutputStream;
import org.apache.nifi.controller.repository.metrics.PerformanceTracker;
import org.apache.nifi.controller.repository.metrics.PerformanceTrackingInputStream;
import org.apache.nifi.controller.repository.metrics.StandardFlowFileEvent;
import org.apache.nifi.controller.state.StandardStateMap;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.FlowFileFilter;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.FlowFileAccessException;
import org.apache.nifi.processor.exception.FlowFileHandlingException;
import org.apache.nifi.processor.exception.MissingFlowFileException;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.exception.TerminatedTaskException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.provenance.InternalProvenanceReporter;
import org.apache.nifi.provenance.ProvenanceEventBuilder;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventRepository;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.provenance.ProvenanceReporter;
import org.apache.nifi.stream.io.ByteCountingInputStream;
import org.apache.nifi.stream.io.ByteCountingOutputStream;
import org.apache.nifi.stream.io.LimitingInputStream;
import org.apache.nifi.stream.io.NonFlushableOutputStream;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.FormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * <p>
 * Provides a ProcessSession that ensures all accesses, changes and transfers
 * occur in an atomic manner for all FlowFiles including their contents and
 * attributes
 * </p>
 * <p>
 * NOT THREAD SAFE
 * </p>
 * <p/>
 */
public class StandardProcessSession implements ProcessSession, ProvenanceEventEnricher {
    private static final Set<String> REQUIRED_ATTRIBUTES = Set.of(
        CoreAttributes.UUID.key(),
        CoreAttributes.FILENAME.key(),
        CoreAttributes.PATH.key()
    );

    private static final long VERSION_INCREMENT = 1;
    private static final String INITIAL_VERSION = String.valueOf(VERSION_INCREMENT);
    private static final AtomicLong idGenerator = new AtomicLong(0L);
    private static final AtomicLong enqueuedIndex = new AtomicLong(0L);
    private static final StateMap EMPTY_STATE_MAP = new StandardStateMap(Collections.emptyMap(), Optional.empty());

    // determines how many things must be transferred, removed, modified in order to avoid logging the FlowFile ID's on commit/rollback
    public static final int VERBOSE_LOG_THRESHOLD = 10;
    public static final String DEFAULT_FLOWFILE_PATH = "./";

    private static final Logger LOG = LoggerFactory.getLogger(StandardProcessSession.class);
    private static final Logger claimLog = LoggerFactory.getLogger(StandardProcessSession.class.getSimpleName() + ".claims");
    private static final int MAX_ROLLBACK_FLOWFILES_TO_LOG = 5;

    private final Map<Long, StandardRepositoryRecord> records = new ConcurrentHashMap<>();
    private final Map<String, StandardFlowFileEvent> connectionCounts = new ConcurrentHashMap<>();
    private final Map<FlowFileQueue, Set<FlowFileRecord>> unacknowledgedFlowFiles = new ConcurrentHashMap<>();
    private final Map<ContentClaim, ByteCountingOutputStream> appendableStreams = new ConcurrentHashMap<>();
    private final RepositoryContext context;
    private final TaskTermination taskTermination;
    private final Map<FlowFile, Integer> readRecursionSet = new HashMap<>(); // set used to track what is currently being operated on to prevent logic failures if recursive calls occurring
    private final Set<FlowFile> writeRecursionSet = new HashSet<>();
    private final Map<FlowFile, Path> deleteOnCommit = new HashMap<>();
    private final long sessionId;
    private final String connectableDescription;
    private final PerformanceTracker performanceTracker;

    private Map<String, Long> countersOnCommit;
    private Map<String, Long> immediateCounters;

    private final Set<String> removedFlowFiles = new HashSet<>();
    private final Set<String> createdFlowFiles = new HashSet<>(); // UUID of any FlowFile that was created in this session
    private final Set<String> createdFlowFilesWithoutLineage = new HashSet<>(); // UUID of any FlowFile that was created without a parent. This is a subset of createdFlowFiles.

    private final InternalProvenanceReporter provenanceReporter;

    private int removedCount = 0; // number of flowfiles removed in this session
    private long removedBytes = 0L; // size of all flowfiles removed in this session
    private long bytesRead = 0L;
    private long bytesWritten = 0L;
    private int flowFilesIn = 0, flowFilesOut = 0;
    private long contentSizeIn = 0L, contentSizeOut = 0L;

    private ResourceClaim currentReadClaim = null;
    private ByteCountingInputStream currentReadClaimStream = null;
    private long processingStartTime;

    // List of InputStreams that have been opened by calls to {@link #read(FlowFile)} and not yet closed
    private final Map<FlowFile, InputStream> openInputStreams = new ConcurrentHashMap<>();
    // List of OutputStreams that have been opened by calls to {@link #write(FlowFile)} and not yet closed
    private final Map<FlowFile, OutputStream> openOutputStreams = new ConcurrentHashMap<>();

    // maps a FlowFile to all Provenance Events that were generated for that FlowFile.
    // we do this so that if we generate a Fork event, for example, and then remove the event in the same
    // Session, we will not send that event to the Provenance Repository
    private final Map<FlowFile, List<ProvenanceEventRecord>> generatedProvenanceEvents = new HashMap<>();

    // when Forks are generated for a single parent, we add the Fork event to this map, with the Key being the parent
    // so that we are able to aggregate many into a single Fork Event.
    private final Map<FlowFile, ProvenanceEventBuilder> forkEventBuilders = new HashMap<>();

    private Checkpoint checkpoint = null;
    private final ContentClaimWriteCache claimCache;

    private StateMap localState;
    private StateMap clusterState;
    private final String retryAttribute;
    private final FlowFileLinkage flowFileLinkage = new FlowFileLinkage();

    public StandardProcessSession(final RepositoryContext context, final TaskTermination taskTermination, final PerformanceTracker performanceTracker) {
        this.context = context;
        this.taskTermination = taskTermination;
        this.performanceTracker = performanceTracker;

        this.provenanceReporter = context.createProvenanceReporter(this::isFlowFileKnown, this);
        this.sessionId = idGenerator.getAndIncrement();
        this.connectableDescription = context.getConnectableDescription();
        this.claimCache = context.createContentClaimWriteCache(performanceTracker);
        LOG.trace("Session {} created for {}", this, connectableDescription);
        processingStartTime = System.nanoTime();
        retryAttribute = "retryCount." + context.getConnectable().getIdentifier();
    }

    private void verifyTaskActive() {
        if (taskTermination.isTerminated()) {
            rollback(false, true);
            throw new TerminatedTaskException();
        }
    }

    protected RepositoryContext getRepositoryContext() {
        return context;
    }

    protected long getSessionId() {
        return sessionId;
    }

    private void closeStreams(final Map<FlowFile, ? extends Closeable> streamMap, final String action, final String streamType) {
        if (streamMap.isEmpty()) {
            return;
        }

        final Map<FlowFile, ? extends Closeable> openStreamCopy = new HashMap<>(streamMap); // avoid ConcurrentModificationException by creating a copy of the List
        for (final Map.Entry<FlowFile, ? extends Closeable> entry : openStreamCopy.entrySet()) {
            final FlowFile flowFile = entry.getKey();
            final Closeable openStream = entry.getValue();

            LOG.warn("{} closing {} for {} because the session was {} without the {} stream being closed.", this, openStream, flowFile, action, streamType);

            try {
                openStream.close();
            } catch (final Exception e) {
                LOG.warn("{} Attempted to close {} for {} due to session commit but close failed", this, openStream, this.connectableDescription);
                LOG.warn("", e);
            }
        }
    }

    public void checkpoint() {
        checkpoint(true);
        resetState();
    }

    private void validateCommitState() {
        verifyTaskActive();

        if (!readRecursionSet.isEmpty()) {
            throw new IllegalStateException("Cannot commit session while reading from FlowFile");
        }
        if (!writeRecursionSet.isEmpty()) {
            throw new IllegalStateException("Cannot commit session while writing to FlowFile");
        }

        for (final StandardRepositoryRecord record : records.values()) {
            if (record.isMarkedForDelete()) {
                continue;
            }

            final Relationship relationship = record.getTransferRelationship();
            if (relationship == null) {
                final String createdThisSession = record.getOriginalQueue() == null ? "was created" : "was not created";
                throw new FlowFileHandlingException(record.getCurrent() + " transfer relationship not specified. This FlowFile " + createdThisSession + " in this session and was not transferred " +
                    "to any Relationship via ProcessSession.transfer()");
            }

            final Collection<Connection> destinations = context.getConnections(relationship);
            if (destinations.isEmpty() && !context.getConnectable().isAutoTerminated(relationship)) {
                if (relationship != Relationship.SELF) {
                    throw new FlowFileHandlingException(relationship + " does not have any destinations for " + context.getConnectable());
                }
            }
        }
    }

    private void checkpoint(final boolean copyCollections) {
        try {
            validateCommitState();
        } catch (final Exception e) {
            rollback();
            throw e;
        }

        resetWriteClaims(false);

        closeStreams(openInputStreams, "committed", "input");
        closeStreams(openOutputStreams, "committed", "output");

        if (this.checkpoint == null) {
            this.checkpoint = new Checkpoint();
        }

        if (records.isEmpty() && (countersOnCommit == null || countersOnCommit.isEmpty())) {
            LOG.trace("{} checkpointed, but no events were performed by this ProcessSession", this);
            checkpoint.checkpoint(this, Collections.emptyList(), copyCollections);
            return;
        }

        // any drop event that is the result of an auto-terminate should happen at the very end, so we keep the
        // records in a separate List so that they can be persisted to the Provenance Repo after all of the
        // Processor-reported events.
        List<ProvenanceEventRecord> autoTerminatedEvents = null;

        // validate that all records have a transfer relationship for them and if so determine the destination node and clone as necessary
        final Map<Long, StandardRepositoryRecord> toAdd = new HashMap<>();

        final Connectable connectable = context.getConnectable();
        final long maxBackoffMillis = Math.round(FormatUtils.getPreciseTimeDuration(connectable.getMaxBackoffPeriod(), TimeUnit.MILLISECONDS));

        // Determine which FlowFiles need to be retried
        final Set<Long> retryIds = new HashSet<>();
        for (final StandardRepositoryRecord record : records.values()) {
            if (isRetry(record)) {
                final long flowFileId = record.getCurrent().getId();
                retryIds.add(flowFileId);

                final Collection<Long> linkedIds = flowFileLinkage.getLinkedIds(flowFileId);
                retryIds.addAll(linkedIds);
            }
        }

        for (final StandardRepositoryRecord record : records.values()) {
            // Check if this Record should be retried. If so, perform the necessary actions to retry the Record and then continue on to the next record.
            if (retryIds.contains(record.getCurrent().getId())) {
                retry(record, maxBackoffMillis);
            }

            if (record.isMarkedForDelete()) {
                continue;
            }

            final Relationship relationship = record.getTransferRelationship();
            final List<Connection> destinations = new ArrayList<>(context.getConnections(relationship));

            if (destinations.isEmpty() && relationship == Relationship.SELF) {
                record.setDestination(record.getOriginalQueue());
            } else if (destinations.isEmpty()) {
                record.markForDelete();

                if (autoTerminatedEvents == null) {
                    autoTerminatedEvents = new ArrayList<>();
                }

                final ProvenanceEventRecord dropEvent;
                try {
                    dropEvent = provenanceReporter.generateDropEvent(record.getCurrent(), "Auto-Terminated by " + relationship.getName() + " Relationship");
                    autoTerminatedEvents.add(dropEvent);
                } catch (final Exception e) {
                    LOG.warn("Unable to generate Provenance Event for {} on behalf of {}", record.getCurrent(), connectableDescription, e);
                }
            } else {
                FlowFileRecord currRec = record.getCurrent();

                // If there's a retry attribute present, remove it. The attribute should only live while the FlowFile is being processed by the current component
                if (currRec.getAttribute(retryAttribute) != null) {
                    currRec = new StandardFlowFileRecord.Builder().fromFlowFile(currRec).removeAttributes(retryAttribute).build();
                    record.setWorking(currRec, retryAttribute, null, false);
                }

                final Connection finalDestination = destinations.remove(destinations.size() - 1); // remove last element
                record.setDestination(finalDestination.getFlowFileQueue());
                incrementConnectionInputCounts(finalDestination, record);

                for (final Connection destination : destinations) { // iterate over remaining destinations and "clone" as needed
                    incrementConnectionInputCounts(destination, record);

                    final StandardFlowFileRecord.Builder builder = new StandardFlowFileRecord.Builder().fromFlowFile(currRec);
                    builder.id(context.getNextFlowFileSequence());

                    final String newUuid = UUID.randomUUID().toString();
                    builder.addAttribute(CoreAttributes.UUID.key(), newUuid);

                    final FlowFileRecord clone = builder.build();
                    final StandardRepositoryRecord newRecord = new StandardRepositoryRecord(destination.getFlowFileQueue());
                    provenanceReporter.clone(currRec, clone, false);

                    final ContentClaim claim = clone.getContentClaim();
                    if (claim != null) {
                        context.getContentRepository().incrementClaimaintCount(claim);
                    }
                    newRecord.setWorking(clone, Collections.emptyMap(), false);

                    newRecord.setDestination(destination.getFlowFileQueue());
                    newRecord.setTransferRelationship(record.getTransferRelationship());
                    // put the mapping into toAdd because adding to records now will cause a ConcurrentModificationException
                    toAdd.put(clone.getId(), newRecord);

                    createdFlowFiles.add(newUuid);
                }
            }
        }

        records.putAll(toAdd);
        toAdd.clear();

        checkpoint.checkpoint(this, autoTerminatedEvents, copyCollections);
    }

    private boolean isRetry(final StandardRepositoryRecord record) {
        final Relationship relationship = record.getTransferRelationship();
        if (relationship == null) {
            return false;
        }

        final Connectable connectable = context.getConnectable();
        if (!connectable.isRelationshipRetried(relationship)) {
            return false;
        }

        // If the FlowFile was created in this session and has no lineage (i.e., it was created by a source component),
        // we do not want to retry the FlowFile, as there is no way to roll back.
        final String uuid = record.getCurrent().getAttribute(CoreAttributes.UUID.key());
        if (createdFlowFilesWithoutLineage.contains(uuid)) {
            return false;
        }

        final int retryCount = getRetries(record.getCurrent());
        return retryCount < connectable.getRetryCount();
    }

    private void retry(final StandardRepositoryRecord record, final long maxBackoffMillis) {
        LOG.debug("Updating state to retry {}", record.getCurrent());

        final Connectable connectable = context.getConnectable();
        final int currentRetries = getRetries(record.getCurrent());

        // Account for any statistics that have been added to for FlowFiles/Bytes In/Out
        final Relationship relationship = record.getTransferRelationship();
        if (relationship != null) {
            final int numDestinations = context.getConnections(relationship).size();
            final int multiplier = Math.max(1, numDestinations);
            final boolean autoTerminated = connectable.isAutoTerminated(relationship);
            if (!autoTerminated) {
                flowFilesOut -= multiplier;
                contentSizeOut -= record.getCurrent().getSize() * multiplier;
            }
        }

        final FlowFileRecord original = record.getOriginal();
        if (original != null) {
            flowFilesIn--;
            contentSizeIn -= original.getSize();
        }

        // If any content has been created but is no longer being used (i.e., the FlowFile was written to but is now being reverted back to its
        // previous content), then remove the temporary content.
        removeTemporaryClaim(record);

        // Adjust for any state that has been updated for the Record that is no longer relevant.
        final String uuid = record.getCurrent().getAttribute(CoreAttributes.UUID.key());
        final FlowFileRecord updatedFlowFile = new StandardFlowFileRecord.Builder()
            .fromFlowFile(record.getOriginal())
            .addAttribute(retryAttribute, String.valueOf(currentRetries + 1))
            .build();

        if (original == null) {
            record.markForDelete();
        } else {
            record.setTransferRelationship(Relationship.SELF);
        }

        record.setWorking(updatedFlowFile, false);

        // Remove any Provenance Events that have been generated for this Record
        provenanceReporter.removeEventsForFlowFile(uuid);
        forkEventBuilders.remove(record.getCurrent());
        createdFlowFiles.remove(uuid);
        createdFlowFilesWithoutLineage.remove(uuid);
        removedFlowFiles.remove(uuid);

        // Penalize the FlowFile or yield the connectable, according to the component configuration.
        final BackoffMechanism backoffMechanism = connectable.getBackoffMechanism();
        if (backoffMechanism == BackoffMechanism.PENALIZE_FLOWFILE) {
            if (!record.isMarkedForDelete()) {
                final long backoffTime = calculateBackoffTime(currentRetries, maxBackoffMillis, connectable.getPenalizationPeriod(TimeUnit.MILLISECONDS));
                penalize(record.getCurrent(), backoffTime, TimeUnit.MILLISECONDS);
            }
        } else {
            final long backoffTime = calculateBackoffTime(currentRetries, maxBackoffMillis, connectable.getYieldPeriod(TimeUnit.MILLISECONDS));
            connectable.yield(backoffTime, TimeUnit.MILLISECONDS);
        }
    }

    private int getRetries(final FlowFile flowFile) {
        if (flowFile == null) {
            return 0;
        }

        final String attributeValue = flowFile.getAttribute(retryAttribute);
        if (attributeValue == null) {
            return 0;
        }

        try {
            return Integer.parseInt(attributeValue);
        } catch (final Exception e) {
            return 0;
        }
    }

    private long calculateBackoffTime(final int retryCount, final long maxBackoffPeriod, final long baseBackoffTime) {
        return (long) Math.min(maxBackoffPeriod, Math.pow(2, retryCount) * baseBackoffTime);
    }

    @Override
    public synchronized void commit() {
        commit(false);
    }

    @Override
    public void commitAsync() {
        try {
            commit(true);
        } catch (final Throwable t) {
            try {
                rollback();
            } catch (final Throwable t2) {
                t.addSuppressed(t2);
                LOG.error("Failed to roll back session {} for {}", this, connectableDescription, t2);
            }

            throw t;
        }
    }

    @Override
    public void commitAsync(final Runnable onSuccess, final Consumer<Throwable> onFailure) {
        try {
            commit(true);
        } catch (final Throwable t) {
            LOG.error("Failed to asynchronously commit session {} for {}", this, connectableDescription, t);

            try {
                rollback();
            } catch (final Throwable t2) {
                LOG.error("Failed to roll back session {} for {}", this, connectableDescription, t2);
            }

            if (onFailure != null) {
                onFailure.accept(t);
            }

            throw t;
        }

        if (onSuccess != null) {
            try {
                onSuccess.run();
            } catch (final Exception e) {
                LOG.error("Successfully committed session {} for {} but failed to trigger success callback", this, connectableDescription, e);
            }
        }

        LOG.debug("Successfully committed session {} for {}", this, connectableDescription);
    }

    private synchronized void commit(final boolean asynchronous) {
        checkpoint(this.checkpoint != null); // If a checkpoint already exists, we need to copy the collection
        commit(this.checkpoint, asynchronous);
        this.checkpoint = null;
    }

    /**
     * Commits the given checkpoint, updating repositories as necessary, and performing any necessary cleanup of resources, etc.
     * Subclasses may choose to perform these tasks asynchronously if the asynchronous flag indicates that it is acceptable to do so.
     * However, this implementation will perform the commit synchronously, regardless of the {@code asynchronous} flag.
     *
     * @param checkpoint the session checkpoint to commit
     * @param asynchronous whether or not the commit is allowed to be performed asynchronously.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    protected void commit(final Checkpoint checkpoint, final boolean asynchronous) {
        try {
            performanceTracker.beginSessionCommit();
            final long commitStartNanos = System.nanoTime();

            resetReadClaim();
            try {
                claimCache.flush();
            } finally {
                claimCache.reset();
            }

            final long updateProvenanceStart = System.nanoTime();
            updateProvenanceRepo(checkpoint);

            final long flowFileRepoUpdateStart = System.nanoTime();
            final long updateProvenanceNanos = flowFileRepoUpdateStart - updateProvenanceStart;

            // Update the FlowFile Repository
            try {
                final Collection<StandardRepositoryRecord> repoRecords = checkpoint.records.values();
                context.getFlowFileRepository().updateRepository((Collection) repoRecords);
            } catch (final IOException ioe) {
                // if we fail to commit the session, we need to roll back
                // the checkpoints as well because none of the checkpoints
                // were ever committed.
                rollback(false, true);
                throw new ProcessException("FlowFile Repository failed to update", ioe);
            }

            final long flowFileRepoUpdateFinishNanos = System.nanoTime();
            final long flowFileRepoUpdateNanos = flowFileRepoUpdateFinishNanos - flowFileRepoUpdateStart;

            if (LOG.isDebugEnabled()) {
                for (final RepositoryRecord record : checkpoint.records.values()) {
                    if (record.isMarkedForAbort()) {
                        final FlowFileRecord flowFile = record.getCurrent();
                        final long flowFileLife = System.currentTimeMillis() - flowFile.getEntryDate();
                        final Connectable connectable = context.getConnectable();
                        final Object terminator = connectable instanceof ProcessorNode ? ((ProcessorNode) connectable).getProcessor() : connectable;
                        LOG.debug("{} terminated by {}; life of FlowFile = {} ms", flowFile, terminator, flowFileLife);
                    }
                }
            }

            updateEventRepository(checkpoint);

            final long updateEventRepositoryFinishNanos = System.nanoTime();
            final long updateEventRepositoryNanos = updateEventRepositoryFinishNanos - flowFileRepoUpdateFinishNanos;

            // transfer the flowfiles to the connections' queues.
            final Map<FlowFileQueue, Collection<FlowFileRecord>> recordMap = new HashMap<>();
            for (final StandardRepositoryRecord record : checkpoint.records.values()) {
                if (record.isMarkedForAbort() || record.isMarkedForDelete()) {
                    continue; // these don't need to be transferred
                }
                // record.getCurrent() will return null if this record was created in this session --
                // in this case, we just ignore it, and it will be cleaned up by clearing the records map.
                if (record.getCurrent() != null) {
                    Collection<FlowFileRecord> collection = recordMap.get(record.getDestination());
                    if (collection == null) {
                        collection = new ArrayList<>();
                        recordMap.put(record.getDestination(), collection);
                    }
                    collection.add(record.getCurrent());
                }
            }

            for (final Map.Entry<FlowFileQueue, Collection<FlowFileRecord>> entry : recordMap.entrySet()) {
                entry.getKey().putAll(entry.getValue());
            }

            final long enqueueFlowFileFinishNanos = System.nanoTime();
            final long enqueueFlowFileNanos = enqueueFlowFileFinishNanos - updateEventRepositoryFinishNanos;

            // Delete any files from disk that need to be removed.
            for (final Path path : checkpoint.deleteOnCommit.values()) {
                try {
                    Files.deleteIfExists(path);
                } catch (final IOException e) {
                    throw new FlowFileAccessException("Unable to delete " + path.toFile().getAbsolutePath(), e);
                }
            }
            checkpoint.deleteOnCommit.clear();

            if (LOG.isDebugEnabled()) {
                final String sessionSummary = summarizeEvents(checkpoint);
                if (!sessionSummary.isEmpty()) {
                    LOG.debug("{} for {}, committed the following events: {}", this, connectableDescription, sessionSummary);
                }
            }

            for (final Map.Entry<String, Long> entry : checkpoint.countersOnCommit.entrySet()) {
                context.adjustCounter(entry.getKey(), entry.getValue());
            }

            if (LOG.isDebugEnabled()) {
                final StringBuilder timingInfo = new StringBuilder();
                timingInfo.append("Session commit for ").append(this).append(" [").append(connectableDescription).append("]").append(" took ");

                final long commitNanos = System.nanoTime() - commitStartNanos;
                formatNanos(commitNanos, timingInfo);
                timingInfo.append("; FlowFile Repository Update took ");
                formatNanos(flowFileRepoUpdateNanos, timingInfo);
                timingInfo.append("; FlowFile Event Update took ");
                formatNanos(updateEventRepositoryNanos, timingInfo);
                timingInfo.append("; Enqueuing FlowFiles took ");
                formatNanos(enqueueFlowFileNanos, timingInfo);
                timingInfo.append("; Updating Provenance Event Repository took ");
                formatNanos(updateProvenanceNanos, timingInfo);

                LOG.debug("{}", timingInfo);
            }

            // Update local state
            final StateManager stateManager = context.getStateManager();
            if (checkpoint.localState != null) {
                try {
                    final StateMap stateMap = stateManager.getState(Scope.LOCAL);
                    final Optional<String> stateVersion = stateMap.getStateVersion();
                    if (!stateVersion.equals(checkpoint.localState.getStateVersion())) {
                        LOG.debug("Updating State Manager's Local State");
                        stateManager.setState(checkpoint.localState.toMap(), Scope.LOCAL);
                    } else {
                        LOG.debug("Will not update State Manager's Local State because the State Manager reports the latest version as {}, which is newer than the session's known version of {}.",
                                stateVersion, checkpoint.localState.getStateVersion());
                    }
                } catch (final Exception e) {
                    LOG.warn("Failed to update Local State for {}. If NiFi is restarted before the state is able to be updated, it could result in data duplication.", connectableDescription, e);
                }
            }

            // Update cluster state
            if (checkpoint.clusterState != null) {
                try {
                    final StateMap stateMap = stateManager.getState(Scope.CLUSTER);
                    final Optional<String> stateVersion = stateMap.getStateVersion();
                    if (!stateVersion.equals(checkpoint.clusterState.getStateVersion())) {
                        LOG.debug("Updating State Manager's Cluster State");
                        stateManager.setState(checkpoint.clusterState.toMap(), Scope.CLUSTER);
                    } else {
                        LOG.debug("Will not update State Manager's Cluster State because the State Manager reports the latest version as {}, which is newer than the session's known version of {}.",
                                stateVersion, checkpoint.clusterState.getStateVersion());
                    }
                } catch (final Exception e) {
                    LOG.warn("Failed to update Cluster State for {}. If NiFi is restarted before the state is able to be updated, it could result in data duplication.", connectableDescription, e);
                }
            }

            // Acknowledge records in order to update counts for incoming connections' queues
            acknowledgeRecords();

            // Reset the internal state, now that the session has been committed
            resetState();
        } catch (final Exception e) {
            LOG.error("Failed to commit session {}. Will roll back.", this, e);

            try {
                // if we fail to commit the session, we need to roll back
                // the checkpoints as well because none of the checkpoints
                // were ever committed.
                rollback(false, true);
            } catch (final Exception e1) {
                e.addSuppressed(e1);
            }

            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            } else {
                throw new ProcessException(e);
            }
        } finally {
            performanceTracker.endSessionCommit();
        }
    }


    private void updateEventRepository(final Checkpoint checkpoint) {
        try {
            // update event repository
            final Connectable connectable = context.getConnectable();
            final StandardFlowFileEvent flowFileEvent = new StandardFlowFileEvent();
            flowFileEvent.setBytesRead(checkpoint.bytesRead);
            flowFileEvent.setBytesWritten(checkpoint.bytesWritten);
            flowFileEvent.setContentSizeIn(checkpoint.contentSizeIn);
            flowFileEvent.setContentSizeOut(checkpoint.contentSizeOut);
            flowFileEvent.setContentSizeRemoved(checkpoint.removedBytes);
            flowFileEvent.setFlowFilesIn(checkpoint.flowFilesIn);
            flowFileEvent.setFlowFilesOut(checkpoint.flowFilesOut);
            flowFileEvent.setFlowFilesRemoved(checkpoint.removedCount);
            flowFileEvent.setFlowFilesReceived(checkpoint.flowFilesReceived);
            flowFileEvent.setBytesReceived(checkpoint.bytesReceived);
            flowFileEvent.setFlowFilesSent(checkpoint.flowFilesSent);
            flowFileEvent.setBytesSent(checkpoint.bytesSent);

            final long now = System.currentTimeMillis();
            long lineageMillis = 0L;
            for (final StandardRepositoryRecord record : checkpoint.records.values()) {
                final FlowFile flowFile = record.getCurrent();
                final long lineageDuration = now - flowFile.getLineageStartDate();
                lineageMillis += lineageDuration;
            }
            flowFileEvent.setAggregateLineageMillis(lineageMillis);

            final Map<String, Long> counters = combineCounters(checkpoint.countersOnCommit, checkpoint.immediateCounters);
            flowFileEvent.setCounters(counters);

            context.getFlowFileEventRepository().updateRepository(flowFileEvent, connectable.getIdentifier());

            for (final Map.Entry<String, StandardFlowFileEvent> entry : checkpoint.connectionCounts.entrySet()) {
                context.getFlowFileEventRepository().updateRepository(entry.getValue(), entry.getKey());
            }
        } catch (final IOException ioe) {
            LOG.error("FlowFile Event Repository failed to update", ioe);
        }
    }

    private Map<String, Long> combineCounters(final Map<String, Long> first, final Map<String, Long> second) {
        final boolean firstEmpty = first == null || first.isEmpty();
        final boolean secondEmpty = second == null || second.isEmpty();

        if (firstEmpty && secondEmpty) {
            return null;
        }
        if (firstEmpty) {
            return second;
        }
        if (secondEmpty) {
            return first;
        }

        final Map<String, Long> combined = new HashMap<>();
        combined.putAll(first);
        second.forEach((key, value) -> combined.merge(key, value, Long::sum));
        return combined;
    }

    private void addEventType(final Map<String, BitSet> map, final String id, final ProvenanceEventType eventType) {
        final BitSet eventTypes = map.computeIfAbsent(id, key -> new BitSet());
        eventTypes.set(eventType.ordinal());
    }

    private StandardRepositoryRecord getRecord(final FlowFile flowFile) {
        return records.get(flowFile.getId());
    }

    protected void updateProvenanceRepo(final Checkpoint checkpoint) {
        // Update Provenance Repository
        final ProvenanceEventRepository provenanceRepo = context.getProvenanceRepository();

        // We need to de-dupe the events that we've created and those reported to the provenance reporter,
        // in case the Processor developer submitted the same events to the reporter. So we use a LinkedHashSet
        // for this, so that we are able to ensure that the events are submitted in the proper order.
        final Set<ProvenanceEventRecord> recordsToSubmit = new LinkedHashSet<>();
        final Map<String, BitSet> eventTypesPerFlowFileId = new HashMap<>();

        final Set<ProvenanceEventRecord> processorGenerated = checkpoint.reportedEvents;

        // We first want to submit FORK events because if the Processor is going to create events against
        // a FlowFile, that FlowFile needs to be shown to be created first.
        // However, if the Processor has generated a FORK event, we don't want to use the Framework-created one --
        // we prefer to use the event generated by the Processor. We can determine this by checking if the Set of events genereated
        // by the Processor contains any of the FORK events that we generated
        for (final Map.Entry<FlowFile, ProvenanceEventBuilder> entry : checkpoint.forkEventBuilders.entrySet()) {
            final ProvenanceEventBuilder builder = entry.getValue();
            final FlowFile flowFile = entry.getKey();

            updateEventContentClaims(builder, flowFile, checkpoint.getRecord(flowFile));
            final ProvenanceEventRecord event = builder.build();

            if (!event.getChildUuids().isEmpty() && !isSpuriousForkEvent(event, checkpoint.removedFlowFiles)) {
                // If framework generated the event, add it to the 'recordsToSubmit' Set.
                if (!processorGenerated.contains(event)) {
                    recordsToSubmit.add(event);
                }

                // Register the FORK event for each child and each parent.
                for (final String childUuid : event.getChildUuids()) {
                    addEventType(eventTypesPerFlowFileId, childUuid, event.getEventType());
                }
                for (final String parentUuid : event.getParentUuids()) {
                    addEventType(eventTypesPerFlowFileId, parentUuid, event.getEventType());
                }
            }
        }

        // Next, process any JOIN events because we need to ensure that the JOINed FlowFile is created before any processor-emitted events occur.
        for (final Map.Entry<FlowFile, List<ProvenanceEventRecord>> entry : checkpoint.generatedProvenanceEvents.entrySet()) {
            for (final ProvenanceEventRecord event : entry.getValue()) {
                final ProvenanceEventType eventType = event.getEventType();
                if (eventType == ProvenanceEventType.JOIN) {
                    recordsToSubmit.add(event);
                    addEventType(eventTypesPerFlowFileId, event.getFlowFileUuid(), event.getEventType());
                }
            }
        }

        // Now add any Processor-reported events.
        for (final ProvenanceEventRecord event : processorGenerated) {
            if (isSpuriousForkEvent(event, checkpoint.removedFlowFiles)) {
                continue;
            }

            // Check if the event indicates that the FlowFile was routed to the same
            // connection from which it was pulled (and only this connection). If so, discard the event.
            if (isSpuriousRouteEvent(event, checkpoint.records)) {
                continue;
            }

            recordsToSubmit.add(event);
            addEventType(eventTypesPerFlowFileId, event.getFlowFileUuid(), event.getEventType());

            final List<String> childUuids = event.getChildUuids();
            if (childUuids != null) {
                for (final String childUuid : childUuids) {
                    addEventType(eventTypesPerFlowFileId, childUuid, event.getEventType());
                }
            }
        }

        // Finally, add any other events that we may have generated.
        for (final List<ProvenanceEventRecord> eventList : checkpoint.generatedProvenanceEvents.values()) {
            for (final ProvenanceEventRecord event : eventList) {
                if (event.getEventType() == ProvenanceEventType.JOIN) {
                    continue; // JOIN events are handled above.
                }

                if (isSpuriousForkEvent(event, checkpoint.removedFlowFiles)) {
                    continue;
                }

                recordsToSubmit.add(event);
                addEventType(eventTypesPerFlowFileId, event.getFlowFileUuid(), event.getEventType());
            }
        }

        // Check if content or attributes changed. If so, register the appropriate events.
        for (final StandardRepositoryRecord repoRecord : checkpoint.records.values()) {
            final ContentClaim original = repoRecord.getOriginalClaim();
            final ContentClaim current = repoRecord.getCurrentClaim();

            final boolean contentChanged = !Objects.equals(original, current);
            final FlowFileRecord curFlowFile = repoRecord.getCurrent();
            final String flowFileId = curFlowFile.getAttribute(CoreAttributes.UUID.key());
            boolean eventAdded = false;

            if (checkpoint.removedFlowFiles.contains(flowFileId)) {
                continue;
            }

            final boolean newFlowFile = repoRecord.getOriginal() == null;
            if (contentChanged && !newFlowFile) {
                recordsToSubmit.add(provenanceReporter.build(curFlowFile, ProvenanceEventType.CONTENT_MODIFIED).build());
                addEventType(eventTypesPerFlowFileId, flowFileId, ProvenanceEventType.CONTENT_MODIFIED);
                eventAdded = true;
            }

            if (checkpoint.createdFlowFiles.contains(flowFileId)) {
                final BitSet registeredTypes = eventTypesPerFlowFileId.get(flowFileId);
                boolean creationEventRegistered = false;
                if (registeredTypes != null) {
                    if (registeredTypes.get(ProvenanceEventType.CREATE.ordinal())
                        || registeredTypes.get(ProvenanceEventType.FORK.ordinal())
                        || registeredTypes.get(ProvenanceEventType.CLONE.ordinal())
                        || registeredTypes.get(ProvenanceEventType.JOIN.ordinal())
                        || registeredTypes.get(ProvenanceEventType.RECEIVE.ordinal())
                        || registeredTypes.get(ProvenanceEventType.FETCH.ordinal())) {

                        creationEventRegistered = true;
                    }
                }

                if (!creationEventRegistered) {
                    recordsToSubmit.add(provenanceReporter.build(curFlowFile, ProvenanceEventType.CREATE).build());
                    eventAdded = true;
                }
            }

            if (!eventAdded && !repoRecord.getUpdatedAttributes().isEmpty() && curFlowFile.getAttribute(retryAttribute) == null) {
                // We generate an ATTRIBUTES_MODIFIED event only if no other event has been
                // created for the FlowFile. We do this because all events contain both the
                // newest and the original attributes, so generating an ATTRIBUTES_MODIFIED
                // event is redundant if another already exists.
                // We don't generate ATTRIBUTES_MODIFIED event for retry.
                if (!eventTypesPerFlowFileId.containsKey(flowFileId)) {
                    recordsToSubmit.add(provenanceReporter.build(curFlowFile, ProvenanceEventType.ATTRIBUTES_MODIFIED).build());
                    addEventType(eventTypesPerFlowFileId, flowFileId, ProvenanceEventType.ATTRIBUTES_MODIFIED);
                }
            }
        }

        // We want to submit the 'recordsToSubmit' collection, followed by the auto-terminated events to the Provenance Repository.
        // We want to do this with a single call to ProvenanceEventRepository#registerEvents because it may be much more efficient
        // to do so.
        // However, we want to modify the events in 'recordsToSubmit' to obtain the data from the most recent version of the FlowFiles
        // (except for SEND events); see note below as to why this is
        // Therefore, we create an Iterable that can iterate over each of these events, modifying them as needed, and returning them
        // in the appropriate order. This prevents an unnecessary step of creating an intermediate List and adding all of those values
        // to the List.
        // This is done in a similar veign to how Java 8's streams work, iterating over the events and returning a processed version
        // one-at-a-time as opposed to iterating over the entire Collection and putting the results in another Collection. However,
        // we don't want to change the Framework to require Java 8 at this time, because it's not yet as prevalent as we would desire
        final Map<String, FlowFileRecord> flowFileRecordMap = new HashMap<>();
        for (final StandardRepositoryRecord repoRecord : checkpoint.records.values()) {
            final FlowFileRecord flowFile = repoRecord.getCurrent();
            flowFileRecordMap.put(flowFile.getAttribute(CoreAttributes.UUID.key()), flowFile);
        }

        final long commitNanos = System.nanoTime();
        final List<ProvenanceEventRecord> autoTermEvents = checkpoint.autoTerminatedEvents;
        final Iterable<ProvenanceEventRecord> iterable = new Iterable<>() {
            final Iterator<ProvenanceEventRecord> recordsToSubmitIterator = recordsToSubmit.iterator();
            final Iterator<ProvenanceEventRecord> autoTermIterator = autoTermEvents == null ? null : autoTermEvents.iterator();

            @Override
            public Iterator<ProvenanceEventRecord> iterator() {
                return new Iterator<>() {
                    @Override
                    public boolean hasNext() {
                        return recordsToSubmitIterator.hasNext() || autoTermIterator != null && autoTermIterator.hasNext();
                    }

                    @Override
                    public ProvenanceEventRecord next() {
                        if (recordsToSubmitIterator.hasNext()) {
                            final ProvenanceEventRecord rawEvent = recordsToSubmitIterator.next();

                            // Update the Provenance Event Record with all of the info that we know about the event.
                            // For SEND events, we do not want to update the FlowFile info on the Event, because the event should
                            // reflect the FlowFile as it was sent to the remote system. However, for other events, we want to use
                            // the representation of the FlowFile as it is committed, as this is the only way in which it really
                            // exists in our system -- all other representations are volatile representations that have not been
                            // exposed.
                            final boolean isUpdateAttributesAndContent = rawEvent.getEventType() != ProvenanceEventType.SEND && rawEvent.getEventType() != ProvenanceEventType.UPLOAD
                                    && rawEvent.getEventType() != ProvenanceEventType.CLONE;
                            return enrich(rawEvent, flowFileRecordMap, checkpoint.records, isUpdateAttributesAndContent, commitNanos);
                        } else if (autoTermIterator != null && autoTermIterator.hasNext()) {
                            return enrich(autoTermIterator.next(), flowFileRecordMap, checkpoint.records, true, commitNanos);
                        }

                        throw new NoSuchElementException();
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };

        provenanceRepo.registerEvents(iterable);
    }


    private void updateEventContentClaims(final ProvenanceEventBuilder builder, final FlowFile flowFile, final StandardRepositoryRecord repoRecord) {
        final ContentClaim originalClaim = repoRecord.getOriginalClaim();
        if (originalClaim == null) {
            builder.setCurrentContentClaim(null, null, null, null, 0L);
        } else {
            final ResourceClaim resourceClaim = originalClaim.getResourceClaim();
            builder.setCurrentContentClaim(
                resourceClaim.getContainer(), resourceClaim.getSection(), resourceClaim.getId(),
                repoRecord.getOriginal().getContentClaimOffset() + originalClaim.getOffset(), repoRecord.getOriginal().getSize());
            builder.setPreviousContentClaim(
                resourceClaim.getContainer(), resourceClaim.getSection(), resourceClaim.getId(),
                repoRecord.getOriginal().getContentClaimOffset() + originalClaim.getOffset(), repoRecord.getOriginal().getSize());
        }
    }

    @Override
    public ProvenanceEventRecord enrich(final ProvenanceEventRecord rawEvent, final FlowFile flowFile, final long commitNanos) {
        verifyTaskActive();

        final StandardRepositoryRecord repoRecord = getRecord(flowFile);
        if (repoRecord == null) {
            throw new FlowFileHandlingException(flowFile + " is not known in this session (" + toString() + ")");
        }

        final ProvenanceEventBuilder recordBuilder = context.createProvenanceEventBuilder().fromEvent(rawEvent);
        if (repoRecord.getCurrent() != null && repoRecord.getCurrentClaim() != null) {
            final ContentClaim currentClaim = repoRecord.getCurrentClaim();
            final long currentOffset = repoRecord.getCurrentClaimOffset();
            final long size = flowFile.getSize();

            final ResourceClaim resourceClaim = currentClaim.getResourceClaim();
            recordBuilder.setCurrentContentClaim(resourceClaim.getContainer(), resourceClaim.getSection(), resourceClaim.getId(), currentOffset + currentClaim.getOffset(), size);
        }

        if (repoRecord.getOriginal() != null && repoRecord.getOriginalClaim() != null) {
            final ContentClaim originalClaim = repoRecord.getOriginalClaim();
            final long originalOffset = repoRecord.getOriginal().getContentClaimOffset();
            final long originalSize = repoRecord.getOriginal().getSize();

            final ResourceClaim resourceClaim = originalClaim.getResourceClaim();
            recordBuilder.setPreviousContentClaim(resourceClaim.getContainer(), resourceClaim.getSection(), resourceClaim.getId(), originalOffset + originalClaim.getOffset(), originalSize);
        }

        final FlowFileQueue originalQueue = repoRecord.getOriginalQueue();
        if (originalQueue != null) {
            recordBuilder.setSourceQueueIdentifier(originalQueue.getIdentifier());
        }

        recordBuilder.setAttributes(repoRecord.getOriginalAttributes(), repoRecord.getUpdatedAttributes());
        if (rawEvent.getEventDuration() < 0) {
            recordBuilder.setEventDuration(TimeUnit.NANOSECONDS.toMillis(commitNanos - repoRecord.getStartNanos()));
        }
        return recordBuilder.build();
    }

    private ProvenanceEventRecord enrich(
        final ProvenanceEventRecord rawEvent, final Map<String, FlowFileRecord> flowFileRecordMap, final Map<Long, StandardRepositoryRecord> records,
        final boolean updateAttributesAndContent, final long commitNanos) {
        final ProvenanceEventBuilder recordBuilder = context.createProvenanceEventBuilder().fromEvent(rawEvent);
        final FlowFileRecord eventFlowFile = flowFileRecordMap.get(rawEvent.getFlowFileUuid());
        if (eventFlowFile != null) {
            final StandardRepositoryRecord repoRecord = records.get(eventFlowFile.getId());

            if (updateAttributesAndContent && repoRecord.getCurrent() != null && repoRecord.getCurrentClaim() != null) {
                final ContentClaim currentClaim = repoRecord.getCurrentClaim();
                final long currentOffset = repoRecord.getCurrentClaimOffset();
                final long size = eventFlowFile.getSize();

                final ResourceClaim resourceClaim = currentClaim.getResourceClaim();
                recordBuilder.setCurrentContentClaim(resourceClaim.getContainer(), resourceClaim.getSection(), resourceClaim.getId(), currentOffset + currentClaim.getOffset(), size);
            }

            if (updateAttributesAndContent && repoRecord.getOriginal() != null && repoRecord.getOriginalClaim() != null) {
                final ContentClaim originalClaim = repoRecord.getOriginalClaim();
                final long originalOffset = repoRecord.getOriginal().getContentClaimOffset();
                final long originalSize = repoRecord.getOriginal().getSize();

                final ResourceClaim resourceClaim = originalClaim.getResourceClaim();
                recordBuilder.setPreviousContentClaim(resourceClaim.getContainer(), resourceClaim.getSection(), resourceClaim.getId(), originalOffset + originalClaim.getOffset(), originalSize);
            }

            final FlowFileQueue originalQueue = repoRecord.getOriginalQueue();
            if (originalQueue != null) {
                recordBuilder.setSourceQueueIdentifier(originalQueue.getIdentifier());
            }

            if (updateAttributesAndContent) {
                recordBuilder.setAttributes(repoRecord.getOriginalAttributes(), repoRecord.getUpdatedAttributes());
            }

            if (rawEvent.getEventDuration() < 0) {
                recordBuilder.setEventDuration(TimeUnit.NANOSECONDS.toMillis(commitNanos - repoRecord.getStartNanos()));
            }
        }
        return recordBuilder.build();
    }

    /**
     * Checks if the given event is a spurious FORK, meaning that the FORK has a
     * single child and that child was removed in this session. This happens
     * when a Processor calls #create(FlowFile) and then removes the created
     * FlowFile.
     *
     * @param event event
     * @return true if spurious fork
     */
    private boolean isSpuriousForkEvent(final ProvenanceEventRecord event, final Set<String> removedFlowFiles) {
        if (event.getEventType() == ProvenanceEventType.FORK) {
            final List<String> childUuids = event.getChildUuids();
            if (childUuids != null && childUuids.size() == 1 && removedFlowFiles.contains(childUuids.get(0))) {
                return true;
            }
        }

        return false;
    }

    /**
     * Checks if the given event is a spurious ROUTE, meaning that the ROUTE
     * indicates that a FlowFile was routed to a relationship with only 1
     * connection and that Connection is the Connection from which the FlowFile
     * was pulled. I.e., the FlowFile was really routed nowhere.
     *
     * @param event event
     * @param records records
     * @return true if spurious route
     */
    private boolean isSpuriousRouteEvent(final ProvenanceEventRecord event, final Map<Long, StandardRepositoryRecord> records) {
        if (event.getEventType() == ProvenanceEventType.ROUTE) {
            final String relationshipName = event.getRelationship();
            final Relationship relationship = new Relationship.Builder().name(relationshipName).build();
            final Collection<Connection> connectionsForRelationship = this.context.getConnections(relationship);

            // If the number of connections for this relationship is not 1, then we can't ignore this ROUTE event,
            // as it may be cloning the FlowFile and adding to multiple connections.
            if (connectionsForRelationship.size() == 1) {
                for (final StandardRepositoryRecord repoRecord : records.values()) {
                    final FlowFileRecord flowFileRecord = repoRecord.getCurrent();
                    if (event.getFlowFileUuid().equals(flowFileRecord.getAttribute(CoreAttributes.UUID.key()))) {
                        if (repoRecord.getOriginalQueue() == null) {
                            return false;
                        }

                        final String originalQueueId = repoRecord.getOriginalQueue().getIdentifier();
                        final Connection destinationConnection = connectionsForRelationship.iterator().next();
                        final String destinationQueueId = destinationConnection.getFlowFileQueue().getIdentifier();
                        return originalQueueId.equals(destinationQueueId);
                    }
                }
            }
        }

        return false;
    }

    @Override
    public void rollback() {
        rollback(false);
    }

    @Override
    public void rollback(final boolean penalize) {
        rollback(penalize, false);
        verifyTaskActive();
    }

    protected synchronized void rollback(final boolean penalize, final boolean rollbackCheckpoint) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("{} session rollback called, FlowFile records are {} {}",
                    this, loggableFlowfileInfo(), new Throwable("Stack Trace on rollback"));
        }

        deleteOnCommit.clear();

        closeStreams(openInputStreams, "rolled back", "input");
        closeStreams(openOutputStreams, "rolled back", "output");

        try {
            claimCache.reset();
        } catch (IOException e1) {
            LOG.warn("{} Attempted to close Output Stream for {} due to session rollback but close failed", this, this.connectableDescription, e1);
        }

        if (localState != null || clusterState != null) {
            LOG.debug("Rolling back session that has state stored. This state will not be updated.");
        }
        if (rollbackCheckpoint && checkpoint != null && (checkpoint.localState != null || checkpoint.clusterState != null)) {
            LOG.debug("Rolling back checkpoint that has state stored. This state will not be updated.");
        }

        // Gather all of the StandardRepositoryRecords that we need to operate on.
        // If we are rolling back the checkpoint, we must create a copy of the Collection so that we can merge the
        // session's records with the checkpoint's. Otherwise, we can operate on the session's records directly.
        // Because every session is rolled back, we want to avoid creating this defensive copy of the HashSet if
        // we don't need to.
        final Collection<StandardRepositoryRecord> recordValues = records.values();
        final Collection<StandardRepositoryRecord> recordsToHandle = rollbackCheckpoint ? new HashSet<>(recordValues) : recordValues;
        if (rollbackCheckpoint) {
            final Checkpoint existingCheckpoint = this.checkpoint;
            this.checkpoint = null;
            if (existingCheckpoint != null && existingCheckpoint.records != null) {
                recordsToHandle.addAll(existingCheckpoint.records.values());
            }
        }

        resetWriteClaims();
        resetReadClaim();

        if (recordsToHandle.isEmpty()) {
            LOG.trace("{} was rolled back, but no events were performed by this ProcessSession", this);
            acknowledgeRecords();
            resetState();
            return;
        }

        for (final StandardRepositoryRecord record : recordsToHandle) {
            // remove the working claims if they are different than the originals.
            removeTemporaryClaim(record);
        }

        final Set<RepositoryRecord> abortedRecords = new HashSet<>();
        final Set<StandardRepositoryRecord> transferRecords = new HashSet<>();
        for (final StandardRepositoryRecord record : recordsToHandle) {
            if (record.isMarkedForAbort()) {
                decrementClaimCount(record.getWorkingClaim());
                abortedRecords.add(record);
            } else {
                transferRecords.add(record);
            }
        }

        // Put the FlowFiles that are not marked for abort back to their original queues
        for (final StandardRepositoryRecord record : transferRecords) {
            rollbackRecord(record, penalize);
        }

        if (!abortedRecords.isEmpty()) {
            try {
                context.getFlowFileRepository().updateRepository(abortedRecords);
            } catch (final IOException ioe) {
                LOG.error("Unable to update FlowFile repository for aborted records", ioe);
            }
        }

        // If we have transient claims that need to be cleaned up, do so.
        final List<ContentClaim> transientClaims = recordsToHandle.stream()
            .flatMap(record -> record.getTransientClaims().stream())
            .collect(Collectors.toList());

        if (!transientClaims.isEmpty()) {
            final RepositoryRecord repoRecord = new TransientClaimRepositoryRecord(transientClaims);
            try {
                context.getFlowFileRepository().updateRepository(Collections.singletonList(repoRecord));
            } catch (final IOException ioe) {
                LOG.error("Unable to update FlowFile repository to cleanup transient claims", ioe);
            }
        }

        final Connectable connectable = context.getConnectable();
        final StandardFlowFileEvent flowFileEvent = new StandardFlowFileEvent();
        flowFileEvent.setBytesRead(bytesRead);
        flowFileEvent.setBytesWritten(bytesWritten);
        flowFileEvent.setCounters(immediateCounters);

        // update event repository
        try {
            context.getFlowFileEventRepository().updateRepository(flowFileEvent, connectable.getIdentifier());
        } catch (final Exception e) {
            LOG.error("Failed to update FlowFileEvent Repository", e);
        }

        acknowledgeRecords();
        resetState();
    }

    /**
     * Rolls back the Record in a manner that is appropriate for the context. The default implementation
     * is to place the queue back on its original queue, if it exists, or just ignore it if it has no original queue.
     * However, subclasses may wish to change the behavior for how Records are handled when a rollback occurs.
     * @param record the Record that is to be rolled back
     * @param penalize whether or not the Record should be penalized
     */
    protected void rollbackRecord(final StandardRepositoryRecord record, final boolean penalize) {
        if (record.getOriginal() != null) {
            final FlowFileQueue originalQueue = record.getOriginalQueue();
            if (originalQueue != null) {
                if (penalize) {
                    final long expirationEpochMillis = System.currentTimeMillis() + context.getConnectable().getPenalizationPeriod(TimeUnit.MILLISECONDS);
                    final FlowFileRecord newFile = new StandardFlowFileRecord.Builder().fromFlowFile(record.getOriginal()).penaltyExpirationTime(expirationEpochMillis).build();
                    originalQueue.put(newFile);
                } else {
                    originalQueue.put(record.getOriginal());
                }
            }
        }
    }

    private String loggableFlowfileInfo() {
        final StringBuilder details = new StringBuilder(1024).append("[");
        final int initLen = details.length();
        int filesListed = 0;
        for (StandardRepositoryRecord repoRecord : records.values()) {
            if (filesListed >= MAX_ROLLBACK_FLOWFILES_TO_LOG) {
                break;
            }
            filesListed++;

            if (details.length() > initLen) {
                details.append(", ");
            }
            if (repoRecord.getOriginalQueue() != null && repoRecord.getOriginalQueue().getIdentifier() != null) {
                details.append("queue=")
                        .append(repoRecord.getOriginalQueue().getIdentifier())
                        .append(", ");
            }
            details.append("filename=")
                    .append(repoRecord.getCurrent().getAttribute(CoreAttributes.FILENAME.key()))
                    .append(", uuid=")
                    .append(repoRecord.getCurrent().getAttribute(CoreAttributes.UUID.key()));
        }
        if (records.size() > MAX_ROLLBACK_FLOWFILES_TO_LOG) {
            if (details.length() > initLen) {
                details.append(", ");
            }
            details.append(records.size() - MAX_ROLLBACK_FLOWFILES_TO_LOG)
                    .append(" additional FlowFiles not listed");
        } else if (filesListed == 0) {
            details.append("none");
        }

        details.append("]");
        return details.toString();
    }

    private void decrementClaimCount(final ContentClaim claim) {
        if (claim == null) {
            return;
        }

        context.getContentRepository().decrementClaimantCount(claim);
    }

    /**
     * Destroys a ContentClaim that was being written to but is no longer needed
     *
     * @param claim claim to destroy
     */
    private void destroyContent(final ContentClaim claim, final StandardRepositoryRecord repoRecord) {
        if (claim == null) {
            return;
        }

        final int decrementedClaimCount = context.getContentRepository().decrementClaimantCount(claim);
        boolean removed = false;
        if (decrementedClaimCount <= 0) {
            resetWriteClaims(); // Have to ensure that we are not currently writing to the claim before we can destroy it.
            removed = context.getContentRepository().remove(claim);
        }

        // If we were not able to remove the content claim yet, mark it as a transient claim so that it will be cleaned up when the
        // FlowFile Repository is updated if it's available for cleanup at that time.
        if (!removed) {
            repoRecord.addTransientClaim(claim);
        }
    }

    private void resetState() {
        records.clear();
        readRecursionSet.clear();
        writeRecursionSet.clear();
        contentSizeIn = 0L;
        contentSizeOut = 0L;
        flowFilesIn = 0;
        flowFilesOut = 0;
        removedCount = 0;
        removedBytes = 0L;
        bytesRead = 0L;
        bytesWritten = 0L;
        connectionCounts.clear();
        createdFlowFiles.clear();
        createdFlowFilesWithoutLineage.clear();
        removedFlowFiles.clear();
        flowFileLinkage.clear();

        if (countersOnCommit != null) {
            countersOnCommit.clear();
        }
        if (immediateCounters != null) {
            immediateCounters.clear();
        }

        generatedProvenanceEvents.clear();
        forkEventBuilders.clear();
        provenanceReporter.clear();

        localState = null;
        clusterState = null;

        processingStartTime = System.nanoTime();
    }

    private void acknowledgeRecords() {
        final Iterator<Map.Entry<FlowFileQueue, Set<FlowFileRecord>>> itr = unacknowledgedFlowFiles.entrySet().iterator();
        while (itr.hasNext()) {
            final Map.Entry<FlowFileQueue, Set<FlowFileRecord>> entry = itr.next();
            itr.remove();

            entry.getKey().acknowledge(entry.getValue());
        }
    }


    @Override
    public void migrate(final ProcessSession newOwner) {
        final List<FlowFile> allFlowFiles = new ArrayList<>();

        for (final StandardRepositoryRecord repositoryRecord : records.values()) {
            allFlowFiles.add(repositoryRecord.getCurrent());
        }

        migrate(newOwner, allFlowFiles);
    }

    @Override
    public void migrate(final ProcessSession newOwner, final Collection<FlowFile> flowFiles) {
        if (Objects.requireNonNull(newOwner) == this) {
            throw new IllegalArgumentException("Cannot migrate FlowFiles from a Process Session to itself");
        }

        if (flowFiles == null || flowFiles.isEmpty()) {
            throw new IllegalArgumentException("Must supply at least one FlowFile to migrate");
        }

        if (!(newOwner instanceof StandardProcessSession)) {
            throw new IllegalArgumentException("Cannot migrate from a StandardProcessSession to a " + newOwner.getClass());
        }

        migrate((StandardProcessSession) newOwner, flowFiles);
    }

    private synchronized void migrate(final StandardProcessSession newOwner, Collection<FlowFile> flowFiles) {
        // This method will update many member variables/internal state of both `this` and `newOwner`. These member variables may also be updated during
        // session rollback, such as when a Processor is terminated. As such, we need to ensure that we synchronize on both `this` and `newOwner` so that
        // neither can be rolled back while we are in the process of migrating FlowFiles from one session to another.
        //
        // We must also ensure that we verify that both sessions are in an amenable state to perform this transference after obtaining the synchronization lock.
        // We synchronize on 'this' by marking the method synchronized. Because the only way in which one Process Session will call into another is via this migrate() method,
        // we do not need to worry about the order in which the synchronized lock is obtained.
        synchronized (newOwner) {
            verifyTaskActive();
            newOwner.verifyTaskActive();

            // We don't call validateRecordState() here because we want to allow migration of FlowFiles that have already been marked as removed or transferred, etc.
            flowFiles = flowFiles.stream().map(this::getMostRecent).collect(Collectors.toList());

            for (final FlowFile flowFile : flowFiles) {
                if (openInputStreams.containsKey(flowFile)) {
                    throw new IllegalStateException(flowFile + " cannot be migrated to a new Process Session because this session currently "
                        + "has an open InputStream for the FlowFile, created by calling ProcessSession.read(FlowFile)");
                }

                if (openOutputStreams.containsKey(flowFile)) {
                    throw new IllegalStateException(flowFile + " cannot be migrated to a new Process Session because this session currently "
                        + "has an open OutputStream for the FlowFile, created by calling ProcessSession.write(FlowFile)");
                }

                if (readRecursionSet.containsKey(flowFile)) {
                    throw new IllegalStateException(flowFile + " already in use for an active callback or InputStream created by ProcessSession.read(FlowFile) has not been closed");
                }
                if (writeRecursionSet.contains(flowFile)) {
                    throw new IllegalStateException(flowFile + " already in use for an active callback or OutputStream created by ProcessSession.write(FlowFile) has not been closed");
                }

                final StandardRepositoryRecord record = getRecord(flowFile);
                if (record == null) {
                    throw new FlowFileHandlingException(flowFile + " is not known in this session (" + toString() + ")");
                }
            }

            // If we have a FORK event for one of the given FlowFiles, then all children must also be migrated. Otherwise, we
            // could have a case where we have FlowFile A transferred and eventually exiting the flow and later the 'newOwner'
            // ProcessSession is committed, claiming to have created FlowFiles from the parent, which is no longer even in
            // the flow. This would be very confusing when looking at the provenance for the FlowFile, so it is best to avoid this.
            final Set<String> flowFileIds = flowFiles.stream()
                .map(ff -> ff.getAttribute(CoreAttributes.UUID.key()))
                .collect(Collectors.toSet());

            for (final Map.Entry<FlowFile, ProvenanceEventBuilder> entry : forkEventBuilders.entrySet()) {
                final FlowFile eventFlowFile = entry.getKey();
                if (flowFiles.contains(eventFlowFile)) {
                    final ProvenanceEventBuilder eventBuilder = entry.getValue();
                    for (final String childId : eventBuilder.getChildFlowFileIds()) {
                        if (!flowFileIds.contains(childId)) {
                            throw new FlowFileHandlingException("Cannot migrate " + eventFlowFile + " to a new session because it was forked to create " + eventBuilder.getChildFlowFileIds().size()
                                + " children and not all children are being migrated. If any FlowFile is forked, all of its children must also be migrated at the same time as the forked FlowFile");
                        }
                    }
                } else {
                    final ProvenanceEventBuilder eventBuilder = entry.getValue();
                    for (final String childId : eventBuilder.getChildFlowFileIds()) {
                        if (flowFileIds.contains(childId)) {
                            throw new FlowFileHandlingException("Cannot migrate " + eventFlowFile + " to a new session because it was forked from a Parent FlowFile, " +
                                "but the parent is not being migrated. If any FlowFile is forked, the parent and all children must be migrated at the same time.");
                        }
                    }
                }
            }

            // If we have a FORK event where a FlowFile is a child of the FORK event, we want to create a FORK
            // event builder for the new owner of the FlowFile and remove the child from our fork event builder.
            final Set<FlowFile> forkedFlowFilesMigrated = new HashSet<>();
            for (final Map.Entry<FlowFile, ProvenanceEventBuilder> entry : forkEventBuilders.entrySet()) {
                final FlowFile eventFlowFile = entry.getKey();
                final ProvenanceEventBuilder eventBuilder = entry.getValue();

                // If the FlowFile that the event is attached to is not being migrated, we should not migrate the fork event builder either.
                if (!flowFiles.contains(eventFlowFile)) {
                    continue;
                }

                final Set<String> childrenIds = new HashSet<>(eventBuilder.getChildFlowFileIds());

                ProvenanceEventBuilder copy = null;
                for (final FlowFile flowFile : flowFiles) {
                    final String flowFileId = flowFile.getAttribute(CoreAttributes.UUID.key());
                    if (childrenIds.contains(flowFileId)) {
                        eventBuilder.removeChildFlowFile(flowFile);

                        if (copy == null) {
                            copy = eventBuilder.copy();
                            copy.getChildFlowFileIds().clear();
                        }
                        copy.addChildFlowFile(flowFileId);
                    }
                }

                if (copy != null) {
                    newOwner.forkEventBuilders.put(eventFlowFile, copy);
                    forkedFlowFilesMigrated.add(eventFlowFile);
                }
            }

            forkedFlowFilesMigrated.forEach(forkEventBuilders::remove);

            newOwner.processingStartTime = Math.min(newOwner.processingStartTime, processingStartTime);

            for (final FlowFile flowFile : flowFiles) {
                final FlowFileRecord flowFileRecord = (FlowFileRecord) flowFile;

                final long flowFileId = flowFile.getId();
                final StandardRepositoryRecord repoRecord = this.records.remove(flowFileId);
                newOwner.records.put(flowFileId, repoRecord);

                final Collection<Long> linkedIds = this.flowFileLinkage.getLinkedIds(flowFileId);
                linkedIds.forEach(linkedId -> newOwner.flowFileLinkage.addLink(flowFileId, linkedId));

                // Adjust the counts for Connections for each FlowFile that was pulled from a Connection.
                // We do not have to worry about accounting for 'input counts' on connections because those
                // are incremented only during a checkpoint, and anything that's been checkpointed has
                // also been committed above.
                final FlowFileQueue inputQueue = repoRecord.getOriginalQueue();
                if (inputQueue != null) {
                    final String connectionId = inputQueue.getIdentifier();
                    incrementConnectionOutputCounts(connectionId, -1, -repoRecord.getOriginal().getSize());
                    newOwner.incrementConnectionOutputCounts(connectionId, 1, repoRecord.getOriginal().getSize());

                    unacknowledgedFlowFiles.get(inputQueue).remove(flowFile);
                    newOwner.unacknowledgedFlowFiles.computeIfAbsent(inputQueue, queue -> new HashSet<>()).add(flowFileRecord);

                    flowFilesIn--;
                    contentSizeIn -= flowFile.getSize();

                    newOwner.flowFilesIn++;
                    newOwner.contentSizeIn += flowFile.getSize();
                }

                final String flowFileUuid = flowFile.getAttribute(CoreAttributes.UUID.key());
                if (removedFlowFiles.remove(flowFileUuid)) {
                    newOwner.removedFlowFiles.add(flowFileUuid);
                    newOwner.removedCount++;
                    newOwner.removedBytes += flowFile.getSize();

                    removedCount--;
                    removedBytes -= flowFile.getSize();
                }

                if (createdFlowFiles.remove(flowFileUuid)) {
                    newOwner.createdFlowFiles.add(flowFileUuid);
                }

                if (repoRecord.getTransferRelationship() != null) {
                    final Relationship transferRelationship = repoRecord.getTransferRelationship();
                    final Collection<Connection> destinations = context.getConnections(transferRelationship);
                    final int numDestinations = destinations.size();
                    final boolean autoTerminated = numDestinations == 0 && context.getConnectable().isAutoTerminated(transferRelationship);

                    if (autoTerminated) {
                        removedCount--;
                        removedBytes -= flowFile.getSize();

                        newOwner.removedCount++;
                        newOwner.removedBytes += flowFile.getSize();
                    } else {
                        flowFilesOut--;
                        contentSizeOut -= flowFile.getSize();

                        newOwner.flowFilesOut++;
                        newOwner.contentSizeOut += flowFile.getSize();
                    }
                }

                final List<ProvenanceEventRecord> events = generatedProvenanceEvents.remove(flowFile);
                if (events != null) {
                    newOwner.generatedProvenanceEvents.put(flowFile, events);
                }

                final ContentClaim currentClaim = repoRecord.getCurrentClaim();
                if (currentClaim != null) {
                    final ByteCountingOutputStream appendableStream = appendableStreams.remove(currentClaim);
                    if (appendableStream != null) {
                        newOwner.appendableStreams.put(currentClaim, appendableStream);
                    }
                }

                final Path toDelete = deleteOnCommit.remove(flowFile);
                if (toDelete != null) {
                    newOwner.deleteOnCommit.put(flowFile, toDelete);
                }
            }

            provenanceReporter.migrate(newOwner.provenanceReporter, flowFileIds);
        }
    }


    private String summarizeEvents(final Checkpoint checkpoint) {
        final Map<Relationship, Set<String>> transferMap = new HashMap<>(); // relationship to flowfile ID's
        final Set<String> modifiedFlowFileIds = new HashSet<>();
        int largestTransferSetSize = 0;

        for (final Map.Entry<Long, StandardRepositoryRecord> entry : checkpoint.records.entrySet()) {
            final StandardRepositoryRecord record = entry.getValue();
            final FlowFile flowFile = record.getCurrent();

            final Relationship relationship = record.getTransferRelationship();
            if (Relationship.SELF.equals(relationship)) {
                continue;
            }

            Set<String> transferIds = transferMap.get(relationship);
            if (transferIds == null) {
                transferIds = new HashSet<>();
                transferMap.put(relationship, transferIds);
            }
            transferIds.add(flowFile.getAttribute(CoreAttributes.UUID.key()));
            largestTransferSetSize = Math.max(largestTransferSetSize, transferIds.size());

            final ContentClaim workingClaim = record.getWorkingClaim();
            if (workingClaim != null && workingClaim != record.getOriginalClaim() && record.getTransferRelationship() != null) {
                modifiedFlowFileIds.add(flowFile.getAttribute(CoreAttributes.UUID.key()));
            }
        }

        final int numRemoved = checkpoint.removedFlowFiles.size();
        final int numModified = modifiedFlowFileIds.size();
        final int numCreated = checkpoint.createdFlowFiles.size();

        final StringBuilder sb = new StringBuilder(512);
        if (!LOG.isDebugEnabled() && (largestTransferSetSize > VERBOSE_LOG_THRESHOLD
            || numModified > VERBOSE_LOG_THRESHOLD || numCreated > VERBOSE_LOG_THRESHOLD || numRemoved > VERBOSE_LOG_THRESHOLD)) {
            if (numCreated > 0) {
                sb.append("created ").append(numCreated).append(" FlowFiles, ");
            }
            if (numModified > 0) {
                sb.append("modified ").append(modifiedFlowFileIds.size()).append(" FlowFiles, ");
            }
            if (numRemoved > 0) {
                sb.append("removed ").append(numRemoved).append(" FlowFiles, ");
            }
            for (final Map.Entry<Relationship, Set<String>> entry : transferMap.entrySet()) {
                if (entry.getKey() != null) {
                    sb.append("Transferred ").append(entry.getValue().size()).append(" FlowFiles");

                    final Relationship relationship = entry.getKey();
                    if (relationship != Relationship.ANONYMOUS) {
                        sb.append(" to '").append(relationship.getName()).append("', ");
                    }
                }
            }
        } else {
            if (numCreated > 0) {
                sb.append("created FlowFiles ").append(checkpoint.createdFlowFiles).append(", ");
            }
            if (numModified > 0) {
                sb.append("modified FlowFiles ").append(modifiedFlowFileIds).append(", ");
            }
            if (numRemoved > 0) {
                sb.append("removed FlowFiles ").append(checkpoint.removedFlowFiles).append(", ");
            }
            for (final Map.Entry<Relationship, Set<String>> entry : transferMap.entrySet()) {
                if (entry.getKey() != null) {
                    sb.append("Transferred FlowFiles ").append(entry.getValue());

                    final Relationship relationship = entry.getKey();
                    if (relationship != Relationship.ANONYMOUS) {
                        sb.append(" to '").append(relationship.getName()).append("', ");
                    }
                }
            }
        }

        if (sb.length() > 2 && sb.subSequence(sb.length() - 2, sb.length()).equals(", ")) {
            sb.delete(sb.length() - 2, sb.length());
        }

        // don't add processing time if we did nothing, because we don't log the summary anyway
        if (sb.length() > 0) {
            final long processingNanos = checkpoint.processingTime;
            sb.append(", Processing Time = ");
            formatNanos(processingNanos, sb);
        }

        return sb.toString();
    }

    private void formatNanos(final long nanos, final StringBuilder sb) {
        final long seconds = nanos > 1000000000L ? nanos / 1000000000L : 0L;
        long millis = nanos > 1000000L ? nanos / 1000000L : 0L;

        final long nanosLeft = nanos % 1000000L;

        if (seconds > 0) {
            sb.append(seconds).append(" seconds");
        }
        if (millis > 0) {
            if (seconds > 0) {
                sb.append(", ");
                millis -= seconds * 1000L;
            }

            sb.append(millis).append(" millis");
        }
        if (seconds == 0 && millis == 0) {
            sb.append(nanosLeft).append(" nanos");
        }

        sb.append(" (").append(nanos).append(" nanos)");
    }

    private void incrementConnectionInputCounts(final Connection connection, final RepositoryRecord record) {
        incrementConnectionInputCounts(connection.getIdentifier(), 1, record.getCurrent().getSize());
    }

    private void incrementConnectionInputCounts(final String connectionId, final int flowFileCount, final long bytes) {
        final StandardFlowFileEvent connectionEvent = connectionCounts.computeIfAbsent(connectionId, id -> new StandardFlowFileEvent());
        connectionEvent.setContentSizeIn(connectionEvent.getContentSizeIn() + bytes);
        connectionEvent.setFlowFilesIn(connectionEvent.getFlowFilesIn() + flowFileCount);
    }

    private void incrementConnectionOutputCounts(final Connection connection, final FlowFileRecord record) {
        incrementConnectionOutputCounts(connection.getIdentifier(), 1, record.getSize());
    }

    private void incrementConnectionOutputCounts(final String connectionId, final int flowFileCount, final long bytes) {
        final StandardFlowFileEvent connectionEvent = connectionCounts.computeIfAbsent(connectionId, id -> new StandardFlowFileEvent());
        connectionEvent.setContentSizeOut(connectionEvent.getContentSizeOut() + bytes);
        connectionEvent.setFlowFilesOut(connectionEvent.getFlowFilesOut() + flowFileCount);
    }

    private void registerDequeuedRecord(final FlowFileRecord flowFile, final Connection connection) {
        final StandardRepositoryRecord record = new StandardRepositoryRecord(connection.getFlowFileQueue(), flowFile);

        // Ensure that the checkpoint does not have a FlowFile with the same ID already. This should not occur,
        // but this is a safety check just to make sure, because if it were to occur, and we did process the FlowFile,
        // we would have a lot of problems, since the map is keyed off of the FlowFile ID.
        if (this.checkpoint != null) {
            final StandardRepositoryRecord checkpointedRecord = this.checkpoint.getRecord(flowFile);
            handleConflictingId(flowFile, connection, checkpointedRecord);
        }

        final StandardRepositoryRecord existingRecord = records.putIfAbsent(flowFile.getId(), record);
        handleConflictingId(flowFile, connection, existingRecord); // Ensure that we have no conflicts

        flowFilesIn++;
        contentSizeIn += flowFile.getSize();

        final Set<FlowFileRecord> set = unacknowledgedFlowFiles.computeIfAbsent(connection.getFlowFileQueue(), k -> new HashSet<>());
        set.add(flowFile);

        incrementConnectionOutputCounts(connection, flowFile);
    }

    private void handleConflictingId(final FlowFileRecord flowFile, final Connection connection, final StandardRepositoryRecord conflict) {
        if (conflict == null) {
            // No conflict
            return;
        }

        LOG.error("Attempted to pull {} from {} but the Session already has a FlowFile with the same ID ({}): {}, which was pulled from {}. This means that the system has two FlowFiles with the" +
            " same ID, which should not happen.", flowFile, connection, flowFile.getId(), conflict.getCurrent(), conflict.getOriginalQueue());
        connection.getFlowFileQueue().put(flowFile);

        rollback(true, false);
        throw new FlowFileAccessException("Attempted to pull a FlowFile with ID " + flowFile.getId() + " from Connection "
            + connection + " but a FlowFile with that ID already exists in the session");
    }

    @Override
    public void adjustCounter(final String name, final long delta, final boolean immediate) {
        verifyTaskActive();

        final Map<String, Long> counters;
        if (immediate) {
            if (immediateCounters == null) {
                immediateCounters = new HashMap<>();
            }
            counters = immediateCounters;
        } else {
            if (countersOnCommit == null) {
                countersOnCommit = new HashMap<>();
            }
            counters = countersOnCommit;
        }

        adjustCounter(name, delta, counters);

        if (immediate) {
            context.adjustCounter(name, delta);
        }
    }

    private void adjustCounter(final String name, final long delta, final Map<String, Long> map) {
        Long curVal = map.get(name);
        if (curVal == null) {
            curVal = Long.valueOf(0L);
        }

        final long newValue = curVal.longValue() + delta;
        map.put(name, Long.valueOf(newValue));
    }

    @Override
    public FlowFile get() {
        verifyTaskActive();
        final List<Connection> connections = context.getPollableConnections();
        final int numConnections = connections.size();
        for (int numAttempts = 0; numAttempts < numConnections; numAttempts++) {
            final Connection conn = connections.get(context.getNextIncomingConnectionIndex() % numConnections);
            // TODO: We create this Set<FlowFileRecord> every time. Instead, add FlowFileQueue.isExpirationConfigured(). If false, pass Collections.emptySet(). Same for all get() methods.
            final Set<FlowFileRecord> expired = new HashSet<>();
            final FlowFileRecord flowFile = conn.poll(expired);
            removeExpired(expired, conn);

            if (flowFile != null) {
                registerDequeuedRecord(flowFile, conn);
                return flowFile;
            }
        }

        return null;
    }

    @Override
    public List<FlowFile> get(final int maxResults) {
        verifyTaskActive();

        if (maxResults < 0) {
            throw new IllegalArgumentException();
        }
        if (maxResults == 0) {
            return Collections.emptyList();
        }

        // get batch of flow files in a round-robin manner
        final List<Connection> connections = context.getPollableConnections();
        if (connections.isEmpty()) {
            return Collections.emptyList();
        }

        return get((connection, expiredRecords) -> connection.poll(new FlowFileFilter() {
            int polled = 0;

            @Override
            public FlowFileFilterResult filter(final FlowFile flowFile) {
                if (++polled < maxResults) {
                    return FlowFileFilterResult.ACCEPT_AND_CONTINUE;
                } else {
                    return FlowFileFilterResult.ACCEPT_AND_TERMINATE;
                }
            }
        }, expiredRecords), false);
    }

    @Override
    public List<FlowFile> get(final FlowFileFilter filter) {
        verifyTaskActive();

        return get((connection, expiredRecords) -> connection.poll(filter, expiredRecords), true);
    }


    private List<FlowFile> get(final ConnectionPoller poller, final boolean lockAllQueues) {
        List<Connection> connections = context.getPollableConnections();
        final boolean sortConnections = lockAllQueues && connections.size() > 1;
        if (sortConnections) {
            // Sort by identifier so that if there are two arbitrary connections, we always lock them in the same order.
            // Otherwise, we could encounter a deadlock, if another thread were to lock the same two connections in a different order.
            // So we always lock ordered on connection identifier. And unlock in the opposite order.
            // Before doing this, we must create a copy of the List because the one provided by context.getPollableConnections() is usually an unmodifiableList
            connections = new ArrayList<>(connections);
            connections.sort(Comparator.comparing(Connection::getIdentifier));
            for (final Connection connection : connections) {
                connection.lock();
            }
        }

        final int startIndex = context.getNextIncomingConnectionIndex();

        try {
            for (int i = 0; i < connections.size(); i++) {
                final int connectionIndex = (startIndex + i) % connections.size();
                final Connection conn = connections.get(connectionIndex);

                final Set<FlowFileRecord> expired = new HashSet<>();
                final List<FlowFileRecord> newlySelected = poller.poll(conn, expired);
                removeExpired(expired, conn);

                if (newlySelected.isEmpty() && expired.isEmpty()) {
                    continue;
                }

                for (final FlowFileRecord flowFile : newlySelected) {
                    registerDequeuedRecord(flowFile, conn);
                }

                return new ArrayList<>(newlySelected);
            }

            return new ArrayList<>();
        } finally {
            if (sortConnections) {
                // Reverse ordering in order to unlock
                connections.sort(Comparator.comparing(Connection::getIdentifier).reversed());

                for (final Connection connection : connections) {
                    connection.unlock();
                }
            }
        }
    }

    @Override
    public QueueSize getQueueSize() {
        verifyTaskActive();

        int flowFileCount = 0;
        long byteCount = 0L;
        for (final Connection conn : context.getPollableConnections()) {
            final QueueSize queueSize = conn.getFlowFileQueue().size();
            flowFileCount += queueSize.getObjectCount();
            byteCount += queueSize.getByteCount();
        }
        return new QueueSize(flowFileCount, byteCount);
    }

    @Override
    public FlowFile create() {
        verifyTaskActive();

        final Map<String, String> attrs = new HashMap<>();
        final String uuid = UUID.randomUUID().toString();
        attrs.put(CoreAttributes.FILENAME.key(), uuid);
        attrs.put(CoreAttributes.PATH.key(), DEFAULT_FLOWFILE_PATH);
        attrs.put(CoreAttributes.UUID.key(), uuid);

        final FlowFileRecord fFile = new StandardFlowFileRecord.Builder().id(context.getNextFlowFileSequence())
            .addAttributes(attrs)
            .build();
        final StandardRepositoryRecord record = new StandardRepositoryRecord((FlowFileQueue) null);
        record.setWorking(fFile, attrs, false);
        records.put(fFile.getId(), record);

        createdFlowFiles.add(uuid);
        createdFlowFilesWithoutLineage.add(uuid);

        return fFile;
    }

    @Override
    public FlowFile create(FlowFile parent) {
        verifyTaskActive();
        parent = getMostRecent(parent);

        final String uuid = UUID.randomUUID().toString();

        final Map<String, String> newAttributes = new HashMap<>(3);
        newAttributes.put(CoreAttributes.FILENAME.key(), uuid);
        newAttributes.put(CoreAttributes.PATH.key(), DEFAULT_FLOWFILE_PATH);
        newAttributes.put(CoreAttributes.UUID.key(), uuid);

        final StandardFlowFileRecord.Builder fFileBuilder = new StandardFlowFileRecord.Builder().id(context.getNextFlowFileSequence());

        // copy all attributes from parent except for the "special" attributes. Copying the special attributes
        // can cause problems -- especially the ALTERNATE_IDENTIFIER, because copying can cause Provenance Events
        // to be incorrectly created.
        for (final Map.Entry<String, String> entry : parent.getAttributes().entrySet()) {
            final String key = entry.getKey();
            final String value = entry.getValue();
            if (CoreAttributes.ALTERNATE_IDENTIFIER.key().equals(key)
                || CoreAttributes.DISCARD_REASON.key().equals(key)
                || CoreAttributes.UUID.key().equals(key)) {
                continue;
            }
            newAttributes.put(key, value);
        }

        fFileBuilder.lineageStart(parent.getLineageStartDate(), parent.getLineageStartIndex());
        fFileBuilder.addAttributes(newAttributes);

        final FlowFileRecord fFile = fFileBuilder.build();
        final StandardRepositoryRecord record = new StandardRepositoryRecord((FlowFileQueue) null);
        record.setWorking(fFile, newAttributes, false);
        records.put(fFile.getId(), record);
        createdFlowFiles.add(fFile.getAttribute(CoreAttributes.UUID.key()));

        registerForkEvent(parent, fFile);
        flowFileLinkage.addLink(parent.getId(), fFile.getId());
        return fFile;
    }

    @Override
    public FlowFile create(Collection<FlowFile> parents) {
        verifyTaskActive();

        parents = parents.stream().map(this::getMostRecent).collect(Collectors.toList());

        final Map<String, String> newAttributes = intersectAttributes(parents);
        newAttributes.remove(CoreAttributes.UUID.key());
        newAttributes.remove(CoreAttributes.ALTERNATE_IDENTIFIER.key());
        newAttributes.remove(CoreAttributes.DISCARD_REASON.key());

        // When creating a new FlowFile from multiple parents, we need to add all of the Lineage Identifiers
        // and use the earliest lineage start date
        long lineageStartDate = 0L;
        for (final FlowFile parent : parents) {

            final long parentLineageStartDate = parent.getLineageStartDate();
            if (lineageStartDate == 0L || parentLineageStartDate < lineageStartDate) {
                lineageStartDate = parentLineageStartDate;
            }
        }

        // find the smallest lineage start index that has the same lineage start date as the one we've chosen.
        long lineageStartIndex = 0L;
        for (final FlowFile parent : parents) {
            if (parent.getLineageStartDate() == lineageStartDate && parent.getLineageStartIndex() < lineageStartIndex) {
                lineageStartIndex = parent.getLineageStartIndex();
            }
        }

        final String uuid = UUID.randomUUID().toString();
        newAttributes.put(CoreAttributes.FILENAME.key(), uuid);
        newAttributes.put(CoreAttributes.PATH.key(), DEFAULT_FLOWFILE_PATH);
        newAttributes.put(CoreAttributes.UUID.key(), uuid);

        final FlowFileRecord fFile = new StandardFlowFileRecord.Builder().id(context.getNextFlowFileSequence())
            .addAttributes(newAttributes)
            .lineageStart(lineageStartDate, lineageStartIndex)
            .build();

        final StandardRepositoryRecord record = new StandardRepositoryRecord((FlowFileQueue) null);
        record.setWorking(fFile, newAttributes, false);
        records.put(fFile.getId(), record);
        createdFlowFiles.add(fFile.getAttribute(CoreAttributes.UUID.key()));

        registerJoinEvent(fFile, parents);

        final long flowFileId = fFile.getId();
        for (final FlowFile parent : parents) {
            flowFileLinkage.addLink(flowFileId, parent.getId());
        }

        return fFile;
    }


    @Override
    public FlowFile clone(FlowFile example) {
        verifyTaskActive();
        example = validateRecordState(example);
        return clone(example, 0L, example.getSize());
    }

    @Override
    public FlowFile clone(FlowFile example, final long offset, final long size) {
        verifyTaskActive();

        example = validateRecordState(example);
        final StandardRepositoryRecord exampleRepoRecord = getRecord(example);
        final FlowFileRecord currRec = exampleRepoRecord.getCurrent();
        final ContentClaim claim = exampleRepoRecord.getCurrentClaim();
        if (offset + size > example.getSize()) {
            throw new FlowFileHandlingException("Specified offset of " + offset + " and size " + size + " exceeds size of " + example);
        }

        final StandardFlowFileRecord.Builder builder = new StandardFlowFileRecord.Builder().fromFlowFile(currRec);
        builder.id(context.getNextFlowFileSequence());
        builder.contentClaimOffset(currRec.getContentClaimOffset() + offset);
        builder.size(size);

        final String newUuid = UUID.randomUUID().toString();
        builder.addAttribute(CoreAttributes.UUID.key(), newUuid);

        final FlowFileRecord clone = builder.build();
        if (claim != null) {
            context.getContentRepository().incrementClaimaintCount(claim);
        }
        final StandardRepositoryRecord record = new StandardRepositoryRecord((FlowFileQueue) null);
        record.setWorking(clone, clone.getAttributes(), false);
        records.put(clone.getId(), record);

        if (offset == 0L && size == example.getSize()) {
            provenanceReporter.clone(example, clone);
        } else {
            registerForkEvent(example, clone);
        }

        flowFileLinkage.addLink(example.getId(), clone.getId());

        return clone;
    }

    private void registerForkEvent(final FlowFile parent, final FlowFile child) {
        ProvenanceEventBuilder eventBuilder = forkEventBuilders.get(parent);
        if (eventBuilder == null) {
            eventBuilder = context.getProvenanceRepository().eventBuilder();
            eventBuilder.setEventType(ProvenanceEventType.FORK);

            eventBuilder.setFlowFileEntryDate(parent.getEntryDate());
            eventBuilder.setLineageStartDate(parent.getLineageStartDate());
            eventBuilder.setFlowFileUUID(parent.getAttribute(CoreAttributes.UUID.key()));

            eventBuilder.setComponentId(context.getConnectable().getIdentifier());

            final Connectable connectable = context.getConnectable();
            final String processorType = connectable.getComponentType();
            eventBuilder.setComponentType(processorType);
            eventBuilder.addParentFlowFile(parent);

            updateEventContentClaims(eventBuilder, parent, getRecord(parent));
            forkEventBuilders.put(parent, eventBuilder);
        }

        eventBuilder.addChildFlowFile(child);
    }

    private void registerJoinEvent(final FlowFile child, final Collection<FlowFile> parents) {
        final ProvenanceEventRecord eventRecord = provenanceReporter.generateJoinEvent(parents, child);
        final List<ProvenanceEventRecord> existingRecords = generatedProvenanceEvents.computeIfAbsent(child, k -> new ArrayList<>());
        existingRecords.add(eventRecord);
    }

    @Override
    public FlowFile penalize(FlowFile flowFile) {
        verifyTaskActive();
        flowFile = validateRecordState(flowFile, false);

        return penalize(flowFile, context.getConnectable().getPenalizationPeriod(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
    }

    public FlowFile penalize(FlowFile flowFile, final long period, final TimeUnit timeUnit) {
        flowFile = getRecord(flowFile).getCurrent();
        final StandardRepositoryRecord record = getRecord(flowFile);
        final long penalizeMillis = TimeUnit.MILLISECONDS.convert(period, timeUnit);
        final long expirationEpochMillis = System.currentTimeMillis() + penalizeMillis;
        final FlowFileRecord newFile = new StandardFlowFileRecord.Builder().fromFlowFile(record.getCurrent()).penaltyExpirationTime(expirationEpochMillis).build();
        record.setWorking(newFile, false);
        return newFile;
    }

    @Override
    public FlowFile putAttribute(FlowFile flowFile, final String key, final String value) {
        verifyTaskActive();
        flowFile = validateRecordState(flowFile);

        if (CoreAttributes.UUID.key().equals(key)) {
            return flowFile;
        }

        final StandardRepositoryRecord record = getRecord(flowFile);
        final FlowFileRecord newFile = new StandardFlowFileRecord.Builder().fromFlowFile(record.getCurrent()).addAttribute(key, value).build();
        record.setWorking(newFile, key, value, false);

        return newFile;
    }

    @Override
    public FlowFile putAllAttributes(FlowFile flowFile, final Map<String, String> attributes) {
        verifyTaskActive();

        if (attributes.isEmpty()) {
            return flowFile;
        }

        flowFile = validateRecordState(flowFile);
        final StandardRepositoryRecord record = getRecord(flowFile);

        final Map<String, String> updatedAttributes;
        if (attributes.containsKey(CoreAttributes.UUID.key())) {
            updatedAttributes = new HashMap<>(attributes);
            updatedAttributes.remove(CoreAttributes.UUID.key());
        } else {
            updatedAttributes = attributes;
        }

        final StandardFlowFileRecord.Builder ffBuilder = new StandardFlowFileRecord.Builder().fromFlowFile(record.getCurrent()).addAttributes(updatedAttributes);
        final FlowFileRecord newFile = ffBuilder.build();

        record.setWorking(newFile, updatedAttributes, false);

        return newFile;
    }

    @Override
    public FlowFile removeAttribute(FlowFile flowFile, final String key) {
        verifyTaskActive();
        flowFile = validateRecordState(flowFile);

        if (REQUIRED_ATTRIBUTES.contains(key)) {
            return flowFile;
        }

        final StandardRepositoryRecord record = getRecord(flowFile);
        final FlowFileRecord newFile = new StandardFlowFileRecord.Builder().fromFlowFile(record.getCurrent()).removeAttributes(key).build();
        record.setWorking(newFile, key, null, false);
        return newFile;
    }

    @Override
    public FlowFile removeAllAttributes(FlowFile flowFile, final Set<String> keys) {
        verifyTaskActive();
        flowFile = validateRecordState(flowFile);

        if (keys == null) {
            return flowFile;
        }

        final StandardRepositoryRecord record = getRecord(flowFile);
        final FlowFileRecord newFile = new StandardFlowFileRecord.Builder().fromFlowFile(record.getCurrent()).removeAttributes(keys).build();

        final Map<String, String> updatedAttrs = new HashMap<>();
        for (final String key : keys) {
            if (REQUIRED_ATTRIBUTES.contains(key)) {
                continue;
            }

            if (CoreAttributes.UUID.key().equals(key)) {
                continue;
            }

            updatedAttrs.put(key, null);
        }

        record.setWorking(newFile, updatedAttrs, false);
        return newFile;
    }

    @Override
    public FlowFile removeAllAttributes(FlowFile flowFile, final Pattern keyPattern) {
        verifyTaskActive();

        flowFile = validateRecordState(flowFile);
        final StandardRepositoryRecord record = getRecord(flowFile);
        final FlowFileRecord newFile = new StandardFlowFileRecord.Builder().fromFlowFile(record.getCurrent()).removeAttributes(keyPattern).build();

        if (keyPattern == null) {
            record.setWorking(newFile, false);
        } else {
            final Map<String, String> curAttrs = record.getCurrent().getAttributes();

            final Map<String, String> removed = new HashMap<>();
            for (final String key : curAttrs.keySet()) {
                if (REQUIRED_ATTRIBUTES.contains(key)) {
                    continue;
                }

                if (keyPattern.matcher(key).matches()) {
                    removed.put(key, null);
                }
            }

            record.setWorking(newFile, removed, false);
        }

        return newFile;
    }

    private void updateLastQueuedDate(final StandardRepositoryRecord record, final Long lastQueueDate) {
        final FlowFileRecord newFile = new StandardFlowFileRecord.Builder().fromFlowFile(record.getCurrent())
                .lastQueued(lastQueueDate, enqueuedIndex.getAndIncrement()).build();
        record.setWorking(newFile, false);
    }

    private void updateLastQueuedDate(final StandardRepositoryRecord record) {
        updateLastQueuedDate(record, System.currentTimeMillis());
    }

    @Override
    public void transfer(FlowFile flowFile, final Relationship relationship) {
        verifyTaskActive();
        flowFile = validateRecordState(flowFile);
        final int numDestinations = context.getConnections(relationship).size();
        final int multiplier = Math.max(1, numDestinations);

        boolean autoTerminated = false;
        boolean selfRelationship = false;
        if (numDestinations == 0 && context.getConnectable().isAutoTerminated(relationship)) {
            // auto terminated.
            autoTerminated = true;
        } else if (numDestinations == 0 && relationship == Relationship.SELF) {
            selfRelationship = true;
        } else if (numDestinations == 0) {
            // the relationship specified is not known in this session/context
            throw new IllegalArgumentException("Relationship '" + relationship.getName() + "' is not known");
        }
        final StandardRepositoryRecord record = getRecord(flowFile);
        record.setTransferRelationship(relationship);
        updateLastQueuedDate(record);

        if (autoTerminated) {
            removedCount += multiplier;
            removedBytes += flowFile.getSize();
        } else if (!selfRelationship) {
            flowFilesOut += multiplier;
            contentSizeOut += flowFile.getSize() * multiplier;
        }
    }

    @Override
    public void transfer(FlowFile flowFile) {
        verifyTaskActive();

        flowFile = validateRecordState(flowFile);
        final StandardRepositoryRecord record = getRecord(flowFile);
        if (record.getOriginalQueue() == null) {
            throw new IllegalArgumentException("Cannot transfer FlowFiles that are created in this Session back to self");
        }
        record.setTransferRelationship(Relationship.SELF);
        updateLastQueuedDate(record);
    }

    @Override
    public void transfer(final Collection<FlowFile> flowFiles) {
        for (final FlowFile flowFile : flowFiles) {
            transfer(flowFile);
        }
    }

    @Override
    public void transfer(Collection<FlowFile> flowFiles, final Relationship relationship) {
        verifyTaskActive();
        flowFiles = validateRecordState(flowFiles);

        boolean autoTerminated = false;
        boolean selfRelationship = false;
        final int numDestinations = context.getConnections(relationship).size();
        if (numDestinations == 0 && context.getConnectable().isAutoTerminated(relationship)) {
            // auto terminated.
            autoTerminated = true;
        } else if (numDestinations == 0 && relationship == Relationship.SELF) {
            selfRelationship = true;
        } else if (numDestinations == 0) {
            // the relationship specified is not known in this session/context
            throw new IllegalArgumentException("Relationship '" + relationship.getName() + "' is not known");
        }

        final int multiplier = Math.max(1, numDestinations);

        final long queuedTime = System.currentTimeMillis();
        long contentSize = 0L;
        for (final FlowFile flowFile : flowFiles) {
            final FlowFileRecord flowFileRecord = (FlowFileRecord) flowFile;
            final StandardRepositoryRecord record = getRecord(flowFileRecord);
            record.setTransferRelationship(relationship);
            updateLastQueuedDate(record, queuedTime);

            contentSize += flowFile.getSize();
        }

        if (autoTerminated) {
            removedCount += multiplier * flowFiles.size();
            removedBytes += multiplier * contentSize;
        } else if (!selfRelationship) {
            flowFilesOut += multiplier * flowFiles.size();
            contentSizeOut += multiplier * contentSize;
        }
    }

    @Override
    public void remove(FlowFile flowFile) {
        verifyTaskActive();

        flowFile = validateRecordState(flowFile);
        final StandardRepositoryRecord record = getRecord(flowFile);
        record.markForDelete();
        removedFlowFiles.add(flowFile.getAttribute(CoreAttributes.UUID.key()));

        // if original connection is null, the FlowFile was created in this session, so we
        // do not want to count it toward the removed count.
        if (record.getOriginalQueue() == null) {
            // if we've generated any Fork events, remove them because the FlowFile was created
            // and then removed in this session.
            generatedProvenanceEvents.remove(flowFile);
            removeForkEvents(flowFile);
        } else {
            removedCount++;
            removedBytes += flowFile.getSize();
            provenanceReporter.drop(flowFile, flowFile.getAttribute(CoreAttributes.DISCARD_REASON.key()));
        }
    }

    @Override
    public void remove(Collection<FlowFile> flowFiles) {
        verifyTaskActive();

        flowFiles = validateRecordState(flowFiles);
        for (final FlowFile flowFile : flowFiles) {
            final StandardRepositoryRecord record = getRecord(flowFile);
            record.markForDelete();
            removedFlowFiles.add(flowFile.getAttribute(CoreAttributes.UUID.key()));

            // if original connection is null, the FlowFile was created in this session, so we
            // do not want to count it toward the removed count.
            if (record.getOriginalQueue() == null) {
                generatedProvenanceEvents.remove(flowFile);
                removeForkEvents(flowFile);
            } else {
                removedCount++;
                removedBytes += flowFile.getSize();
                provenanceReporter.drop(flowFile, flowFile.getAttribute(CoreAttributes.DISCARD_REASON.key()));
            }
        }
    }

    private void removeForkEvents(final FlowFile flowFile) {
        for (final ProvenanceEventBuilder builder : forkEventBuilders.values()) {
            final ProvenanceEventRecord event = builder.build();

            if (event.getEventType() == ProvenanceEventType.FORK) {
                builder.removeChildFlowFile(flowFile);
            }
        }
    }

    public void expireFlowFiles() {
        final Set<FlowFileRecord> expired = new HashSet<>();
        final FlowFileFilter filter = flowFile -> FlowFileFilter.FlowFileFilterResult.REJECT_AND_CONTINUE;

        for (final Connection conn : context.getConnectable().getIncomingConnections()) {
            do {
                expired.clear();
                conn.getFlowFileQueue().poll(filter, expired, PollStrategy.ALL_FLOWFILES);
                removeExpired(expired, conn);
            } while (!expired.isEmpty());
        }
    }

    private void removeExpired(final Set<FlowFileRecord> flowFiles, final Connection connection) {
        if (flowFiles.isEmpty()) {
            return;
        }

        LOG.info("{} {} FlowFiles have expired and will be removed", this, flowFiles.size());
        final List<RepositoryRecord> expiredRecords = new ArrayList<>(flowFiles.size());

        final Connectable connectable = context.getConnectable();
        final InternalProvenanceReporter expiredReporter = context.createProvenanceReporter(this::isFlowFileKnown, this);

        final Map<String, FlowFileRecord> recordIdMap = new HashMap<>();
        for (final FlowFileRecord flowFile : flowFiles) {
            recordIdMap.put(flowFile.getAttribute(CoreAttributes.UUID.key()), flowFile);

            final StandardRepositoryRecord record = new StandardRepositoryRecord(connection.getFlowFileQueue(), flowFile);
            record.markForDelete();
            expiredRecords.add(record);
            expiredReporter.expire(flowFile, "Expiration Threshold = " + connection.getFlowFileQueue().getFlowFileExpiration());

            final long flowFileLife = System.currentTimeMillis() - flowFile.getEntryDate();
            final Object terminator = connectable instanceof ProcessorNode ? ((ProcessorNode) connectable).getProcessor() : connectable;
            LOG.debug("{} terminated by {} due to FlowFile expiration; life of FlowFile = {} ms", flowFile, terminator, flowFileLife);
        }

        try {
            final Iterable<ProvenanceEventRecord> iterable = () -> {
                final Iterator<ProvenanceEventRecord> expiredEventIterator = expiredReporter.getEvents().iterator();
                final Iterator<ProvenanceEventRecord> enrichingIterator = new Iterator<>() {
                    @Override
                    public boolean hasNext() {
                        return expiredEventIterator.hasNext();
                    }

                    @Override
                    public ProvenanceEventRecord next() {
                        final ProvenanceEventRecord event = expiredEventIterator.next();
                        final ProvenanceEventBuilder enriched = context.createProvenanceEventBuilder().fromEvent(event);
                        final FlowFileRecord record = recordIdMap.get(event.getFlowFileUuid());
                        if (record == null) {
                            return null;
                        }

                        final ContentClaim claim = record.getContentClaim();
                        if (claim != null) {
                            final ResourceClaim resourceClaim = claim.getResourceClaim();
                            enriched.setCurrentContentClaim(resourceClaim.getContainer(), resourceClaim.getSection(), resourceClaim.getId(),
                                    record.getContentClaimOffset() + claim.getOffset(), record.getSize());
                            enriched.setPreviousContentClaim(resourceClaim.getContainer(), resourceClaim.getSection(), resourceClaim.getId(),
                                    record.getContentClaimOffset() + claim.getOffset(), record.getSize());
                        }

                        enriched.setAttributes(record.getAttributes(), Collections.<String, String>emptyMap());
                        return enriched.build();
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };

                return enrichingIterator;
            };

            context.getProvenanceRepository().registerEvents(iterable);
            context.getFlowFileRepository().updateRepository(expiredRecords);
        } catch (final IOException e) {
            LOG.error("Failed to update FlowFile Repository to record expired records", e);
        }

    }

    private InputStream getInputStream(final FlowFile flowFile, final ContentClaim claim, final long contentClaimOffset, final boolean allowCachingOfStream) throws ContentNotFoundException {
        // If there's no content, don't bother going to the Content Repository because it is generally expensive and we know
        // that there is no actual content.
        if (flowFile.getSize() == 0L) {
            return new ByteArrayInputStream(new byte[0]);
        }

        try {
            // If the recursion set is empty, we can use the same input stream that we already have open. However, if
            // the recursion set is NOT empty, we can't do this because we may be reading the input of FlowFile 1 while in the
            // callback for reading FlowFile 1 and if we used the same stream we'd be destroying the ability to read from FlowFile 1.
            if (allowCachingOfStream && readRecursionSet.isEmpty() && !writeRecursionSet.contains(flowFile) && context.getContentRepository().isResourceClaimStreamSupported()) {
                if (currentReadClaim == claim.getResourceClaim()) {
                    final long resourceClaimOffset = claim.getOffset() + contentClaimOffset;
                    if (currentReadClaimStream != null && currentReadClaimStream.getBytesConsumed() <= resourceClaimOffset) {
                        final long bytesToSkip = resourceClaimOffset - currentReadClaimStream.getBytesConsumed();
                        if (bytesToSkip > 0) {
                            StreamUtils.skip(currentReadClaimStream, bytesToSkip);
                        }

                        final InputStream limitingInputStream = new LimitingInputStream(new DisableOnCloseInputStream(currentReadClaimStream), flowFile.getSize());
                        final ContentClaimInputStream contentClaimInputStream = new ContentClaimInputStream(context.getContentRepository(), claim,
                            contentClaimOffset, limitingInputStream, performanceTracker);
                        return contentClaimInputStream;
                    }
                }

                claimCache.flush(claim);

                if (currentReadClaimStream != null) {
                    currentReadClaimStream.close();
                }

                currentReadClaim = claim.getResourceClaim();

                performanceTracker.beginContentRead();
                final InputStream contentRepoStream = context.getContentRepository().read(claim.getResourceClaim());
                performanceTracker.endContentRead();

                final InputStream performanceTrackInputStream = new PerformanceTrackingInputStream(contentRepoStream, performanceTracker);
                StreamUtils.skip(performanceTrackInputStream, claim.getOffset() + contentClaimOffset);
                final InputStream bufferedContentStream = new BufferedInputStream(contentRepoStream);
                final ByteCountingInputStream byteCountingInputStream = new ByteCountingInputStream(bufferedContentStream, claim.getOffset() + contentClaimOffset);
                currentReadClaimStream = byteCountingInputStream;

                // Use a non-closeable stream (DisableOnCloseInputStream) because we want to keep it open after the callback has finished so that we can
                // reuse the same InputStream for the next FlowFile. We then need to use a LimitingInputStream to ensure that we don't allow the InputStream
                // to be read past the end of the FlowFile (since multiple FlowFiles' contents may be in the given Resource Claim Input Stream).
                // Finally, we need to wrap the InputStream in a ContentClaimInputStream so that if mark/reset is used, we can provide that capability
                // without buffering data in memory.
                final InputStream limitingInputStream = new LimitingInputStream(new DisableOnCloseInputStream(currentReadClaimStream), flowFile.getSize());
                final ContentClaimInputStream contentClaimInputStream = new ContentClaimInputStream(context.getContentRepository(), claim, contentClaimOffset, limitingInputStream, performanceTracker);
                return contentClaimInputStream;
            } else {
                claimCache.flush(claim);

                final InputStream rawInStream = new ContentClaimInputStream(context.getContentRepository(), claim, contentClaimOffset, performanceTracker);
                return rawInStream;
            }
        } catch (final ContentNotFoundException cnfe) {
            throw cnfe;
        } catch (final EOFException eof) {
            throw new ContentNotFoundException(claim, eof);
        } catch (final IOException ioe) {
            throw new FlowFileAccessException("Failed to read content of " + flowFile, ioe);
        }
    }

    @Override
    public void read(FlowFile source, final InputStreamCallback reader) {
        verifyTaskActive();

        source = validateRecordState(source, true);
        final StandardRepositoryRecord record = getRecord(source);

        try {
            ensureNotAppending(record.getCurrentClaim());
            claimCache.flush(record.getCurrentClaim());
        } catch (final IOException e) {
            throw new FlowFileAccessException("Failed to access ContentClaim for " + source.toString(), e);
        }

        try (final InputStream rawIn = getInputStream(source, record.getCurrentClaim(), record.getCurrentClaimOffset(), true);
            final InputStream limitedIn = new LimitedInputStream(rawIn, source.getSize());
            final InputStream disableOnCloseIn = new DisableOnCloseInputStream(limitedIn);
            final ByteCountingInputStream countingStream = new ByteCountingInputStream(disableOnCloseIn, this.bytesRead)) {

            // We want to differentiate between IOExceptions thrown by the repository and IOExceptions thrown from
            // Processor code. As a result, as have the FlowFileAccessInputStream that catches IOException from the repository
            // and translates into either FlowFileAccessException or ContentNotFoundException. We keep track of any
            // ContentNotFoundException because if it is thrown, the Processor code may catch it and do something else with it
            // but in reality, if it is thrown, we want to know about it and handle it, even if the Processor code catches it.
            final FlowFileAccessInputStream ffais = new FlowFileAccessInputStream(countingStream, source, record.getCurrentClaim());
            boolean cnfeThrown = false;

            try {
                incrementReadCount(source);
                reader.process(createTaskTerminationStream(ffais));
            } catch (final ContentNotFoundException cnfe) {
                cnfeThrown = true;
                throw cnfe;
            } finally {
                decrementReadCount(source);
                bytesRead += countingStream.getBytesRead();

                // if cnfeThrown is true, we don't need to re-thrown the Exception; it will propagate.
                if (!cnfeThrown && ffais.getContentNotFoundException() != null) {
                    throw ffais.getContentNotFoundException();
                }
            }

        } catch (final ContentNotFoundException nfe) {
            handleContentNotFound(nfe, record);
        } catch (final IOException ex) {
            throw new ProcessException("IOException thrown from " + connectableDescription + ": " + ex.toString(), ex);
        }
    }

    @Override
    public InputStream read(FlowFile source) {
        verifyTaskActive();

        source = validateRecordState(source, true);
        final StandardRepositoryRecord record = getRecord(source);

        try {
            final ContentClaim currentClaim = record.getCurrentClaim();
            ensureNotAppending(currentClaim);
            claimCache.flush(currentClaim);
        } catch (final IOException e) {
            throw new FlowFileAccessException("Failed to access ContentClaim for " + source.toString(), e);
        }

        final InputStream rawIn;
        try {
            rawIn = getInputStream(source, record.getCurrentClaim(), record.getCurrentClaimOffset(), true);
        } catch (final ContentNotFoundException nfe) {
            handleContentNotFound(nfe, record);
            throw nfe;
        }

        final InputStream limitedIn = new LimitedInputStream(rawIn, source.getSize());
        final ByteCountingInputStream countingStream = new ByteCountingInputStream(limitedIn);
        final FlowFileAccessInputStream ffais = new FlowFileAccessInputStream(countingStream, source, record.getCurrentClaim());

        final FlowFile sourceFlowFile = source;
        final InputStream errorHandlingStream = new InputStream() {
            private boolean closed = false;

            @Override
            public int read() throws IOException {
                try {
                    return ffais.read();
                } catch (final ContentNotFoundException cnfe) {
                    close();
                    handleContentNotFound(cnfe, record);
                    throw cnfe;
                } catch (final FlowFileAccessException ffae) {
                    LOG.error("Failed to read content from {}; rolling back session", sourceFlowFile, ffae);
                    close();
                    rollback(true);
                    throw ffae;
                }
            }

            @Override
            public int read(final byte[] b) throws IOException {
                return read(b, 0, b.length);
            }

            @Override
            public int read(final byte[] b, final int off, final int len) throws IOException {
                try {
                    return ffais.read(b, off, len);
                } catch (final ContentNotFoundException cnfe) {
                    close();
                    handleContentNotFound(cnfe, record);
                    throw cnfe;
                } catch (final FlowFileAccessException ffae) {
                    LOG.error("Failed to read content from {}; rolling back session", sourceFlowFile, ffae);
                    close();
                    rollback(true);
                    throw ffae;
                }
            }

            @Override
            public void close() throws IOException {
                decrementReadCount(sourceFlowFile);

                if (!closed) {
                    StandardProcessSession.this.bytesRead += countingStream.getBytesRead();
                    closed = true;
                }

                ffais.close();
                openInputStreams.remove(sourceFlowFile);
            }

            @Override
            public int available() throws IOException {
                return ffais.available();
            }

            @Override
            public long skip(long n) throws IOException {
                return ffais.skip(n);
            }

            @Override
            public boolean markSupported() {
                return ffais.markSupported();
            }

            @Override
            public synchronized void mark(int readlimit) {
                ffais.mark(readlimit);
            }

            @Override
            public synchronized void reset() throws IOException {
                ffais.reset();
            }

            @Override
            public String toString() {
                return "ErrorHandlingInputStream[FlowFile=" + sourceFlowFile + "]";
            }
        };

        incrementReadCount(sourceFlowFile);
        openInputStreams.put(sourceFlowFile, errorHandlingStream);

        return createTaskTerminationStream(errorHandlingStream);
    }

    private InputStream createTaskTerminationStream(final InputStream delegate) {
        return new TaskTerminationInputStream(delegate, taskTermination, () -> rollback(false, true));
    }

    private OutputStream createTaskTerminationStream(final OutputStream delegate) {
        return new TaskTerminationOutputStream(delegate, taskTermination, () -> rollback(false, true));
    }

    private void incrementReadCount(final FlowFile flowFile) {
        readRecursionSet.compute(flowFile, (ff, count) -> count == null ? 1 : count + 1);
    }

    private void decrementReadCount(final FlowFile flowFile) {
        final Integer count = readRecursionSet.get(flowFile);
        if (count == null) {
            return;
        }

        final int updatedCount = count - 1;
        if (updatedCount == 0) {
            readRecursionSet.remove(flowFile);
        } else {
            readRecursionSet.put(flowFile, updatedCount);
        }
    }

    @Override
    public FlowFile merge(final Collection<FlowFile> sources, final FlowFile destination) {
        verifyTaskActive();

        return merge(sources, destination, null, null, null);
    }

    @Override
    public FlowFile merge(Collection<FlowFile> sources, FlowFile destination, final byte[] header, final byte[] footer, final byte[] demarcator) {
        verifyTaskActive();

        sources = validateRecordState(sources);
        destination = validateRecordState(destination);
        if (sources.contains(destination)) {
            throw new IllegalArgumentException("Destination cannot be within sources");
        }

        final Collection<StandardRepositoryRecord> sourceRecords = new ArrayList<>();
        for (final FlowFile source : sources) {
            final StandardRepositoryRecord record = getRecord(source);
            sourceRecords.add(record);

            try {
                ensureNotAppending(record.getCurrentClaim());
                claimCache.flush(record.getCurrentClaim());
            } catch (final IOException e) {
                throw new FlowFileAccessException("Unable to read from source " + source + " due to " + e.toString(), e);
            }
        }

        final StandardRepositoryRecord destinationRecord = getRecord(destination);
        final ContentRepository contentRepo = context.getContentRepository();
        final ContentClaim newClaim;
        try {
            newClaim = contentRepo.create(context.getConnectable().isLossTolerant());
            claimLog.debug("Creating ContentClaim {} for 'merge' for {}", newClaim, destinationRecord.getCurrent());
        } catch (final IOException e) {
            throw new FlowFileAccessException("Unable to create ContentClaim due to " + e.toString(), e);
        }

        long readCount = 0L;
        long writtenCount = 0L;

        try {
            try (final OutputStream rawOut = contentRepo.write(newClaim);
                final OutputStream out = new BufferedOutputStream(rawOut)) {

                if (header != null && header.length > 0) {
                    out.write(header);
                    writtenCount += header.length;
                }

                int objectIndex = 0;
                final boolean useDemarcator = demarcator != null && demarcator.length > 0;
                final int numSources = sources.size();
                for (final FlowFile source : sources) {
                    final StandardRepositoryRecord sourceRecord = getRecord(source);

                    final long copied = contentRepo.exportTo(sourceRecord.getCurrentClaim(), out, sourceRecord.getCurrentClaimOffset(), source.getSize());
                    writtenCount += copied;
                    readCount += copied;

                    // don't add demarcator after the last claim
                    if (useDemarcator && ++objectIndex < numSources) {
                        out.write(demarcator);
                        writtenCount += demarcator.length;
                    }
                }

                if (footer != null && footer.length > 0) {
                    out.write(footer);
                    writtenCount += footer.length;
                }
            } finally {
                bytesWritten += writtenCount;
                bytesRead += readCount;
            }
        } catch (final ContentNotFoundException nfe) {
            destroyContent(newClaim, destinationRecord);
            handleContentNotFound(nfe, destinationRecord);
            handleContentNotFound(nfe, sourceRecords);
        } catch (final IOException ioe) {
            destroyContent(newClaim, destinationRecord);
            throw new FlowFileAccessException("Failed to merge " + sources.size() + " into " + destination + " due to " + ioe.toString(), ioe);
        } catch (final Throwable t) {
            destroyContent(newClaim, destinationRecord);
            throw t;
        }

        removeTemporaryClaim(destinationRecord);
        final FlowFileRecord newFile = new StandardFlowFileRecord.Builder()
            .fromFlowFile(destinationRecord.getCurrent())
            .contentClaim(newClaim)
            .contentClaimOffset(0L)
            .size(writtenCount)
            .build();
        destinationRecord.setWorking(newFile, true);
        return newFile;
    }

    private void ensureNotAppending(final ContentClaim claim) throws IOException {
        if (claim == null) {
            return;
        }

        final ByteCountingOutputStream outStream = appendableStreams.remove(claim);
        if (outStream == null) {
            return;
        }

        outStream.flush();
        outStream.close();
    }

    @Override
    public OutputStream write(FlowFile source) {
        verifyTaskActive();
        source = validateRecordState(source);
        final StandardRepositoryRecord record = getRecord(source);

        ContentClaim newClaim = null;
        try {
            newClaim = claimCache.getContentClaim();
            claimLog.debug("Creating ContentClaim {} for 'write' for {}", newClaim, source);
            ensureNotAppending(newClaim);

            final OutputStream rawStream = claimCache.write(newClaim);
            final OutputStream nonFlushable = new NonFlushableOutputStream(rawStream);
            final OutputStream disableOnClose = new DisableOnCloseOutputStream(nonFlushable);
            final ByteCountingOutputStream countingOut = new ByteCountingOutputStream(disableOnClose);

            final FlowFile sourceFlowFile = source;
            final ContentClaim updatedClaim = newClaim;
            final OutputStream errorHandlingOutputStream = new OutputStream() {
                private boolean closed = false;

                @Override
                public void write(final int b) throws IOException {
                    try {
                        countingOut.write(b);
                    } catch (final IOException ioe) {
                        LOG.error("Failed to write content to {}; rolling back session", sourceFlowFile, ioe);
                        rollback(true);
                        close();
                        throw new FlowFileAccessException("Failed to write to Content Repository for " + sourceFlowFile, ioe);
                    }
                }

                @Override
                public void write(final byte[] b) throws IOException {
                    try {
                        countingOut.write(b);
                    } catch (final IOException ioe) {
                        LOG.error("Failed to write content to {}; rolling back session", sourceFlowFile, ioe);
                        rollback(true);
                        close();
                        throw new FlowFileAccessException("Failed to write to Content Repository for " + sourceFlowFile, ioe);
                    }
                }

                @Override
                public void write(final byte[] b, final int off, final int len) throws IOException {
                    try {
                        countingOut.write(b, off, len);
                    } catch (final IOException ioe) {
                        LOG.error("Failed to write content to {}; rolling back session", sourceFlowFile, ioe);
                        rollback(true);
                        close();
                        throw new FlowFileAccessException("Failed to write to Content Repository for " + sourceFlowFile, ioe);
                    }
                }

                @Override
                public void flush() throws IOException {
                    try {
                        countingOut.flush();
                    } catch (final IOException ioe) {
                        LOG.error("Failed to write content to {}; rolling back session", sourceFlowFile, ioe);
                        rollback(true);
                        close();
                        throw new FlowFileAccessException("Failed to write to Content Repository for " + sourceFlowFile, ioe);
                    }
                }

                @Override
                public void close() throws IOException {
                    if (closed) {
                        return;
                    }

                    closed = true;

                    countingOut.close();
                    rawStream.close();
                    writeRecursionSet.remove(sourceFlowFile);

                    final long bytesWritten = countingOut.getBytesWritten();
                    StandardProcessSession.this.bytesWritten += bytesWritten;

                    final OutputStream removed = openOutputStreams.remove(sourceFlowFile);
                    if (removed == null) {
                        LOG.error("Closed Session's OutputStream but there was no entry for it in the map; sourceFlowFile={}; map={}", sourceFlowFile, openOutputStreams);
                    }

                    flush();
                    removeTemporaryClaim(record);

                    final FlowFileRecord newFile;
                    if (bytesWritten == 0) {
                        newFile = new StandardFlowFileRecord.Builder()
                            .fromFlowFile(record.getCurrent())
                            .contentClaim(null)
                            .contentClaimOffset(0)
                            .size(bytesWritten)
                            .build();

                        context.getContentRepository().decrementClaimantCount(updatedClaim);
                        record.addTransientClaim(updatedClaim);
                    } else {
                        newFile = new StandardFlowFileRecord.Builder()
                            .fromFlowFile(record.getCurrent())
                            .contentClaim(updatedClaim)
                            .contentClaimOffset(Math.max(0, updatedClaim.getLength() - bytesWritten))
                            .size(bytesWritten)
                            .build();
                    }

                    record.setWorking(newFile, true);
                }
            };

            writeRecursionSet.add(source);
            openOutputStreams.put(source, errorHandlingOutputStream);
            return createTaskTerminationStream(errorHandlingOutputStream);
        } catch (final ContentNotFoundException nfe) {
            resetWriteClaims(); // need to reset write claim before we can remove the claim
            destroyContent(newClaim, record);
            handleContentNotFound(nfe, record);
            throw nfe;
        } catch (final IOException ioe) {
            resetWriteClaims(); // need to reset write claim before we can remove the claim
            destroyContent(newClaim, record);
            throw new ProcessException("IOException thrown from " + connectableDescription + ": " + ioe.toString(), ioe);
        } catch (final Throwable t) {
            resetWriteClaims(); // need to reset write claim before we can remove the claim
            destroyContent(newClaim, record);
            throw t;
        }
    }

    @Override
    public FlowFile write(FlowFile source, final OutputStreamCallback writer) {
        verifyTaskActive();
        source = validateRecordState(source);
        final StandardRepositoryRecord record = getRecord(source);

        long writtenToFlowFile = 0L;
        ContentClaim newClaim = null;
        try {
            newClaim = claimCache.getContentClaim();
            claimLog.debug("Creating ContentClaim {} for 'write' for {}", newClaim, source);

            ensureNotAppending(newClaim);
            try (final OutputStream stream = claimCache.write(newClaim);
                final NonFlushableOutputStream nonFlushableOutputStream = new NonFlushableOutputStream(stream);
                final OutputStream disableOnClose = new DisableOnCloseOutputStream(nonFlushableOutputStream);
                final ByteCountingOutputStream countingOut = new ByteCountingOutputStream(disableOnClose)) {
                try {
                    writeRecursionSet.add(source);
                    final OutputStream ffaos = new FlowFileAccessOutputStream(countingOut, source);
                    writer.process(createTaskTerminationStream(ffaos));
                } finally {
                    writtenToFlowFile = countingOut.getBytesWritten();
                    bytesWritten += countingOut.getBytesWritten();
                }
            } finally {
                writeRecursionSet.remove(source);
            }
        } catch (final ContentNotFoundException nfe) {
            resetWriteClaims(); // need to reset write claim before we can remove the claim
            destroyContent(newClaim, record);
            handleContentNotFound(nfe, record);
        } catch (final IOException ioe) {
            resetWriteClaims(); // need to reset write claim before we can remove the claim
            destroyContent(newClaim, record);
            throw new ProcessException("IOException thrown from " + connectableDescription + ": " + ioe.toString(), ioe);
        } catch (final Throwable t) {
            resetWriteClaims(); // need to reset write claim before we can remove the claim
            destroyContent(newClaim, record);
            throw t;
        }

        removeTemporaryClaim(record);
        final FlowFileRecord newFile;
        if (writtenToFlowFile == 0) {
            newFile = new StandardFlowFileRecord.Builder()
                .fromFlowFile(record.getCurrent())
                .contentClaim(null)
                .contentClaimOffset(0)
                .size(0)
                .build();

            context.getContentRepository().decrementClaimantCount(newClaim);
            record.addTransientClaim(newClaim);
        } else {
            newFile = new StandardFlowFileRecord.Builder()
                .fromFlowFile(record.getCurrent())
                .contentClaim(newClaim)
                .contentClaimOffset(Math.max(0, newClaim.getLength() - writtenToFlowFile))
                .size(writtenToFlowFile)
                .build();
        }

        record.setWorking(newFile, true);
        return newFile;
    }


    @Override
    public FlowFile append(FlowFile source, final OutputStreamCallback writer) {
        verifyTaskActive();

        source = validateRecordState(source);
        final StandardRepositoryRecord record = getRecord(source);
        long newSize = 0L;

        // Get the current Content Claim from the record and see if we already have
        // an OutputStream that we can append to.
        final ContentClaim oldClaim = record.getCurrentClaim();
        ByteCountingOutputStream outStream = oldClaim == null ? null : appendableStreams.get(oldClaim);
        long originalByteWrittenCount = 0;

        ContentClaim newClaim = null;
        try {
            if (outStream == null) {
                claimCache.flush(oldClaim);

                try (final InputStream oldClaimIn = read(source)) {
                    newClaim = context.getContentRepository().create(context.getConnectable().isLossTolerant());
                    claimLog.debug("Creating ContentClaim {} for 'append' for {}", newClaim, source);

                    final OutputStream rawOutStream = context.getContentRepository().write(newClaim);
                    final OutputStream bufferedOutStream = new BufferedOutputStream(rawOutStream);
                    outStream = new ByteCountingOutputStream(bufferedOutStream);
                    originalByteWrittenCount = 0;

                    appendableStreams.put(newClaim, outStream);

                    // We need to copy all of the data from the old claim to the new claim
                    StreamUtils.copy(oldClaimIn, outStream);

                    // Don't allow flushing of the BufferedOutputStream. The callback may well call wrap our stream in another object that needs to be flushed.
                    // This is OK, but append() is often used many times to append just a small bit of data, over & over. If we allow flushing of our buffered output stream
                    // each time, performance suffers. Instead, we prevent the flushing at this level, and we flush in commit.
                    final NonFlushableOutputStream nonFlushable = new NonFlushableOutputStream(outStream);

                    // Wrap our OutputStreams so that the processor cannot close it
                    try (final OutputStream disableOnClose = new DisableOnCloseOutputStream(nonFlushable)) {
                        writeRecursionSet.add(source);
                        writer.process(new FlowFileAccessOutputStream(disableOnClose, source));
                    } finally {
                        writeRecursionSet.remove(source);
                    }
                }
            } else {
                newClaim = oldClaim;
                originalByteWrittenCount = outStream.getBytesWritten();

                // Don't allow flushing of the BufferedOutputStream. The callback may well call wrap our stream in another object that needs to be flushed.
                // This is OK, but append() is often used many times to append just a small bit of data, over & over. If we allow flushing of our buffered output stream
                // each time, performance suffers. Instead, we prevent the flushing at this level, and we flush in commit.
                final NonFlushableOutputStream nonFlushable = new NonFlushableOutputStream(outStream);

                // Wrap our OutputStreams so that the processor cannot close it
                try (final OutputStream disableOnClose = new DisableOnCloseOutputStream(nonFlushable);
                    final OutputStream flowFileAccessOutStream = new FlowFileAccessOutputStream(disableOnClose, source)) {
                    writeRecursionSet.add(source);
                    writer.process(flowFileAccessOutStream);
                } finally {
                    writeRecursionSet.remove(source);
                }
            }

            // update the newSize to reflect the number of bytes written
            newSize = outStream.getBytesWritten();
        } catch (final ContentNotFoundException nfe) {
            resetWriteClaims(); // need to reset write claim before we can remove the claim

            // If the content claim changed, then we should destroy the new one. We do this
            // because the new content claim will never get set as the 'working claim' for the FlowFile
            // record since we will throw an Exception. As a result, we need to ensure that we have
            // appropriately decremented the claimant count and can destroy the content if it is no
            // longer in use. However, it is critical that we do this ONLY if the content claim has
            // changed. Otherwise, the FlowFile already has a reference to this Content Claim and
            // whenever the FlowFile is removed, the claim count will be decremented; if we decremented
            // it here also, we would be decrementing the claimant count twice!
            if (newClaim != oldClaim) {
                destroyContent(newClaim, record);
            }

            handleContentNotFound(nfe, record);
        } catch (final IOException ioe) {
            resetWriteClaims(); // need to reset write claim before we can remove the claim

            // See above explanation for why this is done only if newClaim != oldClaim
            if (newClaim != oldClaim) {
                destroyContent(newClaim, record);
            }

            throw new ProcessException("IOException thrown from " + connectableDescription + ": " + ioe.toString(), ioe);
        } catch (final Throwable t) {
            resetWriteClaims(); // need to reset write claim before we can remove the claim

            // See above explanation for why this is done only if newClaim != oldClaim
            if (newClaim != oldClaim) {
                destroyContent(newClaim, record);
            }

            throw t;
        } finally {
            if (outStream != null) {
                final long bytesWrittenThisIteration = outStream.getBytesWritten() - originalByteWrittenCount;
                bytesWritten += bytesWrittenThisIteration;
            }
        }

        // If the record already has a working claim, and this is the first time that we are appending to the FlowFile,
        // destroy the current working claim because it is a temporary claim that
        // is no longer going to be used, as we are about to set a new working claim. This would happen, for instance, if
        // the FlowFile was written to, via #write() and then append() was called.
        if (newClaim != oldClaim) {
            removeTemporaryClaim(record);
        }

        final FlowFileRecord newFile;
        if (newSize == 0) {
            newFile = new StandardFlowFileRecord.Builder()
                .fromFlowFile(record.getCurrent())
                .contentClaim(null)
                .contentClaimOffset(0)
                .size(0)
                .build();

            context.getContentRepository().decrementClaimantCount(newClaim);
            record.addTransientClaim(newClaim);
        } else {
            newFile = new StandardFlowFileRecord.Builder()
                .fromFlowFile(record.getCurrent())
                .contentClaim(newClaim)
                .contentClaimOffset(0)
                .size(newSize)
                .build();
        }

        record.setWorking(newFile, true);
        return newFile;
    }

    /**
     * Checks if the ContentClaim associated with this record should be removed,
     * since the record is about to be updated to point to a new content claim.
     * If so, removes the working claim.
     *
     * This happens if & only if the content of this FlowFile has been modified
     * since it was last committed to the FlowFile repository, because this
     * indicates that the content is no longer needed and should be cleaned up.
     *
     * @param record record
     */
    private void removeTemporaryClaim(final StandardRepositoryRecord record) {
        // If the content of the FlowFile has already been modified, we need to remove the newly created content (the working claim). However, if
        // they are the same, we cannot just remove the claim because record.getWorkingClaim() will return
        // the original claim if the record is "working" but the content has not been modified
        // (e.g., in the case of attributes only were updated).
        //
        // In other words:
        // If we modify the attributes of a FlowFile, and then we call record.getWorkingClaim(), this will
        // return the same claim as record.getOriginalClaim(). So we cannot just remove the working claim because
        // that may decrement the original claim (because the 2 claims are the same), and that's NOT what we want to do
        // because we will do that later, in the session.commit() and that would result in decrementing the count for
        // the original claim twice.
        //
        // Additionally, If the Record Type is CREATE, that means any content that exists is a temporary claim.
        // This will happen, for example, if a FlowFile is created and then written to multiple times or a FlowFile is cloned and then the clone's contents
        // are overwritten. In such a case, we can confidently decrement the count/remove the claim because either the claim is null (in which case calling
        // decrementClaimantCount/addTransientClaim will have no effect, or the claim points to the previously written content that is no longer relevant because
        // the FlowFile's content is being overwritten.

        if (record.isContentModified() || record.getType() == RepositoryRecordType.CREATE) {
            // In this case, it's ok to decrement the claimant count for the content because we know that the working claim is going to be
            // updated and the given working claim is referenced only by FlowFiles in this session (because it's the Working Claim).
            // Therefore, we need to decrement the claimant count, and since the Working Claim is being changed, that means that
            // the Working Claim is a transient claim (the content need not be persisted because no FlowFile refers to it). We cannot simply
            // remove the content because there may be other FlowFiles that reference the same Resource Claim. Marking the Content Claim as
            // transient, though, will result in the FlowFile Repository cleaning up as appropriate.
            context.getContentRepository().decrementClaimantCount(record.getWorkingClaim());
            record.addTransientClaim(record.getWorkingClaim());
        }
    }

    private void resetWriteClaims() {
        resetWriteClaims(true);
    }

    private void resetWriteClaims(final boolean suppressExceptions) {
        for (final ByteCountingOutputStream out : appendableStreams.values()) {
            try {
                try {
                    out.flush();
                } finally {
                    out.close();
                }
            } catch (final IOException e) {
                if (!suppressExceptions) {
                    throw new FlowFileAccessException("Unable to flush the output of FlowFile to the Content Repository");
                }
            }
        }
        appendableStreams.clear();
    }

    private void resetReadClaim() {
        try {
            if (currentReadClaimStream != null) {
                currentReadClaimStream.close();
            }
        } catch (final Exception ignored) {
        }
        currentReadClaimStream = null;
        currentReadClaim = null;
    }


    @Override
    public FlowFile write(FlowFile source, final StreamCallback writer) {
        verifyTaskActive();
        source = validateRecordState(source);
        final StandardRepositoryRecord record = getRecord(source);
        final ContentClaim currClaim = record.getCurrentClaim();

        long writtenToFlowFile = 0L;
        ContentClaim newClaim = null;
        try {
            newClaim = claimCache.getContentClaim();
            claimLog.debug("Creating ContentClaim {} for 'write' for {}", newClaim, source);

            ensureNotAppending(newClaim);

            if (currClaim != null) {
                claimCache.flush(currClaim.getResourceClaim());
            }

            try (final InputStream is = getInputStream(source, currClaim, record.getCurrentClaimOffset(), true);
                final InputStream limitedIn = new LimitedInputStream(is, source.getSize());
                final InputStream disableOnCloseIn = new DisableOnCloseInputStream(limitedIn);
                final ByteCountingInputStream countingIn = new ByteCountingInputStream(disableOnCloseIn, bytesRead);
                final OutputStream os = claimCache.write(newClaim);
                final OutputStream nonFlushableOut = new NonFlushableOutputStream(os);
                final OutputStream disableOnCloseOut = new DisableOnCloseOutputStream(nonFlushableOut);
                final ByteCountingOutputStream countingOut = new ByteCountingOutputStream(disableOnCloseOut)) {

                writeRecursionSet.add(source);

                // We want to differentiate between IOExceptions thrown by the repository and IOExceptions thrown from
                // Processor code. As a result, as have the FlowFileAccessInputStream that catches IOException from the repository
                // and translates into either FlowFileAccessException or ContentNotFoundException. We keep track of any
                // ContentNotFoundException because if it is thrown, the Processor code may catch it and do something else with it
                // but in reality, if it is thrown, we want to know about it and handle it, even if the Processor code catches it.
                final FlowFileAccessInputStream ffais = new FlowFileAccessInputStream(countingIn, source, currClaim);
                final FlowFileAccessOutputStream ffaos = new FlowFileAccessOutputStream(countingOut, source);
                boolean cnfeThrown = false;

                try {
                    writer.process(createTaskTerminationStream(ffais), createTaskTerminationStream(ffaos));
                } catch (final ContentNotFoundException cnfe) {
                    cnfeThrown = true;
                    throw cnfe;
                } finally {
                    writtenToFlowFile = countingOut.getBytesWritten();
                    this.bytesWritten += writtenToFlowFile;
                    this.bytesRead += countingIn.getBytesRead();
                    writeRecursionSet.remove(source);

                    // if cnfeThrown is true, we don't need to re-thrown the Exception; it will propagate.
                    if (!cnfeThrown && ffais.getContentNotFoundException() != null) {
                        throw ffais.getContentNotFoundException();
                    }
                }
            }
        } catch (final ContentNotFoundException nfe) {
            destroyContent(newClaim, record);
            handleContentNotFound(nfe, record);
        } catch (final IOException ioe) {
            destroyContent(newClaim, record);
            throw new ProcessException("IOException thrown from " + connectableDescription + ": " + ioe.toString(), ioe);
        } catch (final Throwable t) {
            destroyContent(newClaim, record);
            throw t;
        }

        removeTemporaryClaim(record);
        final FlowFileRecord newFile;

        if (writtenToFlowFile == 0) {
            newFile = new StandardFlowFileRecord.Builder()
                .fromFlowFile(record.getCurrent())
                .contentClaim(null)
                .contentClaimOffset(0)
                .size(0)
                .build();

            context.getContentRepository().decrementClaimantCount(newClaim);
            record.addTransientClaim(newClaim);
        } else {
            newFile = new StandardFlowFileRecord.Builder()
                .fromFlowFile(record.getCurrent())
                .contentClaim(newClaim)
                .contentClaimOffset(Math.max(0L, newClaim.getLength() - writtenToFlowFile))
                .size(writtenToFlowFile)
                .build();
        }

        record.setWorking(newFile, true);

        return newFile;
    }

    @Override
    public FlowFile importFrom(final Path source, final boolean keepSourceFile, FlowFile destination) {
        verifyTaskActive();

        destination = validateRecordState(destination);
        // TODO: find a better solution. With Windows 7 and Java 7 (very early update, at least), Files.isWritable(source.getParent()) returns false, even when it should be true.
        if (!keepSourceFile && !Files.isWritable(source.getParent()) && !source.getParent().toFile().canWrite()) {
            // If we do NOT want to keep the file, ensure that we can delete it, or else error.
            throw new FlowFileAccessException("Cannot write to path " + source.getParent().toFile().getAbsolutePath() + " so cannot delete file; will not import.");
        }

        final StandardRepositoryRecord record = getRecord(destination);

        final ContentClaim newClaim;
        final long claimOffset;

        try {
            newClaim = context.getContentRepository().create(context.getConnectable().isLossTolerant());
            claimLog.debug("Creating ContentClaim {} for 'importFrom' for {}", newClaim, destination);
        } catch (final IOException e) {
            throw new FlowFileAccessException("Unable to create ContentClaim due to " + e.toString(), e);
        }

        claimOffset = 0L;
        final long newSize;
        try {
            newSize = context.getContentRepository().importFrom(source, newClaim);
            bytesWritten += newSize;
            bytesRead += newSize;
        } catch (final Throwable t) {
            destroyContent(newClaim, record);
            throw new FlowFileAccessException("Failed to import data from " + source + " for " + destination + " due to " + t.toString(), t);
        }

        removeTemporaryClaim(record);

        final FlowFileRecord newFile;
        if (newSize == 0) {
            newFile = new StandardFlowFileRecord.Builder()
                .fromFlowFile(record.getCurrent())
                .contentClaim(null)
                .contentClaimOffset(0)
                .size(0)
                .addAttribute(CoreAttributes.FILENAME.key(), source.toFile().getName())
                .build();

            context.getContentRepository().decrementClaimantCount(newClaim);
            record.addTransientClaim(newClaim);
        } else {
            newFile = new StandardFlowFileRecord.Builder()
                .fromFlowFile(record.getCurrent())
                .contentClaim(newClaim)
                .contentClaimOffset(claimOffset)
                .size(newSize)
                .addAttribute(CoreAttributes.FILENAME.key(), source.toFile().getName())
                .build();
        }

        record.setWorking(newFile, CoreAttributes.FILENAME.key(), source.toFile().getName(), true);

        if (!keepSourceFile) {
            deleteOnCommit.put(newFile, source);
        }

        return newFile;
    }

    @Override
    public FlowFile importFrom(final InputStream source, FlowFile destination) {
        verifyTaskActive();

        destination = validateRecordState(destination);
        final StandardRepositoryRecord record = getRecord(destination);
        ContentClaim newClaim = null;
        final long claimOffset = 0L;

        final long newSize;
        try {
            try {
                newClaim = context.getContentRepository().create(context.getConnectable().isLossTolerant());
                claimLog.debug("Creating ContentClaim {} for 'importFrom' for {}", newClaim, destination);

                newSize = context.getContentRepository().importFrom(createTaskTerminationStream(source), newClaim);
                bytesWritten += newSize;
            } catch (final IOException e) {
                throw new FlowFileAccessException("Unable to create ContentClaim due to " + e.toString(), e);
            }
        } catch (final Throwable t) {
            if (newClaim != null) {
                destroyContent(newClaim, record);
            }

            throw new FlowFileAccessException("Failed to import data from " + source + " for " + destination + " due to " + t.toString(), t);
        }

        removeTemporaryClaim(record);
        final FlowFileRecord newFile;
        if (newSize == 0) {
            newFile = new StandardFlowFileRecord.Builder()
                .fromFlowFile(record.getCurrent())
                .contentClaim(null)
                .contentClaimOffset(0)
                .size(0)
                .build();

            context.getContentRepository().decrementClaimantCount(newClaim);
            record.addTransientClaim(newClaim);
        } else {
            newFile = new StandardFlowFileRecord.Builder()
                .fromFlowFile(record.getCurrent())
                .contentClaim(newClaim)
                .contentClaimOffset(claimOffset)
                .size(newSize)
                .build();
        }

        record.setWorking(newFile, true);
        return newFile;
    }

    @Override
    public void exportTo(FlowFile source, final Path destination, final boolean append) {
        verifyTaskActive();
        source = validateRecordState(source);
        final StandardRepositoryRecord record = getRecord(source);
        try {
            ensureNotAppending(record.getCurrentClaim());

            claimCache.flush(record.getCurrentClaim());
            final long copyCount = context.getContentRepository().exportTo(record.getCurrentClaim(), destination, append, record.getCurrentClaimOffset(), source.getSize());
            bytesRead += copyCount;
        } catch (final ContentNotFoundException nfe) {
            handleContentNotFound(nfe, record);
        } catch (final Throwable t) {
            throw new FlowFileAccessException("Failed to export " + source + " to " + destination + " due to " + t.toString(), t);
        }
    }

    @Override
    public void exportTo(FlowFile source, final OutputStream destination) {
        verifyTaskActive();
        source = validateRecordState(source);
        final StandardRepositoryRecord record = getRecord(source);

        if (record.getCurrentClaim() == null) {
            return;
        }

        try {
            ensureNotAppending(record.getCurrentClaim());
            claimCache.flush(record.getCurrentClaim());
        } catch (final IOException e) {
            throw new FlowFileAccessException("Failed to access ContentClaim for " + source.toString(), e);
        }

        try (final InputStream rawIn = getInputStream(source, record.getCurrentClaim(), record.getCurrentClaimOffset(), true);
                final InputStream limitedIn = new LimitedInputStream(rawIn, source.getSize());
                final InputStream disableOnCloseIn = new DisableOnCloseInputStream(limitedIn);
                final ByteCountingInputStream countingStream = new ByteCountingInputStream(disableOnCloseIn, this.bytesRead)) {

            // We want to differentiate between IOExceptions thrown by the repository and IOExceptions thrown from
            // Processor code. As a result, as have the FlowFileAccessInputStream that catches IOException from the repository
            // and translates into either FlowFileAccessException or ContentNotFoundException. We keep track of any
            // ContentNotFoundException because if it is thrown, the Processor code may catch it and do something else with it
            // but in reality, if it is thrown, we want to know about it and handle it, even if the Processor code catches it.
            try (final FlowFileAccessInputStream ffais = new FlowFileAccessInputStream(countingStream, source, record.getCurrentClaim())) {
                boolean cnfeThrown = false;

                try {
                    incrementReadCount(source);
                    StreamUtils.copy(ffais, createTaskTerminationStream(destination), source.getSize());
                } catch (final ContentNotFoundException cnfe) {
                    cnfeThrown = true;
                    throw cnfe;
                } finally {
                    decrementReadCount(source);
                    final long streamBytesRead = countingStream.getBytesRead();
                    bytesRead += streamBytesRead;

                    // if cnfeThrown is true, we don't need to re-throw the Exception; it will propagate.
                    if (!cnfeThrown && ffais.getContentNotFoundException() != null) {
                        throw ffais.getContentNotFoundException();
                    }
                }
            }
        } catch (final ContentNotFoundException nfe) {
            handleContentNotFound(nfe, record);
        } catch (final IOException ex) {
            throw new ProcessException("IOException thrown from " + connectableDescription + ": " + ex.toString(), ex);
        }
    }

    private void handleContentNotFound(final ContentNotFoundException nfe, final Collection<StandardRepositoryRecord> suspectRecords) {
        for (final StandardRepositoryRecord record : suspectRecords) {
            handleContentNotFound(nfe, record);
        }
    }

    private void handleContentNotFound(final ContentNotFoundException nfe, final StandardRepositoryRecord suspectRecord) {
        final ContentClaim registeredClaim = suspectRecord.getOriginalClaim();
        final ContentClaim transientClaim = suspectRecord.getWorkingClaim();
        final ContentClaim missingClaim = nfe.getMissingClaim();

        final ProvenanceEventRecord dropEvent = provenanceReporter.drop(suspectRecord.getCurrent(), nfe.getMessage() == null ? "Content Not Found" : nfe.getMessage());
        if (dropEvent != null) {
            context.getProvenanceRepository().registerEvent(dropEvent);
        }

        if (missingClaim == registeredClaim) {
            suspectRecord.markForAbort();
            rollback();
            throw new MissingFlowFileException("Unable to find content for FlowFile", nfe);
        }

        if (missingClaim == transientClaim) {
            rollback();
            throw new MissingFlowFileException("Unable to find content for FlowFile", nfe);
        }
    }

    private FlowFile validateRecordState(final FlowFile flowFile) {
        return validateRecordState(flowFile, false);
    }

    private FlowFile validateRecordState(final FlowFile flowFile, final boolean allowRecursiveRead) {
        if (!allowRecursiveRead && readRecursionSet.containsKey(flowFile)) {
            throw new IllegalStateException(flowFile + " already in use for an active callback or an InputStream created by ProcessSession.read(FlowFile) has not been closed");
        }
        if (writeRecursionSet.contains(flowFile)) {
            throw new IllegalStateException(flowFile + " already in use for an active callback or an OutputStream created by ProcessSession.write(FlowFile) has not been closed");
        }

        final StandardRepositoryRecord record = getRecord(flowFile);
        if (record == null) {
            rollback();
            throw new FlowFileHandlingException(flowFile + " is not known in this session (" + toString() + ")");
        }
        if (record.getTransferRelationship() != null) {
            rollback();
            throw new FlowFileHandlingException(flowFile + " is already marked for transfer");
        }
        if (record.isMarkedForDelete()) {
            rollback();
            throw new FlowFileHandlingException(flowFile + " has already been marked for removal");
        }

        return record.getCurrent();
    }

    private List<FlowFile> validateRecordState(final Collection<FlowFile> flowFiles) {
        final List<FlowFile> current = new ArrayList<>(flowFiles.size());
        for (final FlowFile flowFile : flowFiles) {
            current.add(validateRecordState(flowFile));
        }
        return current;
    }

    /**
     * Checks if a FlowFile is known in this session.
     *
     * @param flowFile the FlowFile to check
     * @return <code>true</code> if the FlowFile is known in this session,
     *         <code>false</code> otherwise.
     */
    boolean isFlowFileKnown(final FlowFile flowFile) {
        return records.containsKey(flowFile.getId());
    }

    private FlowFile getMostRecent(final FlowFile flowFile) {
        final StandardRepositoryRecord existingRecord = getRecord(flowFile);
        return existingRecord == null ? flowFile : existingRecord.getCurrent();
    }


    /**
     * Returns the attributes that are common to every FlowFile given. The key
     * and value must match exactly.
     *
     * @param flowFileList a list of FlowFiles
     *
     * @return the common attributes
     */
    private static Map<String, String> intersectAttributes(final Collection<FlowFile> flowFileList) {
        final Map<String, String> result = new HashMap<>();
        // trivial cases
        if (flowFileList == null || flowFileList.isEmpty()) {
            return result;
        } else if (flowFileList.size() == 1) {
            result.putAll(flowFileList.iterator().next().getAttributes());
        }

        /*
         * Start with the first attribute map and only put an entry to the
         * resultant map if it is common to every map.
         */
        final Map<String, String> firstMap = flowFileList.iterator().next().getAttributes();

        outer: for (final Map.Entry<String, String> mapEntry : firstMap.entrySet()) {
            final String key = mapEntry.getKey();
            final String value = mapEntry.getValue();
            for (final FlowFile flowFile : flowFileList) {
                final Map<String, String> currMap = flowFile.getAttributes();
                final String curVal = currMap.get(key);
                if (curVal == null || !curVal.equals(value)) {
                    continue outer;
                }
            }
            result.put(key, value);
        }

        return result;
    }

    @Override
    public ProvenanceReporter getProvenanceReporter() {
        verifyTaskActive();
        return provenanceReporter;
    }

    @Override
    public void setState(final Map<String, String> state, final Scope scope) throws IOException {
        final Optional<String> currentVersion = getState(scope).getStateVersion();
        final String version = currentVersion.map(this::getIncrementedVersion).orElse(INITIAL_VERSION);
        final StateMap stateMap = new StandardStateMap(state, Optional.of(version));
        setState(stateMap, scope);
    }

    private void setState(final StateMap stateMap, final Scope scope) {
        if (scope == Scope.LOCAL) {
            localState = stateMap;
        } else {
            clusterState = stateMap;
        }
    }

    @Override
    public StateMap getState(final Scope scope) throws IOException {
        if (scope == Scope.LOCAL) {
            if (localState != null) {
                return localState;
            }

            if (checkpoint != null && checkpoint.localState != null) {
                return checkpoint.localState;
            }

            // If no state is held locally, get it from the State Manager.
            return context.getStateManager().getState(scope);
        }

        if (clusterState != null) {
            return clusterState;
        }

        if (checkpoint != null && checkpoint.clusterState != null) {
            return checkpoint.clusterState;
        }

        return context.getStateManager().getState(scope);
    }

    @Override
    public boolean replaceState(final StateMap oldValue, final Map<String, String> newValue, final Scope scope) throws IOException {
        final StateMap current = getState(scope);
        if (!current.getStateVersion().isPresent() && (oldValue == null || !oldValue.getStateVersion().isPresent())) {
            final StateMap stateMap = new StandardStateMap(newValue, Optional.of(INITIAL_VERSION));
            setState(stateMap, scope);
            return true;
        }

        if (oldValue == null) {
            return false;
        }

        if (current.getStateVersion().equals(oldValue.getStateVersion()) && current.toMap().equals(oldValue.toMap())) {
            final String version = current.getStateVersion().map(this::getIncrementedVersion).orElse(INITIAL_VERSION);
            final StateMap stateMap = new StandardStateMap(newValue, Optional.of(version));
            setState(stateMap, scope);
            return true;
        }

        return false;
    }

    @Override
    public void clearState(final Scope scope) {
        setState(EMPTY_STATE_MAP, scope);
    }

    @Override
    public String toString() {
        return "StandardProcessSession[id=" + sessionId + "]";
    }

    private String getIncrementedVersion(final String currentVersion) {
        final long versionNumber = Long.parseLong(currentVersion);
        final long version = versionNumber + VERSION_INCREMENT;
        return String.valueOf(version);
    }

    /**
     * Callback interface used to poll a FlowFileQueue, in order to perform
     * functional programming-type of polling a queue
     */
    private static interface ConnectionPoller {

        List<FlowFileRecord> poll(Connection connection, Set<FlowFileRecord> expiredRecords);
    }

    protected static class Checkpoint {

        private long processingTime = 0L;

        private Map<FlowFile, List<ProvenanceEventRecord>> generatedProvenanceEvents;
        private Map<FlowFile, ProvenanceEventBuilder> forkEventBuilders;
        private List<ProvenanceEventRecord> autoTerminatedEvents;
        private Set<ProvenanceEventRecord> reportedEvents;

        private Map<Long, StandardRepositoryRecord> records;
        private Map<String, StandardFlowFileEvent> connectionCounts;

        private Map<String, Long> countersOnCommit;
        private Map<String, Long> immediateCounters;

        private Map<FlowFile, Path> deleteOnCommit;
        private Set<String> removedFlowFiles;
        private Set<String> createdFlowFiles;

        private int removedCount = 0; // number of flowfiles removed in this session
        private long removedBytes = 0L; // size of all flowfiles removed in this session
        private long bytesRead = 0L;
        private long bytesWritten = 0L;
        private int flowFilesIn = 0, flowFilesOut = 0;
        private long contentSizeIn = 0L, contentSizeOut = 0L;
        private int flowFilesReceived = 0, flowFilesSent = 0;
        private long bytesReceived = 0L, bytesSent = 0L;

        private boolean initialized = false;
        private StateMap localState;
        private StateMap clusterState;

        private void initializeForCopy() {
            if (initialized) {
                return;
            }

            generatedProvenanceEvents = new HashMap<>();
            forkEventBuilders = new HashMap<>();
            autoTerminatedEvents = new ArrayList<>();
            reportedEvents = new LinkedHashSet<>();

            records = new ConcurrentHashMap<>();
            connectionCounts = new ConcurrentHashMap<>();

            countersOnCommit = new HashMap<>();
            immediateCounters = new HashMap<>();

            deleteOnCommit = new HashMap<>();
            removedFlowFiles = new HashSet<>();
            createdFlowFiles = new HashSet<>();

            initialized = true;
        }

        private void checkpoint(final StandardProcessSession session, final List<ProvenanceEventRecord> autoTerminatedEvents, final boolean copy) {
            if (copy) {
                copyCheckpoint(session, autoTerminatedEvents);
            } else {
                directCheckpoint(session, autoTerminatedEvents);
            }
        }

        /**
         * Checkpoints the Process Session by adding references to the existing collections within the Process Session. Any modifications made to the Checkpoint's
         * Collections or the Process Session's collections will be reflected by the other. I.e., this is a copy-by-reference.
         */
        private void directCheckpoint(final StandardProcessSession session, final List<ProvenanceEventRecord> autoTerminatedEvents) {
            this.processingTime = System.nanoTime() - session.processingStartTime;

            this.generatedProvenanceEvents = session.generatedProvenanceEvents;
            this.forkEventBuilders = session.forkEventBuilders;
            this.autoTerminatedEvents = autoTerminatedEvents;
            this.reportedEvents = session.provenanceReporter.getEvents();

            this.records = session.records;

            this.connectionCounts = session.connectionCounts;
            this.countersOnCommit = session.countersOnCommit == null ? Collections.emptyMap() : session.countersOnCommit;
            this.immediateCounters = session.immediateCounters == null ? Collections.emptyMap() : session.immediateCounters;

            this.deleteOnCommit = session.deleteOnCommit;
            this.removedFlowFiles = session.removedFlowFiles;
            this.createdFlowFiles = session.createdFlowFiles;

            this.removedCount = session.removedCount;
            this.removedBytes = session.removedBytes;
            this.bytesRead = session.bytesRead;
            this.bytesWritten = session.bytesWritten;
            this.flowFilesIn = session.flowFilesIn;
            this.flowFilesOut = session.flowFilesOut;
            this.contentSizeIn = session.contentSizeIn;
            this.contentSizeOut = session.contentSizeOut;
            this.flowFilesReceived = session.provenanceReporter.getFlowFilesReceived() + session.provenanceReporter.getFlowFilesFetched();
            this.bytesReceived = session.provenanceReporter.getBytesReceived() + session.provenanceReporter.getBytesFetched();
            this.flowFilesSent = session.provenanceReporter.getFlowFilesSent();
            this.bytesSent = session.provenanceReporter.getBytesSent();

            if (session.localState != null) {
                this.localState = session.localState;
            }
            if (session.clusterState != null) {
                this.clusterState = session.clusterState;
            }
        }

        /**
         * Checkpoints the Process Session by copying all information from the session's collections into this Checkpoint's collections.
         * This is necessary if multiple Process Sessions are to be batched together. I.e., this is a copy-by-value
         */
        private void copyCheckpoint(final StandardProcessSession session, final List<ProvenanceEventRecord> autoTerminatedEvents) {
            initializeForCopy();

            this.processingTime += System.nanoTime() - session.processingStartTime;

            this.generatedProvenanceEvents.putAll(session.generatedProvenanceEvents);
            this.forkEventBuilders.putAll(session.forkEventBuilders);
            if (autoTerminatedEvents != null) {
                this.autoTerminatedEvents.addAll(autoTerminatedEvents);
            }
            this.reportedEvents.addAll(session.provenanceReporter.getEvents());

            this.records.putAll(session.records);

            mergeMapsWithMutableValue(this.connectionCounts, session.connectionCounts, (destination, toMerge) -> destination.add(toMerge));
            mergeMaps(this.countersOnCommit, session.countersOnCommit, Long::sum);
            mergeMaps(this.immediateCounters, session.immediateCounters, Long::sum);

            this.deleteOnCommit.putAll(session.deleteOnCommit);
            this.removedFlowFiles.addAll(session.removedFlowFiles);
            this.createdFlowFiles.addAll(session.createdFlowFiles);

            this.removedCount += session.removedCount;
            this.removedBytes += session.removedBytes;
            this.bytesRead += session.bytesRead;
            this.bytesWritten += session.bytesWritten;
            this.flowFilesIn += session.flowFilesIn;
            this.flowFilesOut += session.flowFilesOut;
            this.contentSizeIn += session.contentSizeIn;
            this.contentSizeOut += session.contentSizeOut;
            this.flowFilesReceived += session.provenanceReporter.getFlowFilesReceived() + session.provenanceReporter.getFlowFilesFetched();
            this.bytesReceived += session.provenanceReporter.getBytesReceived() + session.provenanceReporter.getBytesFetched();
            this.flowFilesSent += session.provenanceReporter.getFlowFilesSent();
            this.bytesSent += session.provenanceReporter.getBytesSent();

            if (session.localState != null) {
                this.localState = session.localState;
            }
            if (session.clusterState != null) {
                this.clusterState = session.clusterState;
            }
        }

        private <K, V> void mergeMaps(final Map<K, V> destination, final Map<K, V> toMerge, final BiFunction<? super V, ? super V, ? extends V> merger) {
            if (toMerge == null) {
                return;
            }

            if (destination.isEmpty()) {
                destination.putAll(toMerge);
            } else {
                toMerge.forEach((key, value) -> destination.merge(key, value, merger));
            }
        }

        private <K, V> void mergeMapsWithMutableValue(final Map<K, V> destination, final Map<K, V> toMerge, final BiConsumer<? super V, ? super V> merger) {
            if (toMerge == null) {
                return;
            }

            if (destination.isEmpty()) {
                destination.putAll(toMerge);
                return;
            }

            for (final Map.Entry<K, V> entry : toMerge.entrySet()) {
                final K key = entry.getKey();
                final V value = entry.getValue();

                final V destinationValue = destination.get(key);
                if (destinationValue == null) {
                    destination.put(key, value);
                } else {
                    merger.accept(destinationValue, value);
                }
            }
        }

        private StandardRepositoryRecord getRecord(final FlowFile flowFile) {
            return records.get(flowFile.getId());
        }

        public int getFlowFilesIn() {
            return flowFilesIn;
        }

        public int getFlowFilesOut() {
            return flowFilesOut;
        }

        public int getFlowFilesRemoved() {
            return removedCount;
        }

        public long getBytesIn() {
            return contentSizeIn;
        }

        public long getBytesOut() {
            return contentSizeOut;
        }

        public long getBytesRemoved() {
            return removedBytes;
        }
    }

    private static class FlowFileLinkage {
        private final Map<Long, List<Long>> linkedIds = new HashMap<>();

        public void addLink(final long id, final long other) {
            if (id == other) {
                return;
            }

            linkedIds.computeIfAbsent(id, key -> new ArrayList<>()).add(other);
            linkedIds.computeIfAbsent(other, key -> new ArrayList<>()).add(id);
        }

        public Collection<Long> getLinkedIds(final long id) {
            final List<Long> linked = linkedIds.get(id);
            final Set<Long> allLinked = new HashSet<>();
            if (linked != null) {
                allLinked.addAll(linked);

                for (final Long linkedId : linked) {
                    final List<Long> onceRemoved = linkedIds.get(linkedId);
                    if (onceRemoved != null) {
                        allLinked.addAll(onceRemoved);
                    }
                }
            }
            return allLinked;
        }


        public void clear() {
            linkedIds.clear();
        }
    }
}
