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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.controller.repository.claim.ContentClaimWriteCache;
import org.apache.nifi.controller.repository.claim.ResourceClaim;
import org.apache.nifi.controller.repository.io.DisableOnCloseInputStream;
import org.apache.nifi.controller.repository.io.DisableOnCloseOutputStream;
import org.apache.nifi.controller.repository.io.FlowFileAccessInputStream;
import org.apache.nifi.controller.repository.io.FlowFileAccessOutputStream;
import org.apache.nifi.controller.repository.io.LimitedInputStream;
import org.apache.nifi.controller.repository.metrics.StandardFlowFileEvent;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.FlowFileFilter;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.FlowFileAccessException;
import org.apache.nifi.processor.exception.FlowFileHandlingException;
import org.apache.nifi.processor.exception.MissingFlowFileException;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.provenance.ProvenanceEventBuilder;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventRepository;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.provenance.ProvenanceReporter;
import org.apache.nifi.provenance.StandardProvenanceEventRecord;
import org.apache.nifi.stream.io.ByteCountingInputStream;
import org.apache.nifi.stream.io.ByteCountingOutputStream;
import org.apache.nifi.stream.io.StreamUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public final class StandardProcessSession implements ProcessSession, ProvenanceEventEnricher {

    private static final AtomicLong idGenerator = new AtomicLong(0L);
    private static final AtomicLong enqueuedIndex = new AtomicLong(0L);

    // determines how many things must be transferred, removed, modified in order to avoid logging the FlowFile ID's on commit/rollback
    public static final int VERBOSE_LOG_THRESHOLD = 10;
    public static final String DEFAULT_FLOWFILE_PATH = "./";

    private static final Logger LOG = LoggerFactory.getLogger(StandardProcessSession.class);
    private static final Logger claimLog = LoggerFactory.getLogger(StandardProcessSession.class.getSimpleName() + ".claims");
    private static final int MAX_ROLLBACK_FLOWFILES_TO_LOG = 5;

    private final Map<FlowFileRecord, StandardRepositoryRecord> records = new HashMap<>();
    private final Map<String, StandardFlowFileEvent> connectionCounts = new HashMap<>();
    private final Map<FlowFileQueue, Set<FlowFileRecord>> unacknowledgedFlowFiles = new HashMap<>();
    private final Map<ContentClaim, ByteCountingOutputStream> appendableStreams = new HashMap<>();
    private final ProcessContext context;
    private final Map<FlowFile, Integer> readRecursionSet = new HashMap<>();// set used to track what is currently being operated on to prevent logic failures if recursive calls occurring
    private final Set<FlowFile> writeRecursionSet = new HashSet<>();
    private final Map<FlowFile, Path> deleteOnCommit = new HashMap<>();
    private final long sessionId;
    private final String connectableDescription;

    private Map<String, Long> countersOnCommit;
    private Map<String, Long> immediateCounters;

    private final Set<String> removedFlowFiles = new HashSet<>();
    private final Set<String> createdFlowFiles = new HashSet<>();

    private final StandardProvenanceReporter provenanceReporter;

    private int removedCount = 0; // number of flowfiles removed in this session
    private long removedBytes = 0L; // size of all flowfiles removed in this session
    private long bytesRead = 0L;
    private long bytesWritten = 0L;
    private int flowFilesIn = 0, flowFilesOut = 0;
    private long contentSizeIn = 0L, contentSizeOut = 0L;

    private ContentClaim currentReadClaim = null;
    private ByteCountingInputStream currentReadClaimStream = null;
    private long processingStartTime;

    // List of InputStreams that have been opened by calls to {@link #read(FlowFile)} and not yet closed
    private final Map<FlowFile, InputStream> openInputStreams = new HashMap<>();
    // List of OutputStreams that have been opened by calls to {@link #write(FlowFile)} and not yet closed
    private final Map<FlowFile, OutputStream> openOutputStreams = new HashMap<>();

    // maps a FlowFile to all Provenance Events that were generated for that FlowFile.
    // we do this so that if we generate a Fork event, for example, and then remove the event in the same
    // Session, we will not send that event to the Provenance Repository
    private final Map<FlowFile, List<ProvenanceEventRecord>> generatedProvenanceEvents = new HashMap<>();

    // when Forks are generated for a single parent, we add the Fork event to this map, with the Key being the parent
    // so that we are able to aggregate many into a single Fork Event.
    private final Map<FlowFile, ProvenanceEventBuilder> forkEventBuilders = new HashMap<>();

    private Checkpoint checkpoint = new Checkpoint();
    private final ContentClaimWriteCache claimCache;

    public StandardProcessSession(final ProcessContext context) {
        this.context = context;

        final Connectable connectable = context.getConnectable();
        final String componentType;

        String description = connectable.toString();
        switch (connectable.getConnectableType()) {
            case PROCESSOR:
                final ProcessorNode procNode = (ProcessorNode) connectable;
                componentType = procNode.getComponentType();
                description = procNode.getProcessor().toString();
                break;
            case INPUT_PORT:
                componentType = "Input Port";
                break;
            case OUTPUT_PORT:
                componentType = "Output Port";
                break;
            case REMOTE_INPUT_PORT:
                componentType = ProvenanceEventRecord.REMOTE_INPUT_PORT_TYPE;
                break;
            case REMOTE_OUTPUT_PORT:
                componentType = ProvenanceEventRecord.REMOTE_OUTPUT_PORT_TYPE;
                break;
            case FUNNEL:
                componentType = "Funnel";
                break;
            default:
                throw new AssertionError("Connectable type is " + connectable.getConnectableType());
        }

        this.provenanceReporter = new StandardProvenanceReporter(this, connectable.getIdentifier(), componentType,
            context.getProvenanceRepository(), this);
        this.sessionId = idGenerator.getAndIncrement();
        this.connectableDescription = description;
        this.claimCache = new ContentClaimWriteCache(context.getContentRepository());
        LOG.trace("Session {} created for {}", this, connectableDescription);
        processingStartTime = System.nanoTime();
    }

    private void closeStreams(final Map<FlowFile, ? extends Closeable> streamMap, final String action, final String streamType) {
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

        resetWriteClaims(false);

        closeStreams(openInputStreams, "committed", "input");
        closeStreams(openOutputStreams, "committed", "output");

        if (!readRecursionSet.isEmpty()) {
            throw new IllegalStateException();
        }
        if (!writeRecursionSet.isEmpty()) {
            throw new IllegalStateException();
        }

        if (this.checkpoint == null) {
            this.checkpoint = new Checkpoint();
        }

        if (records.isEmpty()) {
            LOG.trace("{} checkpointed, but no events were performed by this ProcessSession", this);
            return;
        }

        // any drop event that is the result of an auto-terminate should happen at the very end, so we keep the
        // records in a separate List so that they can be persisted to the Provenance Repo after all of the
        // Processor-reported events.
        List<ProvenanceEventRecord> autoTerminatedEvents = null;

        // validate that all records have a transfer relationship for them and if so determine the destination node and clone as necessary
        final Map<FlowFileRecord, StandardRepositoryRecord> toAdd = new HashMap<>();
        for (final StandardRepositoryRecord record : records.values()) {
            if (record.isMarkedForDelete()) {
                continue;
            }
            final Relationship relationship = record.getTransferRelationship();
            if (relationship == null) {
                rollback();
                throw new FlowFileHandlingException(record.getCurrent() + " transfer relationship not specified");
            }
            final List<Connection> destinations = new ArrayList<>(context.getConnections(relationship));
            if (destinations.isEmpty() && !context.getConnectable().isAutoTerminated(relationship)) {
                if (relationship != Relationship.SELF) {
                    rollback();
                    throw new FlowFileHandlingException(relationship + " does not have any destinations for " + context.getConnectable());
                }
            }

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
                    LOG.warn("Unable to generate Provenance Event for {} on behalf of {} due to {}", record.getCurrent(), connectableDescription, e);
                    if (LOG.isDebugEnabled()) {
                        LOG.warn("", e);
                    }
                }
            } else {
                final Connection finalDestination = destinations.remove(destinations.size() - 1); // remove last element
                record.setDestination(finalDestination.getFlowFileQueue());
                incrementConnectionInputCounts(finalDestination, record);

                for (final Connection destination : destinations) { // iterate over remaining destinations and "clone" as needed
                    incrementConnectionInputCounts(destination, record);
                    final FlowFileRecord currRec = record.getCurrent();
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
                    newRecord.setWorking(clone, Collections.<String, String> emptyMap());

                    newRecord.setDestination(destination.getFlowFileQueue());
                    newRecord.setTransferRelationship(record.getTransferRelationship());
                    // put the mapping into toAdd because adding to records now will cause a ConcurrentModificationException
                    toAdd.put(clone, newRecord);
                }
            }
        }

        records.putAll(toAdd);
        toAdd.clear();

        checkpoint.checkpoint(this, autoTerminatedEvents);
        resetState();
    }

    @Override
    public void commit() {
        checkpoint();
        commit(this.checkpoint);
        this.checkpoint = null;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void commit(final Checkpoint checkpoint) {
        try {
            final long commitStartNanos = System.nanoTime();

            resetReadClaim();
            try {
                claimCache.flush();
            } finally {
                claimCache.reset();
            }

            final long updateProvenanceStart = System.nanoTime();
            updateProvenanceRepo(checkpoint);

            final long claimRemovalStart = System.nanoTime();
            final long updateProvenanceNanos = claimRemovalStart - updateProvenanceStart;

            /**
             * Figure out which content claims can be released. At this point,
             * we will decrement the Claimant Count for the claims via the
             * Content Repository. We do not actually destroy the content
             * because otherwise, we could remove the Original Claim and
             * crash/restart before the FlowFileRepository is updated. This will
             * result in the FlowFile being restored such that the content claim
             * points to the Original Claim -- which has already been removed!
             *
             */
            for (final Map.Entry<FlowFileRecord, StandardRepositoryRecord> entry : checkpoint.records.entrySet()) {
                final FlowFile flowFile = entry.getKey();
                final StandardRepositoryRecord record = entry.getValue();

                if (record.isMarkedForDelete()) {
                    // if the working claim is not the same as the original claim, we can immediately destroy the working claim
                    // because it was created in this session and is to be deleted. We don't need to wait for the FlowFile Repo to sync.
                    decrementClaimCount(record.getWorkingClaim());

                    if (record.getOriginalClaim() != null && !record.getOriginalClaim().equals(record.getWorkingClaim())) {
                        // if working & original claim are same, don't remove twice; we only want to remove the original
                        // if it's different from the working. Otherwise, we remove two claimant counts. This causes
                        // an issue if we only updated the FlowFile attributes.
                        decrementClaimCount(record.getOriginalClaim());
                    }
                    final long flowFileLife = System.currentTimeMillis() - flowFile.getEntryDate();
                    final Connectable connectable = context.getConnectable();
                    final Object terminator = connectable instanceof ProcessorNode ? ((ProcessorNode) connectable).getProcessor() : connectable;
                    LOG.info("{} terminated by {}; life of FlowFile = {} ms", new Object[] {flowFile, terminator, flowFileLife});
                } else if (record.isWorking() && record.getWorkingClaim() != record.getOriginalClaim()) {
                    // records which have been updated - remove original if exists
                    decrementClaimCount(record.getOriginalClaim());
                }
            }

            final long claimRemovalFinishNanos = System.nanoTime();
            final long claimRemovalNanos = claimRemovalFinishNanos - claimRemovalStart;

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
            final long flowFileRepoUpdateNanos = flowFileRepoUpdateFinishNanos - claimRemovalFinishNanos;

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

            if (LOG.isInfoEnabled()) {
                final String sessionSummary = summarizeEvents(checkpoint);
                if (!sessionSummary.isEmpty()) {
                    LOG.info("{} for {}, committed the following events: {}", new Object[] {this, connectableDescription, sessionSummary});
                }
            }

            for (final Map.Entry<String, Long> entry : checkpoint.countersOnCommit.entrySet()) {
                context.adjustCounter(entry.getKey(), entry.getValue());
            }

            acknowledgeRecords();
            resetState();

            if (LOG.isDebugEnabled()) {
                final StringBuilder timingInfo = new StringBuilder();
                timingInfo.append("Session commit for ").append(this).append(" [").append(connectableDescription).append("]").append(" took ");

                final long commitNanos = System.nanoTime() - commitStartNanos;
                formatNanos(commitNanos, timingInfo);
                timingInfo.append("; FlowFile Repository Update took ");
                formatNanos(flowFileRepoUpdateNanos, timingInfo);
                timingInfo.append("; Claim Removal took ");
                formatNanos(claimRemovalNanos, timingInfo);
                timingInfo.append("; FlowFile Event Update took ");
                formatNanos(updateEventRepositoryNanos, timingInfo);
                timingInfo.append("; Enqueuing FlowFiles took ");
                formatNanos(enqueueFlowFileNanos, timingInfo);
                timingInfo.append("; Updating Provenance Event Repository took ");
                formatNanos(updateProvenanceNanos, timingInfo);

                LOG.debug(timingInfo.toString());
            }
        } catch (final Exception e) {
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
        }
    }

    private void updateEventRepository(final Checkpoint checkpoint) {
        int flowFilesReceived = 0;
        int flowFilesSent = 0;
        long bytesReceived = 0L;
        long bytesSent = 0L;

        for (final ProvenanceEventRecord event : checkpoint.reportedEvents) {
            if (isSpuriousForkEvent(event, checkpoint.removedFlowFiles)) {
                continue;
            }

            switch (event.getEventType()) {
                case SEND:
                    flowFilesSent++;
                    bytesSent += event.getFileSize();
                    break;
                case RECEIVE:
                case FETCH:
                    flowFilesReceived++;
                    bytesReceived += event.getFileSize();
                    break;
                default:
                    break;
            }
        }

        try {
            // update event repository
            final Connectable connectable = context.getConnectable();
            final StandardFlowFileEvent flowFileEvent = new StandardFlowFileEvent(connectable.getIdentifier());
            flowFileEvent.setBytesRead(checkpoint.bytesRead);
            flowFileEvent.setBytesWritten(checkpoint.bytesWritten);
            flowFileEvent.setContentSizeIn(checkpoint.contentSizeIn);
            flowFileEvent.setContentSizeOut(checkpoint.contentSizeOut);
            flowFileEvent.setContentSizeRemoved(checkpoint.removedBytes);
            flowFileEvent.setFlowFilesIn(checkpoint.flowFilesIn);
            flowFileEvent.setFlowFilesOut(checkpoint.flowFilesOut);
            flowFileEvent.setFlowFilesRemoved(checkpoint.removedCount);
            flowFileEvent.setFlowFilesReceived(flowFilesReceived);
            flowFileEvent.setBytesReceived(bytesReceived);
            flowFileEvent.setFlowFilesSent(flowFilesSent);
            flowFileEvent.setBytesSent(bytesSent);

            long lineageMillis = 0L;
            for (final Map.Entry<FlowFileRecord, StandardRepositoryRecord> entry : checkpoint.records.entrySet()) {
                final FlowFile flowFile = entry.getKey();
                final long lineageDuration = System.currentTimeMillis() - flowFile.getLineageStartDate();
                lineageMillis += lineageDuration;
            }
            flowFileEvent.setAggregateLineageMillis(lineageMillis);

            final Map<String, Long> counters = combineCounters(checkpoint.countersOnCommit, checkpoint.immediateCounters);
            flowFileEvent.setCounters(counters);

            context.getFlowFileEventRepository().updateRepository(flowFileEvent);

            for (final FlowFileEvent connectionEvent : checkpoint.connectionCounts.values()) {
                context.getFlowFileEventRepository().updateRepository(connectionEvent);
            }
        } catch (final IOException ioe) {
            LOG.error("FlowFile Event Repository failed to update", ioe);
        }
    }

    private Map<String, Long> combineCounters(final Map<String, Long> first, final Map<String, Long> second) {
        if (first == null && second == null) {
            return null;
        }
        if (first == null) {
            return second;
        }
        if (second == null) {
            return first;
        }

        final Map<String, Long> combined = new HashMap<>();
        combined.putAll(first);
        combined.putAll(second);
        return combined;
    }

    private void addEventType(final Map<String, Set<ProvenanceEventType>> map, final String id, final ProvenanceEventType eventType) {
        Set<ProvenanceEventType> eventTypes = map.get(id);
        if (eventTypes == null) {
            eventTypes = new HashSet<>();
            map.put(id, eventTypes);
        }

        eventTypes.add(eventType);
    }

    private void updateProvenanceRepo(final Checkpoint checkpoint) {
        // Update Provenance Repository
        final ProvenanceEventRepository provenanceRepo = context.getProvenanceRepository();

        // We need to de-dupe the events that we've created and those reported to the provenance reporter,
        // in case the Processor developer submitted the same events to the reporter. So we use a LinkedHashSet
        // for this, so that we are able to ensure that the events are submitted in the proper order.
        final Set<ProvenanceEventRecord> recordsToSubmit = new LinkedHashSet<>();
        final Map<String, Set<ProvenanceEventType>> eventTypesPerFlowFileId = new HashMap<>();

        final Set<ProvenanceEventRecord> processorGenerated = checkpoint.reportedEvents;

        // We first want to submit FORK events because if the Processor is going to create events against
        // a FlowFile, that FlowFile needs to be shown to be created first.
        // However, if the Processor has generated a FORK event, we don't want to use the Framework-created one --
        // we prefer to use the event generated by the Processor. We can determine this by checking if the Set of events genereated
        // by the Processor contains any of the FORK events that we generated
        for (final Map.Entry<FlowFile, ProvenanceEventBuilder> entry : checkpoint.forkEventBuilders.entrySet()) {
            final ProvenanceEventBuilder builder = entry.getValue();
            final FlowFile flowFile = entry.getKey();

            updateEventContentClaims(builder, flowFile, checkpoint.records.get(flowFile));
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
        }

        // Finally, add any other events that we may have generated.
        for (final List<ProvenanceEventRecord> eventList : checkpoint.generatedProvenanceEvents.values()) {
            for (final ProvenanceEventRecord event : eventList) {
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

            boolean contentChanged = false;
            if (original == null && current != null) {
                contentChanged = true;
            }
            if (original != null && current == null) {
                contentChanged = true;
            }
            if (original != null && current != null && !original.equals(current)) {
                contentChanged = true;
            }

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
                final Set<ProvenanceEventType> registeredTypes = eventTypesPerFlowFileId.get(flowFileId);
                boolean creationEventRegistered = false;
                if (registeredTypes != null) {
                    if (registeredTypes.contains(ProvenanceEventType.CREATE)
                        || registeredTypes.contains(ProvenanceEventType.FORK)
                        || registeredTypes.contains(ProvenanceEventType.JOIN)
                        || registeredTypes.contains(ProvenanceEventType.RECEIVE)
                        || registeredTypes.contains(ProvenanceEventType.FETCH)) {
                        creationEventRegistered = true;
                    }
                }

                if (!creationEventRegistered) {
                    recordsToSubmit.add(provenanceReporter.build(curFlowFile, ProvenanceEventType.CREATE).build());
                    eventAdded = true;
                }
            }

            if (!eventAdded && !repoRecord.getUpdatedAttributes().isEmpty()) {
                // We generate an ATTRIBUTES_MODIFIED event only if no other event has been
                // created for the FlowFile. We do this because all events contain both the
                // newest and the original attributes, so generating an ATTRIBUTES_MODIFIED
                // event is redundant if another already exists.
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

        final List<ProvenanceEventRecord> autoTermEvents = checkpoint.autoTerminatedEvents;
        final Iterable<ProvenanceEventRecord> iterable = new Iterable<ProvenanceEventRecord>() {
            final Iterator<ProvenanceEventRecord> recordsToSubmitIterator = recordsToSubmit.iterator();
            final Iterator<ProvenanceEventRecord> autoTermIterator = autoTermEvents == null ? null : autoTermEvents.iterator();

            @Override
            public Iterator<ProvenanceEventRecord> iterator() {
                return new Iterator<ProvenanceEventRecord>() {
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
                            return enrich(rawEvent, flowFileRecordMap, checkpoint.records, rawEvent.getEventType() != ProvenanceEventType.SEND);
                        } else if (autoTermIterator != null && autoTermIterator.hasNext()) {
                            return enrich(autoTermIterator.next(), flowFileRecordMap, checkpoint.records, true);
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
    public StandardProvenanceEventRecord enrich(final ProvenanceEventRecord rawEvent, final FlowFile flowFile) {
        final StandardRepositoryRecord repoRecord = records.get(flowFile);
        if (repoRecord == null) {
            throw new FlowFileHandlingException(flowFile + " is not known in this session (" + toString() + ")");
        }

        final StandardProvenanceEventRecord.Builder recordBuilder = new StandardProvenanceEventRecord.Builder().fromEvent(rawEvent);
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
        return recordBuilder.build();
    }

    private StandardProvenanceEventRecord enrich(
        final ProvenanceEventRecord rawEvent, final Map<String, FlowFileRecord> flowFileRecordMap, final Map<FlowFileRecord, StandardRepositoryRecord> records, final boolean updateAttributes) {
        final StandardProvenanceEventRecord.Builder recordBuilder = new StandardProvenanceEventRecord.Builder().fromEvent(rawEvent);
        final FlowFileRecord eventFlowFile = flowFileRecordMap.get(rawEvent.getFlowFileUuid());
        if (eventFlowFile != null) {
            final StandardRepositoryRecord repoRecord = records.get(eventFlowFile);

            if (repoRecord.getCurrent() != null && repoRecord.getCurrentClaim() != null) {
                final ContentClaim currentClaim = repoRecord.getCurrentClaim();
                final long currentOffset = repoRecord.getCurrentClaimOffset();
                final long size = eventFlowFile.getSize();

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
        }

        if (updateAttributes) {
            final FlowFileRecord flowFileRecord = flowFileRecordMap.get(rawEvent.getFlowFileUuid());
            if (flowFileRecord != null) {
                final StandardRepositoryRecord record = records.get(flowFileRecord);
                if (record != null) {
                    recordBuilder.setAttributes(record.getOriginalAttributes(), record.getUpdatedAttributes());
                }
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
    private boolean isSpuriousRouteEvent(final ProvenanceEventRecord event, final Map<FlowFileRecord, StandardRepositoryRecord> records) {
        if (event.getEventType() == ProvenanceEventType.ROUTE) {
            final String relationshipName = event.getRelationship();
            final Relationship relationship = new Relationship.Builder().name(relationshipName).build();
            final Collection<Connection> connectionsForRelationship = this.context.getConnections(relationship);

            // If the number of connections for this relationship is not 1, then we can't ignore this ROUTE event,
            // as it may be cloning the FlowFile and adding to multiple connections.
            if (connectionsForRelationship.size() == 1) {
                for (final Map.Entry<FlowFileRecord, StandardRepositoryRecord> entry : records.entrySet()) {
                    final FlowFileRecord flowFileRecord = entry.getKey();
                    if (event.getFlowFileUuid().equals(flowFileRecord.getAttribute(CoreAttributes.UUID.key()))) {
                        final StandardRepositoryRecord repoRecord = entry.getValue();
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
    }

    private void rollback(final boolean penalize, final boolean rollbackCheckpoint) {
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

        final Set<StandardRepositoryRecord> recordsToHandle = new HashSet<>();
        recordsToHandle.addAll(records.values());
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
                if (record.getCurrentClaim() != null && !record.getCurrentClaim().equals(record.getWorkingClaim())) {
                    // if working & original claim are same, don't remove twice; we only want to remove the original
                    // if it's different from the working. Otherwise, we remove two claimant counts. This causes
                    // an issue if we only updated the flowfile attributes.
                    decrementClaimCount(record.getCurrentClaim());
                }
                abortedRecords.add(record);
            } else {
                transferRecords.add(record);
            }
        }

        // Put the FlowFiles that are not marked for abort back to their original queues
        for (final StandardRepositoryRecord record : transferRecords) {
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

        if (!abortedRecords.isEmpty()) {
            try {
                context.getFlowFileRepository().updateRepository(abortedRecords);
            } catch (final IOException ioe) {
                LOG.error("Unable to update FlowFile repository for aborted records due to {}", ioe.toString());
                if (LOG.isDebugEnabled()) {
                    LOG.error("", ioe);
                }
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
                LOG.error("Unable to update FlowFile repository to cleanup transient claims due to {}", ioe.toString());
                if (LOG.isDebugEnabled()) {
                    LOG.error("", ioe);
                }
            }
        }

        final Connectable connectable = context.getConnectable();
        final StandardFlowFileEvent flowFileEvent = new StandardFlowFileEvent(connectable.getIdentifier());
        flowFileEvent.setBytesRead(bytesRead);
        flowFileEvent.setBytesWritten(bytesWritten);
        flowFileEvent.setCounters(immediateCounters);

        // update event repository
        try {
            context.getFlowFileEventRepository().updateRepository(flowFileEvent);
        } catch (final Exception e) {
            LOG.error("Failed to update FlowFileEvent Repository due to " + e);
            if (LOG.isDebugEnabled()) {
                LOG.error("", e);
            }
        }

        acknowledgeRecords();
        resetState();
    }

    private String loggableFlowfileInfo() {
        final StringBuilder details = new StringBuilder(1024).append("[");
        final int initLen = details.length();
        int filesListed = 0;
        for (Map.Entry<FlowFileRecord, StandardRepositoryRecord> entry : records.entrySet()) {
            if (filesListed >= MAX_ROLLBACK_FLOWFILES_TO_LOG) {
                break;
            }
            filesListed++;
            final FlowFileRecord entryKey = entry.getKey();
            final StandardRepositoryRecord entryValue = entry.getValue();
            if (details.length() > initLen) {
                details.append(", ");
            }
            if (entryValue.getOriginalQueue() != null && entryValue.getOriginalQueue().getIdentifier() != null) {
                details.append("queue=")
                        .append(entryValue.getOriginalQueue().getIdentifier())
                        .append("/");
            }
            details.append("filename=")
                    .append(entryKey.getAttribute(CoreAttributes.FILENAME.key()))
                    .append("/uuid=")
                    .append(entryKey.getAttribute(CoreAttributes.UUID.key()));
        }
        if (records.entrySet().size() > MAX_ROLLBACK_FLOWFILES_TO_LOG) {
            if (details.length() > initLen) {
                details.append(", ");
            }
            details.append(records.entrySet().size() - MAX_ROLLBACK_FLOWFILES_TO_LOG)
                    .append(" additional Flowfiles not listed");
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
    private void destroyContent(final ContentClaim claim) {
        if (claim == null) {
            return;
        }

        final int decrementedClaimCount = context.getContentRepository().decrementClaimantCount(claim);
        if (decrementedClaimCount <= 0) {
            resetWriteClaims(); // Have to ensure that we are not currently writing to the claim before we can destroy it.
            context.getContentRepository().remove(claim);
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
        removedFlowFiles.clear();
        if (countersOnCommit != null) {
            countersOnCommit.clear();
        }
        if (immediateCounters != null) {
            immediateCounters.clear();
        }

        generatedProvenanceEvents.clear();
        forkEventBuilders.clear();
        provenanceReporter.clear();

        processingStartTime = System.nanoTime();
    }

    private void acknowledgeRecords() {
        for (final Map.Entry<FlowFileQueue, Set<FlowFileRecord>> entry : unacknowledgedFlowFiles.entrySet()) {
            entry.getKey().acknowledge(entry.getValue());
        }
        unacknowledgedFlowFiles.clear();
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

    private void migrate(final StandardProcessSession newOwner, final Collection<FlowFile> flowFiles) {
        // We don't call validateRecordState() here because we want to allow migration of FlowFiles that have already been marked as removed or transferred, etc.
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

            final StandardRepositoryRecord record = records.get(flowFile);
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
                        throw new IllegalStateException("Cannot migrate " + eventFlowFile + " to a new session because it was forked to create " + eventBuilder.getChildFlowFileIds().size()
                            + " children and not all children are being migrated. If any FlowFile is forked, all of its children must also be migrated at the same time as the forked FlowFile");
                    }
                }
            }
        }

        // If we have a FORK event where a FlowFile is a child of the FORK event, we want to create a FORK
        // event builder for the new owner of the FlowFile and remove the child from our fork event builder.
        for (final Map.Entry<FlowFile, ProvenanceEventBuilder> entry : forkEventBuilders.entrySet()) {
            final FlowFile eventFlowFile = entry.getKey();
            final ProvenanceEventBuilder eventBuilder = entry.getValue();

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
            }
        }

        newOwner.processingStartTime = Math.min(newOwner.processingStartTime, processingStartTime);

        for (final FlowFile flowFile : flowFiles) {
            final FlowFileRecord flowFileRecord = (FlowFileRecord) flowFile;

            final StandardRepositoryRecord repoRecord = this.records.remove(flowFile);
            newOwner.records.put(flowFileRecord, repoRecord);

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

            final String flowFileId = flowFile.getAttribute(CoreAttributes.UUID.key());
            if (removedFlowFiles.remove(flowFileId)) {
                newOwner.removedFlowFiles.add(flowFileId);
                newOwner.removedCount++;
                newOwner.removedBytes += flowFile.getSize();

                removedCount--;
                removedBytes -= flowFile.getSize();
            }

            if (createdFlowFiles.remove(flowFileId)) {
                newOwner.createdFlowFiles.add(flowFileId);
            }

            if (repoRecord.getTransferRelationship() != null) {
                flowFilesOut--;
                contentSizeOut -= flowFile.getSize();

                newOwner.flowFilesOut++;
                newOwner.contentSizeOut += flowFile.getSize();
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


    private String summarizeEvents(final Checkpoint checkpoint) {
        final Map<Relationship, Set<String>> transferMap = new HashMap<>(); // relationship to flowfile ID's
        final Set<String> modifiedFlowFileIds = new HashSet<>();
        int largestTransferSetSize = 0;

        for (final Map.Entry<FlowFileRecord, StandardRepositoryRecord> entry : checkpoint.records.entrySet()) {
            final FlowFile flowFile = entry.getKey();
            final StandardRepositoryRecord record = entry.getValue();

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
        ;
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
        final StandardFlowFileEvent connectionEvent = connectionCounts.computeIfAbsent(connectionId, id -> new StandardFlowFileEvent(id));
        connectionEvent.setContentSizeIn(connectionEvent.getContentSizeIn() + bytes);
        connectionEvent.setFlowFilesIn(connectionEvent.getFlowFilesIn() + flowFileCount);
    }

    private void incrementConnectionOutputCounts(final Connection connection, final FlowFileRecord record) {
        incrementConnectionOutputCounts(connection.getIdentifier(), 1, record.getSize());
    }

    private void incrementConnectionOutputCounts(final String connectionId, final int flowFileCount, final long bytes) {
        final StandardFlowFileEvent connectionEvent = connectionCounts.computeIfAbsent(connectionId, id -> new StandardFlowFileEvent(id));
        connectionEvent.setContentSizeOut(connectionEvent.getContentSizeOut() + bytes);
        connectionEvent.setFlowFilesOut(connectionEvent.getFlowFilesOut() + flowFileCount);
    }

    private void registerDequeuedRecord(final FlowFileRecord flowFile, final Connection connection) {
        final StandardRepositoryRecord record = new StandardRepositoryRecord(connection.getFlowFileQueue(), flowFile);
        records.put(flowFile, record);
        flowFilesIn++;
        contentSizeIn += flowFile.getSize();

        Set<FlowFileRecord> set = unacknowledgedFlowFiles.get(connection.getFlowFileQueue());
        if (set == null) {
            set = new HashSet<>();
            unacknowledgedFlowFiles.put(connection.getFlowFileQueue(), set);
        }
        set.add(flowFile);

        incrementConnectionOutputCounts(connection, flowFile);
    }

    @Override
    public void adjustCounter(final String name, final long delta, final boolean immediate) {
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
        final List<Connection> connections = context.getPollableConnections();
        final int numConnections = connections.size();
        for (int numAttempts = 0; numAttempts < numConnections; numAttempts++) {
            final Connection conn = connections.get(context.getNextIncomingConnectionIndex() % numConnections);
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
        if (maxResults < 0) {
            throw new IllegalArgumentException();
        }
        if (maxResults == 0) {
            return Collections.emptyList();
        }

        // get batch of flow files in a round-robin manner
        final List<Connection> connections = context.getPollableConnections();
        if(connections.isEmpty()) {
            return Collections.emptyList();
        }

        final Connection connection = connections.get(context.getNextIncomingConnectionIndex() % connections.size());

        return get(connection, new ConnectionPoller() {
            @Override
            public List<FlowFileRecord> poll(final Connection connection, final Set<FlowFileRecord> expiredRecords) {
                return connection.poll(new FlowFileFilter() {
                    int polled = 0;

                    @Override
                    public FlowFileFilterResult filter(final FlowFile flowFile) {
                        if (++polled < maxResults) {
                            return FlowFileFilterResult.ACCEPT_AND_CONTINUE;
                        } else {
                            return FlowFileFilterResult.ACCEPT_AND_TERMINATE;
                        }
                    }
                }, expiredRecords);
            }
        }, false);
    }

    @Override
    public List<FlowFile> get(final FlowFileFilter filter) {
        return get(new ConnectionPoller() {
            @Override
            public List<FlowFileRecord> poll(final Connection connection, final Set<FlowFileRecord> expiredRecords) {
                return connection.poll(filter, expiredRecords);
            }
        }, true);
    }

    private List<FlowFile> get(final Connection connection, final ConnectionPoller poller, final boolean lockQueue) {
        if (lockQueue) {
            connection.lock();
        }

        try {
            final Set<FlowFileRecord> expired = new HashSet<>();
            final List<FlowFileRecord> newlySelected = poller.poll(connection, expired);
            removeExpired(expired, connection);

            if (newlySelected.isEmpty() && expired.isEmpty()) {
                return new ArrayList<>();
            }

            for (final FlowFileRecord flowFile : newlySelected) {
                registerDequeuedRecord(flowFile, connection);
            }

            return new ArrayList<FlowFile>(newlySelected);
        } finally {
            if (lockQueue) {
                connection.unlock();
            }
        }
    }

    private List<FlowFile> get(final ConnectionPoller poller, final boolean lockAllQueues) {
        final List<Connection> connections = context.getPollableConnections();
        if (lockAllQueues) {
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

                return new ArrayList<FlowFile>(newlySelected);
            }

            return new ArrayList<>();
        } finally {
            if (lockAllQueues) {
                for (final Connection connection : connections) {
                    connection.unlock();
                }
            }
        }
    }

    @Override
    public QueueSize getQueueSize() {
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
        final Map<String, String> attrs = new HashMap<>();
        attrs.put(CoreAttributes.FILENAME.key(), String.valueOf(System.nanoTime()));
        attrs.put(CoreAttributes.PATH.key(), DEFAULT_FLOWFILE_PATH);
        attrs.put(CoreAttributes.UUID.key(), UUID.randomUUID().toString());

        final FlowFileRecord fFile = new StandardFlowFileRecord.Builder().id(context.getNextFlowFileSequence())
            .addAttributes(attrs)
            .build();
        final StandardRepositoryRecord record = new StandardRepositoryRecord(null);
        record.setWorking(fFile, attrs);
        records.put(fFile, record);
        createdFlowFiles.add(fFile.getAttribute(CoreAttributes.UUID.key()));
        return fFile;
    }

    @Override
    public FlowFile clone(final FlowFile example) {
        return clone(example, 0L, example.getSize());
    }

    @Override
    public FlowFile clone(FlowFile example, final long offset, final long size) {
        example = validateRecordState(example);
        final StandardRepositoryRecord exampleRepoRecord = records.get(example);
        final FlowFileRecord currRec = exampleRepoRecord.getCurrent();
        final ContentClaim claim = exampleRepoRecord.getCurrentClaim();
        if (offset + size > example.getSize()) {
            throw new FlowFileHandlingException("Specified offset of " + offset + " and size " + size + " exceeds size of " + example.toString());
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
        final StandardRepositoryRecord record = new StandardRepositoryRecord(null);
        record.setWorking(clone, clone.getAttributes());
        records.put(clone, record);

        if (offset == 0L && size == example.getSize()) {
            provenanceReporter.clone(example, clone);
        } else {
            registerForkEvent(example, clone);
        }

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

            updateEventContentClaims(eventBuilder, parent, records.get(parent));
            forkEventBuilders.put(parent, eventBuilder);
        }

        eventBuilder.addChildFlowFile(child);
    }

    private void registerJoinEvent(final FlowFile child, final Collection<FlowFile> parents) {
        final ProvenanceEventRecord eventRecord = provenanceReporter.generateJoinEvent(parents, child);
        List<ProvenanceEventRecord> existingRecords = generatedProvenanceEvents.get(child);
        if (existingRecords == null) {
            existingRecords = new ArrayList<>();
            generatedProvenanceEvents.put(child, existingRecords);
        }
        existingRecords.add(eventRecord);
    }

    @Override
    public FlowFile penalize(FlowFile flowFile) {
        flowFile = validateRecordState(flowFile);
        final StandardRepositoryRecord record = records.get(flowFile);
        final long expirationEpochMillis = System.currentTimeMillis() + context.getConnectable().getPenalizationPeriod(TimeUnit.MILLISECONDS);
        final FlowFileRecord newFile = new StandardFlowFileRecord.Builder().fromFlowFile(record.getCurrent()).penaltyExpirationTime(expirationEpochMillis).build();
        record.setWorking(newFile);
        return newFile;
    }

    @Override
    public FlowFile putAttribute(FlowFile flowFile, final String key, final String value) {
        flowFile = validateRecordState(flowFile);

        if (CoreAttributes.UUID.key().equals(key)) {
            return flowFile;
        }

        final StandardRepositoryRecord record = records.get(flowFile);
        final FlowFileRecord newFile = new StandardFlowFileRecord.Builder().fromFlowFile(record.getCurrent()).addAttribute(key, value).build();
        record.setWorking(newFile, key, value);

        return newFile;
    }

    @Override
    public FlowFile putAllAttributes(FlowFile flowFile, final Map<String, String> attributes) {
        flowFile = validateRecordState(flowFile);
        final StandardRepositoryRecord record = records.get(flowFile);

        final Map<String, String> updatedAttributes;
        if (attributes.containsKey(CoreAttributes.UUID.key())) {
            updatedAttributes = new HashMap<>(attributes);
            updatedAttributes.remove(CoreAttributes.UUID.key());
        } else {
            updatedAttributes = attributes;
        }

        final StandardFlowFileRecord.Builder ffBuilder = new StandardFlowFileRecord.Builder().fromFlowFile(record.getCurrent()).addAttributes(updatedAttributes);
        final FlowFileRecord newFile = ffBuilder.build();

        record.setWorking(newFile, updatedAttributes);
        return newFile;
    }

    @Override
    public FlowFile removeAttribute(FlowFile flowFile, final String key) {
        flowFile = validateRecordState(flowFile);

        if (CoreAttributes.UUID.key().equals(key)) {
            return flowFile;
        }

        final StandardRepositoryRecord record = records.get(flowFile);
        final FlowFileRecord newFile = new StandardFlowFileRecord.Builder().fromFlowFile(record.getCurrent()).removeAttributes(key).build();
        record.setWorking(newFile, key, null);
        return newFile;
    }

    @Override
    public FlowFile removeAllAttributes(FlowFile flowFile, final Set<String> keys) {
        flowFile = validateRecordState(flowFile);

        if (keys == null) {
            return flowFile;
        }

        final StandardRepositoryRecord record = records.get(flowFile);
        final FlowFileRecord newFile = new StandardFlowFileRecord.Builder().fromFlowFile(record.getCurrent()).removeAttributes(keys).build();

        final Map<String, String> updatedAttrs = new HashMap<>();
        for (final String key : keys) {
            if (CoreAttributes.UUID.key().equals(key)) {
                continue;
            }

            updatedAttrs.put(key, null);
        }

        record.setWorking(newFile, updatedAttrs);
        return newFile;
    }

    @Override
    public FlowFile removeAllAttributes(FlowFile flowFile, final Pattern keyPattern) {
        flowFile = validateRecordState(flowFile);
        final StandardRepositoryRecord record = records.get(flowFile);
        final FlowFileRecord newFile = new StandardFlowFileRecord.Builder().fromFlowFile(record.getCurrent()).removeAttributes(keyPattern).build();

        if (keyPattern == null) {
            record.setWorking(newFile);
        } else {
            final Map<String, String> curAttrs = record.getCurrent().getAttributes();

            final Map<String, String> removed = new HashMap<>();
            for (final String key : curAttrs.keySet()) {
                if (CoreAttributes.UUID.key().equals(key)) {
                    continue;
                }

                if (keyPattern.matcher(key).matches()) {
                    removed.put(key, null);
                }
            }

            record.setWorking(newFile, removed);
        }

        return newFile;
    }

    private void updateLastQueuedDate(final StandardRepositoryRecord record) {
        final FlowFileRecord newFile = new StandardFlowFileRecord.Builder().fromFlowFile(record.getCurrent())
            .lastQueued(System.currentTimeMillis(), enqueuedIndex.getAndIncrement()).build();
        record.setWorking(newFile);
    }

    @Override
    public void transfer(FlowFile flowFile, final Relationship relationship) {
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
        final StandardRepositoryRecord record = records.get(flowFile);
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
        flowFile = validateRecordState(flowFile);
        final StandardRepositoryRecord record = records.get(flowFile);
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

        long contentSize = 0L;
        for (final FlowFile flowFile : flowFiles) {
            final StandardRepositoryRecord record = records.get(flowFile);
            record.setTransferRelationship(relationship);
            updateLastQueuedDate(record);

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
        flowFile = validateRecordState(flowFile);
        final StandardRepositoryRecord record = records.get(flowFile);
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
        flowFiles = validateRecordState(flowFiles);
        for (final FlowFile flowFile : flowFiles) {
            final StandardRepositoryRecord record = records.get(flowFile);
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
        final FlowFileFilter filter = new FlowFileFilter() {
            @Override
            public FlowFileFilterResult filter(final FlowFile flowFile) {
                return FlowFileFilterResult.REJECT_AND_CONTINUE;
            }
        };

        for (final Connection conn : context.getConnectable().getIncomingConnections()) {
            do {
                expired.clear();
                conn.getFlowFileQueue().poll(filter, expired);
                removeExpired(expired, conn);
            } while (!expired.isEmpty());
        }
    }

    private void removeExpired(final Set<FlowFileRecord> flowFiles, final Connection connection) {
        if (flowFiles.isEmpty()) {
            return;
        }

        LOG.info("{} {} FlowFiles have expired and will be removed", new Object[] {this, flowFiles.size()});
        final List<RepositoryRecord> expiredRecords = new ArrayList<>(flowFiles.size());

        final Connectable connectable = context.getConnectable();
        final String processorType = connectable.getComponentType();
        final StandardProvenanceReporter expiredReporter = new StandardProvenanceReporter(this, connectable.getIdentifier(),
            processorType, context.getProvenanceRepository(), this);

        final Map<String, FlowFileRecord> recordIdMap = new HashMap<>();
        for (final FlowFileRecord flowFile : flowFiles) {
            recordIdMap.put(flowFile.getAttribute(CoreAttributes.UUID.key()), flowFile);

            final StandardRepositoryRecord record = new StandardRepositoryRecord(connection.getFlowFileQueue(), flowFile);
            record.markForDelete();
            expiredRecords.add(record);
            expiredReporter.expire(flowFile, "Expiration Threshold = " + connection.getFlowFileQueue().getFlowFileExpiration());
            decrementClaimCount(flowFile.getContentClaim());

            final long flowFileLife = System.currentTimeMillis() - flowFile.getEntryDate();
            final Object terminator = connectable instanceof ProcessorNode ? ((ProcessorNode) connectable).getProcessor() : connectable;
            LOG.info("{} terminated by {} due to FlowFile expiration; life of FlowFile = {} ms", new Object[] {flowFile, terminator, flowFileLife});
        }

        try {
            final Iterable<ProvenanceEventRecord> iterable = new Iterable<ProvenanceEventRecord>() {
                @Override
                public Iterator<ProvenanceEventRecord> iterator() {
                    final Iterator<ProvenanceEventRecord> expiredEventIterator = expiredReporter.getEvents().iterator();
                    final Iterator<ProvenanceEventRecord> enrichingIterator = new Iterator<ProvenanceEventRecord>() {
                        @Override
                        public boolean hasNext() {
                            return expiredEventIterator.hasNext();
                        }

                        @Override
                        public ProvenanceEventRecord next() {
                            final ProvenanceEventRecord event = expiredEventIterator.next();
                            final StandardProvenanceEventRecord.Builder enriched = new StandardProvenanceEventRecord.Builder().fromEvent(event);
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

                            enriched.setAttributes(record.getAttributes(), Collections.<String, String> emptyMap());
                            return enriched.build();
                        }

                        @Override
                        public void remove() {
                            throw new UnsupportedOperationException();
                        }
                    };

                    return enrichingIterator;
                }
            };

            context.getProvenanceRepository().registerEvents(iterable);
            context.getFlowFileRepository().updateRepository(expiredRecords);
        } catch (final IOException e) {
            LOG.error("Failed to update FlowFile Repository to record expired records due to {}", e);
        }

    }

    private InputStream getInputStream(final FlowFile flowFile, final ContentClaim claim, final long offset, final boolean allowCachingOfStream) throws ContentNotFoundException {
        // If there's no content, don't bother going to the Content Repository because it is generally expensive and we know
        // that there is no actual content.
        if (flowFile.getSize() == 0L) {
            return new ByteArrayInputStream(new byte[0]);
        }

        try {
            // If the recursion set is empty, we can use the same input stream that we already have open. However, if
            // the recursion set is NOT empty, we can't do this because we may be reading the input of FlowFile 1 while in the
            // callback for reading FlowFile 1 and if we used the same stream we'd be destroying the ability to read from FlowFile 1.
            if (allowCachingOfStream && readRecursionSet.isEmpty() && writeRecursionSet.isEmpty()) {
                if (currentReadClaim == claim) {
                    if (currentReadClaimStream != null && currentReadClaimStream.getBytesConsumed() <= offset) {
                        final long bytesToSkip = offset - currentReadClaimStream.getBytesConsumed();
                        if (bytesToSkip > 0) {
                            StreamUtils.skip(currentReadClaimStream, bytesToSkip);
                        }

                        return new DisableOnCloseInputStream(currentReadClaimStream);
                    }
                }

                claimCache.flush(claim);
                final InputStream rawInStream = context.getContentRepository().read(claim);

                if (currentReadClaimStream != null) {
                    currentReadClaimStream.close();
                }

                currentReadClaim = claim;
                currentReadClaimStream = new ByteCountingInputStream(rawInStream);
                StreamUtils.skip(currentReadClaimStream, offset);

                // Use a non-closeable stream because we want to keep it open after the callback has finished so that we can
                // reuse the same InputStream for the next FlowFile
                return new DisableOnCloseInputStream(currentReadClaimStream);
            } else {
                claimCache.flush(claim);
                final InputStream rawInStream = context.getContentRepository().read(claim);
                try {
                    StreamUtils.skip(rawInStream, offset);
                } catch(IOException ioe) {
                    IOUtils.closeQuietly(rawInStream);
                    throw ioe;
                }
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
    public void read(final FlowFile source, final InputStreamCallback reader) {
        read(source, false, reader);
    }

    @Override
    public void read(FlowFile source, boolean allowSessionStreamManagement, InputStreamCallback reader) {
        source = validateRecordState(source, true);
        final StandardRepositoryRecord record = records.get(source);

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
                reader.process(ffais);

                // Allow processors to close the file after reading to avoid too many files open or do smart session stream management.
                if (this.currentReadClaimStream != null && !allowSessionStreamManagement) {
                    currentReadClaimStream.close();
                    currentReadClaimStream = null;
                }
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
        source = validateRecordState(source, true);
        final StandardRepositoryRecord record = records.get(source);

        try {
            ensureNotAppending(record.getCurrentClaim());
        } catch (final IOException e) {
            throw new FlowFileAccessException("Failed to access ContentClaim for " + source.toString(), e);
        }

        final InputStream rawIn = getInputStream(source, record.getCurrentClaim(), record.getCurrentClaimOffset(), true);
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
                    handleContentNotFound(cnfe, record);
                    close();
                    throw cnfe;
                } catch (final FlowFileAccessException ffae) {
                    LOG.error("Failed to read content from " + sourceFlowFile + "; rolling back session", ffae);
                    rollback(true);
                    close();
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
                    handleContentNotFound(cnfe, record);
                    close();
                    throw cnfe;
                } catch (final FlowFileAccessException ffae) {
                    LOG.error("Failed to read content from " + sourceFlowFile + "; rolling back session", ffae);
                    rollback(true);
                    close();
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
        return errorHandlingStream;
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
        return merge(sources, destination, null, null, null);
    }

    @Override
    public FlowFile merge(Collection<FlowFile> sources, FlowFile destination, final byte[] header, final byte[] footer, final byte[] demarcator) {
        sources = validateRecordState(sources);
        destination = validateRecordState(destination);
        if (sources.contains(destination)) {
            throw new IllegalArgumentException("Destination cannot be within sources");
        }

        final Collection<StandardRepositoryRecord> sourceRecords = new ArrayList<>();
        for (final FlowFile source : sources) {
            final StandardRepositoryRecord record = records.get(source);
            sourceRecords.add(record);

            try {
                ensureNotAppending(record.getCurrentClaim());
                claimCache.flush(record.getCurrentClaim());
            } catch (final IOException e) {
                throw new FlowFileAccessException("Unable to read from source " + source + " due to " + e.toString(), e);
            }
        }

        final StandardRepositoryRecord destinationRecord = records.get(destination);
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
                    final StandardRepositoryRecord sourceRecord = records.get(source);

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
            destroyContent(newClaim);
            handleContentNotFound(nfe, destinationRecord);
            handleContentNotFound(nfe, sourceRecords);
        } catch (final IOException ioe) {
            destroyContent(newClaim);
            throw new FlowFileAccessException("Failed to merge " + sources.size() + " into " + destination + " due to " + ioe.toString(), ioe);
        } catch (final Throwable t) {
            destroyContent(newClaim);
            throw t;
        }

        removeTemporaryClaim(destinationRecord);
        final FlowFileRecord newFile = new StandardFlowFileRecord.Builder().fromFlowFile(destinationRecord.getCurrent()).contentClaim(newClaim).contentClaimOffset(0L).size(writtenCount).build();
        destinationRecord.setWorking(newFile);
        records.put(newFile, destinationRecord);
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
        source = validateRecordState(source);
        final StandardRepositoryRecord record = records.get(source);

        ContentClaim newClaim = null;
        try {
            newClaim = claimCache.getContentClaim();
            claimLog.debug("Creating ContentClaim {} for 'write' for {}", newClaim, source);
            ensureNotAppending(newClaim);

            final OutputStream rawStream = claimCache.write(newClaim);
            final OutputStream disableOnClose = new DisableOnCloseOutputStream(rawStream);
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
                        LOG.error("Failed to write content to " + sourceFlowFile + "; rolling back session", ioe);
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
                        LOG.error("Failed to write content to " + sourceFlowFile + "; rolling back session", ioe);
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
                        LOG.error("Failed to write content to " + sourceFlowFile + "; rolling back session", ioe);
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
                        LOG.error("Failed to write content to " + sourceFlowFile + "; rolling back session", ioe);
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
                    writeRecursionSet.remove(sourceFlowFile);

                    final long bytesWritten = countingOut.getBytesWritten();
                    StandardProcessSession.this.bytesWritten += bytesWritten;

                    final OutputStream removed = openOutputStreams.remove(sourceFlowFile);
                    if (removed == null) {
                        LOG.error("Closed Session's OutputStream but there was no entry for it in the map; sourceFlowFile={}; map={}", sourceFlowFile, openOutputStreams);
                    }

                    flush();
                    removeTemporaryClaim(record);

                    final FlowFileRecord newFile = new StandardFlowFileRecord.Builder()
                        .fromFlowFile(record.getCurrent())
                        .contentClaim(updatedClaim)
                        .contentClaimOffset(Math.max(0, updatedClaim.getLength() - bytesWritten))
                        .size(bytesWritten)
                        .build();

                    record.setWorking(newFile);
                }
            };

            writeRecursionSet.add(source);
            openOutputStreams.put(source, errorHandlingOutputStream);
            return errorHandlingOutputStream;
        } catch (final ContentNotFoundException nfe) {
            resetWriteClaims(); // need to reset write claim before we can remove the claim
            destroyContent(newClaim);
            handleContentNotFound(nfe, record);
            throw nfe;
        } catch (final FlowFileAccessException ffae) {
            resetWriteClaims(); // need to reset write claim before we can remove the claim
            destroyContent(newClaim);
            throw ffae;
        } catch (final IOException ioe) {
            resetWriteClaims(); // need to reset write claim before we can remove the claim
            destroyContent(newClaim);
            throw new ProcessException("IOException thrown from " + connectableDescription + ": " + ioe.toString(), ioe);
        } catch (final Throwable t) {
            resetWriteClaims(); // need to reset write claim before we can remove the claim
            destroyContent(newClaim);
            throw t;
        }
    }

    @Override
    public FlowFile write(FlowFile source, final OutputStreamCallback writer) {
        source = validateRecordState(source);
        final StandardRepositoryRecord record = records.get(source);

        long writtenToFlowFile = 0L;
        ContentClaim newClaim = null;
        try {
            newClaim = claimCache.getContentClaim();
            claimLog.debug("Creating ContentClaim {} for 'write' for {}", newClaim, source);

            ensureNotAppending(newClaim);
            try (final OutputStream stream = claimCache.write(newClaim);
                final OutputStream disableOnClose = new DisableOnCloseOutputStream(stream);
                final ByteCountingOutputStream countingOut = new ByteCountingOutputStream(disableOnClose)) {
                try {
                    writeRecursionSet.add(source);
                    writer.process(new FlowFileAccessOutputStream(countingOut, source));
                } finally {
                    writtenToFlowFile = countingOut.getBytesWritten();
                    bytesWritten += countingOut.getBytesWritten();
                }
            } finally {
                writeRecursionSet.remove(source);
            }
        } catch (final ContentNotFoundException nfe) {
            resetWriteClaims(); // need to reset write claim before we can remove the claim
            destroyContent(newClaim);
            handleContentNotFound(nfe, record);
        } catch (final FlowFileAccessException ffae) {
            resetWriteClaims(); // need to reset write claim before we can remove the claim
            destroyContent(newClaim);
            throw ffae;
        } catch (final IOException ioe) {
            resetWriteClaims(); // need to reset write claim before we can remove the claim
            destroyContent(newClaim);
            throw new ProcessException("IOException thrown from " + connectableDescription + ": " + ioe.toString(), ioe);
        } catch (final Throwable t) {
            resetWriteClaims(); // need to reset write claim before we can remove the claim
            destroyContent(newClaim);
            throw t;
        }

        removeTemporaryClaim(record);
        final FlowFileRecord newFile = new StandardFlowFileRecord.Builder()
            .fromFlowFile(record.getCurrent())
            .contentClaim(newClaim)
            .contentClaimOffset(Math.max(0, newClaim.getLength() - writtenToFlowFile))
            .size(writtenToFlowFile)
            .build();

        record.setWorking(newFile);
        return newFile;
    }


    @Override
    public FlowFile append(FlowFile source, final OutputStreamCallback writer) {
        source = validateRecordState(source);
        final StandardRepositoryRecord record = records.get(source);
        long newSize = 0L;

        // Get the current Content Claim from the record and see if we already have
        // an OutputStream that we can append to.
        final ContentClaim oldClaim = record.getCurrentClaim();
        ByteCountingOutputStream outStream = appendableStreams.get(oldClaim);
        long originalByteWrittenCount = 0;

        ContentClaim newClaim = null;
        try {
            if (outStream == null) {
                claimCache.flush(oldClaim);

                try (final InputStream oldClaimIn = context.getContentRepository().read(oldClaim)) {
                    newClaim = context.getContentRepository().create(context.getConnectable().isLossTolerant());
                    claimLog.debug("Creating ContentClaim {} for 'append' for {}", newClaim, source);

                    final OutputStream rawOutStream = context.getContentRepository().write(newClaim);
                    final OutputStream bufferedOutStream = new BufferedOutputStream(rawOutStream);
                    outStream = new ByteCountingOutputStream(bufferedOutStream);
                    originalByteWrittenCount = 0;

                    appendableStreams.put(newClaim, outStream);

                    // We need to copy all of the data from the old claim to the new claim
                    StreamUtils.copy(oldClaimIn, outStream);

                    // wrap our OutputStreams so that the processor cannot close it
                    try (final OutputStream disableOnClose = new DisableOnCloseOutputStream(outStream)) {
                        writeRecursionSet.add(source);
                        writer.process(new FlowFileAccessOutputStream(disableOnClose, source));
                    } finally {
                        writeRecursionSet.remove(source);
                    }
                }
            } else {
                newClaim = oldClaim;
                originalByteWrittenCount = outStream.getBytesWritten();

                // wrap our OutputStreams so that the processor cannot close it
                try (final OutputStream disableOnClose = new DisableOnCloseOutputStream(outStream);
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
                destroyContent(newClaim);
            }

            handleContentNotFound(nfe, record);
        } catch (final IOException ioe) {
            resetWriteClaims(); // need to reset write claim before we can remove the claim

            // See above explanation for why this is done only if newClaim != oldClaim
            if (newClaim != oldClaim) {
                destroyContent(newClaim);
            }

            throw new ProcessException("IOException thrown from " + connectableDescription + ": " + ioe.toString(), ioe);
        } catch (final Throwable t) {
            resetWriteClaims(); // need to reset write claim before we can remove the claim

            // See above explanation for why this is done only if newClaim != oldClaim
            if (newClaim != oldClaim) {
                destroyContent(newClaim);
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

        final FlowFileRecord newFile = new StandardFlowFileRecord.Builder().fromFlowFile(record.getCurrent()).contentClaim(newClaim).contentClaimOffset(0).size(newSize).build();
        record.setWorking(newFile);
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
        final boolean contentModified = record.getWorkingClaim() != null && record.getWorkingClaim() != record.getOriginalClaim();

        // If the working claim is not the same as the original claim, we have modified the content of
        // the FlowFile, and we need to remove the newly created content (the working claim). However, if
        // they are the same, we cannot just remove the claim because record.getWorkingClaim() will return
        // the original claim if the record is "working" but the content has not been modified
        // (e.g., in the case of attributes only were updated)
        //
        // In other words:
        // If we modify the attributes of a FlowFile, and then we call record.getWorkingClaim(), this will
        // return the same claim as record.getOriginalClaim(). So we cannot just remove the working claim because
        // that may decrement the original claim (because the 2 claims are the same), and that's NOT what we want to do
        // because we will do that later, in the session.commit() and that would result in decrementing the count for
        // the original claim twice.
        if (contentModified) {
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
        } catch (final Exception e) {
        }
        currentReadClaimStream = null;
        currentReadClaim = null;
    }


    @Override
    public FlowFile write(FlowFile source, final StreamCallback writer) {
        source = validateRecordState(source);
        final StandardRepositoryRecord record = records.get(source);
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
                final OutputStream disableOnCloseOut = new DisableOnCloseOutputStream(os);
                final ByteCountingOutputStream countingOut = new ByteCountingOutputStream(disableOnCloseOut)) {

                writeRecursionSet.add(source);

                // We want to differentiate between IOExceptions thrown by the repository and IOExceptions thrown from
                // Processor code. As a result, as have the FlowFileAccessInputStream that catches IOException from the repository
                // and translates into either FlowFileAccessException or ContentNotFoundException. We keep track of any
                // ContentNotFoundException because if it is thrown, the Processor code may catch it and do something else with it
                // but in reality, if it is thrown, we want to know about it and handle it, even if the Processor code catches it.
                final FlowFileAccessInputStream ffais = new FlowFileAccessInputStream(countingIn, source, currClaim);
                boolean cnfeThrown = false;

                try {
                    writer.process(ffais, new FlowFileAccessOutputStream(countingOut, source));
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
            destroyContent(newClaim);
            handleContentNotFound(nfe, record);
        } catch (final IOException ioe) {
            destroyContent(newClaim);
            throw new ProcessException("IOException thrown from " + connectableDescription + ": " + ioe.toString(), ioe);
        } catch (final FlowFileAccessException ffae) {
            destroyContent(newClaim);
            throw ffae;
        } catch (final Throwable t) {
            destroyContent(newClaim);
            throw t;
        }

        removeTemporaryClaim(record);
        final FlowFileRecord newFile = new StandardFlowFileRecord.Builder()
            .fromFlowFile(record.getCurrent())
            .contentClaim(newClaim)
            .contentClaimOffset(Math.max(0L, newClaim.getLength() - writtenToFlowFile))
            .size(writtenToFlowFile)
            .build();

        record.setWorking(newFile);
        return newFile;
    }

    @Override
    public FlowFile importFrom(final Path source, final boolean keepSourceFile, FlowFile destination) {
        destination = validateRecordState(destination);
        // TODO: find a better solution. With Windows 7 and Java 7 (very early update, at least), Files.isWritable(source.getParent()) returns false, even when it should be true.
        if (!keepSourceFile && !Files.isWritable(source.getParent()) && !source.getParent().toFile().canWrite()) {
            // If we do NOT want to keep the file, ensure that we can delete it, or else error.
            throw new FlowFileAccessException("Cannot write to path " + source.getParent().toFile().getAbsolutePath() + " so cannot delete file; will not import.");
        }

        final StandardRepositoryRecord record = records.get(destination);

        final ContentClaim newClaim;
        final long claimOffset;

        try {
            newClaim = context.getContentRepository().create(context.getConnectable().isLossTolerant());
            claimLog.debug("Creating ContentClaim {} for 'importFrom' for {}", newClaim, destination);
        } catch (final IOException e) {
            throw new FlowFileAccessException("Unable to create ContentClaim due to " + e.toString(), e);
        }

        claimOffset = 0L;
        long newSize = 0L;
        try {
            newSize = context.getContentRepository().importFrom(source, newClaim);
            bytesWritten += newSize;
            bytesRead += newSize;
        } catch (final Throwable t) {
            destroyContent(newClaim);
            throw new FlowFileAccessException("Failed to import data from " + source + " for " + destination + " due to " + t.toString(), t);
        }

        removeTemporaryClaim(record);

        final FlowFileRecord newFile = new StandardFlowFileRecord.Builder()
            .fromFlowFile(record.getCurrent())
            .contentClaim(newClaim)
            .contentClaimOffset(claimOffset)
            .size(newSize)
            .addAttribute(CoreAttributes.FILENAME.key(), source.toFile().getName())
            .build();
        record.setWorking(newFile, CoreAttributes.FILENAME.key(), source.toFile().getName());

        if (!keepSourceFile) {
            deleteOnCommit.put(newFile, source);
        }

        return newFile;
    }

    @Override
    public FlowFile importFrom(final InputStream source, FlowFile destination) {
        destination = validateRecordState(destination);
        final StandardRepositoryRecord record = records.get(destination);
        ContentClaim newClaim = null;
        final long claimOffset = 0L;

        final long newSize;
        try {
            try {
                newClaim = context.getContentRepository().create(context.getConnectable().isLossTolerant());
                claimLog.debug("Creating ContentClaim {} for 'importFrom' for {}", newClaim, destination);

                newSize = context.getContentRepository().importFrom(source, newClaim);
                bytesWritten += newSize;
            } catch (final IOException e) {
                throw new FlowFileAccessException("Unable to create ContentClaim due to " + e.toString(), e);
            }
        } catch (final Throwable t) {
            if (newClaim != null) {
                destroyContent(newClaim);
            }

            throw new FlowFileAccessException("Failed to import data from " + source + " for " + destination + " due to " + t.toString(), t);
        }

        removeTemporaryClaim(record);
        final FlowFileRecord newFile = new StandardFlowFileRecord.Builder()
            .fromFlowFile(record.getCurrent())
            .contentClaim(newClaim)
            .contentClaimOffset(claimOffset)
            .size(newSize)
            .build();
        record.setWorking(newFile);
        return newFile;
    }

    @Override
    public void exportTo(FlowFile source, final Path destination, final boolean append) {
        source = validateRecordState(source);
        final StandardRepositoryRecord record = records.get(source);
        try {
            ensureNotAppending(record.getCurrentClaim());

            claimCache.flush(record.getCurrentClaim());
            final long copyCount = context.getContentRepository().exportTo(record.getCurrentClaim(), destination, append, record.getCurrentClaimOffset(), source.getSize());
            bytesRead += copyCount;
            bytesWritten += copyCount;
        } catch (final ContentNotFoundException nfe) {
            handleContentNotFound(nfe, record);
        } catch (final Throwable t) {
            throw new FlowFileAccessException("Failed to export " + source + " to " + destination + " due to " + t.toString(), t);
        }
    }

    @Override
    public void exportTo(FlowFile source, final OutputStream destination) {
        source = validateRecordState(source);
        final StandardRepositoryRecord record = records.get(source);

        if(record.getCurrentClaim() == null) {
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
            final FlowFileAccessInputStream ffais = new FlowFileAccessInputStream(countingStream, source, record.getCurrentClaim());
            boolean cnfeThrown = false;

            try {
                incrementReadCount(source);
                StreamUtils.copy(ffais, destination, source.getSize());
            } catch (final ContentNotFoundException cnfe) {
                cnfeThrown = true;
                throw cnfe;
            } finally {
                decrementReadCount(source);

                IOUtils.closeQuietly(ffais);
                // if cnfeThrown is true, we don't need to re-throw the Exception; it will propagate.
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

        final StandardRepositoryRecord record = records.get(flowFile);
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
        return records.containsKey(flowFile);
    }

    @Override
    public FlowFile create(final FlowFile parent) {
        final Map<String, String> newAttributes = new HashMap<>(3);
        newAttributes.put(CoreAttributes.FILENAME.key(), String.valueOf(System.nanoTime()));
        newAttributes.put(CoreAttributes.PATH.key(), DEFAULT_FLOWFILE_PATH);
        newAttributes.put(CoreAttributes.UUID.key(), UUID.randomUUID().toString());

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
        final StandardRepositoryRecord record = new StandardRepositoryRecord(null);
        record.setWorking(fFile, newAttributes);
        records.put(fFile, record);
        createdFlowFiles.add(fFile.getAttribute(CoreAttributes.UUID.key()));

        registerForkEvent(parent, fFile);
        return fFile;
    }

    @Override
    public FlowFile create(final Collection<FlowFile> parents) {
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

        newAttributes.put(CoreAttributes.FILENAME.key(), String.valueOf(System.nanoTime()));
        newAttributes.put(CoreAttributes.PATH.key(), DEFAULT_FLOWFILE_PATH);
        newAttributes.put(CoreAttributes.UUID.key(), UUID.randomUUID().toString());

        final FlowFileRecord fFile = new StandardFlowFileRecord.Builder().id(context.getNextFlowFileSequence())
            .addAttributes(newAttributes)
            .lineageStart(lineageStartDate, lineageStartIndex)
            .build();

        final StandardRepositoryRecord record = new StandardRepositoryRecord(null);
        record.setWorking(fFile, newAttributes);
        records.put(fFile, record);
        createdFlowFiles.add(fFile.getAttribute(CoreAttributes.UUID.key()));

        registerJoinEvent(fFile, parents);
        return fFile;
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
    protected void finalize() throws Throwable {
        rollback();
        super.finalize();
    }

    @Override
    public ProvenanceReporter getProvenanceReporter() {
        return provenanceReporter;
    }

    @Override
    public String toString() {
        return "StandardProcessSession[id=" + sessionId + "]";
    }

    /**
     * Callback interface used to poll a FlowFileQueue, in order to perform
     * functional programming-type of polling a queue
     */
    private static interface ConnectionPoller {

        List<FlowFileRecord> poll(Connection connection, Set<FlowFileRecord> expiredRecords);
    }

    private static class Checkpoint {

        private long processingTime = 0L;

        private final Map<FlowFile, List<ProvenanceEventRecord>> generatedProvenanceEvents = new HashMap<>();
        private final Map<FlowFile, ProvenanceEventBuilder> forkEventBuilders = new HashMap<>();
        private final List<ProvenanceEventRecord> autoTerminatedEvents = new ArrayList<>();
        private final Set<ProvenanceEventRecord> reportedEvents = new LinkedHashSet<>();

        private final Map<FlowFileRecord, StandardRepositoryRecord> records = new HashMap<>();
        private final Map<String, StandardFlowFileEvent> connectionCounts = new HashMap<>();
        private final Map<FlowFileQueue, Set<FlowFileRecord>> unacknowledgedFlowFiles = new HashMap<>();

        private Map<String, Long> countersOnCommit = new HashMap<>();
        private Map<String, Long> immediateCounters = new HashMap<>();

        private final Map<FlowFile, Path> deleteOnCommit = new HashMap<>();
        private final Set<String> removedFlowFiles = new HashSet<>();
        private final Set<String> createdFlowFiles = new HashSet<>();

        private int removedCount = 0; // number of flowfiles removed in this session
        private long removedBytes = 0L; // size of all flowfiles removed in this session
        private long bytesRead = 0L;
        private long bytesWritten = 0L;
        private int flowFilesIn = 0, flowFilesOut = 0;
        private long contentSizeIn = 0L, contentSizeOut = 0L;

        private void checkpoint(final StandardProcessSession session, final List<ProvenanceEventRecord> autoTerminatedEvents) {
            this.processingTime += System.nanoTime() - session.processingStartTime;

            this.generatedProvenanceEvents.putAll(session.generatedProvenanceEvents);
            this.forkEventBuilders.putAll(session.forkEventBuilders);
            if (autoTerminatedEvents != null) {
                this.autoTerminatedEvents.addAll(autoTerminatedEvents);
            }
            this.reportedEvents.addAll(session.provenanceReporter.getEvents());

            this.records.putAll(session.records);
            this.connectionCounts.putAll(session.connectionCounts);
            this.unacknowledgedFlowFiles.putAll(session.unacknowledgedFlowFiles);

            if (session.countersOnCommit != null) {
                if (this.countersOnCommit == null) {
                    this.countersOnCommit = new HashMap<>();
                }

                this.countersOnCommit.putAll(session.countersOnCommit);
            }

            if (session.immediateCounters != null) {
                if (this.immediateCounters == null) {
                    this.immediateCounters = new HashMap<>();
                }

                this.immediateCounters.putAll(session.immediateCounters);
            }

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
        }
    }
}
