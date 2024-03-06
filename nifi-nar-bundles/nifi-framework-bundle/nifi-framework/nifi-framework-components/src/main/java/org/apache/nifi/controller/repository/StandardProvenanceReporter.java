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

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.FlowFileHandlingException;
import org.apache.nifi.provenance.InternalProvenanceReporter;
import org.apache.nifi.provenance.ProvenanceEventBuilder;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventRepository;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

public class StandardProvenanceReporter implements InternalProvenanceReporter {

    private final Logger logger = LoggerFactory.getLogger(StandardProvenanceReporter.class);
    private final String processorId;
    private final String processorType;
    private final Set<ProvenanceEventRecord> events = new LinkedHashSet<>();
    private final ProvenanceEventRepository repository;
    private final ProvenanceEventEnricher eventEnricher;
    private final Predicate<FlowFile> flowfileKnownCheck;
    private long bytesSent = 0L;
    private long bytesReceived = 0L;
    private int flowFilesSent = 0;
    private int flowFilesReceived = 0;
    private int flowFilesFetched = 0;
    private long bytesFetched = 0L;

    public StandardProvenanceReporter(final Predicate<FlowFile> flowfileKnownCheck, final String processorId, final String processorType,
                                      final ProvenanceEventRepository repository, final ProvenanceEventEnricher enricher) {
        this.flowfileKnownCheck = flowfileKnownCheck;
        this.processorId = processorId;
        this.processorType = processorType;
        this.repository = repository;
        this.eventEnricher = enricher;
    }

    @Override
    public Set<ProvenanceEventRecord> getEvents() {
        return Collections.unmodifiableSet(events);
    }

    /**
     * Removes the given event from the reporter
     *
     * @param event event
     */
    @Override
    public void remove(final ProvenanceEventRecord event) {
        events.remove(event);
    }

    @Override
    public void removeEventsForFlowFile(final String uuid) {
        events.removeIf(event -> event.getFlowFileUuid().equals(uuid));
    }

    @Override
    public void clear() {
        events.clear();

        flowFilesSent = 0;
        bytesSent = 0;

        flowFilesReceived = 0;
        bytesReceived = 0;

        flowFilesFetched = 0;
        bytesFetched = 0;
    }

    @Override
    public void migrate(final InternalProvenanceReporter newOwner, final Collection<String> flowFileIds) {
        final Set<ProvenanceEventRecord> toMove = new LinkedHashSet<>();
        for (final ProvenanceEventRecord event : events) {
            if (flowFileIds.contains(event.getFlowFileUuid())) {
                toMove.add(event);
            } else if (event.getEventType() == ProvenanceEventType.CLONE) {
                if (flowFileIds.containsAll(event.getChildUuids())) {
                    toMove.add(event);
                }
            }
        }

        newOwner.receiveMigration(events);
        events.removeAll(toMove);
    }

    @Override
    public void receiveMigration(final Set<ProvenanceEventRecord> events) {
        this.events.addAll(events);
    }

    /**
     * Generates a Join event for the given child and parents but does not register the event. This is useful so that a ProcessSession has the ability to de-dupe events, since one or more events may
     * be created by the session itself, as well as by the Processor
     *
     * @param parents parents
     * @param child   child
     * @return record
     */
    @Override
    public ProvenanceEventRecord generateJoinEvent(final Collection<FlowFile> parents, final FlowFile child) {
        final ProvenanceEventBuilder eventBuilder = build(child, ProvenanceEventType.JOIN);
        eventBuilder.addChildFlowFile(child);

        List<Long> parentEventIds = new ArrayList<>(parents.size());
        for (final FlowFile parent : parents) {
            eventBuilder.addParentFlowFile(parent);
            parentEventIds.addAll(repository.getPreviousEventIds(parent.getAttribute(CoreAttributes.UUID.key())));
        }
        eventBuilder.setPreviousEventIds(parentEventIds);

        ProvenanceEventRecord record = eventBuilder.build();
        repository.updatePreviousEventIds(record, parentEventIds);
        return record;
    }

    @Override
    public ProvenanceEventRecord generateDropEvent(final FlowFile flowFile, final String details) {
        final String flowFileUUID = flowFile.getAttribute(CoreAttributes.UUID.key());
        final ProvenanceEventRecord record = build(flowFile, ProvenanceEventType.DROP)
                .setDetails(details)
                .setPreviousEventIds(repository.getPreviousEventIds(flowFileUUID))
                .build();
        repository.updatePreviousEventIds(record, null);
        return record;
    }

    @Override
    public ProvenanceEventRecord generateModifyContentEvent(final FlowFile flowFile, final String details) {
        final String flowFileUUID = flowFile.getAttribute(CoreAttributes.UUID.key());
        final ProvenanceEventRecord record = build(flowFile, ProvenanceEventType.CONTENT_MODIFIED)
                .setDetails(details)
                .setPreviousEventIds(repository.getPreviousEventIds(flowFileUUID))
                .build();
        repository.updatePreviousEventIds(record, Collections.singletonList(record.getEventId()));
        return record;
    }

    @Override
    public ProvenanceEventRecord generateModifyAttributesEvent(final FlowFile flowFile, final String details) {
        final String flowFileUUID = flowFile.getAttribute(CoreAttributes.UUID.key());
        final ProvenanceEventRecord record = build(flowFile, ProvenanceEventType.ATTRIBUTES_MODIFIED)
                .setDetails(details)
                .setPreviousEventIds(repository.getPreviousEventIds(flowFileUUID))
                .build();
        repository.updatePreviousEventIds(record, Collections.singletonList(record.getEventId()));
        return record;
    }

    @Override
    public ProvenanceEventRecord generateCreateEvent(final FlowFile flowFile, final String details) {
        final ProvenanceEventRecord record = build(flowFile, ProvenanceEventType.CREATE)
                .setDetails(details)
                .setPreviousEventIds(Collections.emptyList())
                .build();
        repository.updatePreviousEventIds(record, Collections.singletonList(record.getEventId()));
        return record;
    }

    private void verifyFlowFileKnown(final FlowFile flowFile) {
        if (flowfileKnownCheck != null && !flowfileKnownCheck.test(flowFile)) {
            throw new FlowFileHandlingException(flowFile + " is not known");
        }
    }

    @Override
    public void receive(final FlowFile flowFile, final String transitUri, final Relationship relationship) {
        receive(flowFile, transitUri, -1L, relationship);
    }

    @Override
    public void receive(FlowFile flowFile, String transitUri, String sourceSystemFlowFileIdentifier, final Relationship relationship) {
        receive(flowFile, transitUri, sourceSystemFlowFileIdentifier, -1L, relationship);
    }

    @Override
    public void receive(final FlowFile flowFile, final String transitUri, final long transmissionMillis, final Relationship relationship) {
        receive(flowFile, transitUri, null, transmissionMillis, relationship);
    }

    @Override
    public void receive(final FlowFile flowFile, final String transitUri, final String sourceSystemFlowFileIdentifier, final long transmissionMillis, final Relationship relationship) {
        receive(flowFile, transitUri, sourceSystemFlowFileIdentifier, null, transmissionMillis, relationship);
    }

    @Override
    public void receive(final FlowFile flowFile, final String transitUri, final String sourceSystemFlowFileIdentifier, final String details,
                        final long transmissionMillis, final Relationship relationship) {
        verifyFlowFileKnown(flowFile);

        try {
            final ProvenanceEventRecord record = build(flowFile, ProvenanceEventType.RECEIVE)
                    .setTransitUri(transitUri)
                    .setSourceSystemFlowFileIdentifier(sourceSystemFlowFileIdentifier)
                    .setEventDuration(transmissionMillis)
                    .setRelationship(relationship)
                    .setDetails(details)
                    .setPreviousEventIds(Collections.singletonList(-1L))
                    .build();
            events.add(record);
            repository.updatePreviousEventIds(record, Collections.singletonList(record.getEventId()));

            bytesReceived += flowFile.getSize();
            flowFilesReceived++;
        } catch (final Exception e) {
            logger.error("Failed to generate Provenance Event due to " + e);
            if (logger.isDebugEnabled()) {
                logger.error("", e);
            }
        }
    }

    @Override
    public void fetch(final FlowFile flowFile, final String transitUri, final Relationship relationship) {
        fetch(flowFile, transitUri, -1L, relationship);
    }

    @Override
    public void fetch(final FlowFile flowFile, final String transitUri, final long transmissionMillis, final Relationship relationship) {
        fetch(flowFile, transitUri, null, transmissionMillis, relationship);
    }

    @Override
    public void fetch(final FlowFile flowFile, final String transitUri, final String details, final long transmissionMillis, final Relationship relationship) {
        verifyFlowFileKnown(flowFile);

        try {
            final String flowFileUUID = flowFile.getAttribute(CoreAttributes.UUID.key());
            final ProvenanceEventRecord record = build(flowFile, ProvenanceEventType.FETCH)
                    .setTransitUri(transitUri)
                    .setEventDuration(transmissionMillis)
                    .setDetails(details)
                    .setRelationship(relationship)
                    .setPreviousEventIds(repository.getPreviousEventIds(flowFileUUID))
                    .build();

            events.add(record);
            repository.updatePreviousEventIds(record, Collections.singletonList(record.getEventId()));

            bytesFetched += flowFile.getSize();
            flowFilesFetched++;
        } catch (final Exception e) {
            logger.error("Failed to generate Provenance Event due to " + e);
            if (logger.isDebugEnabled()) {
                logger.error("", e);
            }
        }
    }

    @Override
    public void send(final FlowFile flowFile, final String transitUri, final long transmissionMillis, final Relationship relationship) {
        send(flowFile, transitUri, transmissionMillis, true, relationship);
    }

    @Override
    public void send(final FlowFile flowFile, final String transitUri, final Relationship relationship) {
        send(flowFile, transitUri, null, -1L, true, relationship);
    }

    @Override
    public void send(final FlowFile flowFile, final String transitUri, final String details, final Relationship relationship) {
        send(flowFile, transitUri, details, -1L, true, relationship);
    }

    @Override
    public void send(final FlowFile flowFile, final String transitUri, final long transmissionMillis, final boolean force, final Relationship relationship) {
        send(flowFile, transitUri, null, transmissionMillis, force, relationship);
    }

    @Override
    public void send(final FlowFile flowFile, final String transitUri, final String details, final boolean force, final Relationship relationship) {
        send(flowFile, transitUri, details, -1L, force, relationship);
    }

    @Override
    public void send(final FlowFile flowFile, final String transitUri, final String details, final long transmissionMillis, final Relationship relationship) {
        send(flowFile, transitUri, details, transmissionMillis, true, relationship);
    }

    @Override
    public void send(final FlowFile flowFile, final String transitUri, final String details, final long transmissionMillis, final boolean force, final Relationship relationship) {
        try {
            final ProvenanceEventRecord record = build(flowFile, ProvenanceEventType.SEND)
                    .setTransitUri(transitUri)
                    .setEventDuration(transmissionMillis)
                    .setDetails(details)
                    .setRelationship(relationship)
                    .setPreviousEventIds(repository.getPreviousEventIds(flowFile.getAttribute(CoreAttributes.UUID.key())))
                    .build();
            // If the transmissionMillis field has been populated, use zero as the value of commitNanos (the call to System.nanoTime() is expensive but the value will be ignored).
            final long commitNanos = transmissionMillis < 0 ? System.nanoTime() : 0L;
            final ProvenanceEventRecord enriched = eventEnricher == null ? record : eventEnricher.enrich(record, flowFile, commitNanos);

            if (force) {
                repository.registerEvent(enriched);
            } else {
                events.add(enriched);
            }
            repository.updatePreviousEventIds(enriched, enriched.getPreviousEventIds());

            bytesSent += flowFile.getSize();
            flowFilesSent++;
        } catch (final Exception e) {
            logger.error("Failed to generate Provenance Event due to " + e);
            if (logger.isDebugEnabled()) {
                logger.error("", e);
            }
        }
    }

    @Override
    public void send(final FlowFile flowFile, final String transitUri, final boolean force, final Relationship relationship) {
        send(flowFile, transitUri, -1L, force, relationship);
    }

    @Override
    public void upload(final FlowFile flowFile, final long size, final String transitUri, final Relationship relationship) {
        upload(flowFile, size, transitUri, null, -1L, true, relationship);
    }

    @Override
    public void upload(final FlowFile flowFile, final long size, final String transitUri, final String details, final long transmissionMillis,
                       final boolean force, final Relationship relationship) {
        try {
            final String displayedSizeInBytes = size + " bytes";
            final String enrichedDetails = details == null ? displayedSizeInBytes : details + " " + displayedSizeInBytes;
            final ProvenanceEventRecord record = build(flowFile, ProvenanceEventType.UPLOAD)
                    .setTransitUri(transitUri)
                    .setEventDuration(transmissionMillis)
                    .setDetails(enrichedDetails)
                    .setRelationship(relationship)
                    .build();
            // If the transmissionMillis field has been populated, use zero as the value of commitNanos (the call to System.nanoTime() is expensive but the value will be ignored).
            final long commitNanos = transmissionMillis < 0 ? System.nanoTime() : 0L;
            final ProvenanceEventRecord enriched = eventEnricher == null ? record : eventEnricher.enrich(record, flowFile, commitNanos);

            if (force) {
                repository.registerEvent(enriched);
            } else {
                events.add(enriched);
            }
            repository.updatePreviousEventIds(record, Collections.singletonList(record.getEventId()));


            bytesSent += size;
        } catch (final Exception e) {
            logger.error("Failed to generate Provenance Event", e);
        }
    }

    @Override
    public void invokeRemoteProcess(final FlowFile flowFile, final String transitUri, final Relationship relationship) {
        invokeRemoteProcess(flowFile, transitUri, null, relationship);
    }

    @Override
    public void invokeRemoteProcess(FlowFile flowFile, String transitUri, String details, final Relationship relationship) {
        invokeRemoteProcess(flowFile, transitUri, details, -1L, relationship);
    }

    @Override
    public void invokeRemoteProcess(FlowFile flowFile, String transitUri, String details, long transmissionMillis, final Relationship relationship) {
        try {
            final ProvenanceEventRecord record = build(flowFile, ProvenanceEventType.REMOTE_INVOCATION)
                    .setTransitUri(transitUri)
                    .setDetails(details)
                    .setEventDuration(transmissionMillis)
                    .setRelationship(relationship)
                    .setPreviousEventIds(repository.getPreviousEventIds(flowFile.getAttribute(CoreAttributes.UUID.key())))
                    .build();
            events.add(record);
            repository.updatePreviousEventIds(record, Collections.singletonList(record.getEventId()));
        } catch (final Exception e) {
            logger.error("Failed to generate Provenance Event due to " + e);
            if (logger.isDebugEnabled()) {
                logger.error("", e);
            }
        }
    }

    @Override
    public void associate(final FlowFile flowFile, final String alternateIdentifierNamespace, final String alternateIdentifier, final Relationship relationship) {
        try {
            String trimmedNamespace = alternateIdentifierNamespace.trim();
            if (trimmedNamespace.endsWith(":")) {
                trimmedNamespace = trimmedNamespace.substring(0, trimmedNamespace.length() - 1);
            }

            String trimmedIdentifier = alternateIdentifier.trim();
            if (trimmedIdentifier.startsWith(":")) {
                if (trimmedIdentifier.length() == 1) {
                    throw new IllegalArgumentException("Illegal alternateIdentifier: " + alternateIdentifier);
                }
                trimmedIdentifier = trimmedIdentifier.substring(1);
            }

            final String alternateIdentifierUri = trimmedNamespace + ":" + trimmedIdentifier;
            final ProvenanceEventRecord record = build(flowFile, ProvenanceEventType.ADDINFO)
                    .setAlternateIdentifierUri(alternateIdentifierUri)
                    .setPreviousEventIds(repository.getPreviousEventIds(flowFile.getAttribute(CoreAttributes.UUID.key())))
                    .build();
            events.add(record);
            repository.updatePreviousEventIds(record, Collections.singletonList(record.getEventId()));
        } catch (final Exception e) {
            logger.error("Failed to generate Provenance Event due to " + e);
            if (logger.isDebugEnabled()) {
                logger.error("", e);
            }
        }
    }

    @Override
    public ProvenanceEventRecord drop(final FlowFile flowFile, final String reason) {
        try {
            final ProvenanceEventBuilder builder = build(flowFile, ProvenanceEventType.DROP);
            if (reason != null) {
                builder.setDetails("Discard reason: " + reason);
            }
            builder.setPreviousEventIds(repository.getPreviousEventIds(flowFile.getAttribute(CoreAttributes.UUID.key())));
            final ProvenanceEventRecord record = builder.build();
            events.add(record);
            repository.updatePreviousEventIds(record, null);
            return record;
        } catch (final Exception e) {
            logger.error("Failed to generate Provenance Event due to " + e);
            if (logger.isDebugEnabled()) {
                logger.error("", e);
            }
            return null;
        }
    }

    @Override
    public void expire(final FlowFile flowFile, final String details) {
        try {
            final ProvenanceEventRecord record = build(flowFile, ProvenanceEventType.EXPIRE)
                    .setDetails(details)
                    .setPreviousEventIds(repository.getPreviousEventIds(flowFile.getAttribute(CoreAttributes.UUID.key())))
                    .build();
            events.add(record);
            repository.updatePreviousEventIds(record, null);
        } catch (final Exception e) {
            logger.error("Failed to generate Provenance Event due to " + e);
            if (logger.isDebugEnabled()) {
                logger.error("", e);
            }
        }
    }

    @Override
    public void fork(final FlowFile parent, final Collection<FlowFile> children) {
        fork(parent, children, null, -1L, null);
    }

    public void fork(final FlowFile parent, final Collection<FlowFile> children, final Relationship relationship) {
        fork(parent, children, null, -1L, null);
    }

    @Override
    public void fork(final FlowFile parent, final Collection<FlowFile> children, final long forkDuration, final Relationship relationship) {
        fork(parent, children, null, forkDuration, relationship);
    }

    @Override
    public void fork(final FlowFile parent, final Collection<FlowFile> children, final String details, final Relationship relationship) {
        fork(parent, children, details, -1L, relationship);
    }

    @Override
    public void fork(final FlowFile parent, final Collection<FlowFile> children, final String details, final long forkDuration, final Relationship relationship) {
        verifyFlowFileKnown(parent);

        try {
            final ProvenanceEventBuilder eventBuilder = build(parent, ProvenanceEventType.FORK).setRelationship(relationship);
            eventBuilder.addParentFlowFile(parent);
            for (final FlowFile child : children) {
                eventBuilder.addChildFlowFile(child);
            }

            if (forkDuration > -1L) {
                eventBuilder.setEventDuration(forkDuration);
            }

            if (details != null) {
                eventBuilder.setDetails(details);
            }

            final ProvenanceEventRecord record = eventBuilder.build();
            events.add(record);
            for (final FlowFile child : children) {
                // Add the child FlowFiles to the previous event ID map with the parent's entry in the map
                repository.updatePreviousEventIds(
                        record,
                        repository.getPreviousEventIds(parent.getAttribute(CoreAttributes.UUID.key())));
            }
        } catch (final Exception e) {
            logger.error("Failed to generate Provenance Event due to " + e);
            if (logger.isDebugEnabled()) {
                logger.error("", e);
            }
        }
    }

    @Override
    public void join(final Collection<FlowFile> parents, final FlowFile child) {
        join(parents, child, null, -1L, null);
    }

    @Override
    public void join(final Collection<FlowFile> parents, final FlowFile child, final Relationship relationship) {
        join(parents, child, null, -1L, null);
    }

    @Override
    public void join(final Collection<FlowFile> parents, final FlowFile child, final long joinDuration, final Relationship relationship) {
        join(parents, child, null, joinDuration, relationship);
    }

    @Override
    public void join(final Collection<FlowFile> parents, final FlowFile child, final String details, final Relationship relationship) {
        join(parents, child, details, -1L, relationship);
    }

    @Override
    public void join(final Collection<FlowFile> parents, final FlowFile child, final String details, final long joinDuration, final Relationship relationship) {
        verifyFlowFileKnown(child);

        try {
            final ProvenanceEventBuilder eventBuilder = build(child, ProvenanceEventType.JOIN);
            eventBuilder.addChildFlowFile(child);
            eventBuilder.setDetails(details);

            List<Long> parentEventIds = new ArrayList<>(parents.size());
            for (final FlowFile parent : parents) {
                eventBuilder.addParentFlowFile(parent);
                parentEventIds.addAll(repository.getPreviousEventIds(parent.getAttribute(CoreAttributes.UUID.key())));
            }
            eventBuilder.setPreviousEventIds(parentEventIds);
            eventBuilder.setEventDuration(joinDuration).setRelationship(relationship);

            final ProvenanceEventRecord record = eventBuilder.build();
            events.add(record);
            repository.updatePreviousEventIds(record, parentEventIds);
        } catch (final Exception e) {
            logger.error("Failed to generate Provenance Event due to " + e);
            if (logger.isDebugEnabled()) {
                logger.error("", e);
            }
        }
    }

    @Override
    public void clone(final FlowFile parent, final FlowFile child) {
        clone(parent, child, true);
    }

    @Override
    public void clone(final FlowFile parent, final FlowFile child, final boolean verifyFlowFile) {
        if (verifyFlowFile) {
            verifyFlowFileKnown(child);
        }

        try {
            final ProvenanceEventBuilder eventBuilder = build(parent, ProvenanceEventType.CLONE);
            final ProvenanceEventRecord event = eventBuilder
                    .addChildFlowFile(child)
                    .addParentFlowFile(parent)
                    .setPreviousEventIds(repository.getPreviousEventIds(parent.getAttribute(CoreAttributes.UUID.key())))
                    .build();
            events.add(event);
            repository.updatePreviousEventIds(event, Collections.singletonList(event.getEventId()));
        } catch (final Exception e) {
            logger.error("Failed to generate Provenance Event due to " + e);
            if (logger.isDebugEnabled()) {
                logger.error("", e);
            }
        }
    }

    @Override
    public void modifyContent(final FlowFile flowFile, final Relationship relationship) {
        modifyContent(flowFile, null, -1L, relationship);
    }

    @Override
    public void modifyContent(final FlowFile flowFile, final String details, final Relationship relationship) {
        modifyContent(flowFile, details, -1L, relationship);
    }

    @Override
    public void modifyContent(final FlowFile flowFile, final long processingMillis, final Relationship relationship) {
        modifyContent(flowFile, null, processingMillis, relationship);
    }

    @Override
    public void modifyContent(final FlowFile flowFile, final String details, final long processingMillis, final Relationship relationship) {
        verifyFlowFileKnown(flowFile);

        try {
            final ProvenanceEventRecord record = build(flowFile, ProvenanceEventType.CONTENT_MODIFIED)
                    .setEventDuration(processingMillis)
                    .setDetails(details)
                    .setRelationship(relationship)
                    .setPreviousEventIds(repository.getPreviousEventIds(flowFile.getAttribute(CoreAttributes.UUID.key())))
                    .build();
            events.add(record);
            repository.updatePreviousEventIds(record, Collections.singletonList(record.getEventId()));
        } catch (final Exception e) {
            logger.error("Failed to generate Provenance Event due to " + e);
            if (logger.isDebugEnabled()) {
                logger.error("", e);
            }
        }
    }

    @Override
    public void modifyAttributes(final FlowFile flowFile, final Relationship relationship) {
        modifyAttributes(flowFile, null, relationship);
    }

    @Override
    public void modifyAttributes(final FlowFile flowFile, final String details, final Relationship relationship) {
        verifyFlowFileKnown(flowFile);

        try {
            final ProvenanceEventRecord record = build(flowFile, ProvenanceEventType.ATTRIBUTES_MODIFIED)
                    .setDetails(details)
                    .setRelationship(relationship)
                    .setPreviousEventIds(repository.getPreviousEventIds(flowFile.getAttribute(CoreAttributes.UUID.key())))
                    .build();
            events.add(record);
            repository.updatePreviousEventIds(record, Collections.singletonList(record.getEventId()));
        } catch (final Exception e) {
            logger.error("Failed to generate Provenance Event due to " + e);
            if (logger.isDebugEnabled()) {
                logger.error("", e);
            }
        }
    }

    @Override
    public void route(final FlowFile flowFile, final Relationship relationship) {
        route(flowFile, relationship, null);
    }

    @Override
    public void route(final FlowFile flowFile, final Relationship relationship, final long processingDuration) {
        route(flowFile, relationship, null, processingDuration);
    }

    @Override
    public void route(final FlowFile flowFile, final Relationship relationship, final String details) {
        route(flowFile, relationship, details, -1L);
    }

    @Override
    public void route(final FlowFile flowFile, final Relationship relationship, final String details, final long processingDuration) {
        verifyFlowFileKnown(flowFile);

        try {
            final ProvenanceEventRecord record = build(flowFile, ProvenanceEventType.ROUTE)
                    .setRelationship(relationship)
                    .setDetails(details)
                    .setEventDuration(processingDuration)
                    .setPreviousEventIds(repository.getPreviousEventIds(flowFile.getAttribute(CoreAttributes.UUID.key())))
                    .build();
            events.add(record);
            repository.updatePreviousEventIds(record, Collections.singletonList(record.getEventId()));
        } catch (final Exception e) {
            logger.error("Failed to generate Provenance Event due to " + e);
            if (logger.isDebugEnabled()) {
                logger.error("", e);
            }
        }
    }

    @Override
    public void create(final FlowFile flowFile, final Relationship relationship) {
        create(flowFile, null, relationship);
    }

    @Override
    public void create(final FlowFile flowFile, final String details, final Relationship relationship) {
        verifyFlowFileKnown(flowFile);

        try {
            final ProvenanceEventRecord record = build(flowFile, ProvenanceEventType.CREATE)
                    .setDetails(details)
                    .setRelationship(relationship)
                    .setPreviousEventIds(Collections.emptyList())
                    .build();
            events.add(record);
            repository.updatePreviousEventIds(record, Collections.singletonList(record.getEventId()));
        } catch (final Exception e) {
            logger.error("Failed to generate Provenance Event due to " + e);
            if (logger.isDebugEnabled()) {
                logger.error("", e);
            }
        }
    }

    @Override
    public ProvenanceEventBuilder build(final FlowFile flowFile, final ProvenanceEventType eventType) {
        final ProvenanceEventBuilder builder = repository.eventBuilder();
        builder.setEventType(eventType);
        builder.fromFlowFile(flowFile);
        builder.setLineageStartDate(flowFile.getLineageStartDate());
        builder.setComponentId(processorId);
        builder.setComponentType(processorType);
        return builder;
    }

    @Override
    public int getFlowFilesSent() {
        return flowFilesSent;
    }

    @Override
    public long getBytesSent() {
        return bytesSent;
    }

    @Override
    public int getFlowFilesReceived() {
        return flowFilesReceived;
    }

    @Override
    public long getBytesReceived() {
        return bytesReceived;
    }

    @Override
    public int getFlowFilesFetched() {
        return flowFilesFetched;
    }

    @Override
    public long getBytesFetched() {
        return bytesFetched;
    }
}
