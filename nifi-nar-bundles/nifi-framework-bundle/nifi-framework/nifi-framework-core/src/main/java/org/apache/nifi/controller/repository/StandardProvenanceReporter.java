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

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.FlowFileHandlingException;
import org.apache.nifi.provenance.ProvenanceEventBuilder;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventRepository;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.provenance.ProvenanceReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StandardProvenanceReporter implements ProvenanceReporter {

    public static final String NIFI_NAMESPACE = "nifi";
    private final Logger logger = LoggerFactory.getLogger(StandardProvenanceReporter.class);
    private final String processorId;
    private final String processorType;
    private final Set<ProvenanceEventRecord> events = new LinkedHashSet<>();
    private final ProvenanceEventRepository repository;
    private final ProvenanceEventEnricher eventEnricher;
    private final StandardProcessSession session;

    public StandardProvenanceReporter(final StandardProcessSession session, final String processorId, final String processorType,
        final ProvenanceEventRepository repository, final ProvenanceEventEnricher enricher) {
        this.session = session;
        this.processorId = processorId;
        this.processorType = processorType;
        this.repository = repository;
        this.eventEnricher = enricher;
    }

    Set<ProvenanceEventRecord> getEvents() {
        return Collections.unmodifiableSet(events);
    }

    /**
     * Removes the given event from the reporter
     *
     * @param event event
     */
    void remove(final ProvenanceEventRecord event) {
        events.remove(event);
    }

    void clear() {
        events.clear();
    }

    void migrate(final StandardProvenanceReporter newOwner, final Collection<String> flowFileIds) {
        final Set<ProvenanceEventRecord> toMove = new LinkedHashSet<>();
        for (final ProvenanceEventRecord event : events) {
            if (flowFileIds.contains(event.getFlowFileUuid())) {
                toMove.add(event);
            }
        }

        events.removeAll(toMove);
        newOwner.events.addAll(toMove);
    }

    /**
     * Generates a Fork event for the given child and parents but does not register the event. This is useful so that a ProcessSession has the ability to de-dupe events, since one or more events may
     * be created by the session itself, as well as by the Processor
     *
     * @param parents parents
     * @param child child
     * @return record
     */
    ProvenanceEventRecord generateJoinEvent(final Collection<FlowFile> parents, final FlowFile child) {
        final ProvenanceEventBuilder eventBuilder = build(child, ProvenanceEventType.JOIN);
        eventBuilder.addChildFlowFile(child);

        for (final FlowFile parent : parents) {
            eventBuilder.addParentFlowFile(parent);
        }

        return eventBuilder.build();
    }

    ProvenanceEventRecord generateDropEvent(final FlowFile flowFile, final String details) {
        return build(flowFile, ProvenanceEventType.DROP).setDetails(details).build();
    }

    private void verifyFlowFileKnown(final FlowFile flowFile) {
        if (session != null && !session.isFlowFileKnown(flowFile)) {
            throw new FlowFileHandlingException(flowFile + " is not known to " + session);
        }
    }

    @Override
    public void receive(final FlowFile flowFile, final String transitUri) {
        receive(flowFile, transitUri, -1L);
    }

    @Override
    public void receive(FlowFile flowFile, String transitUri, String sourceSystemFlowFileIdentifier) {
        receive(flowFile, transitUri, sourceSystemFlowFileIdentifier, -1L);
    }

    @Override
    public void receive(final FlowFile flowFile, final String transitUri, final long transmissionMillis) {
        receive(flowFile, transitUri, null, transmissionMillis);
    }

    @Override
    public void receive(final FlowFile flowFile, final String transitUri, final String sourceSystemFlowFileIdentifier, final long transmissionMillis) {
        receive(flowFile, transitUri, sourceSystemFlowFileIdentifier, null, transmissionMillis);
    }

    @Override
    public void receive(final FlowFile flowFile, final String transitUri, final String sourceSystemFlowFileIdentifier, final String details, final long transmissionMillis) {
        verifyFlowFileKnown(flowFile);

        try {
            final ProvenanceEventRecord record = build(flowFile, ProvenanceEventType.RECEIVE)
                .setTransitUri(transitUri).setSourceSystemFlowFileIdentifier(sourceSystemFlowFileIdentifier).setEventDuration(transmissionMillis).setDetails(details).build();
            events.add(record);
        } catch (final Exception e) {
            logger.error("Failed to generate Provenance Event due to " + e);
            if (logger.isDebugEnabled()) {
                logger.error("", e);
            }
        }
    }

    @Override
    public void fetch(final FlowFile flowFile, final String transitUri) {
        fetch(flowFile, transitUri, -1L);
    }

    @Override
    public void fetch(final FlowFile flowFile, final String transitUri, final long transmissionMillis) {
        fetch(flowFile, transitUri, null, transmissionMillis);
    }

    @Override
    public void fetch(final FlowFile flowFile, final String transitUri, final String details, final long transmissionMillis) {
        verifyFlowFileKnown(flowFile);

        try {
            final ProvenanceEventRecord record = build(flowFile, ProvenanceEventType.FETCH)
                .setTransitUri(transitUri)
                .setEventDuration(transmissionMillis)
                .setDetails(details)
                .build();
            events.add(record);
        } catch (final Exception e) {
            logger.error("Failed to generate Provenance Event due to " + e);
            if (logger.isDebugEnabled()) {
                logger.error("", e);
            }
        }
    }

    @Override
    public void send(final FlowFile flowFile, final String transitUri, final long transmissionMillis) {
        send(flowFile, transitUri, transmissionMillis, true);
    }

    @Override
    public void send(final FlowFile flowFile, final String transitUri) {
        send(flowFile, transitUri, null, -1L, true);
    }

    @Override
    public void send(final FlowFile flowFile, final String transitUri, final String details) {
        send(flowFile, transitUri, details, -1L, true);
    }

    @Override
    public void send(final FlowFile flowFile, final String transitUri, final long transmissionMillis, final boolean force) {
        send(flowFile, transitUri, null, transmissionMillis, force);
    }

    @Override
    public void send(final FlowFile flowFile, final String transitUri, final String details, final boolean force) {
        send(flowFile, transitUri, details, -1L, force);
    }

    @Override
    public void send(final FlowFile flowFile, final String transitUri, final String details, final long transmissionMillis) {
        send(flowFile, transitUri, details, transmissionMillis, true);
    }

    @Override
    public void send(final FlowFile flowFile, final String transitUri, final String details, final long transmissionMillis, final boolean force) {
        try {
            final ProvenanceEventRecord record = build(flowFile, ProvenanceEventType.SEND).setTransitUri(transitUri).setEventDuration(transmissionMillis).setDetails(details).build();
            final ProvenanceEventRecord enriched = eventEnricher == null ? record : eventEnricher.enrich(record, flowFile);

            if (force) {
                repository.registerEvent(enriched);
            } else {
                events.add(enriched);
            }
        } catch (final Exception e) {
            logger.error("Failed to generate Provenance Event due to " + e);
            if (logger.isDebugEnabled()) {
                logger.error("", e);
            }
        }
    }

    @Override
    public void send(final FlowFile flowFile, final String transitUri, final boolean force) {
        send(flowFile, transitUri, -1L, true);
    }

    @Override
    public void associate(final FlowFile flowFile, final String alternateIdentifierNamespace, final String alternateIdentifier) {
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
            final ProvenanceEventRecord record = build(flowFile, ProvenanceEventType.ADDINFO).setAlternateIdentifierUri(alternateIdentifierUri).build();
            events.add(record);
        } catch (final Exception e) {
            logger.error("Failed to generate Provenance Event due to " + e);
            if (logger.isDebugEnabled()) {
                logger.error("", e);
            }
        }
    }

    ProvenanceEventRecord drop(final FlowFile flowFile, final String reason) {
        try {
            final ProvenanceEventBuilder builder = build(flowFile, ProvenanceEventType.DROP);
            if (reason != null) {
                builder.setDetails("Discard reason: " + reason);
            }
            final ProvenanceEventRecord record = builder.build();
            events.add(record);
            return record;
        } catch (final Exception e) {
            logger.error("Failed to generate Provenance Event due to " + e);
            if (logger.isDebugEnabled()) {
                logger.error("", e);
            }
            return null;
        }
    }

    void expire(final FlowFile flowFile, final String details) {
        try {
            final ProvenanceEventRecord record = build(flowFile, ProvenanceEventType.EXPIRE).setDetails(details).build();
            events.add(record);
        } catch (final Exception e) {
            logger.error("Failed to generate Provenance Event due to " + e);
            if (logger.isDebugEnabled()) {
                logger.error("", e);
            }
        }
    }

    @Override
    public void fork(final FlowFile parent, final Collection<FlowFile> children) {
        fork(parent, children, null, -1L);
    }

    @Override
    public void fork(final FlowFile parent, final Collection<FlowFile> children, final long forkDuration) {
        fork(parent, children, null, forkDuration);
    }

    @Override
    public void fork(final FlowFile parent, final Collection<FlowFile> children, final String details) {
        fork(parent, children, details, -1L);
    }

    @Override
    public void fork(final FlowFile parent, final Collection<FlowFile> children, final String details, final long forkDuration) {
        verifyFlowFileKnown(parent);

        try {
            final ProvenanceEventBuilder eventBuilder = build(parent, ProvenanceEventType.FORK);
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

            events.add(eventBuilder.build());
        } catch (final Exception e) {
            logger.error("Failed to generate Provenance Event due to " + e);
            if (logger.isDebugEnabled()) {
                logger.error("", e);
            }
        }
    }

    @Override
    public void join(final Collection<FlowFile> parents, final FlowFile child) {
        join(parents, child, null, -1L);
    }

    @Override
    public void join(final Collection<FlowFile> parents, final FlowFile child, final long joinDuration) {
        join(parents, child, null, joinDuration);
    }

    @Override
    public void join(final Collection<FlowFile> parents, final FlowFile child, final String details) {
        join(parents, child, details, -1L);
    }

    @Override
    public void join(final Collection<FlowFile> parents, final FlowFile child, final String details, final long joinDuration) {
        verifyFlowFileKnown(child);

        try {
            final ProvenanceEventBuilder eventBuilder = build(child, ProvenanceEventType.JOIN);
            eventBuilder.addChildFlowFile(child);
            eventBuilder.setDetails(details);

            for (final FlowFile parent : parents) {
                eventBuilder.addParentFlowFile(parent);
            }

            events.add(eventBuilder.build());
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

    void clone(final FlowFile parent, final FlowFile child, final boolean verifyFlowFile) {
        if (verifyFlowFile) {
            verifyFlowFileKnown(child);
        }

        try {
            final ProvenanceEventBuilder eventBuilder = build(parent, ProvenanceEventType.CLONE);
            eventBuilder.addChildFlowFile(child);
            eventBuilder.addParentFlowFile(parent);
            events.add(eventBuilder.build());
        } catch (final Exception e) {
            logger.error("Failed to generate Provenance Event due to " + e);
            if (logger.isDebugEnabled()) {
                logger.error("", e);
            }
        }
    }

    @Override
    public void modifyContent(final FlowFile flowFile) {
        modifyContent(flowFile, null, -1L);
    }

    @Override
    public void modifyContent(final FlowFile flowFile, final String details) {
        modifyContent(flowFile, details, -1L);
    }

    @Override
    public void modifyContent(final FlowFile flowFile, final long processingMillis) {
        modifyContent(flowFile, null, processingMillis);
    }

    @Override
    public void modifyContent(final FlowFile flowFile, final String details, final long processingMillis) {
        verifyFlowFileKnown(flowFile);

        try {
            final ProvenanceEventRecord record = build(flowFile, ProvenanceEventType.CONTENT_MODIFIED).setEventDuration(processingMillis).setDetails(details).build();
            events.add(record);
        } catch (final Exception e) {
            logger.error("Failed to generate Provenance Event due to " + e);
            if (logger.isDebugEnabled()) {
                logger.error("", e);
            }
        }
    }

    @Override
    public void modifyAttributes(final FlowFile flowFile) {
        modifyAttributes(flowFile, null);
    }

    @Override
    public void modifyAttributes(final FlowFile flowFile, final String details) {
        verifyFlowFileKnown(flowFile);

        try {
            final ProvenanceEventRecord record = build(flowFile, ProvenanceEventType.ATTRIBUTES_MODIFIED).setDetails(details).build();
            events.add(record);
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
            final ProvenanceEventRecord record = build(flowFile, ProvenanceEventType.ROUTE).setRelationship(relationship).setDetails(details).setEventDuration(processingDuration).build();
            events.add(record);
        } catch (final Exception e) {
            logger.error("Failed to generate Provenance Event due to " + e);
            if (logger.isDebugEnabled()) {
                logger.error("", e);
            }
        }
    }

    @Override
    public void create(final FlowFile flowFile) {
        create(flowFile, null);
    }

    @Override
    public void create(final FlowFile flowFile, final String details) {
        verifyFlowFileKnown(flowFile);

        try {
            final ProvenanceEventRecord record = build(flowFile, ProvenanceEventType.CREATE).setDetails(details).build();
            events.add(record);
        } catch (final Exception e) {
            logger.error("Failed to generate Provenance Event due to " + e);
            if (logger.isDebugEnabled()) {
                logger.error("", e);
            }
        }
    }

    ProvenanceEventBuilder build(final FlowFile flowFile, final ProvenanceEventType eventType) {
        final ProvenanceEventBuilder builder = repository.eventBuilder();
        builder.setEventType(eventType);
        builder.fromFlowFile(flowFile);
        builder.setLineageStartDate(flowFile.getLineageStartDate());
        builder.setComponentId(processorId);
        builder.setComponentType(processorType);
        return builder;
    }
}
