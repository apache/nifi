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
package org.apache.nifi.provenance;

import java.util.List;
import java.util.Map;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.Relationship;

public interface ProvenanceEventBuilder {

    /**
     * Sets the type of {@link ProvenanceEventRecord}
     *
     * @param eventType of the event
     * @return the builder
     */
    ProvenanceEventBuilder setEventType(ProvenanceEventType eventType);

    /**
     * Populates the values of the Event being built from the values in the
     * given event
     *
     * @param event the event from which to populate the Builders values
     * @return the builder
     */
    ProvenanceEventBuilder fromEvent(ProvenanceEventRecord event);

    /**
     * Sets the date and time at which the FlowFile entered the flow
     *
     * @param entryDate of the flow file
     * @return the builder
     */
    ProvenanceEventBuilder setFlowFileEntryDate(long entryDate);

    /**
     * Sets the Content Claim that the FlowFile was previously associated with
     * before this event occurred.
     *
     * @param container for previous content
     * @param section for previous content
     * @param identifier for previous content
     * @param offset for previous content
     * @param size for previous content
     * @return the builder
     */
    ProvenanceEventBuilder setPreviousContentClaim(String container, String section, String identifier, Long offset, long size);

    /**
     * Sets the Content Claim that the FlowFile is associated with as a result
     * of this event
     *
     * @param container for resulting content
     * @param section for resulting content
     * @param identifier for resulting content
     * @param offset for resulting content
     * @param size for resulting content
     * @return the builder
     */
    ProvenanceEventBuilder setCurrentContentClaim(String container, String section, String identifier, Long offset, long size);

    /**
     * Sets the identifier of the FlowFile Queue from which the FlowFile was
     * pulled
     *
     * @param identifier of the source queue
     * @return the builder
     */
    ProvenanceEventBuilder setSourceQueueIdentifier(String identifier);

    /**
     * Sets the attributes that existed on the FlowFile before this event
     * occurred and any attributes that were added or updated as a result of
     * this event.
     *
     * @param previousAttributes Map of all attributes before the event occurred
     * @param updatedAttributes Map containing all attributes that were added or
     * updated. If any entry has a value of <code>null</code>, that attribute is
     * considered removed
     *
     * @return the builder
     */
    ProvenanceEventBuilder setAttributes(Map<String, String> previousAttributes, Map<String, String> updatedAttributes);

    /**
     * Sets the UUID to associate with the FlowFile
     *
     * @param uuid of the flowfile
     * @return the builder
     */
    ProvenanceEventBuilder setFlowFileUUID(String uuid);

    /**
     * Sets the time at which the Provenance Event took place
     *
     * @param eventTime time of the event
     * @return the builder
     */
    ProvenanceEventBuilder setEventTime(long eventTime);

    /**
     * Sets the amount of time that was required in order to perform the
     * function referred to by this event
     *
     * @param millis of the event
     * @return the builder
     */
    ProvenanceEventBuilder setEventDuration(long millis);

    /**
     * Sets the time at which the FlowFile's lineage began
     *
     * @param startDate start date of the event
     * @return the builder
     */
    ProvenanceEventBuilder setLineageStartDate(long startDate);

    /**
     * Sets the unique identifier of the NiFi Component (such as a
     * {@link Processor}) that is generating the Event
     *
     * @param componentId that produced the event
     * @return the builder
     */
    ProvenanceEventBuilder setComponentId(String componentId);

    /**
     * Sets the type of the Component that is generating the Event. For
     * {@link Processor}s, this is the Simple Class Name of the Processor.
     *
     * @param componentType of the component that made the event
     * @return the builder
     */
    ProvenanceEventBuilder setComponentType(String componentType);

    /**
     * Sets the identifier that is used by the remote system to refer to the
     * FlowFile for which this Event is being created.
     *
     * This is valid only for Event Types {@link ProvenanceEventType#RECEIVE}
     * and {@link ProvenanceEventType#SEND} and will be ignored for any other
     * event types.
     *
     * @param sourceSystemFlowFileIdentifier identifier the remote system used
     * @return the builder
     */
    ProvenanceEventBuilder setSourceSystemFlowFileIdentifier(String sourceSystemFlowFileIdentifier);

    /**
     * Sets the Transit URI that is used for the Event. This is a URI that
     * provides information about the System and Protocol information over which
     * the transfer occurred. The intent of this field is such that both the
     * sender and the receiver can publish the events to an external
     * Enterprise-wide system that is then able to correlate the SEND and
     * RECEIVE events.
     *
     * This is valid only for Event Types {@link ProvenanceEventType#RECEIVE}
     * and {@link ProvenanceEventType#SEND} and will be ignored for any other
     * event types.
     *
     * @param transitUri of the event
     * @return the builder
     */
    ProvenanceEventBuilder setTransitUri(String transitUri);

    /**
     * Adds the given FlowFile as a parent for Events of type,
     * {@link ProvenanceEventType#FORK}, {@link ProvenanceEventType#JOIN}, and
     * {@link ProvenanceEventType#CLONE}
     *
     * This is valid only for
     * {@link ProvenanceEventType#FORK}, {@link ProvenanceEventType#JOIN}, and
     * {@link ProvenanceEventType#CLONE} events and will be ignored for any
     * other event types.
     *
     * @param parent flowfile that this event is derived from
     * @return the builder
     */
    ProvenanceEventBuilder addParentFlowFile(FlowFile parent);

    /**
     * Removes the given FlowFile as a parent for Events of type,
     * {@link ProvenanceEventType#FORK}, {@link ProvenanceEventType#JOIN}, and
     * {@link ProvenanceEventType#CLONE}
     *
     * This is valid only for
     * {@link ProvenanceEventType#FORK}, {@link ProvenanceEventType#JOIN}, and
     * {@link ProvenanceEventType#CLONE} events and will be ignored for any
     * other event types.
     *
     * @param parent previous parent of this event
     * @return the builder
     */
    ProvenanceEventBuilder removeParentFlowFile(FlowFile parent);

    /**
     * Adds the given FlowFile as a child for Events of type
     * {@link ProvenanceEventType#FORK}, {@link ProvenanceEventType#JOIN}, and
     * {@link ProvenanceEventType#CLONE}
     *
     * This is valid only for
     * {@link ProvenanceEventType#FORK}, {@link ProvenanceEventType#JOIN}, and
     * {@link ProvenanceEventType#CLONE} events and will be ignored for any
     * other event types.
     *
     * @param child the child to add
     * @return the builder
     */
    ProvenanceEventBuilder addChildFlowFile(FlowFile child);

    /**
     * Adds the given FlowFile identifier as a child for Events of type
     * {@link ProvenanceEventType#FORK} and {@link ProvenanceEventType#CLONE}
     *
     * @param childId the ID of the FlowFile that is a child
     * @return the builder
     */
    ProvenanceEventBuilder addChildFlowFile(String childId);

    /**
     * Removes the given FlowFile as a child for Events of type
     * {@link ProvenanceEventType#FORK}, {@link ProvenanceEventType#JOIN}, and
     * {@link ProvenanceEventType#CLONE}
     *
     * This is valid only for
     * {@link ProvenanceEventType#FORK}, {@link ProvenanceEventType#JOIN}, and
     * {@link ProvenanceEventType#CLONE} events and will be ignored for any
     * other event types.
     *
     * @param child to remove
     * @return the builder
     */
    ProvenanceEventBuilder removeChildFlowFile(FlowFile child);

    /**
     * Associates the given identifier with the FlowFile for which this Event is
     * created. This provides a linkage between the given FlowFile and alternate
     * identifier. This information can be useful if published to an external,
     * enterprise-wide Provenance tracking system that is able to associate the
     * data between different processes or services.
     *
     * This is valid only for {@link ProvenanceEventType#ADDINFO} events and
     * will be ignored for any other event types.
     *
     * @param alternateIdentifierUri another identifier of the flowfile this event is for
     * @return the builder
     */
    ProvenanceEventBuilder setAlternateIdentifierUri(String alternateIdentifierUri);

    /**
     * Sets the details for this event. This is a free-form String that can
     * contain any information that is relevant to this event.
     *
     * @param details a description of the event
     * @return the builder
     */
    ProvenanceEventBuilder setDetails(String details);

    /**
     * Sets the to which the FlowFile was routed for
     * {@link ProvenanceEventType#ROUTE} events. This is valid only for
     * {@link ProvenanceEventType#ROUTE} events and will be ignored for any
     * other event types.
     *
     * @param relationship to which flowfiles in this event were routed
     * @return the builder
     */
    ProvenanceEventBuilder setRelationship(Relationship relationship);

    /**
     * Populates the builder with as much information as it can from the given
     * FlowFile
     *
     * @param flowFile to source attributes for this event from
     * @return the builder
     */
    ProvenanceEventBuilder fromFlowFile(FlowFile flowFile);

    /**
     * Builds the Provenance Event. It is possible that calling
     * {@link ProvenanceEventRecord#getEventId()} on the
     * {@link ProvenanceEventRecord} that is returned will yield
     * <code>-1</code>. This is because the implementation of the Event may
     * depend on the {@link ProvevenanceEventRepository} to generate the unique
     * identifier.
     *
     * @return the event
     */
    ProvenanceEventRecord build();

    /**
     * @return the ids of all FlowFiles that have been added as children via {@link #addChildFlowFile(FlowFile)}
     */
    List<String> getChildFlowFileIds();

    /**
     * @return the ids of all FlowFiles that have been added as parents via {@link #addParentFlowFile(FlowFile)}
     */
    List<String> getParentFlowFileIds();

    /**
     * @return the id of the FlowFile for which the event is being built
     */
    String getFlowFileId();

    /**
     * @return a new Provenance Event Builder that is identical to this one (a deep copy)
     */
    ProvenanceEventBuilder copy();
}
