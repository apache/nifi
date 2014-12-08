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

import java.util.Map;
import java.util.Set;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.Relationship;

public interface ProvenanceEventBuilder {

    /**
     * Sets the type of {@link ProvenanceEventRecord}
     *
     * @param eventType
     * @return
     */
    ProvenanceEventBuilder setEventType(ProvenanceEventType eventType);

    /**
     * Populates the values of the Event being built from the values in the
     * given event
     *
     * @param event the event from which to populate the Builders values
     * @return
     */
    ProvenanceEventBuilder fromEvent(ProvenanceEventRecord event);

    /**
     * Sets the date and time at which the FlowFile entered the flow
     *
     * @param entryDate
     * @return
     */
    ProvenanceEventBuilder setFlowFileEntryDate(long entryDate);

    /**
     * Sets the Lineage Identifiers. This is a set of all FlowFile UUID's that
     * were involved in making this event occur.
     *
     * @param lineageIdentifiers
     * @return
     */
    ProvenanceEventBuilder setLineageIdentifiers(Set<String> lineageIdentifiers);

    /**
     * Sets the Content Claim that the FlowFile was previously associated with
     * before this event occurred.
     *
     * @param container
     * @param section
     * @param identifier
     * @param offset
     * @param size
     * @return
     */
    ProvenanceEventBuilder setPreviousContentClaim(String container, String section, String identifier, Long offset, long size);

    /**
     * Sets the Content Claim that the FlowFile is associated with as a result
     * of this event
     *
     * @param container
     * @param section
     * @param identifier
     * @param offset
     * @param size
     * @return
     */
    ProvenanceEventBuilder setCurrentContentClaim(String container, String section, String identifier, Long offset, long size);

    /**
     * Sets the identifier of the FlowFile Queue from which the FlowFile was
     * pulled
     *
     * @param identifier
     * @return
     */
    ProvenanceEventBuilder setSourceQueueIdentifier(String identifier);

    /**
     * Sets the attributes that existed on the FlowFile before this event
     * occurred and any attributes that were added or updated as a result of
     * this event.
     *
     * @param previousAttributes
     * @param updatedAttributes Map containing all attributes that were added or
     * updated. If any entry has a value of <code>null</code>, that attribute is
     * considered removed
     *
     * @return
     */
    ProvenanceEventBuilder setAttributes(Map<String, String> previousAttributes, Map<String, String> updatedAttributes);

    /**
     * Sets the UUID to associate with the FlowFile
     *
     * @param uuid
     * @return
     */
    ProvenanceEventBuilder setFlowFileUUID(String uuid);

    /**
     * Sets the time at which the Provenance Event took place
     *
     * @param eventTime
     * @return
     */
    ProvenanceEventBuilder setEventTime(long eventTime);

    /**
     * Sets the amount of time that was required in order to perform the
     * function referred to by this event
     *
     * @param millis
     * @return
     */
    ProvenanceEventBuilder setEventDuration(long millis);

    /**
     * Sets the time at which the FlowFile's lineage began
     *
     * @param startDate
     * @return
     */
    ProvenanceEventBuilder setLineageStartDate(long startDate);

    /**
     * Sets the unique identifier of the NiFi Component (such as a
     * {@link Processor}) that is generating the Event
     *
     * @param componentId
     * @return
     */
    ProvenanceEventBuilder setComponentId(String componentId);

    /**
     * Sets the type of the Component that is generating the Event. For
     * {@link Processor}s, this is the Simple Class Name of the Processor.
     *
     * @param componentType
     * @return
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
     * @param sourceSystemFlowFileIdentifier
     * @return
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
     * @param transitUri
     * @return
     */
    ProvenanceEventBuilder setTransitUri(String transitUri);

    /**
     * Adds the given FlowFile as a parent for Events of type {@link ProvenanceEventType#SPAWN},
     * {@link ProvenanceEventType#FORK}, {@link ProvenanceEventType#JOIN}, and
     * {@link ProvenanceEventType#CLONE}
     *
     * This is valid only for null null null null null     {@link ProvenanceEventType#SPAWN}, 
     * {@link ProvenanceEventType#FORK}, {@link ProvenanceEventType#JOIN}, and
     * {@link ProvenanceEventType#CLONE} events and will be ignored for any
     * other event types.
     *
     * @param parent
     * @return
     */
    ProvenanceEventBuilder addParentFlowFile(FlowFile parent);

    /**
     * Removes the given FlowFile as a parent for Events of type {@link ProvenanceEventType#SPAWN},
     * {@link ProvenanceEventType#FORK}, {@link ProvenanceEventType#JOIN}, and
     * {@link ProvenanceEventType#CLONE}
     *
     * This is valid only for null null null null null     {@link ProvenanceEventType#SPAWN}, 
     * {@link ProvenanceEventType#FORK}, {@link ProvenanceEventType#JOIN}, and
     * {@link ProvenanceEventType#CLONE} events and will be ignored for any
     * other event types.
     *
     * @param parent
     * @return
     */
    ProvenanceEventBuilder removeParentFlowFile(FlowFile parent);

    /**
     * Adds the given FlowFile as a child for Events of type {@link ProvenanceEventType#SPAWN},
     * {@link ProvenanceEventType#FORK}, {@link ProvenanceEventType#JOIN}, and
     * {@link ProvenanceEventType#CLONE}
     *
     * This is valid only for null null null null null     {@link ProvenanceEventType#SPAWN}, 
     * {@link ProvenanceEventType#FORK}, {@link ProvenanceEventType#JOIN}, and
     * {@link ProvenanceEventType#CLONE} events and will be ignored for any
     * other event types.
     *
     * @param child
     * @return
     */
    ProvenanceEventBuilder addChildFlowFile(FlowFile child);

    /**
     * Removes the given FlowFile as a child for Events of type {@link ProvenanceEventType#SPAWN},
     * {@link ProvenanceEventType#FORK}, {@link ProvenanceEventType#JOIN}, and
     * {@link ProvenanceEventType#CLONE}
     *
     * This is valid only for null null null null null     {@link ProvenanceEventType#SPAWN}, 
     * {@link ProvenanceEventType#FORK}, {@link ProvenanceEventType#JOIN}, and
     * {@link ProvenanceEventType#CLONE} events and will be ignored for any
     * other event types.
     *
     * @param child
     * @return
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
     * @param alternateIdentifierUri
     * @return
     */
    ProvenanceEventBuilder setAlternateIdentifierUri(String alternateIdentifierUri);

    /**
     * Sets the details for this event. This is a free-form String that can
     * contain any information that is relevant to this event.
     *
     * @param details
     * @return
     */
    ProvenanceEventBuilder setDetails(String details);

    /**
     * Sets the to which the FlowFile was routed for
     * {@link ProvenanceEventType#ROUTE} events. This is valid only for
     * {@link ProvenanceEventType#ROUTE} events and will be ignored for any
     * other event types.
     *
     * @param relationship
     * @return
     */
    ProvenanceEventBuilder setRelationship(Relationship relationship);

    /**
     * Populates the builder with as much information as it can from the given
     * FlowFile
     *
     * @param flowFile
     * @return
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
     * @return
     */
    ProvenanceEventRecord build();

}
