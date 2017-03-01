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

/**
 * Describes an event that happened to a FlowFile.
 */
public interface ProvenanceEventRecord {

    String REMOTE_INPUT_PORT_TYPE = "Remote Input Port";
    String REMOTE_OUTPUT_PORT_TYPE = "Remote Output Port";

    /**
     * @return a unique ID for this Provenance Event. Depending on the
     * implementation, the Event ID may be set to -1 until the event has been
     * added to the {@link ProvenanceEventRepository}
     */
    long getEventId();

    /**
     * @return the time at which this Provenance Event was created, as the
     * number of milliseconds since epoch
     */
    long getEventTime();

    /**
     * @return the EntryDate of the FlowFile to which this Event is associated
     */
    long getFlowFileEntryDate();

    /**
     * @return the time at which the lineage began
     */
    long getLineageStartDate();

    /**
     * @return the size of the FlowFile to which this Event is associated
     */
    long getFileSize();

    /**
     * @return the previous size of the FlowFile to which this Event is
     * associated, if the FlowFile previously had content and its size was
     * known; otherwise, returns <code>null</code>
     */
    Long getPreviousFileSize();

    /**
     * @return the amount of time in milliseconds that elapsed while performing
     * this event. If not populated, the value -1 will be returned
     */
    long getEventDuration();

    /**
     * @return the type of this Provenance Event
     */
    ProvenanceEventType getEventType();

    /**
     * @return all FlowFile attributes that were associated with the FlowFile at
     * the time that this ProvenanceEvent was created
     */
    Map<String, String> getAttributes();

    /**
     * Returns the attribute with the given name
     *
     * @param attributeName the name of the attribute to get
     * @return the attribute with the given name or <code>null</code> if no attribute exists with the given name
     */
    default String getAttribute(String attributeName) {
        return getAttributes().get(attributeName);
    }

    /**
     * @return all FlowFile attributes that existed on the FlowFile before this
     * event occurred
     */
    Map<String, String> getPreviousAttributes();

    /**
     * @return all FlowFile attributes that were updated as a result of this
     * event
     */
    Map<String, String> getUpdatedAttributes();

    /**
     * @return the ID of the Processor/component that created this Provenance
     * Event
     */
    String getComponentId();

    /**
     * @return the fully-qualified Class Name of the Processor/component that
     * created this Provenance Event
     */
    String getComponentType();

    /**
     * @return whether this event originated from a remote group port
     */
    default boolean isRemotePortType() {
        final String componentType = getComponentType();
        return REMOTE_INPUT_PORT_TYPE.equals(componentType) || REMOTE_OUTPUT_PORT_TYPE.equals(componentType);
    }

    /**
     * @return a URI that provides information about the System and Protocol
     * information over which the transfer occurred. The intent of this field is
     * such that both the sender and the receiver can publish the events to an
     * external Enterprise-wide system that is then able to correlate the SEND
     * and RECEIVE events.
     */
    String getTransitUri();

    /**
     * Since the receiving system will usually refer to the data using a
     * different identifier than the source system, this information is used to
     * correlate the receive system's FlowFile with the sending system's data
     *
     * @return the UUID that the Source System used to refer to this data; this
     * is applicable only when the {@link ProvenanceEventType} is of type
     * {@link ProvenanceEventType#RECEIVE RECEIVE}
     */
    String getSourceSystemFlowFileIdentifier();

    /**
     * @return the UUID of the FlowFile with which this Event is associated
     */
    String getFlowFileUuid();

    /**
     * @return the UUID's of all Parent FlowFiles. This is applicable only when
     * the {@link ProvenanceEventType} is of type
     * {@link ProvenanceEventType#SPAWN SPAWN}
     */
    List<String> getParentUuids();

    /**
     * @return the UUID's of all Child FlowFiles. This is applicable only when
     * the {@link ProvenanceEventType} is of type
     * {@link ProvenanceEventType#SPAWN SPAWN}
     */
    List<String> getChildUuids();

    /**
     * @return the Alternate Identifier associated with the FlowFile with which
     * this Event is associated. This is applicable only when the
     * {@link ProvenanceEventType} is of type
     * {@link ProvenanceEventType#ADDINFO}
     */
    String getAlternateIdentifierUri();

    /**
     * @return the details for this record, if any were supplied. Otherwise,
     * returns <code>null</code>
     */
    String getDetails();

    /**
     * @return the relationship to which this record was routed if the event
     * type is {@link ProvenanceEventType#ROUTE}. The relationship is applicable
     * only to this type
     */
    String getRelationship();

    /**
     * @return the identifier of the queue from which the FlowFile was taken, if
     * any. If the FlowFile is created as a result of this event (in this case,
     * the Event Type is one of null null null null null null null null     {@link ProvenanceEventType#CREATE}, {@link ProvenanceEventType#RECEIVE},
     * {@link ProvenanceEventType#FORK}, {@link ProvenanceEventType#JOIN}, or
     * {@link ProvenanceEventType#CLONE}), or if the queue identifier is
     * unknown, then this method will return <code>null</code>
     *
     */
    String getSourceQueueIdentifier();

    /**
     * @return the Section for the Content Claim that this Event refers to, if
     * any; otherwise, returns <code>null</code>
     *
     */
    String getContentClaimSection();

    /**
     * @return the Section for the Content Claim that the FlowFile previously
     * referenced, if any; otherwise, returns <code>null</code>
     *
     */
    String getPreviousContentClaimSection();

    /**
     * @return the Container for the Content Claim that this Event refers to, if
     * any; otherwise, returns <code>null</code>
     *
     */
    String getContentClaimContainer();

    /**
     * @return the Container for the Content Claim that the FlowFile previously
     * referenced, if any; otherwise, returns <code>null</code>
     */
    String getPreviousContentClaimContainer();

    /**
     * @return the Identifier for the Content Claim that this Event refers to,
     * if any; otherwise, returns <code>null</code>
     *
     */
    String getContentClaimIdentifier();

    /**
     * @return the Identifier for the Content Claim that the FlowFile previously
     * referenced, if any; otherwise, returns <code>null</code>
     *
     */
    String getPreviousContentClaimIdentifier();

    /**
     * @return the offset into the Content Claim at which the FlowFile's content
     * begins, if any; otherwise, returns <code>null</code>
     *
     */
    Long getContentClaimOffset();

    /**
     * @return the offset into the Content Claim at which the FlowFile's
     * previous content began, if any; otherwise, returns <code>null</code>
     *
     */
    Long getPreviousContentClaimOffset();
}
