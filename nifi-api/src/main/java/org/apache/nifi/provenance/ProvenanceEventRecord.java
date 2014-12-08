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
import java.util.Set;

/**
 * Describes an event that happened to a FlowFile.
 */
public interface ProvenanceEventRecord {

    /**
     * Returns a unique ID for this Provenance Event. Depending on the
     * implementation, the Event ID may be set to -1 until the event has been
     * added to the {@link ProvenanceEventRepository}
     *
     * @return
     */
    long getEventId();

    /**
     * Returns the time at which this Provenance Event was created, as the
     * number of milliseconds since epoch
     *
     * @return
     */
    long getEventTime();

    /**
     * Returns the EntryDate of the FlowFile to which this Event is associated
     *
     * @return
     */
    long getFlowFileEntryDate();

    /**
     * @return the time at which the lineage began
     */
    long getLineageStartDate();

    /**
     * @return the set of all lineage identifiers that are associated with the
     * FlowFile for which this Event was created
     */
    Set<String> getLineageIdentifiers();

    /**
     * Returns the size of the FlowFile to which this Event is associated
     *
     * @return
     */
    long getFileSize();

    /**
     * Returns the previous size of the FlowFile to which this Event is
     * associated, if the FlowFile previously had content and its size was
     * known; otherwise, returns <code>null</code>
     *
     * @return
     */
    Long getPreviousFileSize();

    /**
     * Returns the amount of time in milliseconds that elapsed while performing
     * this event. If not populated, the value -1 will be returned.
     *
     * @return
     */
    long getEventDuration();

    /**
     * Returns the type of this Provenance Event
     *
     * @return
     */
    ProvenanceEventType getEventType();

    /**
     * Returns all FlowFile attributes that were associated with the FlowFile at
     * the time that this ProvenanceEvent was created
     *
     * @return
     */
    Map<String, String> getAttributes();

    /**
     * Returns all FlowFile attributes that existed on the FlowFile before this
     * event occurred
     *
     * @return
     */
    Map<String, String> getPreviousAttributes();

    /**
     * Returns all FlowFile attributes that were updated as a result of this
     * event
     *
     * @return
     */
    Map<String, String> getUpdatedAttributes();

    /**
     * Returns the ID of the Processor/component that created this Provenance
     * Event
     *
     * @return
     */
    String getComponentId();

    /**
     * Returns the fully-qualified Class Name of the Processor/component that
     * created this Provenance Event
     *
     * @return
     */
    String getComponentType();

    /**
     * Returns a URI that provides information about the System and Protocol
     * information over which the transfer occurred. The intent of this field is
     * such that both the sender and the receiver can publish the events to an
     * external Enterprise-wide system that is then able to correlate the SEND
     * and RECEIVE events.
     *
     * @return
     */
    String getTransitUri();

    /**
     * Returns the UUID that the Source System used to refer to this data; this
     * is applicable only when the {@link ProvenanceEventType} is of type
     * {@link ProvenanceEventType#RECEIVE RECEIVE}.
     *
     * Since the receiving system will usually refer to the data using a
     * different identifier than the source system, this information is used to
     * correlate the receive system's FlowFile with the sending system's data
     *
     * @return
     */
    String getSourceSystemFlowFileIdentifier();

    /**
     * Returns the UUID of the FlowFile with which this Event is associated
     *
     * @return
     */
    String getFlowFileUuid();

    /**
     * Returns the UUID's of all Parent FlowFiles. This is applicable only when
     * the {@link ProvenanceEventType} is of type
     * {@link ProvenanceEventType#SPAWN SPAWN}.
     *
     * @return
     */
    List<String> getParentUuids();

    /**
     * Returns the UUID's of all Child FlowFiles. This is applicable only when
     * the {@link ProvenanceEventType} is of type
     * {@link ProvenanceEventType#SPAWN SPAWN}.
     *
     * @return
     */
    List<String> getChildUuids();

    /**
     * Returns the Alternate Identifier associated with the FlowFile with which
     * this Event is associated. This is applicable only when the
     * {@link ProvenanceEventType} is of type
     * {@link ProvenanceEventType#ADDINFO}.
     *
     * @return
     */
    String getAlternateIdentifierUri();

    /**
     * Returns the details for this record, if any were supplied. Otherwise,
     * returns <code>null</code>
     *
     * @return
     *
     */
    String getDetails();

    /**
     * Returns the relationship to which this record was routed if the event
     * type is {@link ProvenanceEventType#ROUTE}. The relationship is applicable
     * only to this type.
     *
     * @return
     *
     */
    String getRelationship();

    /**
     * Returns the identifier of the queue from which the FlowFile was taken, if
     * any. If the FlowFile is created as a result of this event (in this case,
     * the Event Type is one of null null null null null     {@link ProvenanceEventType#CREATE}, {@link ProvenanceEventType#RECEIVE},
     * {@link ProvenanceEventType#FORK}, {@link ProvenanceEventType#JOIN}, or
     * {@link ProvenanceEventType#CLONE}), or if the queue identifier is
     * unknown, then this method will return <code>null</code>.
     *
     * @return
     *
     */
    String getSourceQueueIdentifier();

    /**
     * Returns the Section for the Content Claim that this Event refers to, if
     * any; otherwise, returns <code>null</code>
     *
     * @return
     *
     */
    String getContentClaimSection();

    /**
     * Returns the Section for the Content Claim that the FlowFile previously
     * referenced, if any; otherwise, returns <code>null</code>
     *
     * @return
     *
     */
    String getPreviousContentClaimSection();

    /**
     * Returns the Container for the Content Claim that this Event refers to, if
     * any; otherwise, returns <code>null</code>
     *
     * @return
     *
     */
    String getContentClaimContainer();

    /**
     * Returns the Container for the Content Claim that the FlowFile previously
     * referenced, if any; otherwise, returns <code>null</code>
     *
     * @return
     *
     */
    String getPreviousContentClaimContainer();

    /**
     * Returns the Identifier for the Content Claim that this Event refers to,
     * if any; otherwise, returns <code>null</code>
     *
     * @return
     *
     */
    String getContentClaimIdentifier();

    /**
     * Returns the Identifier for the Content Claim that the FlowFile previously
     * referenced, if any; otherwise, returns <code>null</code>
     *
     * @return
     *
     */
    String getPreviousContentClaimIdentifier();

    /**
     * Returns the offset into the Content Claim at which the FlowFile's content
     * begins, if any; otherwise, returns <code>null</code>
     *
     * @return
     *
     */
    Long getContentClaimOffset();

    /**
     * Returns the offset into the Content Claim at which the FlowFile's
     * previous content began, if any; otherwise, returns <code>null</code>
     *
     * @return
     *
     */
    Long getPreviousContentClaimOffset();
}
