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
package org.apache.nifi.web.api.dto.provenance;

import com.wordnik.swagger.annotations.ApiModelProperty;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import org.apache.nifi.web.api.dto.util.TimestampAdapter;

/**
 * A provenance event.
 */
@XmlType(name = "provenanceEvent")
public class ProvenanceEventDTO {

    private String id;

    // in search results table
    private Long eventId;
    private Date eventTime;
    private Long eventDuration;
    private Long lineageDuration;
    private String eventType;
    private String flowFileUuid;
    private String fileSize;
    private Long fileSizeBytes;
    private String clusterNodeId;    // include when clustered
    private String clusterNodeAddress; // include when clustered

    private String groupId;
    private String componentId;
    private String componentType;
    private String componentName;
    private String sourceSystemFlowFileId;
    private String alternateIdentifierUri;
    private Collection<AttributeDTO> attributes;
    private List<String> parentUuids;
    private List<String> childUuids;

    private String transitUri;

    private String relationship;
    private String details;

    // content
    private Boolean contentEqual;
    private Boolean inputContentAvailable;
    private String inputContentClaimSection;
    private String inputContentClaimContainer;
    private String inputContentClaimIdentifier;
    private Long inputContentClaimOffset;
    private String inputContentClaimFileSize;
    private Long inputContentClaimFileSizeBytes;
    private Boolean outputContentAvailable;
    private String outputContentClaimSection;
    private String outputContentClaimContainer;
    private String outputContentClaimIdentifier;
    private Long outputContentClaimOffset;
    private String outputContentClaimFileSize;
    private Long outputContentClaimFileSizeBytes;

    // replay
    private Boolean replayAvailable;
    private String replayExplanation;
    private String sourceConnectionIdentifier;

    /**
     * @return event uuid
     */
    @ApiModelProperty(
            value = "The event uuid."
    )
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    /**
     * @return event id
     */
    @ApiModelProperty(
            value = "The event id. This is a one up number thats unique per node."
    )
    public Long getEventId() {
        return eventId;
    }

    public void setEventId(Long eventId) {
        this.eventId = eventId;
    }

    /**
     * @return time the event occurred
     */
    @XmlJavaTypeAdapter(TimestampAdapter.class)
    @ApiModelProperty(
            value = "The timestamp of the event.",
            dataType = "string"
    )
    public Date getEventTime() {
        return eventTime;
    }

    public void setEventTime(Date eventTime) {
        this.eventTime = eventTime;
    }

    /**
     * @return UUID of the FlowFile for this event
     */
    @ApiModelProperty(
            value = "The uuid of the flowfile for the event."
    )
    public String getFlowFileUuid() {
        return flowFileUuid;
    }

    public void setFlowFileUuid(String flowFileUuid) {
        this.flowFileUuid = flowFileUuid;
    }

    /**
     * @return size of the FlowFile for this event
     */
    @ApiModelProperty(
            value = "The size of the flowfile for the event."
    )
    public String getFileSize() {
        return fileSize;
    }

    public void setFileSize(String fileSize) {
        this.fileSize = fileSize;
    }

    /**
     * @return size of the FlowFile in bytes for this event
     */
    @ApiModelProperty(
            value = "The size of the flowfile in bytes for the event."
    )
    public Long getFileSizeBytes() {
        return fileSizeBytes;
    }

    public void setFileSizeBytes(Long fileSizeBytes) {
        this.fileSizeBytes = fileSizeBytes;
    }

    /**
     * @return type of this event
     */
    @ApiModelProperty(
            value = "The type of the event."
    )
    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    /**
     * @return attributes for the FlowFile for this event
     */
    @ApiModelProperty(
            value = "The attributes of the flowfile for the event."
    )
    public Collection<AttributeDTO> getAttributes() {
        return attributes;
    }

    public void setAttributes(Collection<AttributeDTO> attributes) {
        this.attributes = attributes;
    }

    /**
     * @return id of the group that this component resides in. If the component is no longer in the flow, the group id will not be set
     */
    @ApiModelProperty(
            value = "The id of the group that the component resides in. If the component is no longer in the flow, the group id will not be set."
    )
    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    /**
     * @return id of the component that generated this event
     */
    @ApiModelProperty(
            value = "The id of the component that generated the event."
    )
    public String getComponentId() {
        return componentId;
    }

    public void setComponentId(String componentId) {
        this.componentId = componentId;
    }

    /**
     * @return name of the component that generated this event
     */
    @ApiModelProperty(
            value = "The name of the component that generated the event."
    )
    public String getComponentName() {
        return componentName;
    }

    public void setComponentName(String componentName) {
        this.componentName = componentName;
    }

    /**
     * @return type of the component that generated this event
     */
    @ApiModelProperty(
            value = "The type of the component that generated the event."
    )
    public String getComponentType() {
        return componentType;
    }

    public void setComponentType(String componentType) {
        this.componentType = componentType;
    }

    /**
     * @return source/destination system URI if the event was a RECEIVE/SEND
     */
    @ApiModelProperty(
            value = "The source/destination system uri if the event was a RECEIVE/SEND."
    )
    public String getTransitUri() {
        return transitUri;
    }

    public void setTransitUri(String transitUri) {
        this.transitUri = transitUri;
    }

    /**
     * @return alternate identifier URI for the FlowFile for this event
     */
    @ApiModelProperty(
            value = "The alternate identifier uri for the fileflow for the event."
    )
    public String getAlternateIdentifierUri() {
        return alternateIdentifierUri;
    }

    public void setAlternateIdentifierUri(String alternateIdentifierUri) {
        this.alternateIdentifierUri = alternateIdentifierUri;
    }

    /**
     * @return identifier of the node where this event originated
     */
    @ApiModelProperty(
            value = "The identifier for the node where the event originated."
    )
    public String getClusterNodeId() {
        return clusterNodeId;
    }

    public void setClusterNodeId(String clusterNodeId) {
        this.clusterNodeId = clusterNodeId;
    }

    /**
     * @return label to use to show which node this event originated from
     */
    @ApiModelProperty(
            value = "The label for the node where the event originated."
    )
    public String getClusterNodeAddress() {
        return clusterNodeAddress;
    }

    public void setClusterNodeAddress(String clusterNodeAddress) {
        this.clusterNodeAddress = clusterNodeAddress;
    }

    /**
     * @return parent uuids for this event
     */
    @ApiModelProperty(
            value = "The parent uuids for the event."
    )
    public List<String> getParentUuids() {
        return parentUuids;
    }

    public void setParentUuids(List<String> parentUuids) {
        this.parentUuids = parentUuids;
    }

    /**
     * @return child uuids for this event
     */
    @ApiModelProperty(
            value = "The child uuids for the event."
    )
    public List<String> getChildUuids() {
        return childUuids;
    }

    public void setChildUuids(List<String> childUuids) {
        this.childUuids = childUuids;
    }

    /**
     * @return duration of the event, in milliseconds
     */
    @ApiModelProperty(
            value = "The event duration in milliseconds."
    )
    public Long getEventDuration() {
        return eventDuration;
    }

    public void setEventDuration(Long eventDuration) {
        this.eventDuration = eventDuration;
    }

    /**
     * @return duration since the lineage began, in milliseconds
     */
    @ApiModelProperty(
            value = "The duration since the lineage began, in milliseconds."
    )
    public Long getLineageDuration() {
        return lineageDuration;
    }

    public void setLineageDuration(Long lineageDuration) {
        this.lineageDuration = lineageDuration;
    }

    /**
     * @return source system FlowFile id
     */
    @ApiModelProperty(
            value = "The source system flowfile id."
    )
    public String getSourceSystemFlowFileId() {
        return sourceSystemFlowFileId;
    }

    public void setSourceSystemFlowFileId(String sourceSystemFlowFileId) {
        this.sourceSystemFlowFileId = sourceSystemFlowFileId;
    }

    /**
     * @return If this represents a route event, this is the relationship to which the flowfile was routed
     */
    @ApiModelProperty(
            value = "The relationship to which the flowfile was routed if the event is of type ROUTE."
    )
    public String getRelationship() {
        return relationship;
    }

    public void setRelationship(String relationship) {
        this.relationship = relationship;
    }

    /**
     * @return event details
     */
    @ApiModelProperty(
            value = "The event details."
    )
    public String getDetails() {
        return details;
    }

    public void setDetails(String details) {
        this.details = details;
    }

    /**
     * @return whether or not the input and output content claim is the same
     */
    @ApiModelProperty(
            value = "Whether the input and output content claim is the same."
    )
    public Boolean getContentEqual() {
        return contentEqual;
    }

    public void setContentEqual(Boolean contentEqual) {
        this.contentEqual = contentEqual;
    }

    /**
     * @return whether or not the output content is still available
     */
    @ApiModelProperty(
            value = "Whether the output content is still available."
    )
    public Boolean getOutputContentAvailable() {
        return outputContentAvailable;
    }

    public void setOutputContentAvailable(Boolean outputContentAvailable) {
        this.outputContentAvailable = outputContentAvailable;
    }

    /**
     * @return the Section in which the output Content Claim lives, or <code>null</code> if no Content Claim exists
     */
    @ApiModelProperty(
            value = "The section in which the output content claim lives."
    )
    public String getOutputContentClaimSection() {
        return outputContentClaimSection;
    }

    public void setOutputContentClaimSection(String contentClaimSection) {
        this.outputContentClaimSection = contentClaimSection;
    }

    /**
     * @return the Container in which the output Content Claim lives, or <code>null</code> if no Content Claim exists
     */
    @ApiModelProperty(
            value = "The container in which the output content claim lives."
    )
    public String getOutputContentClaimContainer() {
        return outputContentClaimContainer;
    }

    public void setOutputContentClaimContainer(String outputContentClaimContainer) {
        this.outputContentClaimContainer = outputContentClaimContainer;
    }

    /**
     * @return the Identifier of the output Content Claim, or <code>null</code> if no Content Claim exists
     */
    @ApiModelProperty(
            value = "The identifier of the output content claim."
    )
    public String getOutputContentClaimIdentifier() {
        return outputContentClaimIdentifier;
    }

    public void setOutputContentClaimIdentifier(String outputContentClaimIdentifier) {
        this.outputContentClaimIdentifier = outputContentClaimIdentifier;
    }

    /**
     * @return the offset into the the output Content Claim where the FlowFile's content begins, or <code>null</code> if no Content Claim exists
     */
    @ApiModelProperty(
            value = "The offset into the output content claim where the flowfiles content begins."
    )
    public Long getOutputContentClaimOffset() {
        return outputContentClaimOffset;
    }

    public void setOutputContentClaimOffset(Long outputContentClaimOffset) {
        this.outputContentClaimOffset = outputContentClaimOffset;
    }

    /**
     * @return the formatted file size of the input content claim
     */
    @ApiModelProperty(
            value = "The file size of the output content claim formatted."
    )
    public String getOutputContentClaimFileSize() {
        return outputContentClaimFileSize;
    }

    public void setOutputContentClaimFileSize(String outputContentClaimFileSize) {
        this.outputContentClaimFileSize = outputContentClaimFileSize;
    }

    /**
     * @return the number of bytes of the input content claim
     */
    @ApiModelProperty(
            value = "The file size of the output content claim in bytes."
    )
    public Long getOutputContentClaimFileSizeBytes() {
        return outputContentClaimFileSizeBytes;
    }

    public void setOutputContentClaimFileSizeBytes(Long outputContentClaimFileSizeBytes) {
        this.outputContentClaimFileSizeBytes = outputContentClaimFileSizeBytes;
    }

    /**
     * @return whether or not the input content is still available
     */
    @ApiModelProperty(
            value = "Whether the input content is still available."
    )
    public Boolean getInputContentAvailable() {
        return inputContentAvailable;
    }

    public void setInputContentAvailable(Boolean inputContentAvailable) {
        this.inputContentAvailable = inputContentAvailable;
    }

    /**
     * @return the Section in which the input Content Claim lives, or <code>null</code> if no Content Claim exists
     */
    @ApiModelProperty(
            value = "The section in which the input content claim lives."
    )
    public String getInputContentClaimSection() {
        return inputContentClaimSection;
    }

    public void setInputContentClaimSection(String inputContentClaimSection) {
        this.inputContentClaimSection = inputContentClaimSection;
    }

    /**
     * @return the Container in which the input Content Claim lives, or <code>null</code> if no Content Claim exists
     */
    @ApiModelProperty(
            value = "The container in which the input content claim lives."
    )
    public String getInputContentClaimContainer() {
        return inputContentClaimContainer;
    }

    public void setInputContentClaimContainer(String inputContentClaimContainer) {
        this.inputContentClaimContainer = inputContentClaimContainer;
    }

    /**
     * @return the Identifier of the input Content Claim, or <code>null</code> if no Content Claim exists
     */
    @ApiModelProperty(
            value = "The identifier of the input content claim."
    )
    public String getInputContentClaimIdentifier() {
        return inputContentClaimIdentifier;
    }

    public void setInputContentClaimIdentifier(String inputContentClaimIdentifier) {
        this.inputContentClaimIdentifier = inputContentClaimIdentifier;
    }

    /**
     * @return the offset into the the input Content Claim where the FlowFile's content begins, or <code>null</code> if no Content Claim exists
     */
    @ApiModelProperty(
            value = "The offset into the input content claim where the flowfiles content begins."
    )
    public Long getInputContentClaimOffset() {
        return inputContentClaimOffset;
    }

    public void setInputContentClaimOffset(Long inputContentClaimOffset) {
        this.inputContentClaimOffset = inputContentClaimOffset;
    }

    /**
     * @return the formatted file size of the input content claim
     */
    @ApiModelProperty(
            value = "The file size of the input content claim formatted."
    )
    public String getInputContentClaimFileSize() {
        return inputContentClaimFileSize;
    }

    public void setInputContentClaimFileSize(String inputContentClaimFileSize) {
        this.inputContentClaimFileSize = inputContentClaimFileSize;
    }

    /**
     * @return the number of bytes of the input content claim
     */
    @ApiModelProperty(
            value = "The file size of the intput content claim in bytes."
    )
    public Long getInputContentClaimFileSizeBytes() {
        return inputContentClaimFileSizeBytes;
    }

    public void setInputContentClaimFileSizeBytes(Long inputContentClaimFileSizeBytes) {
        this.inputContentClaimFileSizeBytes = inputContentClaimFileSizeBytes;
    }

    /**
     * @return whether or not replay is available
     */
    @ApiModelProperty(
            value = "Whether or not replay is available."
    )
    public Boolean getReplayAvailable() {
        return replayAvailable;
    }

    public void setReplayAvailable(Boolean replayAvailable) {
        this.replayAvailable = replayAvailable;
    }

    /**
     * @return the explanation as to why replay is unavailable
     */
    @ApiModelProperty(
            value = "Explanation as to why replay is unavailable."
    )
    public String getReplayExplanation() {
        return replayExplanation;
    }

    public void setReplayExplanation(String replayExplanation) {
        this.replayExplanation = replayExplanation;
    }

    /**
     * @return identifier of the FlowFile Queue / Connection from which the FlowFile was pulled to generate this event, or <code>null</code> if either the queue is unknown or the FlowFile was created
     * by this event
     */
    @ApiModelProperty(
            value = "The identifier of the queue/connection from which the flowfile was pulled to genereate this event. May be null if the queue/connection is unknown or the "
                    + "flowfile was generated from this event."
    )
    public String getSourceConnectionIdentifier() {
        return sourceConnectionIdentifier;
    }

    public void setSourceConnectionIdentifier(String sourceConnectionIdentifier) {
        this.sourceConnectionIdentifier = sourceConnectionIdentifier;
    }
}
