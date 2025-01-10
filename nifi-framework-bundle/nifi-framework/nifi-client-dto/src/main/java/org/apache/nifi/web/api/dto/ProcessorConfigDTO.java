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
package org.apache.nifi.web.api.dto;

import io.swagger.v3.oas.annotations.media.Schema;

import jakarta.xml.bind.annotation.XmlType;
import java.util.Map;
import java.util.Set;

/**
 * Configuration details for a processor in this NiFi.
 */
@XmlType(name = "processorConfig")
public class ProcessorConfigDTO {

    private Map<String, String> properties;
    private Map<String, PropertyDescriptorDTO> descriptors;
    private Set<String> sensitiveDynamicPropertyNames;

    // settings
    private String schedulingPeriod;
    private String schedulingStrategy;
    private String executionNode;
    private String penaltyDuration;
    private String yieldDuration;
    private String bulletinLevel;
    private Long runDurationMillis;
    private Integer concurrentlySchedulableTaskCount;
    private Set<String> autoTerminatedRelationships;
    private String comments;
    private String customUiUrl;
    private Boolean lossTolerant;

    // annotation data
    private String annotationData;

    private Map<String, String> defaultConcurrentTasks;
    private Map<String, String> defaultSchedulingPeriod;

    //retry configurations
    private Integer retryCount;
    private Set<String> retriedRelationships;
    private String backoffMechanism;
    private String maxBackoffPeriod;

    public ProcessorConfigDTO() {

    }

    /**
     * The frequency with which to schedule the processor. The format of the value will depend on the value of {@link #getSchedulingStrategy()}.
     *
     * @return The scheduling period
     */
    @Schema(description = "The frequency with which to schedule the processor. The format of the value will depend on th value of schedulingStrategy."
    )
    public String getSchedulingPeriod() {
        return schedulingPeriod;
    }

    public void setSchedulingPeriod(String setSchedulingPeriod) {
        this.schedulingPeriod = setSchedulingPeriod;
    }

    /**
     * Indicates how the processor should be scheduled to run
     *
     * @return scheduling strategy
     */
    @Schema(description = "Indicates how the processor should be scheduled to run."
    )
    public String getSchedulingStrategy() {
        return schedulingStrategy;
    }

    public void setSchedulingStrategy(String schedulingStrategy) {
        this.schedulingStrategy = schedulingStrategy;
    }

    /**
     * Indicates which node the process should run on
     *
     * @return execution node
     */
    @Schema(description = "Indicates the node where the process will execute."
    )
    public String getExecutionNode() {
        return executionNode;
    }

    public void setExecutionNode(String executionNode) {
        this.executionNode = executionNode;
    }

    /**
     * @return the amount of time that is used when this processor penalizes a flowfile
     */
    @Schema(description = "The amount of time that is used when the process penalizes a flowfile."
    )
    public String getPenaltyDuration() {
        return penaltyDuration;
    }

    public void setPenaltyDuration(String penaltyDuration) {
        this.penaltyDuration = penaltyDuration;
    }

    /**
     * @return amount of time must elapse before this processor is scheduled again when yielding
     */
    @Schema(description = "The amount of time that must elapse before this processor is scheduled again after yielding."
    )
    public String getYieldDuration() {
        return yieldDuration;
    }

    public void setYieldDuration(String yieldDuration) {
        this.yieldDuration = yieldDuration;
    }

    /**
     * @return the level at this this processor will report bulletins
     */
    @Schema(description = "The level at which the processor will report bulletins."
    )
    public String getBulletinLevel() {
        return bulletinLevel;
    }

    public void setBulletinLevel(String bulletinLevel) {
        this.bulletinLevel = bulletinLevel;
    }

    /**
     * The number of tasks that should be concurrently scheduled for this processor. If this processor doesn't allow parallel processing then any positive input will be ignored.
     *
     * @return the concurrently schedulable task count
     */
    @Schema(description = "The number of tasks that should be concurrently schedule for the processor. If the processor doesn't allow parallol processing then any positive input will be ignored."
    )
    public Integer getConcurrentlySchedulableTaskCount() {
        return concurrentlySchedulableTaskCount;
    }

    public void setConcurrentlySchedulableTaskCount(Integer concurrentlySchedulableTaskCount) {
        this.concurrentlySchedulableTaskCount = concurrentlySchedulableTaskCount;
    }

    /**
     * @return whether or not this Processor is Loss Tolerant
     */
    @Schema(description = "Whether the processor is loss tolerant."
    )
    public Boolean isLossTolerant() {
        return lossTolerant;
    }

    public void setLossTolerant(final Boolean lossTolerant) {
        this.lossTolerant = lossTolerant;
    }

    /**
     * @return the comments
     */
    @Schema(description = "The comments for the processor."
    )
    public String getComments() {
        return comments;
    }

    public void setComments(String comments) {
        this.comments = comments;
    }

    /**
     * The properties for this processor. Properties whose value is not set will only contain the property name. These properties are (un)marshalled differently since we need/want to control the
     * ordering of the properties. The descriptors and metadata are used as a lookup when processing these properties.
     *
     * @return The optional properties
     */
    @Schema(description = "The properties for the processor. Properties whose value is not set will only contain the property name."
    )
    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    /**
     * @return descriptors for this processor's properties
     */
    @Schema(description = "Descriptors for the processor's properties."
    )
    public Map<String, PropertyDescriptorDTO> getDescriptors() {
        return descriptors;
    }

    public void setDescriptors(Map<String, PropertyDescriptorDTO> descriptors) {
        this.descriptors = descriptors;
    }

    /**
     * @return Set of sensitive dynamic property names
     */
    @Schema(description = "Set of sensitive dynamic property names"
    )
    public Set<String> getSensitiveDynamicPropertyNames() {
        return sensitiveDynamicPropertyNames;
    }

    public void setSensitiveDynamicPropertyNames(final Set<String> sensitiveDynamicPropertyNames) {
        this.sensitiveDynamicPropertyNames = sensitiveDynamicPropertyNames;
    }

    /**
     * Annotation data for this processor.
     *
     * @return The annotation data
     */
    @Schema(description = "The annotation data for the processor used to relay configuration between a custom UI and the procesosr."
    )
    public String getAnnotationData() {
        return annotationData;
    }

    public void setAnnotationData(String annotationData) {
        this.annotationData = annotationData;
    }

    /**
     * @return the URL for this processors custom configuration UI if applicable. Null otherwise.
     */
    @Schema(description = "The URL for the processor's custom configuration UI if applicable."
    )
    public String getCustomUiUrl() {
        return customUiUrl;
    }

    public void setCustomUiUrl(String customUiUrl) {
        this.customUiUrl = customUiUrl;
    }

    /**
     * @return the names of all processor relationships that cause a flow file to be terminated if the relationship is not connected to anything
     */
    @Schema(description = "The names of all relationships that cause a flow file to be terminated if the relationship is not connected elsewhere. This property differs "
            + "from the 'isAutoTerminate' property of the RelationshipDTO in that the RelationshipDTO is meant to depict the current configuration, whereas this "
            + "property can be set in a DTO when updating a Processor in order to change which Relationships should be auto-terminated."
    )
    public Set<String> getAutoTerminatedRelationships() {
        return autoTerminatedRelationships;
    }

    public void setAutoTerminatedRelationships(final Set<String> autoTerminatedRelationships) {
        this.autoTerminatedRelationships = autoTerminatedRelationships;
    }

    /**
     * @return maps default values for concurrent tasks for each applicable scheduling strategy.
     */
    @Schema(description = "Maps default values for concurrent tasks for each applicable scheduling strategy."
    )
    public Map<String, String> getDefaultConcurrentTasks() {
        return defaultConcurrentTasks;
    }

    public void setDefaultConcurrentTasks(Map<String, String> defaultConcurrentTasks) {
        this.defaultConcurrentTasks = defaultConcurrentTasks;
    }

    /**
     * @return run duration in milliseconds
     */
    @Schema(description = "The run duration for the processor in milliseconds."
    )
    public Long getRunDurationMillis() {
        return runDurationMillis;
    }

    public void setRunDurationMillis(Long runDurationMillis) {
        this.runDurationMillis = runDurationMillis;
    }

    /**
     * @return Maps default values for scheduling period for each applicable scheduling strategy
     */
    @Schema(description = "Maps default values for scheduling period for each applicable scheduling strategy."
    )
    public Map<String, String> getDefaultSchedulingPeriod() {
        return defaultSchedulingPeriod;
    }

    public void setDefaultSchedulingPeriod(Map<String, String> defaultSchedulingPeriod) {
        this.defaultSchedulingPeriod = defaultSchedulingPeriod;
    }

    @Schema(description = "Overall number of retries."
    )
    public Integer getRetryCount() {
        return retryCount;
    }

    public void setRetryCount(Integer retryCount) {
        this.retryCount = retryCount;
    }

    @Schema(description = "All the relationships should be retried."
    )
    public Set<String> getRetriedRelationships() {
        return retriedRelationships;
    }

    public void setRetriedRelationships(Set<String> retriedRelationships) {
        this.retriedRelationships = retriedRelationships;
    }

    @Schema(description = "Determines whether the FlowFile should be penalized or the processor should be yielded between retries.",
            allowableValues = {"PENALIZE_FLOWFILE", "YIELD_PROCESSOR"}
    )
    public String getBackoffMechanism() {
        return backoffMechanism;
    }

    public void setBackoffMechanism(String backoffMechanism) {
        this.backoffMechanism = backoffMechanism;
    }

    @Schema(description = "Maximum amount of time to be waited during a retry period."
    )
    public String getMaxBackoffPeriod() {
        return maxBackoffPeriod;
    }

    public void setMaxBackoffPeriod(String maxBackoffPeriod) {
        this.maxBackoffPeriod = maxBackoffPeriod;
    }
}
