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

package org.apache.nifi.flow;

import io.swagger.annotations.ApiModelProperty;

import java.util.Map;
import java.util.Set;

public class VersionedProcessor extends VersionedConfigurableExtension {

    private Map<String, String> style;
    private String annotationData;

    private String schedulingPeriod;
    private String schedulingStrategy;
    private String executionNode;
    private String penaltyDuration;
    private String yieldDuration;
    private String bulletinLevel;
    private Long runDurationMillis;
    private Integer concurrentlySchedulableTaskCount;
    private Set<String> autoTerminatedRelationships;
    private ScheduledState scheduledState;

    private Integer retryCount;
    private Set<String> retriedRelationships;
    private String backoffMechanism;
    private String maxBackoffPeriod;

    @ApiModelProperty("The frequency with which to schedule the processor. The format of the value will depend on th value of schedulingStrategy.")
    public String getSchedulingPeriod() {
        return schedulingPeriod;
    }

    public void setSchedulingPeriod(String setSchedulingPeriod) {
        this.schedulingPeriod = setSchedulingPeriod;
    }

    @ApiModelProperty("Indicates whether the processor should be scheduled to run in event or timer driven mode.")
    public String getSchedulingStrategy() {
        return schedulingStrategy;
    }

    public void setSchedulingStrategy(String schedulingStrategy) {
        this.schedulingStrategy = schedulingStrategy;
    }

    @ApiModelProperty("Indicates the node where the process will execute.")
    public String getExecutionNode() {
        return executionNode;
    }

    public void setExecutionNode(String executionNode) {
        this.executionNode = executionNode;
    }

    @ApiModelProperty("The amout of time that is used when the process penalizes a flowfile.")
    public String getPenaltyDuration() {
        return penaltyDuration;
    }

    public void setPenaltyDuration(String penaltyDuration) {
        this.penaltyDuration = penaltyDuration;
    }

    @ApiModelProperty("The amount of time that must elapse before this processor is scheduled again after yielding.")
    public String getYieldDuration() {
        return yieldDuration;
    }

    public void setYieldDuration(String yieldDuration) {
        this.yieldDuration = yieldDuration;
    }

    @ApiModelProperty("The level at which the processor will report bulletins.")
    public String getBulletinLevel() {
        return bulletinLevel;
    }

    public void setBulletinLevel(String bulletinLevel) {
        this.bulletinLevel = bulletinLevel;
    }

    @ApiModelProperty("The number of tasks that should be concurrently schedule for the processor. If the processor doesn't allow parallol processing then any positive input will be ignored.")
    public Integer getConcurrentlySchedulableTaskCount() {
        return concurrentlySchedulableTaskCount;
    }

    public void setConcurrentlySchedulableTaskCount(Integer concurrentlySchedulableTaskCount) {
        this.concurrentlySchedulableTaskCount = concurrentlySchedulableTaskCount;
    }

    @ApiModelProperty("The annotation data for the processor used to relay configuration between a custom UI and the procesosr.")
    public String getAnnotationData() {
        return annotationData;
    }

    public void setAnnotationData(String annotationData) {
        this.annotationData = annotationData;
    }


    @ApiModelProperty("The names of all relationships that cause a flow file to be terminated if the relationship is not connected elsewhere. This property differs "
        + "from the 'isAutoTerminate' property of the RelationshipDTO in that the RelationshipDTO is meant to depict the current configuration, whereas this "
        + "property can be set in a DTO when updating a Processor in order to change which Relationships should be auto-terminated.")
    public Set<String> getAutoTerminatedRelationships() {
        return autoTerminatedRelationships;
    }

    public void setAutoTerminatedRelationships(final Set<String> autoTerminatedRelationships) {
        this.autoTerminatedRelationships = autoTerminatedRelationships;
    }

    @ApiModelProperty("The run duration for the processor in milliseconds.")
    public Long getRunDurationMillis() {
        return runDurationMillis;
    }

    public void setRunDurationMillis(Long runDurationMillis) {
        this.runDurationMillis = runDurationMillis;
    }

    @ApiModelProperty("Stylistic data for rendering in a UI")
    public Map<String, String> getStyle() {
        return style;
    }

    public void setStyle(Map<String, String> style) {
        this.style = style;
    }

    @ApiModelProperty("The scheduled state of the component")
    public ScheduledState getScheduledState() {
        return scheduledState;
    }

    public void setScheduledState(ScheduledState scheduledState) {
        this.scheduledState = scheduledState;
    }

    @Override
    public ComponentType getComponentType() {
        return ComponentType.PROCESSOR;
    }

    @ApiModelProperty(
            value = "Overall number of retries."
    )
    public Integer getRetryCount() {
        return retryCount;
    }

    public void setRetryCount(Integer retryCount) {
        this.retryCount = retryCount;
    }

    @ApiModelProperty(
            value = "All the relationships should be retried."
    )
    public Set<String> getRetriedRelationships() {
        return retriedRelationships;
    }

    public void setRetriedRelationships(Set<String> retriedRelationships) {
        this.retriedRelationships = retriedRelationships;
    }

    @ApiModelProperty(
            value = "Determines whether the FlowFile should be penalized or the processor should be yielded between retries.",
            allowableValues = "PENALIZE_FLOWFILE, YIELD_PROCESSOR"
    )
    public String getBackoffMechanism() {
        return backoffMechanism;
    }

    public void setBackoffMechanism(String backoffMechanism) {
        this.backoffMechanism = backoffMechanism;
    }

    @ApiModelProperty(
            value = "Maximum amount of time to be waited during a retry period."
    )
    public String getMaxBackoffPeriod() {
        return maxBackoffPeriod;
    }

    public void setMaxBackoffPeriod(String maxBackoffPeriod) {
        this.maxBackoffPeriod = maxBackoffPeriod;
    }
}
