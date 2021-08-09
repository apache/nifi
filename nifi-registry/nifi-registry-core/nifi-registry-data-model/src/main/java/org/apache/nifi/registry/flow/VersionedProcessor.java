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

package org.apache.nifi.registry.flow;

import io.swagger.annotations.ApiModelProperty;

import java.util.Map;
import java.util.Set;

public class VersionedProcessor extends VersionedComponent
        implements VersionedConfigurableComponent, VersionedExtensionComponent {

    private Bundle bundle;
    private Map<String, String> style;

    private String type;
    private Map<String, String> properties;
    private Map<String, VersionedPropertyDescriptor> propertyDescriptors;
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

    @ApiModelProperty("The frequency with which to schedule the processor. The format of the value will depend on th value of schedulingStrategy.")
    public String getSchedulingPeriod() {
        return schedulingPeriod;
    }

    public void setSchedulingPeriod(String setSchedulingPeriod) {
        this.schedulingPeriod = setSchedulingPeriod;
    }

    @ApiModelProperty("Indcates whether the prcessor should be scheduled to run in event or timer driven mode.")
    public String getSchedulingStrategy() {
        return schedulingStrategy;
    }

    public void setSchedulingStrategy(String schedulingStrategy) {
        this.schedulingStrategy = schedulingStrategy;
    }

    @Override
    @ApiModelProperty("The type of Processor")
    public String getType() {
        return type;
    }

    @Override
    public void setType(final String type) {
        this.type = type;
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

    @Override
    @ApiModelProperty("The properties for the processor. Properties whose value is not set will only contain the property name.")
    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    @Override
    @ApiModelProperty("The property descriptors for the processor.")
    public Map<String, VersionedPropertyDescriptor> getPropertyDescriptors() {
        return propertyDescriptors;
    }

    @Override
    public void setPropertyDescriptors(Map<String, VersionedPropertyDescriptor> propertyDescriptors) {
        this.propertyDescriptors = propertyDescriptors;
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

    @Override
    @ApiModelProperty("Information about the bundle from which the component came")
    public Bundle getBundle() {
        return bundle;
    }

    @Override
    public void setBundle(Bundle bundle) {
        this.bundle = bundle;
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

}
