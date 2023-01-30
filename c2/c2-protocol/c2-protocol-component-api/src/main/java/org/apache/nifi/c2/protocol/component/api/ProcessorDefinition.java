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

package org.apache.nifi.c2.protocol.component.api;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;

import java.util.Collections;
import java.util.List;
import java.util.Map;

@ApiModel
public class ProcessorDefinition extends ConfigurableExtensionDefinition {
    private static final long serialVersionUID = 1L;

    private InputRequirement.Requirement inputRequirement;
    private List<Relationship> supportedRelationships;
    private boolean supportsDynamicRelationships;
    private DynamicRelationship dynamicRelationship;

    private boolean triggerSerially;
    private boolean triggerWhenEmpty;
    private boolean triggerWhenAnyDestinationAvailable;
    private boolean supportsBatching;
    private boolean supportsEventDriven;
    private boolean primaryNodeOnly;
    private boolean sideEffectFree;

    private List<String> supportedSchedulingStrategies;
    private String defaultSchedulingStrategy;
    private Map<String, Integer> defaultConcurrentTasksBySchedulingStrategy;
    private Map<String, String> defaultSchedulingPeriodBySchedulingStrategy;

    private String defaultPenaltyDuration;
    private String defaultYieldDuration;
    private String defaultBulletinLevel;

    private List<Attribute> readsAttributes;
    private List<Attribute> writesAttributes;

    @ApiModelProperty("Any input requirements this processor has.")
    public InputRequirement.Requirement getInputRequirement() {
        return inputRequirement;
    }

    public void setInputRequirement(InputRequirement.Requirement inputRequirement) {
        this.inputRequirement = inputRequirement;
    }

    @ApiModelProperty("The supported relationships for this processor.")
    public List<Relationship> getSupportedRelationships() {
        return (supportedRelationships == null ? Collections.emptyList() : Collections.unmodifiableList(supportedRelationships));
    }

    public void setSupportedRelationships(List<Relationship> supportedRelationships) {
        this.supportedRelationships = supportedRelationships;
    }

    @ApiModelProperty("Whether or not this processor supports dynamic relationships.")
    public boolean getSupportsDynamicRelationships() {
        return supportsDynamicRelationships;
    }

    public void setSupportsDynamicRelationships(boolean supportsDynamicRelationships) {
        this.supportsDynamicRelationships = supportsDynamicRelationships;
    }

    @ApiModelProperty("If the processor supports dynamic relationships, this describes the dynamic relationship")
    public DynamicRelationship getDynamicRelationship() {
        return dynamicRelationship;
    }

    public void setDynamicRelationship(DynamicRelationship dynamicRelationship) {
        this.dynamicRelationship = dynamicRelationship;
    }

    @ApiModelProperty("Whether or not this processor should be triggered serially (i.e. no concurrent execution).")
    public boolean getTriggerSerially() {
        return triggerSerially;
    }

    public void setTriggerSerially(boolean triggerSerially) {
        this.triggerSerially = triggerSerially;
    }

    @ApiModelProperty("Whether or not this processor should be triggered when incoming queues are empty.")
    public boolean getTriggerWhenEmpty() {
        return triggerWhenEmpty;
    }

    public void setTriggerWhenEmpty(boolean triggerWhenEmpty) {
        this.triggerWhenEmpty = triggerWhenEmpty;
    }

    @ApiModelProperty("Whether or not this processor should be triggered when any destination queue has room.")
    public boolean getTriggerWhenAnyDestinationAvailable() {
        return triggerWhenAnyDestinationAvailable;
    }

    public void setTriggerWhenAnyDestinationAvailable(boolean triggerWhenAnyDestinationAvailable) {
        this.triggerWhenAnyDestinationAvailable = triggerWhenAnyDestinationAvailable;
    }

    @ApiModelProperty("Whether or not this processor supports batching. If a Processor uses this annotation, " +
            "it allows the Framework to batch calls to session commits, as well as allowing the Framework to return " +
            "the same session multiple times.")
    public boolean getSupportsBatching() {
        return supportsBatching;
    }

    public void setSupportsBatching(boolean supportsBatching) {
        this.supportsBatching = supportsBatching;
    }

    @ApiModelProperty("Whether or not this processor supports event driven scheduling. Indicates to the framework that the " +
            "Processor is eligible to be scheduled to run based on the occurrence of an \"Event\" " +
            "(e.g., when a FlowFile is enqueued in an incoming Connection), rather than being triggered periodically.")
    public boolean getSupportsEventDriven() {
        return supportsEventDriven;
    }

    public void setSupportsEventDriven(boolean supportsEventDriven) {
        this.supportsEventDriven = supportsEventDriven;
    }

    @ApiModelProperty("Whether or not this processor should be scheduled only on the primary node in a cluster.")
    public boolean getPrimaryNodeOnly() {
        return primaryNodeOnly;
    }

    public void setPrimaryNodeOnly(boolean primaryNodeOnly) {
        this.primaryNodeOnly = primaryNodeOnly;
    }

    @ApiModelProperty("Whether or not this processor is considered side-effect free. Side-effect free indicate that the " +
            "processor's operations on FlowFiles can be safely repeated across process sessions.")
    public boolean getSideEffectFree() {
        return sideEffectFree;
    }

    public void setSideEffectFree(boolean sideEffectFree) {
        this.sideEffectFree = sideEffectFree;
    }

    @ApiModelProperty("The supported scheduling strategies, such as TIME_DRIVER, CRON, or EVENT_DRIVEN.")
    public List<String> getSupportedSchedulingStrategies() {
        return supportedSchedulingStrategies;
    }

    public void setSupportedSchedulingStrategies(List<String> supportedSchedulingStrategies) {
        this.supportedSchedulingStrategies = supportedSchedulingStrategies;
    }

    @ApiModelProperty("The default scheduling strategy for the processor.")
    public String getDefaultSchedulingStrategy() {
        return defaultSchedulingStrategy;
    }

    public void setDefaultSchedulingStrategy(String defaultSchedulingStrategy) {
        this.defaultSchedulingStrategy = defaultSchedulingStrategy;
    }

    @ApiModelProperty("The default concurrent tasks for each scheduling strategy.")
    public Map<String, Integer> getDefaultConcurrentTasksBySchedulingStrategy() {
        return defaultConcurrentTasksBySchedulingStrategy != null ? Collections.unmodifiableMap(defaultConcurrentTasksBySchedulingStrategy) : null;
    }

    public void setDefaultConcurrentTasksBySchedulingStrategy(Map<String, Integer> defaultConcurrentTasksBySchedulingStrategy) {
        this.defaultConcurrentTasksBySchedulingStrategy = defaultConcurrentTasksBySchedulingStrategy;
    }

    @ApiModelProperty("The default scheduling period for each scheduling strategy. " +
            "The scheduling period is expected to be a time period, such as \"30 sec\".")
    public Map<String, String> getDefaultSchedulingPeriodBySchedulingStrategy() {
        return defaultSchedulingPeriodBySchedulingStrategy != null ? Collections.unmodifiableMap(defaultSchedulingPeriodBySchedulingStrategy) : null;
    }

    public void setDefaultSchedulingPeriodBySchedulingStrategy(Map<String, String> defaultSchedulingPeriodBySchedulingStrategy) {
        this.defaultSchedulingPeriodBySchedulingStrategy = defaultSchedulingPeriodBySchedulingStrategy;
    }

    @ApiModelProperty("The default penalty duration as a time period, such as \"30 sec\".")
    public String getDefaultPenaltyDuration() {
        return defaultPenaltyDuration;
    }

    public void setDefaultPenaltyDuration(String defaultPenaltyDuration) {
        this.defaultPenaltyDuration = defaultPenaltyDuration;
    }

    @ApiModelProperty("The default yield duration as a time period, such as \"1 sec\".")
    public String getDefaultYieldDuration() {
        return defaultYieldDuration;
    }

    public void setDefaultYieldDuration(String defaultYieldDuration) {
        this.defaultYieldDuration = defaultYieldDuration;
    }

    @ApiModelProperty("The default bulletin level, such as WARN, INFO, DEBUG, etc.")
    public String getDefaultBulletinLevel() {
        return defaultBulletinLevel;
    }

    public void setDefaultBulletinLevel(String defaultBulletinLevel) {
        this.defaultBulletinLevel = defaultBulletinLevel;
    }

    @ApiModelProperty("The FlowFile attributes this processor reads")
    public List<Attribute> getReadsAttributes() {
        return readsAttributes;
    }

    public void setReadsAttributes(List<Attribute> readsAttributes) {
        this.readsAttributes = readsAttributes;
    }

    @ApiModelProperty("The FlowFile attributes this processor writes/updates")
    public List<Attribute> getWritesAttributes() {
        return writesAttributes;
    }

    public void setWritesAttributes(List<Attribute> writesAttributes) {
        this.writesAttributes = writesAttributes;
    }
}
