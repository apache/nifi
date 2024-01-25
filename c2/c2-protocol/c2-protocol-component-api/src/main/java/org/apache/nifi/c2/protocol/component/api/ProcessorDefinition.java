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

import io.swagger.v3.oas.annotations.media.Schema;
import org.apache.nifi.annotation.behavior.InputRequirement;

import java.util.Collections;
import java.util.List;
import java.util.Map;

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

    private List<UseCase> useCases;
    private List<MultiProcessorUseCase> multiProcessorUseCases;


    @Schema(description="Any input requirements this processor has.")
    public InputRequirement.Requirement getInputRequirement() {
        return inputRequirement;
    }

    public void setInputRequirement(InputRequirement.Requirement inputRequirement) {
        this.inputRequirement = inputRequirement;
    }

    @Schema(description = "The supported relationships for this processor.")
    public List<Relationship> getSupportedRelationships() {
        return (supportedRelationships == null ? Collections.emptyList() : Collections.unmodifiableList(supportedRelationships));
    }

    public void setSupportedRelationships(List<Relationship> supportedRelationships) {
        this.supportedRelationships = supportedRelationships;
    }

    @Schema(description = "Whether or not this processor supports dynamic relationships.")
    public boolean getSupportsDynamicRelationships() {
        return supportsDynamicRelationships;
    }

    public void setSupportsDynamicRelationships(boolean supportsDynamicRelationships) {
        this.supportsDynamicRelationships = supportsDynamicRelationships;
    }

    @Schema(description = "If the processor supports dynamic relationships, this describes the dynamic relationship")
    public DynamicRelationship getDynamicRelationship() {
        return dynamicRelationship;
    }

    public void setDynamicRelationship(DynamicRelationship dynamicRelationship) {
        this.dynamicRelationship = dynamicRelationship;
    }

    @Schema(description = "Whether or not this processor should be triggered serially (i.e. no concurrent execution).")
    public boolean getTriggerSerially() {
        return triggerSerially;
    }

    public void setTriggerSerially(boolean triggerSerially) {
        this.triggerSerially = triggerSerially;
    }

    @Schema(description = "Whether or not this processor should be triggered when incoming queues are empty.")
    public boolean getTriggerWhenEmpty() {
        return triggerWhenEmpty;
    }

    public void setTriggerWhenEmpty(boolean triggerWhenEmpty) {
        this.triggerWhenEmpty = triggerWhenEmpty;
    }

    @Schema(description = "Whether or not this processor should be triggered when any destination queue has room.")
    public boolean getTriggerWhenAnyDestinationAvailable() {
        return triggerWhenAnyDestinationAvailable;
    }

    public void setTriggerWhenAnyDestinationAvailable(boolean triggerWhenAnyDestinationAvailable) {
        this.triggerWhenAnyDestinationAvailable = triggerWhenAnyDestinationAvailable;
    }

    @Schema(description = "Whether or not this processor supports batching. If a Processor uses this annotation, " +
            "it allows the Framework to batch calls to session commits, as well as allowing the Framework to return " +
            "the same session multiple times.")
    public boolean getSupportsBatching() {
        return supportsBatching;
    }

    public void setSupportsBatching(boolean supportsBatching) {
        this.supportsBatching = supportsBatching;
    }

    @Schema(description = "Whether or not this processor should be scheduled only on the primary node in a cluster.")
    public boolean getPrimaryNodeOnly() {
        return primaryNodeOnly;
    }

    public void setPrimaryNodeOnly(boolean primaryNodeOnly) {
        this.primaryNodeOnly = primaryNodeOnly;
    }

    @Schema(description = "Whether or not this processor is considered side-effect free. Side-effect free indicate that the " +
            "processor's operations on FlowFiles can be safely repeated across process sessions.")
    public boolean getSideEffectFree() {
        return sideEffectFree;
    }

    public void setSideEffectFree(boolean sideEffectFree) {
        this.sideEffectFree = sideEffectFree;
    }

    @Schema(description = "The supported scheduling strategies, such as TIME_DRIVER, CRON, or EVENT_DRIVEN.")
    public List<String> getSupportedSchedulingStrategies() {
        return supportedSchedulingStrategies;
    }

    public void setSupportedSchedulingStrategies(List<String> supportedSchedulingStrategies) {
        this.supportedSchedulingStrategies = supportedSchedulingStrategies;
    }

    @Schema(description = "The default scheduling strategy for the processor.")
    public String getDefaultSchedulingStrategy() {
        return defaultSchedulingStrategy;
    }

    public void setDefaultSchedulingStrategy(String defaultSchedulingStrategy) {
        this.defaultSchedulingStrategy = defaultSchedulingStrategy;
    }

    @Schema(description = "The default concurrent tasks for each scheduling strategy.")
    public Map<String, Integer> getDefaultConcurrentTasksBySchedulingStrategy() {
        return defaultConcurrentTasksBySchedulingStrategy != null ? Collections.unmodifiableMap(defaultConcurrentTasksBySchedulingStrategy) : null;
    }

    public void setDefaultConcurrentTasksBySchedulingStrategy(Map<String, Integer> defaultConcurrentTasksBySchedulingStrategy) {
        this.defaultConcurrentTasksBySchedulingStrategy = defaultConcurrentTasksBySchedulingStrategy;
    }

    @Schema(description = "The default scheduling period for each scheduling strategy. " +
            "The scheduling period is expected to be a time period, such as \"30 sec\".")
    public Map<String, String> getDefaultSchedulingPeriodBySchedulingStrategy() {
        return defaultSchedulingPeriodBySchedulingStrategy != null ? Collections.unmodifiableMap(defaultSchedulingPeriodBySchedulingStrategy) : null;
    }

    public void setDefaultSchedulingPeriodBySchedulingStrategy(Map<String, String> defaultSchedulingPeriodBySchedulingStrategy) {
        this.defaultSchedulingPeriodBySchedulingStrategy = defaultSchedulingPeriodBySchedulingStrategy;
    }

    @Schema(description = "The default penalty duration as a time period, such as \"30 sec\".")
    public String getDefaultPenaltyDuration() {
        return defaultPenaltyDuration;
    }

    public void setDefaultPenaltyDuration(String defaultPenaltyDuration) {
        this.defaultPenaltyDuration = defaultPenaltyDuration;
    }

    @Schema(description = "The default yield duration as a time period, such as \"1 sec\".")
    public String getDefaultYieldDuration() {
        return defaultYieldDuration;
    }

    public void setDefaultYieldDuration(String defaultYieldDuration) {
        this.defaultYieldDuration = defaultYieldDuration;
    }

    @Schema(description = "The default bulletin level, such as WARN, INFO, DEBUG, etc.")
    public String getDefaultBulletinLevel() {
        return defaultBulletinLevel;
    }

    public void setDefaultBulletinLevel(String defaultBulletinLevel) {
        this.defaultBulletinLevel = defaultBulletinLevel;
    }

    @Schema(description = "The FlowFile attributes this processor reads")
    public List<Attribute> getReadsAttributes() {
        return readsAttributes;
    }

    public void setReadsAttributes(List<Attribute> readsAttributes) {
        this.readsAttributes = readsAttributes;
    }

    @Schema(description = "The FlowFile attributes this processor writes/updates")
    public List<Attribute> getWritesAttributes() {
        return writesAttributes;
    }

    public void setWritesAttributes(List<Attribute> writesAttributes) {
        this.writesAttributes = writesAttributes;
    }

    @Schema(description="A list of use cases that have been documented for this Processor")
    public List<UseCase> getUseCases() {
        return useCases;
    }

    public void setUseCases(final List<UseCase> useCases) {
        this.useCases = useCases;
    }

    @Schema(description="A list of use cases that have been documented that involve this Processor in conjunction with other Processors")
    public List<MultiProcessorUseCase> getMultiProcessorUseCases() {
        return multiProcessorUseCases;
    }

    public void setMultiProcessorUseCases(final List<MultiProcessorUseCase> multiProcessorUseCases) {
        this.multiProcessorUseCases = multiProcessorUseCases;
    }
}
