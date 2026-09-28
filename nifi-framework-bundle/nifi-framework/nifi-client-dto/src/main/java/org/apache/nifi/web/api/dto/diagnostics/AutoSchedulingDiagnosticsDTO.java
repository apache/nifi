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
package org.apache.nifi.web.api.dto.diagnostics;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.xml.bind.annotation.XmlType;

@XmlType(name = "autoSchedulingDiagnostics")
public class AutoSchedulingDiagnosticsDTO {
    private String executionMode;
    private Integer maxConcurrentTasks;
    private Integer currentConcurrentTasks;
    private Integer activeProcessorInvocations;
    private Long currentRunDurationMillis;
    private String concurrencyEvaluationState;
    private String concurrencyUpdateReason;
    private Double flowFilesPerSecond;
    private Long measurementWindowMillis;
    private String lastConcurrencyUpdateReason;
    private Boolean collectingMeasurements;
    private Boolean flowFileMeasurementsAvailable;
    private String concurrencyIncreaseExplanation;

    @Schema(description = "Automatic scheduling execution mode")
    public String getExecutionMode() {
        return executionMode;
    }

    public void setExecutionMode(final String executionMode) {
        this.executionMode = executionMode;
    }

    public Integer getMaxConcurrentTasks() {
        return maxConcurrentTasks;
    }

    public void setMaxConcurrentTasks(final Integer maxConcurrentTasks) {
        this.maxConcurrentTasks = maxConcurrentTasks;
    }

    public Integer getCurrentConcurrentTasks() {
        return currentConcurrentTasks;
    }

    public void setCurrentConcurrentTasks(final Integer currentConcurrentTasks) {
        this.currentConcurrentTasks = currentConcurrentTasks;
    }

    public Integer getActiveProcessorInvocations() {
        return activeProcessorInvocations;
    }

    public void setActiveProcessorInvocations(final Integer activeProcessorInvocations) {
        this.activeProcessorInvocations = activeProcessorInvocations;
    }

    public Long getCurrentRunDurationMillis() {
        return currentRunDurationMillis;
    }

    public void setCurrentRunDurationMillis(final Long currentRunDurationMillis) {
        this.currentRunDurationMillis = currentRunDurationMillis;
    }

    public String getConcurrencyEvaluationState() {
        return concurrencyEvaluationState;
    }

    public void setConcurrencyEvaluationState(final String concurrencyEvaluationState) {
        this.concurrencyEvaluationState = concurrencyEvaluationState;
    }

    public String getConcurrencyUpdateReason() {
        return concurrencyUpdateReason;
    }

    public void setConcurrencyUpdateReason(final String concurrencyUpdateReason) {
        this.concurrencyUpdateReason = concurrencyUpdateReason;
    }

    public Double getFlowFilesPerSecond() {
        return flowFilesPerSecond;
    }

    public void setFlowFilesPerSecond(final Double flowFilesPerSecond) {
        this.flowFilesPerSecond = flowFilesPerSecond;
    }

    public Long getMeasurementWindowMillis() {
        return measurementWindowMillis;
    }

    public void setMeasurementWindowMillis(final Long measurementWindowMillis) {
        this.measurementWindowMillis = measurementWindowMillis;
    }

    public String getLastConcurrencyUpdateReason() {
        return lastConcurrencyUpdateReason;
    }

    public void setLastConcurrencyUpdateReason(final String lastConcurrencyUpdateReason) {
        this.lastConcurrencyUpdateReason = lastConcurrencyUpdateReason;
    }

    public Boolean getCollectingMeasurements() {
        return collectingMeasurements;
    }

    public void setCollectingMeasurements(final Boolean collectingMeasurements) {
        this.collectingMeasurements = collectingMeasurements;
    }

    public Boolean getFlowFileMeasurementsAvailable() {
        return flowFileMeasurementsAvailable;
    }

    public void setFlowFileMeasurementsAvailable(final Boolean flowFileMeasurementsAvailable) {
        this.flowFileMeasurementsAvailable = flowFileMeasurementsAvailable;
    }

    @Schema(description = "Human-readable explanation of why automatic scheduling is not using more concurrent tasks")
    public String getConcurrencyIncreaseExplanation() {
        return concurrencyIncreaseExplanation;
    }

    public void setConcurrencyIncreaseExplanation(final String concurrencyIncreaseExplanation) {
        this.concurrencyIncreaseExplanation = concurrencyIncreaseExplanation;
    }
}
