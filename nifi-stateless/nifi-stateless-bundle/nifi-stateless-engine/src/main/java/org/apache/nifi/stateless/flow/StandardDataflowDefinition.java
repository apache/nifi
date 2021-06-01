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

package org.apache.nifi.stateless.flow;

import org.apache.nifi.registry.flow.Bundle;
import org.apache.nifi.registry.flow.VersionedControllerService;
import org.apache.nifi.registry.flow.VersionedFlowSnapshot;
import org.apache.nifi.registry.flow.VersionedProcessGroup;
import org.apache.nifi.registry.flow.VersionedProcessor;
import org.apache.nifi.stateless.config.ParameterContextDefinition;
import org.apache.nifi.stateless.config.ParameterProviderDefinition;
import org.apache.nifi.stateless.config.ReportingTaskDefinition;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class StandardDataflowDefinition implements DataflowDefinition<VersionedFlowSnapshot> {
    private final VersionedFlowSnapshot flowSnapshot;
    private final Set<String> failurePortNames;
    private final List<ParameterContextDefinition> parameterContexts;
    private final List<ReportingTaskDefinition> reportingTaskDefinitions;
    private final List<ParameterProviderDefinition> parameterProviderDefinitions;
    private final TransactionThresholds transactionThresholds;
    private final String flowName;

    private StandardDataflowDefinition(final Builder builder) {
        flowSnapshot = requireNonNull(builder.flowSnapshot, "Flow Snapshot must be provided");
        failurePortNames = builder.failurePortNames == null ? Collections.emptySet() : builder.failurePortNames;
        parameterContexts = builder.parameterContexts == null ? Collections.emptyList() : builder.parameterContexts;
        reportingTaskDefinitions = builder.reportingTaskDefinitions == null ? Collections.emptyList() : builder.reportingTaskDefinitions;
        transactionThresholds = builder.transactionThresholds == null ? TransactionThresholds.SINGLE_FLOWFILE : builder.transactionThresholds;
        parameterProviderDefinitions = builder.parameterProviderDefinitions == null ? Collections.emptyList() : builder.parameterProviderDefinitions;
        flowName = builder.flowName;
    }

    @Override
    public VersionedFlowSnapshot getFlowSnapshot() {
        return flowSnapshot;
    }

    @Override
    public String getFlowName() {
        return flowName;
    }

    @Override
    public Set<String> getFailurePortNames() {
        return failurePortNames;
    }

    @Override
    public List<ParameterContextDefinition> getParameterContexts() {
        return parameterContexts;
    }

    @Override
    public List<ReportingTaskDefinition> getReportingTaskDefinitions() {
        return reportingTaskDefinitions;
    }

    @Override
    public List<ParameterProviderDefinition> getParameterProviderDefinitions() {
        return parameterProviderDefinitions;
    }

    @Override
    public TransactionThresholds getTransactionThresholds() {
        return transactionThresholds;
    }

    public Set<Bundle> getReferencedBundles() {
        final Set<Bundle> referenced = new HashSet<>();
        final VersionedProcessGroup rootGroup = flowSnapshot.getFlowContents();
        discoverReferencedBundles(rootGroup, referenced);
        return referenced;
    }

    private void discoverReferencedBundles(final VersionedProcessGroup group, final Set<Bundle> referenced) {
        for (final VersionedProcessor processor : group.getProcessors()) {
            referenced.add(processor.getBundle());
        }

        for (final VersionedControllerService controllerService : group.getControllerServices()) {
            referenced.add(controllerService.getBundle());
        }

        for (final VersionedProcessGroup childGroup : group.getProcessGroups()) {
            discoverReferencedBundles(childGroup, referenced);
        }
    }

    public static class Builder {
        private VersionedFlowSnapshot flowSnapshot;
        private Set<String> failurePortNames;
        private List<ParameterContextDefinition> parameterContexts;
        private List<ReportingTaskDefinition> reportingTaskDefinitions;
        private List<ParameterProviderDefinition> parameterProviderDefinitions;
        private TransactionThresholds transactionThresholds;
        private String flowName;

        public Builder flowSnapshot(final VersionedFlowSnapshot flowSnapshot) {
            this.flowSnapshot = flowSnapshot;
            return this;
        }

        public Builder flowName(final String flowName) {
            this.flowName = flowName;
            return this;
        }

        public Builder failurePortNames(final Set<String> failurePortNames) {
            this.failurePortNames = failurePortNames;
            return this;
        }

        public Builder parameterContexts(List<ParameterContextDefinition> parameterContexts) {
            this.parameterContexts = parameterContexts;
            return this;
        }

        public Builder reportingTasks(final List<ReportingTaskDefinition> reportingTasks) {
            this.reportingTaskDefinitions = reportingTasks;
            return this;
        }

        public Builder parameterProviders(final List<ParameterProviderDefinition> parameterProviders) {
            this.parameterProviderDefinitions = parameterProviders;
            return this;
        }

        public Builder transactionThresholds(final TransactionThresholds thresholds) {
            this.transactionThresholds = thresholds;
            return this;
        }

        public StandardDataflowDefinition build() {
            return new StandardDataflowDefinition(this);
        }
    }
}
