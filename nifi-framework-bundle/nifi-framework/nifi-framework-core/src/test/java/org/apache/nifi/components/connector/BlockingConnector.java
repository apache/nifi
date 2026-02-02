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

package org.apache.nifi.components.connector;

import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.DescribedValue;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.connector.components.FlowContext;
import org.apache.nifi.flow.VersionedExternalFlow;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

public class BlockingConnector implements Connector {
    private final CountDownLatch startLatch;
    private final CountDownLatch stopLatch;
    private final CountDownLatch finishUpdateLatch;

    public BlockingConnector(final CountDownLatch startLatch, final CountDownLatch stopLatch, final CountDownLatch finishUpdateLatch) {
        this.startLatch = startLatch;
        this.stopLatch = stopLatch;
        this.finishUpdateLatch = finishUpdateLatch;
    }


    @Override
    public void initialize(final ConnectorInitializationContext connectorInitializationContext) {
    }

    @Override
    public VersionedExternalFlow getInitialFlow() {
        return null;
    }

    @Override
    public void start(final FlowContext activeContext) throws FlowUpdateException {
        try {
            startLatch.await();
        } catch (final InterruptedException e) {
            throw new FlowUpdateException(e);
        }
    }

    @Override
    public void stop(final FlowContext activeContext) throws FlowUpdateException {
        try {
            stopLatch.await();
        } catch (final InterruptedException e) {
            throw new FlowUpdateException(e);
        }
    }

    @Override
    public List<ValidationResult> validate(final FlowContext flowContext, final ConnectorValidationContext connectorValidationContext) {
        return List.of();
    }

    @Override
    public List<ValidationResult> validateConfigurationStep(final ConfigurationStep configurationStep, final ConnectorConfigurationContext connectorConfigurationContext,
            final ConnectorValidationContext connectorValidationContext) {
        return List.of();
    }

    @Override
    public List<ConfigurationStep> getConfigurationSteps() {
        return List.of();
    }

    @Override
    public void onConfigurationStepConfigured(final String stepName, final FlowContext flowContext) {
    }

    @Override
    public void prepareForUpdate(final FlowContext workingContext, final FlowContext activeContext) {
    }

    @Override
    public void abortUpdate(final FlowContext workingContext, final Throwable throwable) {

    }

    @Override
    public void applyUpdate(final FlowContext workingContext, final FlowContext activeContext) throws FlowUpdateException {
        try {
            finishUpdateLatch.await();
        } catch (final InterruptedException e) {
            throw new FlowUpdateException(e);
        }
    }

    @Override
    public List<ConfigVerificationResult> verifyConfigurationStep(final String stepName, final Map<String, String> overrides, final FlowContext flowContext) {
        return List.of();
    }

    @Override
    public List<ConfigVerificationResult> verify(final FlowContext flowContext) {
        return List.of();
    }

    @Override
    public List<DescribedValue> fetchAllowableValues(final String stepName, final String propertyName, final FlowContext workingContext, final String filter) {
        return List.of();
    }

    @Override
    public List<DescribedValue> fetchAllowableValues(final String stepName, final String propertyName, final FlowContext workingContext) {
        return List.of();
    }

    @Override
    public CompletableFuture<Void> drainFlowFiles(final FlowContext flowContext) {
        return CompletableFuture.completedFuture(null);
    }
}
