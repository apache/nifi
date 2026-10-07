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
import org.apache.nifi.components.connector.components.FlowContext;
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedProcessGroup;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Connector that counts its flow evolution hook, update preparation, apply and start calls. The hook can return a future
 * that never completes, and start can fail a given number of times.
 */
public class FlowEvolutionHookConnector extends AbstractConnector {

    volatile FlowEvolutionContext context;
    volatile Bundle preUpgradeBundle;
    volatile int hookCalls;
    volatile int prepareCalls;
    volatile int applyCalls;
    volatile boolean leaveFutureIncomplete;
    volatile CompletableFuture<Void> evolutionFuture;
    volatile int remainingStartFailures;
    final AtomicInteger startCalls = new AtomicInteger();
    final CountDownLatch hookEntered = new CountDownLatch(1);
    final CountDownLatch evolutionFuturePublished = new CountDownLatch(1);

    @Override
    public VersionedExternalFlow getInitialFlow() {
        return null;
    }

    @Override
    public VersionedExternalFlow getActiveFlow(final FlowContext flowContext) {
        final VersionedExternalFlow flow = new VersionedExternalFlow();
        flow.setFlowContents(new VersionedProcessGroup());
        return flow;
    }

    @Override
    public void prepareForUpdate(final FlowContext workingContext, final FlowContext activeContext) throws FlowUpdateException {
        prepareCalls++;
    }

    @Override
    public List<ConfigurationStep> getConfigurationSteps() {
        return List.of();
    }

    @Override
    public void applyUpdate(final FlowContext workingContext, final FlowContext activeContext) throws FlowUpdateException {
        applyCalls++;
    }

    @Override
    public void start(final FlowContext activeContext) throws FlowUpdateException {
        startCalls.incrementAndGet();
        if (remainingStartFailures > 0) {
            remainingStartFailures--;
            throw new FlowUpdateException("Intentional start failure");
        }
    }

    @Override
    protected void onStepConfigured(final String stepName, final FlowContext workingContext) {
    }

    @Override
    public List<ConfigVerificationResult> verifyConfigurationStep(final String stepName, final Map<String, String> overrides, final FlowContext flowContext) {
        return List.of();
    }

    @Override
    public CompletableFuture<Void> prepareForFlowEvolution(final FlowEvolutionContext evolutionContext) throws FlowUpdateException {
        context = evolutionContext;
        preUpgradeBundle = evolutionContext.getPreUpgradeActiveFlowContext().getBundle();
        hookCalls++;
        hookEntered.countDown();

        if (leaveFutureIncomplete) {
            evolutionFuture = new CompletableFuture<>();
        } else {
            evolutionFuture = CompletableFuture.completedFuture(null);
        }
        evolutionFuturePublished.countDown();
        return evolutionFuture;
    }
}
