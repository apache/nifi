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
package org.apache.nifi.controller.scheduling;

import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.tasks.ConnectableTask;
import org.apache.nifi.encrypt.PropertyEncryptor;
import org.apache.nifi.engine.FlowEngine;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.FormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public abstract class AbstractTimeBasedSchedulingAgent extends AbstractSchedulingAgent {
    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    protected final FlowController flowController;
    protected final RepositoryContextFactory contextFactory;
    protected final PropertyEncryptor encryptor;

    protected volatile String adminYieldDuration = "1 sec";

    public AbstractTimeBasedSchedulingAgent(
        final FlowEngine flowEngine,
        final FlowController flowController,
        final RepositoryContextFactory contextFactory,
        final PropertyEncryptor encryptor
    ) {
        super(flowEngine);
        this.flowController = flowController;
        this.contextFactory = contextFactory;
        this.encryptor = encryptor;
    }

    @Override
    public void doScheduleOnce(final Connectable connectable, final LifecycleState scheduleState, Callable<Future<Void>> stopCallback) {
        final List<ScheduledFuture<?>> futures = new ArrayList<>();
        final ConnectableTask connectableTask = new ConnectableTask(this, connectable, flowController, contextFactory, scheduleState, encryptor);

        final Runnable trigger = () -> {
            connectableTask.invoke();
            try {
                stopCallback.call();
            } catch (Exception e) {
                String errorMessage = "Error while stopping " + connectable + " after running once.";
                logger.error(errorMessage, e);
                throw new ProcessException(errorMessage, e);
            }
        };

        final ScheduledFuture<?> future = flowEngine.schedule(trigger, 1, TimeUnit.NANOSECONDS);
        futures.add(future);

        scheduleState.setFutures(futures);
    }

    @Override
    public void setAdministrativeYieldDuration(final String yieldDuration) {
        this.adminYieldDuration = yieldDuration;
    }

    @Override
    public String getAdministrativeYieldDuration() {
        return adminYieldDuration;
    }

    @Override
    public long getAdministrativeYieldDuration(final TimeUnit timeUnit) {
        return FormatUtils.getTimeDuration(adminYieldDuration, timeUnit);
    }

    @Override
    public void incrementMaxThreadCount(int toAdd) {
        final int corePoolSize = flowEngine.getCorePoolSize();
        if (toAdd < 0 && corePoolSize + toAdd < 1) {
            throw new IllegalStateException("Cannot remove " + (-toAdd) + " threads from pool because there are only " + corePoolSize + " threads in the pool");
        }

        flowEngine.setCorePoolSize(corePoolSize + toAdd);
    }
}
