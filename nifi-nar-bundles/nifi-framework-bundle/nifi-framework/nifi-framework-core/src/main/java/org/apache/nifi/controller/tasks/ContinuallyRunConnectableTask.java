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
package org.apache.nifi.controller.tasks;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.controller.repository.StandardProcessSessionFactory;
import org.apache.nifi.controller.scheduling.ProcessContextFactory;
import org.apache.nifi.controller.scheduling.ScheduleState;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.Connectables;
import org.apache.nifi.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Continually runs a Connectable as long as the processor has work to do. {@link #call()} will return <code>true</code> if the Connectable should be yielded, <code>false</code> otherwise.
 */
public class ContinuallyRunConnectableTask implements Callable<Boolean> {

    private static final Logger logger = LoggerFactory.getLogger(ContinuallyRunConnectableTask.class);

    private final Connectable connectable;
    private final ScheduleState scheduleState;
    private final ProcessSessionFactory sessionFactory;
    private final ProcessContext processContext;

    public ContinuallyRunConnectableTask(final ProcessContextFactory contextFactory, final Connectable connectable, final ScheduleState scheduleState, final ProcessContext processContext) {
        this.connectable = connectable;
        this.scheduleState = scheduleState;
        this.sessionFactory = new StandardProcessSessionFactory(contextFactory.newProcessContext(connectable, new AtomicLong(0L)));
        this.processContext = processContext;
    }

    @Override
    public Boolean call() {
        if (!scheduleState.isScheduled()) {
            return false;
        }

        // Connectable should run if the following conditions are met:
        // 1. It is not yielded.
        // 2. It has incoming connections with FlowFiles queued or doesn't expect incoming connections
        // 3. If it is a funnel, it has an outgoing connection (this is needed because funnels are "always on"; other
        //    connectable components cannot even be started if they need an outbound connection and don't have one)
        // 4. There is a connection for each relationship.
        final boolean triggerWhenEmpty = connectable.isTriggerWhenEmpty();
        boolean flowFilesQueued = true;
        boolean funnelWithoutConnections = false;
        boolean relationshipAvailable = true;
        final boolean shouldRun = (connectable.getYieldExpiration() < System.currentTimeMillis())
                && (triggerWhenEmpty || (flowFilesQueued = Connectables.flowFilesQueued(connectable)))
                && (connectable.getConnectableType() != ConnectableType.FUNNEL || !(funnelWithoutConnections = connectable.getConnections().isEmpty()))
            && (connectable.getRelationships().isEmpty() || (relationshipAvailable = Connectables.anyRelationshipAvailable(connectable)));

        if (shouldRun) {
            scheduleState.incrementActiveThreadCount();
            try {
                try (final AutoCloseable ncl = NarCloseable.withComponentNarLoader(connectable.getClass(), connectable.getIdentifier())) {
                    connectable.onTrigger(processContext, sessionFactory);
                } catch (final ProcessException pe) {
                    logger.error("{} failed to process session due to {}", connectable, pe.toString());
                } catch (final Throwable t) {
                    logger.error("{} failed to process session due to {}", connectable, t.toString());
                    logger.error("", t);

                    logger.warn("{} Administratively Pausing for 10 seconds due to processing failure: {}", connectable, t.toString(), t);
                    try {
                        Thread.sleep(10000L);
                    } catch (final InterruptedException e) {
                    }

                }
            } finally {
                if (!scheduleState.isScheduled() && scheduleState.getActiveThreadCount() == 1 && scheduleState.mustCallOnStoppedMethods()) {
                    try (final NarCloseable x = NarCloseable.withComponentNarLoader(connectable.getClass(), connectable.getIdentifier())) {
                        ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnStopped.class, connectable, processContext);
                    }
                }

                scheduleState.decrementActiveThreadCount();
            }
        } else if (!flowFilesQueued || funnelWithoutConnections || !relationshipAvailable) {
            // Either there are no FlowFiles queued, it's a funnel without outgoing connections, or the relationship is not available (i.e., backpressure is applied).
            // We will yield for just a bit.
            return true;
        }

        return false; // do not yield
    }
}
