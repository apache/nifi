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

import java.util.concurrent.atomic.AtomicLong;

import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.controller.repository.StandardProcessSessionFactory;
import org.apache.nifi.controller.scheduling.ConnectableProcessContext;
import org.apache.nifi.controller.scheduling.ProcessContextFactory;
import org.apache.nifi.controller.scheduling.ScheduleState;
import org.apache.nifi.encrypt.StringEncryptor;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.annotation.OnStopped;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.Connectables;
import org.apache.nifi.util.ReflectionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContinuallyRunConnectableTask implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(ContinuallyRunConnectableTask.class);

    private final Connectable connectable;
    private final ScheduleState scheduleState;
    private final ProcessSessionFactory sessionFactory;
    private final ConnectableProcessContext processContext;

    public ContinuallyRunConnectableTask(final ProcessContextFactory contextFactory, final Connectable connectable, final ScheduleState scheduleState, final StringEncryptor encryptor) {
        this.connectable = connectable;
        this.scheduleState = scheduleState;
        this.sessionFactory = new StandardProcessSessionFactory(contextFactory.newProcessContext(connectable, new AtomicLong(0L)));
        this.processContext = new ConnectableProcessContext(connectable, encryptor);
    }

    @Override
    public void run() {
        if (!scheduleState.isScheduled()) {
            return;
        }
        // Connectable should run if the following conditions are met:
        // 1. It's an Input Port or or is a Remote Input Port or has incoming FlowFiles queued
        // 2. Any relationship is available (since there's only 1
        // relationship for a Connectable, we can say "any" or "all" and
        // it means the same thing)
        // 3. It is not yielded.
        final boolean triggerWhenEmpty = connectable.isTriggerWhenEmpty();
        final boolean shouldRun = (connectable.getYieldExpiration() < System.currentTimeMillis())
                && (triggerWhenEmpty || Connectables.flowFilesQueued(connectable)) && (connectable.getRelationships().isEmpty() || Connectables.anyRelationshipAvailable(connectable));

        if (shouldRun) {
            scheduleState.incrementActiveThreadCount();
            try {
                try (final AutoCloseable ncl = NarCloseable.withNarLoader()) {
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
                    try (final NarCloseable x = NarCloseable.withNarLoader()) {
                        ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnStopped.class, connectable, processContext);
                    }
                }

                scheduleState.decrementActiveThreadCount();
            }
        }
    }
}
