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

package org.apache.nifi.processors.tests.system;

import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.annotation.notification.OnPrimaryNodeStateChange;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@TriggerWhenEmpty
@DefaultSchedule(period="100 millis")
public class CountPrimaryNodeChangeEvents extends AbstractSessionFactoryProcessor {
    private static final String nodeNumber = System.getProperty("nodeNumber");

    static final PropertyDescriptor EVENT_SLEEP_DURATION = new PropertyDescriptor.Builder()
        .name("Event Sleep Duration")
        .displayName("Event Sleep Duration")
        .description("The amount of time to sleep when the onPrimaryNodeChange event occurs")
        .required(true)
        .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
        .defaultValue("0 sec")
        .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Collections.singletonList(EVENT_SLEEP_DURATION);
    }

    private final AtomicReference<ProcessSession> sessionReference = new AtomicReference<>();
    private volatile long sleepMillis = 0L;

    @OnPrimaryNodeStateChange
    public void onPrimaryNodeChange() {
        final ProcessSession session = sessionReference.get();
        if (session == null) {
            return;
        }

        session.adjustCounter("PrimaryNodeChangeCalled-" + nodeNumber, 1L, true);

        try {
            Thread.sleep(sleepMillis);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        session.adjustCounter("PrimaryNodeChangeCompleted-" + nodeNumber, 1L, true);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory) throws ProcessException {
        final ProcessSession current = sessionReference.get();
        if (current == null) {
            final ProcessSession session = sessionFactory.createSession();
            sessionReference.compareAndSet(null, session);
        }

        sleepMillis = context.getProperty(EVENT_SLEEP_DURATION).asTimePeriod(TimeUnit.MILLISECONDS);
        sessionReference.get().adjustCounter("Triggers-" + nodeNumber, 1L, true);
    }
}
