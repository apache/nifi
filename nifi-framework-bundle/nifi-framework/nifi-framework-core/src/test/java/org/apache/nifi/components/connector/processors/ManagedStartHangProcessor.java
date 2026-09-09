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

package org.apache.nifi.components.connector.processors;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A Connector-managed Processor whose {@code @OnScheduled} method can be configured to keep throwing an Exception,
 * to block until released while responding to interruption, or to block until released while ignoring interruption.
 * The behavior is controlled through the Processor instance so that a test can select it after the Connector's flow
 * has been created but before the Connector is started.
 */
@InputRequirement(Requirement.INPUT_FORBIDDEN)
public class ManagedStartHangProcessor extends AbstractProcessor {

    public enum StartBehavior {
        REPEATED_FAILURE,
        INTERRUPTIBLE_BLOCK,
        INTERRUPTION_RESISTANT_BLOCK
    }

    private static final Relationship SUCCESS = new Relationship.Builder()
        .name("success")
        .description("All FlowFiles are routed to this relationship")
        .build();

    private volatile StartBehavior startBehavior = StartBehavior.REPEATED_FAILURE;
    private final AtomicInteger onScheduledInvocationCount = new AtomicInteger();
    private final AtomicInteger interruptionCount = new AtomicInteger();
    private final CountDownLatch releaseLatch = new CountDownLatch(1);

    @OnScheduled
    public void onScheduled() throws InterruptedException {
        onScheduledInvocationCount.incrementAndGet();

        switch (startBehavior) {
            case REPEATED_FAILURE -> throw new ProcessException("Intentional repeated @OnScheduled failure for test");
            case INTERRUPTIBLE_BLOCK -> {
                try {
                    releaseLatch.await();
                } catch (final InterruptedException e) {
                    interruptionCount.incrementAndGet();
                    Thread.currentThread().interrupt();
                    throw e;
                }
            }
            case INTERRUPTION_RESISTANT_BLOCK -> {
                while (releaseLatch.getCount() > 0) {
                    try {
                        releaseLatch.await();
                    } catch (final InterruptedException e) {
                        interruptionCount.incrementAndGet();
                    }
                }
            }
        }
    }

    public void setStartBehavior(final StartBehavior startBehavior) {
        this.startBehavior = startBehavior;
    }

    public int getOnScheduledInvocationCount() {
        return onScheduledInvocationCount.get();
    }

    public int getInterruptionCount() {
        return interruptionCount.get();
    }

    public void release() {
        while (releaseLatch.getCount() > 0) {
            releaseLatch.countDown();
        }
    }

    @Override
    public Set<Relationship> getRelationships() {
        return Set.of(SUCCESS);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
    }
}
