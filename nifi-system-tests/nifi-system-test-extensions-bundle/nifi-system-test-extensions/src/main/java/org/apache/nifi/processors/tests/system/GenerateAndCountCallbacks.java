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

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

import java.time.Duration;
import java.util.Set;

@CapabilityDescription("Generates an empty FlowFile and counts how many times the session commit callback is called " +
                       "for both success and failure before and after Processor is stopped")
public class GenerateAndCountCallbacks extends AbstractProcessor {

    public static final String STOPPED_AND_SUCCESS = "Stopped and Success";
    public static final String STOPPED_AND_FAILURE = "Stopped and Failure";
    public static final String RUNNING_AND_SUCCESS = "Running and Success";
    public static final String RUNNING_AND_FAILURE = "Running and Failure";

    static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("All FlowFiles are routed to this relationship")
        .build();

    @Override
    public Set<Relationship> getRelationships() {
        return Set.of(REL_SUCCESS);
    }

    private volatile boolean stopped = false;

    @OnScheduled
    public void onScheduled() {
        stopped = false;
        getLogger().info("Processor started");
    }

    @OnStopped
    public void onStopped() {
        stopped = true;
        getLogger().info("Processor stopped");
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.create();
        session.transfer(flowFile, REL_SUCCESS);

        session.commitAsync(() -> {
            sleepUninterruptibly(Duration.ofSeconds(3));

            if (stopped) {
                session.adjustCounter(STOPPED_AND_SUCCESS, 1, true);
                getLogger().error("Success callback called after Processor Stopped");
            } else {
                session.adjustCounter(RUNNING_AND_SUCCESS, 1, true);
                getLogger().info("Success callback called while Processor Running");
            }
        },
            failureCause -> {
                sleepUninterruptibly(Duration.ofSeconds(3));

                if (stopped) {
                    session.adjustCounter(STOPPED_AND_FAILURE, 1, true);
                    getLogger().error("Failure callback called after Processor Stopped; Failure cause: {}", failureCause.toString());
                } else {
                    session.adjustCounter(RUNNING_AND_FAILURE, 1, true);
                    getLogger().warn("Failure callback called while Processor Running; Failure cause: {}", failureCause.toString());
                }
            });
    }

    private void sleepUninterruptibly(final Duration duration) {
        long endTime = System.currentTimeMillis() + duration.toMillis();
        while (System.currentTimeMillis() < endTime) {
            try {
                Thread.sleep(endTime - System.currentTimeMillis());
            } catch (final InterruptedException ignored) {
                // Ignore interruption and continue sleeping until the specified duration has elapsed
            }
        }
    }
}
