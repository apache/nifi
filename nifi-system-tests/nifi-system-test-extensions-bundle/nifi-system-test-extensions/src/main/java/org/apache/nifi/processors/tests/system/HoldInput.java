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

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@InputRequirement(Requirement.INPUT_REQUIRED)
public class HoldInput extends AbstractSessionFactoryProcessor {

    static final PropertyDescriptor IGNORE_TIME = new PropertyDescriptor.Builder()
        .name("Hold Time")
        .description("The amount of time to hold input FlowFiles without releasing them")
        .required(true)
        .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
        .defaultValue("3 sec")
        .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("All FlowFiles go to this Relationship")
        .build();

    private volatile long lastTransferTime;

    private final Set<FlowFileSession> flowFileSessions = Collections.synchronizedSet(new HashSet<>());

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return List.of(IGNORE_TIME);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return Set.of(REL_SUCCESS);
    }

    @OnScheduled
    public void reset() {
        lastTransferTime = System.nanoTime();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory) throws ProcessException {
        final ProcessSession session = sessionFactory.createSession();
        final List<FlowFile> flowFiles = session.get(10_000);
        if (flowFiles.isEmpty()) {
            return;
        }

        flowFileSessions.add(new FlowFileSession(session, flowFiles));

        final long nextTransferTime = lastTransferTime + context.getProperty(IGNORE_TIME).asTimePeriod(TimeUnit.NANOSECONDS);
        if (System.nanoTime() < nextTransferTime) {
            getLogger().debug("Ignoring input because {} is not yet reached", context.getProperty(IGNORE_TIME).getValue());
            return;
        }

        for (final FlowFileSession flowFileSession : flowFileSessions) {
            final ProcessSession processSession = flowFileSession.session();
            processSession.transfer(flowFileSession.flowFiles(), REL_SUCCESS);
            processSession.commitAsync();
        }

        getLogger().info("After ignoring input for {}, successfully transferred {} FlowFiles",
            context.getProperty(IGNORE_TIME).getValue(), flowFiles.size());
        lastTransferTime = System.nanoTime();
    }

    private record FlowFileSession(ProcessSession session, List<FlowFile> flowFiles) {
    }
}
