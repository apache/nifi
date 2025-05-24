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
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

@InputRequirement(Requirement.INPUT_FORBIDDEN)
public class YieldSource extends AbstractProcessor {
    static final PropertyDescriptor YIELD_AFTER = new PropertyDescriptor.Builder()
        .name("Yield After")
        .description("The number of times to run before yielding")
        .required(true)
        .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
        .defaultValue("1")
        .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("All FlowFiles go to this Relationship")
        .build();

    private final AtomicLong flowFileCount = new AtomicLong(0L);

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return List.of(YIELD_AFTER);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return Set.of(REL_SUCCESS);
    }

    @OnScheduled
    public void reset() {
        flowFileCount.set(0L);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final long alreadyCreated = flowFileCount.getAndIncrement();
        if (alreadyCreated >= context.getProperty(YIELD_AFTER).asLong()) {
            context.yield();
            getLogger().info("Yielding after {} FlowFiles", alreadyCreated);
        } else {
            getLogger().info("Created FlowFile {} without yielding", alreadyCreated);
        }

        FlowFile flowFile = session.create();
        session.transfer(flowFile, REL_SUCCESS);
    }
}
