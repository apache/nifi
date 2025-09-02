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
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.connector.services.CounterService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;

@InputRequirement(Requirement.INPUT_FORBIDDEN)
public class CreateDummyFlowFile extends AbstractProcessor {

    static final PropertyDescriptor TEXT = new PropertyDescriptor.Builder()
        .name("Text")
        .description("The text to write to the FlowFile content")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    static final PropertyDescriptor COUNT_SERVICE = new PropertyDescriptor.Builder()
        .name("Counter Service")
        .description("The Counter Service to use for incrementing a counter each time a FlowFile is created")
        .required(false)
        .identifiesControllerService(CounterService.class)
        .build();

    private final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("All FlowFiles are routed to this relationship")
        .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return List.of(TEXT);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return Set.of(REL_SUCCESS);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.create();
        final String text = context.getProperty(TEXT).getValue();
        if (text != null) {
            flowFile = session.write(flowFile, out -> out.write(text.getBytes(StandardCharsets.UTF_8)));
        }

        final CounterService service = context.getProperty(COUNT_SERVICE).asControllerService(CounterService.class);
        if (service != null) {
            final long count = service.increment();
            flowFile = session.putAttribute(flowFile, "counter.value", String.valueOf(count));
        }

        session.transfer(flowFile, REL_SUCCESS);
    }
}
