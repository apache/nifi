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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyDescriptor.Builder;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.nifi.processor.util.StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR;

public class LoopFlowFile extends AbstractProcessor {
    private static final String ATTRIBUTE_NAME = "loop.count";

    static final PropertyDescriptor COUNT = new Builder()
        .name("Count")
        .displayName("Count")
        .description("The number of times to loop")
        .required(true)
        .addValidator(NON_NEGATIVE_INTEGER_VALIDATOR)
        .defaultValue("1")
        .build();

    protected static Relationship REL_LOOP = new Relationship.Builder()
        .name("loop")
        .build();
    protected static Relationship REL_FINISHED = new Relationship.Builder()
        .name("finished")
        .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Collections.singletonList(COUNT);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return new HashSet<>(Arrays.asList(REL_LOOP, REL_FINISHED));
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String attributeValue = flowFile.getAttribute(ATTRIBUTE_NAME);
        final int iterations = attributeValue == null ? 0 : Integer.parseInt(attributeValue);
        final int desiredCount = context.getProperty(COUNT).asInteger();

        if (iterations >= desiredCount) {
            session.transfer(flowFile, REL_FINISHED);
        } else {
            flowFile = session.putAttribute(flowFile, ATTRIBUTE_NAME, String.valueOf(iterations + 1));
            session.transfer(flowFile, REL_LOOP);
        }
    }
}
