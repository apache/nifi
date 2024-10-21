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
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@CapabilityDescription("Ensures that all FlowFiles that are in the Processor's incoming queue are ordered such that the value of the key attribute " +
                       "is even for all FlowFiles before the first FlowFile with an odd value. If the FlowFiles are ordered correctly, they are transferred to " +
                       "the 'success' relationship; otherwise, they are transferred to the 'failure' relationship. The name of the key attribute is configurable. " +
                       "This is used to ensure that data is properly ordered while running within a Stateless flow.")
public class VerifyEvenThenOdd extends AbstractProcessor {

    protected static final PropertyDescriptor ATTRIBUTE_NAME = new PropertyDescriptor.Builder()
        .name("Attribute Name")
        .description("The name of the attribute to check for even or odd values")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    private static final List<PropertyDescriptor> properties = List.of(ATTRIBUTE_NAME);

    protected static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("All FlowFiles are transferred to this relationship in the event that all queued FlowFiles are ordered correctly.")
        .build();

    protected static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("All FlowFiles are transferred to this relationship in the event that any queued FlowFiles are not ordered correctly.")
        .build();

    private static final Set<Relationship> relationships = Set.of(REL_SUCCESS, REL_FAILURE);

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final List<FlowFile> allFlowFiles = new ArrayList<>();
        List<FlowFile> batch;
        while (!(batch = session.get(1000)).isEmpty()) {
            allFlowFiles.addAll(batch);
        }

        boolean oddSeen = false;
        boolean ordered = true;
        for (final FlowFile flowFile : allFlowFiles) {
            final String value = flowFile.getAttribute(context.getProperty(ATTRIBUTE_NAME).getValue());
            final int intValue = Integer.parseInt(value);

            final boolean even = intValue % 2 == 0;
            if (even && oddSeen) {
                ordered = false;
                break;
            } else if (!even) {
                oddSeen = true;
            }
        }

        if (ordered) {
            session.transfer(allFlowFiles, REL_SUCCESS);
        } else {
            session.transfer(allFlowFiles, REL_FAILURE);
        }
    }
}
