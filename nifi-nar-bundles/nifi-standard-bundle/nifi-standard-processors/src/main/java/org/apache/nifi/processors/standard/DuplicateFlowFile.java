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
package org.apache.nifi.processors.standard;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

@EventDriven
@SupportsBatching
@Tags({"test", "load", "duplicate"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Intended for load testing, this processor will create the configured number of copies of each incoming FlowFile")
public class DuplicateFlowFile extends AbstractProcessor {

    static final PropertyDescriptor NUM_COPIES = new PropertyDescriptor.Builder()
    .name("Number of Copies")
    .description("Specifies how many copies of each incoming FlowFile will be made")
    .required(true)
    .expressionLanguageSupported(false)
    .defaultValue("100")
    .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
    .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
    .name("success")
    .description("The original FlowFile and all copies will be sent to this relationship")
    .build();

    @Override
    public Set<Relationship> getRelationships() {
        return Collections.singleton(REL_SUCCESS);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Collections.singletonList(NUM_COPIES);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        for (int i=0; i < context.getProperty(NUM_COPIES).asInteger(); i++) {
            final FlowFile copy = session.clone(flowFile);
            session.transfer(copy, REL_SUCCESS);
        }

        session.transfer(flowFile, REL_SUCCESS);
    }

}
