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
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.FlowFileFilter;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

import java.util.List;
import java.util.Set;

public class ThrowExceptionInFlowFileFilter extends AbstractProcessor {

    static final PropertyDescriptor THROW_EXCEPTION = new PropertyDescriptor.Builder()
        .name("Throw Exception")
        .description("If true, the processor will throw an exception for each FlowFile that is processed")
        .required(true)
        .allowableValues("true", "false")
        .defaultValue("true")
        .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
        THROW_EXCEPTION
    );

    static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("All FlowFiles are routed to this Relationship")
        .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
        REL_SUCCESS
    );

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final boolean throwException = context.getProperty(THROW_EXCEPTION).asBoolean();

        final List<FlowFile> flowFiles = session.get(new FlowFileFilter() {
            @Override
            public FlowFileFilterResult filter(final FlowFile flowFile) {
                if (throwException) {
                    throw new ProcessException("Throwing exception as configured");
                }
                return FlowFileFilterResult.ACCEPT_AND_CONTINUE;
            }
        });

        session.transfer(flowFiles, REL_SUCCESS);
    }
}
