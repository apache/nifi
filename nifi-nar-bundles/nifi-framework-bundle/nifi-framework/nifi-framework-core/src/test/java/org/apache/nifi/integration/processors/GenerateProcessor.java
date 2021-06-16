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
package org.apache.nifi.integration.processors;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyDescriptor.Builder;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.apache.nifi.processor.util.StandardValidators.DATA_SIZE_VALIDATOR;
import static org.apache.nifi.processor.util.StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR;

public class GenerateProcessor extends AbstractProcessor {
    public static final PropertyDescriptor COUNT = new Builder()
        .name("Count")
        .displayName("Count")
        .description("Number of FlowFiles to generate")
        .required(true)
        .addValidator(NON_NEGATIVE_INTEGER_VALIDATOR)
        .defaultValue("1")
        .build();

    public static final PropertyDescriptor CONTENT_SIZE = new Builder()
        .name("Content Size")
        .displayName("Content Size")
        .description("Size of the FlowFile")
        .required(true)
        .addValidator(DATA_SIZE_VALIDATOR)
        .defaultValue("0 B")
        .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Arrays.asList(COUNT, CONTENT_SIZE);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return Collections.singleton(REL_SUCCESS);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        for (int i=0; i < context.getProperty(COUNT).asInteger(); i++) {
            FlowFile flowFile = session.create();

            final int size = context.getProperty(CONTENT_SIZE).asDataSize(DataUnit.B).intValue();
            final byte[] data = new byte[size];
            session.write(flowFile, out -> out.write(data));
            session.transfer(flowFile, REL_SUCCESS);
        }
    }
}
