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
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.List;
import java.util.Set;

public class MigrateProperties extends AbstractProcessor {

    static PropertyDescriptor INGEST = new PropertyDescriptor.Builder()
            .name("ingest-data")
            .displayName("Ingest Data")
            .allowableValues("true", "false")
            .defaultValue("true")
            .required(true)
            .build();

    static PropertyDescriptor ATTRIBUTE_NAME = new PropertyDescriptor.Builder()
            .name("attr-to-add")
            .displayName("Attribute to add")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static PropertyDescriptor ATTRIBUTE_VALUE = new PropertyDescriptor.Builder()
            .name("attr-value")
            .displayName("Attribute Value")
            .required(true)
            .dependsOn(ATTRIBUTE_NAME)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static PropertyDescriptor IGNORED = new PropertyDescriptor.Builder()
            .name("ignored")
            .displayName("Ignored")
            .required(false)
            .addValidator(Validator.VALID)
            .build();


    static Relationship REL_SUCCESS = new Relationship.Builder().name("success").build();
    static Relationship REL_FAILURE = new Relationship.Builder().name("failure").build();

    private static final Set<Relationship> relationships = Set.of(REL_SUCCESS, REL_FAILURE);
    private static final List<PropertyDescriptor> properties = List.of(
            INGEST,
            ATTRIBUTE_NAME,
            ATTRIBUTE_VALUE,
            IGNORED
    );

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
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String attributeValue = context.getProperty(ATTRIBUTE_VALUE).getValue();
        if (attributeValue != null) {
            final String attributeName = context.getProperty(ATTRIBUTE_NAME).getValue();
            flowFile = session.putAttribute(flowFile, attributeName, attributeValue);
        }

        session.transfer(flowFile, REL_SUCCESS);
    }

}
