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
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.File;
import java.util.List;
import java.util.Set;

/**
 * Generates FlowFiles for connector-upgrade tests. It lives in a NAR that the upgrade swap does not remove,
 * so a restored pre-upgrade flow can still run after the connector NAR changes.
 */
public class UpgradeDrainGenerate extends AbstractProcessor {

    public static final PropertyDescriptor FILE_SIZE = new PropertyDescriptor.Builder()
        .name("File Size")
        .description("Accepted so the upgrade flow's File Size property remains valid. Each FlowFile is one byte.")
        .required(false)
        .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
        .build();

    public static final PropertyDescriptor GATE_FILE = new PropertyDescriptor.Builder()
        .name("Gate File")
        .description("When set, FlowFile generation stops after this file exists.")
        .required(false)
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .build();

    public static final PropertyDescriptor SECRET_VALUE = new PropertyDescriptor.Builder()
        .name("Secret Value")
        .description("Sensitive Parameter reference used to test Connector flow restoration.")
        .required(false)
        .sensitive(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
        .name("success")
        .description("Generated FlowFiles")
        .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return List.of(FILE_SIZE, GATE_FILE, SECRET_VALUE);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return Set.of(SUCCESS);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final String gateFilePath = context.getProperty(GATE_FILE).getValue();
        if (gateFilePath != null && new File(gateFilePath).exists()) {
            context.yield();
            return;
        }

        FlowFile flowFile = session.create();
        flowFile = session.write(flowFile, out -> out.write('x'));
        session.transfer(flowFile, SUCCESS);
    }
}
