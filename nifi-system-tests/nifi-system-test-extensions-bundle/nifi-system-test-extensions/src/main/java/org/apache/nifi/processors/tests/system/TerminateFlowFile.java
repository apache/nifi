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
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.File;
import java.util.List;

public class TerminateFlowFile extends AbstractProcessor {

    public static final PropertyDescriptor GATE_FILE = new PropertyDescriptor.Builder()
            .name("Gate File")
            .description("An optional file path. If specified, the processor will only process FlowFiles when this file exists. " +
                    "If the file does not exist, the processor will yield and return without processing any data.")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return List.of(GATE_FILE);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final String gateFilePath = context.getProperty(GATE_FILE).getValue();
        if (gateFilePath != null) {
            final File gateFile = new File(gateFilePath);
            if (!gateFile.exists()) {
                context.yield();
                return;
            }
        }

        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        session.remove(flowFile);
    }
}
