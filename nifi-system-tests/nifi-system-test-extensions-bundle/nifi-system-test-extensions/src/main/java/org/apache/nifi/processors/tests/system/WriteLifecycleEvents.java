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

import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class WriteLifecycleEvents extends AbstractProcessor {
    static final PropertyDescriptor EVENT_FILE = new PropertyDescriptor.Builder()
        .name("Event File")
        .displayName("Event File")
        .description("Specifies the file to write to that contains a line of text for each lifecycle event that occurs")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .defaultValue("target/CountLifecycleEvents.events")
        .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("All FlowFiles go here")
        .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Collections.singletonList(EVENT_FILE);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return Collections.singleton(REL_SUCCESS);
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws IOException {
        writeEvent(context, "OnScheduled");
    }

    @OnUnscheduled
    public void onUnscheduled(final ProcessContext context) throws IOException {
        writeEvent(context, "OnUnscheduled");
    }

    @OnStopped
    public void onStopped(final ProcessContext context) throws IOException {
        writeEvent(context, "OnStopped");
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        try {
            writeEvent(context, "OnTrigger");
        } catch (IOException e) {
            throw new ProcessException(e);
        }

        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        session.transfer(flowFile, REL_SUCCESS);
    }

    private void writeEvent(final ProcessContext context, final String event) throws IOException {
        final File file = new File(context.getProperty(EVENT_FILE).getValue());

        final byte[] eventBytes = (event + "\n").getBytes(StandardCharsets.UTF_8);

        try (final OutputStream fos = new FileOutputStream(file, true)) {
            fos.write(eventBytes);
        }
    }
}
