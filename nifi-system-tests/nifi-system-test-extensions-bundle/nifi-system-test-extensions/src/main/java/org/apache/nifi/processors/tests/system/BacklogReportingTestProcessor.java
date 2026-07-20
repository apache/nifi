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

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.Backlog;
import org.apache.nifi.components.BacklogReportingException;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.BacklogReportingProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Test Processor that implements {@link BacklogReportingProcessor} and exposes its behavior
 * through property values. Used by backlog system tests to verify happy-path delegation as
 * well as how the framework handles {@link BacklogReportingException} and {@link RuntimeException}
 * thrown from {@code getBacklog}.
 */
@InputRequirement(Requirement.INPUT_ALLOWED)
public class BacklogReportingTestProcessor extends AbstractProcessor implements BacklogReportingProcessor {

    static final AllowableValue MODE_NORMAL = new AllowableValue("NORMAL", "Normal",
            "Return a Backlog populated from the FlowFile Backlog, Byte Backlog and Record Backlog properties");
    static final AllowableValue MODE_THROW_BACKLOG_REPORTING_EXCEPTION = new AllowableValue("THROW_BACKLOG_REPORTING_EXCEPTION",
            "Throw BacklogReportingException", "Throw a BacklogReportingException with the configured Exception Message");
    static final AllowableValue MODE_THROW_RUNTIME_EXCEPTION = new AllowableValue("THROW_RUNTIME_EXCEPTION",
            "Throw RuntimeException", "Throw a RuntimeException with the configured Exception Message");

    static final PropertyDescriptor BACKLOG_MODE = new PropertyDescriptor.Builder()
            .name("Backlog Mode")
            .description("Controls what getBacklog returns or throws")
            .required(true)
            .allowableValues(MODE_NORMAL, MODE_THROW_BACKLOG_REPORTING_EXCEPTION, MODE_THROW_RUNTIME_EXCEPTION)
            .defaultValue(MODE_NORMAL.getValue())
            .build();

    static final PropertyDescriptor FLOWFILE_BACKLOG = new PropertyDescriptor.Builder()
            .name("FlowFile Backlog")
            .description("Number of FlowFiles to report on the source")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("0")
            .build();

    static final PropertyDescriptor BYTE_BACKLOG = new PropertyDescriptor.Builder()
            .name("Byte Backlog")
            .description("Number of bytes to report on the source")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("0")
            .build();

    static final PropertyDescriptor RECORD_BACKLOG = new PropertyDescriptor.Builder()
            .name("Record Backlog")
            .description("Number of records to report on the source")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("0")
            .build();

    static final PropertyDescriptor EXCEPTION_MESSAGE = new PropertyDescriptor.Builder()
            .name("Exception Message")
            .description("Message to include on the thrown exception when Backlog Mode is one of the throw modes")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("Simulated backlog reporting failure")
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All incoming FlowFiles are transferred here")
            .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return List.of(BACKLOG_MODE, FLOWFILE_BACKLOG, BYTE_BACKLOG, RECORD_BACKLOG, EXCEPTION_MESSAGE);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return Set.of(REL_SUCCESS);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        session.transfer(flowFile, REL_SUCCESS);
    }

    @Override
    public Optional<Backlog> getBacklog(final ProcessContext context) throws BacklogReportingException {
        final String mode = context.getProperty(BACKLOG_MODE).getValue();

        if (MODE_THROW_BACKLOG_REPORTING_EXCEPTION.getValue().equals(mode)) {
            throw new BacklogReportingException(context.getProperty(EXCEPTION_MESSAGE).getValue());
        }
        if (MODE_THROW_RUNTIME_EXCEPTION.getValue().equals(mode)) {
            throw new RuntimeException(context.getProperty(EXCEPTION_MESSAGE).getValue());
        }

        final long flowFiles = context.getProperty(FLOWFILE_BACKLOG).asLong();
        final long bytes = context.getProperty(BYTE_BACKLOG).asLong();
        final long records = context.getProperty(RECORD_BACKLOG).asLong();

        return Optional.of(Backlog.builder()
                .flowFiles(flowFiles)
                .bytes(bytes)
                .records(records)
                .build());
    }
}
