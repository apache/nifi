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
import org.apache.nifi.processor.util.StandardValidators;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.apache.nifi.expression.ExpressionLanguageScope.NONE;

public class IngestFile extends AbstractProcessor {
    private static final String COMMIT_SYNCHRONOUS = "synchronous";
    private static final String COMMIT_ASYNC = "asynchronous";

    static final PropertyDescriptor FILENAME = new Builder()
        .name("Filename")
        .displayName("Filename")
        .description("Fully qualified path to the file that should be ingested")
        .required(true)
        .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
        .expressionLanguageSupported(NONE)
        .build();
    static final PropertyDescriptor COMMIT_MODE = new Builder()
        .name("Commit Mode")
        .displayName("Commit Mode")
        .description("How to commit the process session")
        .allowableValues(COMMIT_ASYNC, COMMIT_SYNCHRONOUS)
        .defaultValue(COMMIT_ASYNC)
        .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Arrays.asList(FILENAME, COMMIT_MODE);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return Collections.singleton(REL_SUCCESS);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final String filename = context.getProperty(FILENAME).getValue();
        final File file = new File(filename);

        FlowFile flowFile = session.create();
        flowFile = session.importFrom(file.toPath(), true, flowFile);
        session.transfer(flowFile, REL_SUCCESS);
        session.getProvenanceReporter().receive(flowFile, file.toURI().toString());

        final String commitMode = context.getProperty(COMMIT_MODE).getValue();
        if (COMMIT_SYNCHRONOUS.equalsIgnoreCase(commitMode)) {
            session.commit();
            cleanup(file);
        } else {
            session.commitAsync(() -> cleanup(file));
        }
    }

    private void cleanup(final File file) {
        getLogger().info("Deleting {}", file.getAbsolutePath());
        try {
            Files.delete(file.toPath());
        } catch (final IOException e) {
            getLogger().error("Failed to delete file", e);
        }
    }
}
