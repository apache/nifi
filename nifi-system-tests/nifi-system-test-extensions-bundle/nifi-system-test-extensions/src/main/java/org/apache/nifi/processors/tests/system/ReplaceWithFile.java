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
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.apache.nifi.expression.ExpressionLanguageScope.NONE;

public class ReplaceWithFile extends AbstractProcessor {
    static final PropertyDescriptor FILENAME = new PropertyDescriptor.Builder()
        .name("Filename")
        .displayName("Filename")
        .description("Fully qualified path to the file that should be ingested")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR) // We do not want to validate that the file exists because we want to check that only in the onTrigger
        .expressionLanguageSupported(NONE)
        .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Collections.singletonList(FILENAME);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return Collections.singleton(REL_SUCCESS);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final String filename = context.getProperty(FILENAME).getValue();
        final File file = new File(filename);
        if (!file.exists()) {
            getLogger().debug("File {} does not yet exist so will yield", file.getAbsolutePath());
            context.yield();
            return;
        }

        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        flowFile = session.importFrom(file.toPath(), true, flowFile);
        session.getProvenanceReporter().fetch(flowFile, file.toURI().toString());
        session.transfer(flowFile, REL_SUCCESS);

        getLogger().info("Successfully imported replacement file {}", file.getAbsolutePath());
    }
}
