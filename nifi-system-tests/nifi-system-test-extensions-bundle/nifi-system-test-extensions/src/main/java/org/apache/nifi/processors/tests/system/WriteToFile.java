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
import org.apache.nifi.stream.io.StreamUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.nifi.expression.ExpressionLanguageScope.FLOWFILE_ATTRIBUTES;
import static org.apache.nifi.processor.util.StandardValidators.NON_EMPTY_VALIDATOR;

public class WriteToFile extends AbstractProcessor {

    static final PropertyDescriptor FILENAME = new Builder()
        .name("Filename")
        .displayName("Filename")
        .description("The file to write the FlowFile contents to")
        .required(true)
        .addValidator(NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
        .defaultValue("target/WriteToFile.out")
        .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .build();

    @Override
    public Set<Relationship> getRelationships() {
        return new HashSet<>(Arrays.asList(REL_SUCCESS, REL_FAILURE));
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Collections.singletonList(FILENAME);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String filename = context.getProperty(FILENAME).evaluateAttributeExpressions(flowFile).getValue();
        final File file = new File(filename);
        final File dir = file.getParentFile();
        if (!dir.exists() && !dir.mkdirs()) {
            getLogger().error("Could not write FlowFile to {} because the directory does not exist and could not be created", file.getAbsolutePath());
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        try (final OutputStream out = new FileOutputStream(file);
             final InputStream in = session.read(flowFile)) {
            StreamUtils.copy(in, out);
        } catch (final Exception e) {
            getLogger().error("Could not write FlowFile to {}", file.getAbsolutePath(), e);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        session.transfer(flowFile, REL_SUCCESS);

        getLogger().info("Wrote one FlowFile of size {} to {}", flowFile.getSize(), file.getAbsolutePath());
        session.getProvenanceReporter().send(flowFile, file.toURI().toString());
    }
}
