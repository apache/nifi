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

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.nifi.expression.ExpressionLanguageScope.FLOWFILE_ATTRIBUTES;
import static org.apache.nifi.processor.util.StandardValidators.POSITIVE_INTEGER_VALIDATOR;

public class PartitionText extends AbstractProcessor {
    static final PropertyDescriptor OUTPUT_FLOWFILES = new Builder()
        .name("Number of Output FlowFiles")
        .displayName("Number of Output FlowFiles")
        .description("The number of Output FlowFiles")
        .required(true)
        .addValidator(POSITIVE_INTEGER_VALIDATOR)
        .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
        .defaultValue("2")
        .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Collections.singletonList(OUTPUT_FLOWFILES);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return Collections.singleton(REL_SUCCESS);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final int outputCount = context.getProperty(OUTPUT_FLOWFILES).evaluateAttributeExpressions(flowFile).asInteger();
        final Map<Integer, FlowFile> outputFlowFiles = new HashMap<>();
        final Map<Integer, OutputStream> outputStreams = new HashMap<>();

        try {
            try (final InputStream in = session.read(flowFile);
                 final Reader streamReader = new InputStreamReader(in);
                 final BufferedReader reader = new BufferedReader(streamReader)) {

                long lineCount = 0L;

                String line;
                while ((line = reader.readLine()) != null) {
                    final int flowFileIndex = (int) (lineCount++ % outputCount);

                    OutputStream out = outputStreams.get(flowFileIndex);
                    if (out == null) {
                        FlowFile outputFlowFile = session.create(flowFile);
                        out = session.write(outputFlowFile);
                        outputFlowFiles.put(flowFileIndex, outputFlowFile);
                        outputStreams.put(flowFileIndex, out);
                    }

                    out.write(line.getBytes());
                    out.write("\n".getBytes());
                }
            }

            outputStreams.values().forEach(this::closeQuietly);
            session.transfer(outputFlowFiles.values(), REL_SUCCESS);
            session.remove(flowFile);
        } catch (IOException e) {
            throw new ProcessException(e);
        }
    }

    private void closeQuietly(final Closeable closeable) {
        try {
            closeable.close();
        } catch (final Exception ignored) {
        }
    }
}
