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

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Set;

@Tags({"asset", "test"})
@CapabilityDescription("Reads the contents of a file-backed asset parameter and writes the asset contents to a configured output file.")
public class AssetReadingProcessor extends AbstractProcessor {
    static final PropertyDescriptor SOURCE_FILE = new PropertyDescriptor.Builder()
        .name("Source File")
        .description("The path of the asset file to read")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    static final PropertyDescriptor OUTPUT_FILE = new PropertyDescriptor.Builder()
        .name("Output File")
        .description("The file where the asset contents will be written")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return List.of(SOURCE_FILE, OUTPUT_FILE);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return Set.of(REL_SUCCESS, REL_FAILURE);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final FlowFile incomingFlowFile = session.get();
        final FlowFile flowFile = incomingFlowFile == null ? session.create() : incomingFlowFile;

        final File sourceFile = new File(context.getProperty(SOURCE_FILE).getValue());
        final File outputFile = new File(context.getProperty(OUTPUT_FILE).getValue());
        final File parentFile = outputFile.getParentFile();
        if (parentFile != null && !parentFile.exists() && !parentFile.mkdirs()) {
            getLogger().error("Could not create directory {}", parentFile.getAbsolutePath());
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        try (final InputStream inputStream = new FileInputStream(sourceFile);
             final OutputStream outputStream = new FileOutputStream(outputFile)) {
            StreamUtils.copy(inputStream, outputStream);
            session.transfer(flowFile, REL_SUCCESS);
        } catch (final Exception e) {
            getLogger().error("Failed to read asset file {} to {}", sourceFile.getAbsolutePath(), outputFile.getAbsolutePath(), e);
            session.transfer(flowFile, REL_FAILURE);
        }
    }
}
