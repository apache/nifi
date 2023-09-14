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
package org.apache.nifi.processors.standard;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.flowfile.attributes.StandardFlowFileMediaType;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.FlowFilePackager;
import org.apache.nifi.util.FlowFilePackagerV3;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


@SideEffectFree
@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"flowfile", "flowfile-stream", "flowfile-stream-v3", "package", "attributes"})
@CapabilityDescription("This processor will package FlowFile attributes and content into an output FlowFile that can be exported from NiFi"
        + " and imported back into NiFi, preserving the original attributes and content.")
@WritesAttributes({
        @WritesAttribute(attribute = "mime.type", description = "The mime.type will be changed to application/flowfile-v3")
})
@SeeAlso({UnpackContent.class, MergeContent.class})
public class PackageFlowFile extends AbstractProcessor {

    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("max-batch-size")
            .displayName("Maximum Batch Size")
            .description("Maximum number of FlowFiles to package into one output FlowFile using a best effort, non guaranteed approach."
                    + " Multiple input queues can produce unexpected batching behavior.")
            .required(true)
            .defaultValue("1")
            .addValidator(StandardValidators.createLongValidator(1, 10_000, true))
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("The packaged FlowFile is sent to this relationship")
            .build();
    static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("The FlowFiles that were used to create the package are sent to this relationship")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(
            new LinkedHashSet<>(Arrays.asList(
                    REL_SUCCESS,
                    REL_ORIGINAL
            )));

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = Collections.unmodifiableList(
            Arrays.asList(
                    BATCH_SIZE
            ));

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final List<FlowFile> flowFiles = session.get(context.getProperty(BATCH_SIZE).asInteger());
        if (flowFiles.isEmpty()) {
            return;
        }

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.MIME_TYPE.key(), StandardFlowFileMediaType.VERSION_3.getMediaType());

        final FlowFilePackager packager = new FlowFilePackagerV3();

        FlowFile packagedFlowFile = session.create(flowFiles);
        packagedFlowFile = session.write(packagedFlowFile, out -> {
            try (final OutputStream bufferedOut = new BufferedOutputStream(out)) {
                for (final FlowFile flowFile : flowFiles) {
                    session.read(flowFile, in -> {
                        try (final InputStream bufferedIn = new BufferedInputStream(in)) {
                            packager.packageFlowFile(bufferedIn, bufferedOut, flowFile.getAttributes(), flowFile.getSize());
                        }
                    });
                }
            }
        });

        packagedFlowFile = session.putAllAttributes(packagedFlowFile, attributes);
        session.transfer(packagedFlowFile, REL_SUCCESS);
        session.transfer(flowFiles, REL_ORIGINAL);
    }
}
