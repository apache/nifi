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
package org.apache.nifi.processors.cybersecurity;

import com.idealista.tlsh.exceptions.InsufficientComplexityException;

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StringUtils;

import org.apache.nifi.stream.io.StreamUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;


@EventDriven
@SideEffectFree
@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"hashing", "fuzzy-hashing", "cyber-security"})
@CapabilityDescription("Calculates a fuzzy/locality-sensitive hash value for the Content of a FlowFile and puts that " +
        "hash value on the FlowFile as an attribute whose name is determined by the <Hash Attribute Name> property." +
        "Note: this processor only offers non-cryptographic hash algorithms. And it should be not be " +
        "seen as a replacement to the HashContent processor." +
        "Note: The underlying library loads the entirety of the streamed content into and performs result " +
        "evaluations in memory. Accordingly, it is important to consider the anticipated profile of content being " +
        "evaluated by this processor and the hardware supporting it especially when working against large files.")

@SeeAlso(classNames = {"org.apache.nifi.processors.standard.HashContent"}, value = {CompareFuzzyHash.class})
@WritesAttributes({@WritesAttribute(attribute = "<Hash Attribute Name>", description = "This Processor adds an attribute whose value is the result of Hashing the "
        + "existing FlowFile content. The name of this attribute is specified by the <Hash Attribute Name> property")})

public class FuzzyHashContent extends AbstractFuzzyHashProcessor {



    public static final PropertyDescriptor HASH_ALGORITHM = new PropertyDescriptor.Builder()
            .name("HASH_ALGORITHM")
            .displayName("Hashing Algorithm")
            .description("The hashing algorithm utilised")
            .allowableValues(allowableValueSSDEEP, allowableValueTLSH)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Any FlowFile that is successfully hashed will be sent to this Relationship.")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Any FlowFile that is successfully hashed will be sent to this Relationship.")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(ATTRIBUTE_NAME);
        descriptors.add(HASH_ALGORITHM);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ComponentLog logger = getLogger();
        String algorithm = context.getProperty(HASH_ALGORITHM).getValue();

        // Check if content matches minimum length requirement

        if (checkMinimumAlgorithmRequirements(algorithm, flowFile) == false) {
            logger.error("The content of '{}' is smaller than the minimum required by {}, routing to failure",
                    new Object[]{flowFile, algorithm});
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        final AtomicReference<String> hashValueHolder = new AtomicReference<>(null);

        try {
            session.read(flowFile, new InputStreamCallback() {
                @Override
                public void process(final InputStream in) throws IOException {
                    try (ByteArrayOutputStream holder = new ByteArrayOutputStream()) {
                        StreamUtils.copy(in,holder);

                        String hashValue = generateHash(algorithm, holder.toString());
                        if (StringUtils.isBlank(hashValue) == false) {
                            hashValueHolder.set(hashValue);
                        }


                    }
                }
            });

            final String attributeName = context.getProperty(ATTRIBUTE_NAME).getValue();
            flowFile = session.putAttribute(flowFile, attributeName, hashValueHolder.get());
            logger.info("Successfully added attribute '{}' to {} with a value of {}; routing to success", new Object[]{attributeName, flowFile, hashValueHolder.get()});
            session.getProvenanceReporter().modifyAttributes(flowFile);
            session.transfer(flowFile, REL_SUCCESS);
        } catch (final InsufficientComplexityException | ProcessException e) {
            logger.error("Failed to process {} due to {}; routing to failure", new Object[]{flowFile, e});
            session.transfer(flowFile, REL_FAILURE);
        }
    }

}
