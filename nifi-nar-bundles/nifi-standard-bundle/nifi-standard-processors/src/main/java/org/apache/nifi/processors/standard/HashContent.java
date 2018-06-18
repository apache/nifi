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

import java.io.IOException;
import java.io.InputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.Security;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.DeprecationNotice;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.NullOutputStream;
import org.apache.nifi.stream.io.StreamUtils;

@Deprecated
@DeprecationNotice(classNames = {"org.apache.nifi.processors.standard.CryptographicHashContent"}, reason = "This processor is deprecated and may be removed in future releases.")
@EventDriven
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"hash", "content", "MD5", "SHA-1", "SHA-256"})
@CapabilityDescription("Calculates a hash value for the Content of a FlowFile and puts that hash value on the FlowFile as an attribute whose name "
        + "is determined by the <Hash Attribute Name> property. "
        + "This processor did not provide a consistent offering of hash algorithms, and is now deprecated. For modern cryptographic hashing capabilities, see \"CryptographicHashContent\". ")
@WritesAttribute(attribute = "<Hash Attribute Name>", description = "This Processor adds an attribute whose value is the result of Hashing the "
        + "existing FlowFile content. The name of this attribute is specified by the <Hash Attribute Name> property")
public class HashContent extends AbstractProcessor {

    public static final PropertyDescriptor ATTRIBUTE_NAME = new PropertyDescriptor.Builder()
            .name("Hash Attribute Name")
            .description("The name of the FlowFile Attribute into which the Hash Value should be written. If the value already exists, it will be overwritten")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("hash.value")
            .build();

    public static final PropertyDescriptor HASH_ALGORITHM = new PropertyDescriptor.Builder()
            .name("Hash Algorithm")
            .description("Determines what hashing algorithm should be used to perform the hashing function")
            .required(true)
            .allowableValues(Security.getAlgorithms("MessageDigest"))
            .defaultValue("MD5")
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles that are process successfully will be sent to this relationship")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Any FlowFile that cannot be processed successfully will be sent to this relationship without any attribute being added")
            .build();

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;

    @Override
    protected void init(ProcessorInitializationContext context) {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(ATTRIBUTE_NAME);
        props.add(HASH_ALGORITHM);
        properties = Collections.unmodifiableList(props);

        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        rels.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(rels);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ComponentLog logger = getLogger();
        final String algorithm = context.getProperty(HASH_ALGORITHM).getValue();
        final MessageDigest digest;
        try {
            digest = MessageDigest.getInstance(algorithm);
        } catch (NoSuchAlgorithmException e) {
            logger.error("Failed to process {} due to {}; routing to failure", new Object[]{flowFile, e});
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        final AtomicReference<String> hashValueHolder = new AtomicReference<>(null);

        try {
            session.read(flowFile, new InputStreamCallback() {
                @Override
                public void process(final InputStream in) throws IOException {
                    try (final DigestOutputStream digestOut = new DigestOutputStream(new NullOutputStream(), digest)) {
                        StreamUtils.copy(in, digestOut);

                        final byte[] hash = digest.digest();
                        final StringBuilder strb = new StringBuilder(hash.length * 2);
                        for (int i = 0; i < hash.length; i++) {
                            strb.append(Integer.toHexString((hash[i] & 0xFF) | 0x100), 1, 3);
                        }

                        hashValueHolder.set(strb.toString());
                    }
                }
            });

            final String attributeName = context.getProperty(ATTRIBUTE_NAME).getValue();
            flowFile = session.putAttribute(flowFile, attributeName, hashValueHolder.get());
            logger.info("Successfully added attribute '{}' to {} with a value of {}; routing to success", new Object[]{attributeName, flowFile, hashValueHolder.get()});
            session.getProvenanceReporter().modifyAttributes(flowFile);
            session.transfer(flowFile, REL_SUCCESS);
        } catch (final ProcessException e) {
            logger.error("Failed to process {} due to {}; routing to failure", new Object[]{flowFile, e});
            session.transfer(flowFile, REL_FAILURE);
        }
    }
}
