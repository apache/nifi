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

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;

@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"attributes", "hash"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Calculates a hash value for the value specified attributes and write it to an output attribute")
@WritesAttribute(attribute = "<Specified Attribute Name per Dynamic Property>", description = "This Processor adds an attribute whose value is the result of "
        + "Hashing of the found attribute. The name of this attribute is specified by the value of the dynamic property.")
@DynamicProperty(name = "A flowfile attribute key for attribute inspection", value = "Attribute Name",
        description = "The property name defines the attribute to look for and hash in the incoming flowfile."
                + "The property value defines the name to give the generated attribute."
                + "Attribute names must be unique.")
public class CalculateAttributeHash extends AbstractProcessor {

    public static final Charset UTF8 = Charset.forName("UTF-8");

    static final AllowableValue MD2_VALUE = new AllowableValue("MD2", "MD2 Hashing Algorithm", "MD2 Hashing Algorithm");
    static final AllowableValue MD5_VALUE = new AllowableValue("MD5", "MD5 Hashing Algorithm", "MD5 Hashing Algorithm");
    static final AllowableValue SHA1_VALUE = new AllowableValue("SHA-1", "SHA-1 Hashing Algorithm", "SHA-1 Hashing Algorithm");
    static final AllowableValue SHA256_VALUE = new AllowableValue("SHA-256", "SHA-256 Hashing Algorithm", "SHA-256 Hashing Algorithm");
    static final AllowableValue SHA384_VALUE = new AllowableValue("SHA-384", "SHA-384 Hashing Algorithm", "SHA-384 Hashing Algorithm");
    static final AllowableValue SHA512_VALUE = new AllowableValue("SHA-512", "SHA-512 Hashing Algorithm", "SHA-512 Hashing Algorithm");

    public static final PropertyDescriptor HASH_ALGORITHM = new PropertyDescriptor.Builder()
            .name("hash_algorithm")
            .displayName("Hash Algorithm")
            .description("The Hash Algorithm to use")
            .required(true)
            .allowableValues(MD2_VALUE, MD5_VALUE, SHA1_VALUE, SHA256_VALUE, SHA384_VALUE, SHA512_VALUE)
            .defaultValue(SHA256_VALUE.getValue())
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Used for FlowFiles that have a hash value added")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Used for FlowFiles that are missing required attributes")
            .build();

    private Set<Relationship> relationships;
    private List<PropertyDescriptor> properties;

    private final AtomicReference<Map<String, String>> attributeToGenerateNameMapRef = new AtomicReference<>(Collections.emptyMap());

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_FAILURE);
        relationships.add(REL_SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);

        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(HASH_ALGORITHM);
        this.properties = Collections.unmodifiableList(properties);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName).addValidator(StandardValidators.NON_BLANK_VALIDATOR).build();
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        if (descriptor.isRequired()) {
            return;
        }

        final Map<String, String> attributeToGeneratedNameMap = new HashMap<>(attributeToGenerateNameMapRef.get());
        if (newValue == null) {
            attributeToGeneratedNameMap.remove(descriptor.getName());
        } else {
            attributeToGeneratedNameMap.put(descriptor.getName(), newValue);
        }

        attributeToGenerateNameMapRef.set(Collections.unmodifiableMap(attributeToGeneratedNameMap));
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final Map<String, String> attributeToGeneratedNameMap = attributeToGenerateNameMapRef.get();
        final ComponentLog logger = getLogger();

        final SortedMap<String, String> attributes = getRelevantAttributes(flowFile, attributeToGeneratedNameMap);
        if (attributes.size() != attributeToGeneratedNameMap.size()) {
            final Set<String> wantedKeys = attributeToGeneratedNameMap.keySet();
            final Set<String> foundKeys = attributes.keySet();
            final StringBuilder missingKeys = new StringBuilder();
            for (final String wantedKey : wantedKeys) {
                if (!foundKeys.contains(wantedKey)) {
                    missingKeys.append(wantedKey).append(" ");
                }
            }
            logger.info("routing {} to 'failure' because of missing attributes: {}", new Object[]{flowFile, missingKeys.toString()});
            session.transfer(flowFile, REL_FAILURE);
        } else {
            // Generate a hash with the configured algorithm for each attribute value
            // and create a new attribute with the configured name
            for (final Map.Entry<String, String> entry : attributes.entrySet()) {
                String value = hashValue(context.getProperty(HASH_ALGORITHM).getValue(), entry.getValue());
                session.putAttribute(flowFile, attributeToGeneratedNameMap.get(entry.getKey()), value);
            }
            session.getProvenanceReporter().modifyAttributes(flowFile);
            session.transfer(flowFile, REL_SUCCESS);
        }
    }

    private SortedMap<String, String> getRelevantAttributes(final FlowFile flowFile, final Map<String, String> attributeToGeneratedNameMap) {
        final SortedMap<String, String> attributeMap = new TreeMap<>();
        for (final Map.Entry<String, String> entry : attributeToGeneratedNameMap.entrySet()) {
            final String attributeName = entry.getKey();
            final String attributeValue = flowFile.getAttribute(attributeName);
            if (attributeValue != null) {
                attributeMap.put(attributeName, attributeValue);
            }
        }
        return attributeMap;
    }

    private static String hashValue(String algorithm, String value) {
        if (StringUtils.isBlank(value)) {
            return value;
        }
        return Hex.encodeHexString(DigestUtils.getDigest(algorithm).digest(value.getBytes(UTF8)));
    }
}

