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
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.security.util.crypto.HashAlgorithm;
import org.apache.nifi.security.util.crypto.HashService;

@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"attributes", "hash", "md5", "sha", "keccak", "blake2", "cryptography"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Calculates a hash value for each of the specified attributes using the given algorithm and writes it to an output attribute. Please refer to https://csrc.nist.gov/Projects/Hash-Functions/NIST-Policy-on-Hash-Functions for help to decide which algorithm to use. ")
@WritesAttribute(attribute = "<Specified Attribute Name per Dynamic Property>", description = "This Processor adds an attribute whose value is the result of "
        + "hashing the specified attribute. The name of this attribute is specified by the value of the dynamic property.")
@DynamicProperty(name = "A flowfile attribute key for attribute inspection", value = "Attribute Name",
        description = "The property name defines the attribute to look for and hash in the incoming flowfile. "
                + "The property value defines the name to give the generated attribute. "
                + "Attribute names must be unique.")
public class CryptographicHashAttribute extends AbstractProcessor {
    public enum PartialAttributePolicy {
        ALLOW,
        PROHIBIT
    }

    private static final AllowableValue ALLOW_PARTIAL_ATTRIBUTES_VALUE = new AllowableValue(PartialAttributePolicy.ALLOW.name(),
            "Allow missing attributes",
            "Do not route to failure if there are attributes configured for hashing that are not present in the flowfile");

    private static final AllowableValue FAIL_PARTIAL_ATTRIBUTES_VALUE = new AllowableValue(PartialAttributePolicy.PROHIBIT.name(),
            "Fail if missing attributes",
            "Route to failure if there are attributes configured for hashing that are not present in the flowfile");

    static final PropertyDescriptor CHARACTER_SET = new PropertyDescriptor.Builder()
            .name("character_set")
            .displayName("Character Set")
            .description("The Character Set used to decode the attribute being hashed -- this applies to the incoming data encoding, not the resulting hash encoding. ")
            .required(true)
            .allowableValues(HashService.buildCharacterSetAllowableValues())
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .defaultValue("UTF-8")
            .build();

    static final PropertyDescriptor FAIL_WHEN_EMPTY = new PropertyDescriptor.Builder()
            .name("fail_when_empty")
            .displayName("Fail when no attributes present")
            .description("Route to failure when none of the attributes that are configured for hashing are found. " +
                    "If set to false, then flow files that do not contain any of the attributes that are configured for hashing will just pass through to success.")
            .allowableValues("true", "false")
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("true")
            .build();

    static final PropertyDescriptor HASH_ALGORITHM = new PropertyDescriptor.Builder()
            .name("hash_algorithm")
            .displayName("Hash Algorithm")
            .description("The cryptographic hash algorithm to use. Note that not all of the algorithms available are recommended for use (some are provided for legacy use). " +
                    "There are many things to consider when picking an algorithm; it is recommended to use the most secure algorithm possible.")
            .required(true)
            .allowableValues(HashService.buildHashAlgorithmAllowableValues())
            .defaultValue(HashAlgorithm.SHA256.getName())
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor PARTIAL_ATTR_ROUTE_POLICY = new PropertyDescriptor.Builder()
            .name("missing_attr_policy")
            .displayName("Missing attribute policy")
            .description("Policy for how the processor handles attributes that are configured for hashing but are not found in the flowfile.")
            .required(true)
            .allowableValues(ALLOW_PARTIAL_ATTRIBUTES_VALUE, FAIL_PARTIAL_ATTRIBUTES_VALUE)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .defaultValue(ALLOW_PARTIAL_ATTRIBUTES_VALUE.getValue())
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Used for flowfiles that have a hash value added")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Used for flowfiles that are missing required attributes")
            .build();
    private final static Set<Relationship> relationships;

    private final static List<PropertyDescriptor> properties;

    private final AtomicReference<Map<String, String>> attributeToGenerateNameMapRef = new AtomicReference<>(Collections.emptyMap());

    static {
        final Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_FAILURE);
        _relationships.add(REL_SUCCESS);
        relationships = Collections.unmodifiableSet(_relationships);

        final List<PropertyDescriptor> _properties = new ArrayList<>();
        _properties.add(CHARACTER_SET);
        _properties.add(FAIL_WHEN_EMPTY);
        _properties.add(HASH_ALGORITHM);
        _properties.add(PARTIAL_ATTR_ROUTE_POLICY);
        properties = Collections.unmodifiableList(_properties);
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
                .name(propertyDescriptorName)
                .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
                .build();
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
        final Charset charset = Charset.forName(context.getProperty(CHARACTER_SET).getValue());
        final Map<String, String> attributeToGeneratedNameMap = attributeToGenerateNameMapRef.get();
        final ComponentLog logger = getLogger();

        final SortedMap<String, String> relevantAttributes = getRelevantAttributes(flowFile, attributeToGeneratedNameMap);
        if (relevantAttributes.isEmpty()) {
            if (context.getProperty(FAIL_WHEN_EMPTY).asBoolean()) {
                logger.info("Routing {} to 'failure' because of missing all attributes: {}", new Object[]{flowFile, getMissingKeysString(null, attributeToGeneratedNameMap.keySet())});
                session.transfer(flowFile, REL_FAILURE);
                return;
            }
        }
        if (relevantAttributes.size() != attributeToGeneratedNameMap.size()) {
            if (PartialAttributePolicy.valueOf(context.getProperty(PARTIAL_ATTR_ROUTE_POLICY).getValue()) == PartialAttributePolicy.PROHIBIT) {
                logger.info("Routing {} to 'failure' because of missing attributes: {}", new Object[]{flowFile,
                        getMissingKeysString(relevantAttributes.keySet(), attributeToGeneratedNameMap.keySet())});
                session.transfer(flowFile, REL_FAILURE);
                return;
            }
        }

        // Determine the algorithm to use
        final String algorithmName = context.getProperty(HASH_ALGORITHM).getValue();
        logger.debug("Using algorithm {}", new Object[]{algorithmName});
        HashAlgorithm algorithm = HashAlgorithm.fromName(algorithmName);

        // Generate a hash with the configured algorithm for each attribute value
        // and create a new attribute with the configured name
        for (final Map.Entry<String, String> entry : relevantAttributes.entrySet()) {
            logger.debug("Generating {} hash of attribute '{}'", new Object[]{algorithmName, entry.getKey()});
            String value = hashValue(algorithm, entry.getValue(), charset);
            session.putAttribute(flowFile, attributeToGeneratedNameMap.get(entry.getKey()), value);
        }
        session.getProvenanceReporter().modifyAttributes(flowFile);
        session.transfer(flowFile, REL_SUCCESS);
    }

    private static SortedMap<String, String> getRelevantAttributes(final FlowFile flowFile, final Map<String, String> attributeToGeneratedNameMap) {
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

    private String hashValue(HashAlgorithm algorithm, String value, Charset charset) {
        if (value == null) {
            getLogger().warn("Tried to calculate {} hash of null value; returning empty string", new Object[]{algorithm.getName()});
            return "";
        }
        return HashService.hashValue(algorithm, value, charset);
    }

    private static String getMissingKeysString(Set<String> foundKeys, Set<String> wantedKeys) {
        final StringBuilder missingKeys = new StringBuilder();
        for (final String wantedKey : wantedKeys) {
            if (foundKeys == null || !foundKeys.contains(wantedKey)) {
                missingKeys.append(wantedKey).append(" ");
            }
        }
        return missingKeys.toString();
    }
}

