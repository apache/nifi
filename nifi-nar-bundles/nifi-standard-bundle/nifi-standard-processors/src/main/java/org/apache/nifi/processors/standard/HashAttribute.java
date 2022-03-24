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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

/**
 * This processor <strong>does not calculate a cryptographic hash of one or more attributes</strong>.
 * For that behavior, see {@link CryptographicHashAttribute}.
 *
 * <p>
 * This processor identifies groups of user-specified flowfile attributes and assigns a unique hash value to each group, recording this hash value in the flowfile's attributes using a user-specified
 * attribute key. The groups are identified dynamically and preserved across application restarts. </p>
 *
 * <p>
 * The user must supply optional processor properties during runtime to correctly configure this processor. The optional property key will be used as the flowfile attribute key for attribute
 * inspection. The value must be a valid regular expression. This regular expression is evaluated against the flowfile attribute values. If the regular expression contains a capturing group, the value
 * of that group will be used when comparing flow file attributes. Otherwise, the original flow file attribute's value will be used if and only if the value matches the given regular expression. </p>
 *
 * <p>
 * If a flowfile does not have an attribute entry for one or more processor configured values, then the flowfile is routed to failure. </p>
 *
 * <p>
 * An example hash value identification:
 *
 * Assume Processor Configured with Two Properties ("MDKey1" = ".*" and "MDKey2" = "(.).*").
 *
 * FlowFile 1 has the following attributes: MDKey1 = a MDKey2 = b
 *
 * and will be assigned to group 1 (since no groups exist yet)
 *
 * FlowFile 2 has the following attributes: MDKey1 = 1 MDKey2 = 2
 *
 * and will be assigned to group 2 (attribute keys do not match existing groups)
 *
 * FlowFile 3 has the following attributes: MDKey1 = a MDKey2 = z
 *
 * and will be assigned to group 3 (attribute keys do not match existing groups)
 *
 * FlowFile 4 has the following attribute: MDKey1 = a MDKey2 = bad
 *
 * and will be assigned to group 1 (because the value of MDKey1 has the regular expression ".*" applied to it, and that evaluates to the same as MDKey1 attribute of the first flow file. Similarly, the
 * capturing group for the MDKey2 property indicates that only the first character of the MDKey2 attribute must match, and the first character of MDKey2 for Flow File 1 and Flow File 4 are both 'b'.)
 *
 * FlowFile 5 has the following attributes: MDKey1 = a
 *
 * and will route to failure because it does not have MDKey2 entry in its attribute
 * </p>
 *
 * <p>
 * The following flowfile attributes are created or modified: <ul>
 * <li><b>&lt;group.id.attribute.key&gt;</b> - The hash value.</li> </ul> </p>
 */
@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"attributes", "hash"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Hashes together the key/value pairs of several flowfile attributes and adds the hash as a new attribute. "
        + "Optional properties are to be added such that the name of the property is the name of a flowfile attribute to consider "
        + "and the value of the property is a regular expression that, if matched by the attribute value, will cause that attribute "
        + "to be used as part of the hash. If the regular expression contains a capturing group, only the value of the capturing "
        + "group will be used. " + "For a processor which accepts various attributes and generates a cryptographic hash of each, see \"CryptographicHashAttribute\". ")
@WritesAttribute(attribute = "<Hash Value Attribute Key>", description = "This Processor adds an attribute whose value is the result of "
        + "Hashing the existing flowfile attributes. The name of this attribute is specified by the <Hash Value Attribute Key> property.")
@DynamicProperty(name = "A flowfile attribute key for attribute inspection", value = "A Regular Expression",
        description = "This regular expression is evaluated against the "
        + "flowfile attribute values. If the regular expression contains a capturing "
        + "group, the value of that group will be used when comparing flow file "
        + "attributes. Otherwise, the original flow file attribute's value will be used "
        + "if and only if the value matches the given regular expression.")
public class HashAttribute extends AbstractProcessor {

    public static final PropertyDescriptor HASH_VALUE_ATTRIBUTE = new PropertyDescriptor.Builder()
            .name("Hash Value Attribute Key")
            .displayName("Hash Value Attribute Key")
            .description("The name of the flowfile attribute where the hash value should be stored")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Used for flowfiles that have a hash value added")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Used for flowfiles that are missing required attributes")
            .build();

    private Set<Relationship> relationships;
    private List<PropertyDescriptor> properties;
    private final AtomicReference<Map<String, Pattern>> regexMapRef = new AtomicReference<>(Collections.emptyMap());

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_FAILURE);
        relationships.add(REL_SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);

        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(HASH_VALUE_ATTRIBUTE);
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
                .name(propertyDescriptorName).addValidator(StandardValidators.createRegexValidator(0, 1, false)).required(false).dynamic(true).build();
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        if (descriptor.isRequired()) {
            return;
        }

        final Map<String, Pattern> patternMap = new HashMap<>(regexMapRef.get());
        if (newValue == null) {
            patternMap.remove(descriptor.getName());
        } else {
            if (newValue.equals(".*")) {
                patternMap.put(descriptor.getName(), null);
            } else {
                final Pattern pattern = Pattern.compile(newValue);
                patternMap.put(descriptor.getName(), pattern);
            }
        }

        regexMapRef.set(Collections.unmodifiableMap(patternMap));
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final Map<String, Pattern> patterns = regexMapRef.get();
        final ComponentLog logger = getLogger();

        final SortedMap<String, String> attributes = getRelevantAttributes(flowFile, patterns);
        if (attributes.size() != patterns.size()) {
            final Set<String> wantedKeys = patterns.keySet();
            final Set<String> foundKeys = attributes.keySet();
            final StringBuilder missingKeys = new StringBuilder();
            for (final String wantedKey : wantedKeys) {
                if (!foundKeys.contains(wantedKey)) {
                    missingKeys.append(wantedKey).append(" ");
                }
            }

            logger.error("routing {} to 'failure' because of missing attributes: {}", new Object[]{flowFile, missingKeys.toString()});
            session.transfer(flowFile, REL_FAILURE);
        } else {
            // create single string of attribute key/value pairs to use for group ID hash
            final StringBuilder hashableValue = new StringBuilder();
            for (final Map.Entry<String, String> entry : attributes.entrySet()) {
                hashableValue.append(entry.getKey());
                if (StringUtils.isBlank(entry.getValue())) {
                    hashableValue.append("EMPTY");
                } else {
                    hashableValue.append(entry.getValue());
                }
            }

            // create group ID
            final String hashValue = DigestUtils.md5Hex(hashableValue.toString());

            logger.info("adding Hash Value {} to attributes for {} and routing to success", new Object[]{hashValue, flowFile});
            flowFile = session.putAttribute(flowFile, context.getProperty(HASH_VALUE_ATTRIBUTE).getValue(), hashValue);
            session.getProvenanceReporter().modifyAttributes(flowFile);
            session.transfer(flowFile, REL_SUCCESS);
        }
    }

    private SortedMap<String, String> getRelevantAttributes(final FlowFile flowFile, final Map<String, Pattern> patterns) {
        final SortedMap<String, String> attributeMap = new TreeMap<>();
        for (final Map.Entry<String, Pattern> entry : patterns.entrySet()) {
            final String attributeName = entry.getKey();
            final String attributeValue = flowFile.getAttribute(attributeName);
            if (attributeValue != null) {
                final Pattern pattern = entry.getValue();
                if (pattern == null) {
                    attributeMap.put(attributeName, attributeValue);
                } else {
                    final Matcher matcher = pattern.matcher(attributeValue);
                    if (matcher.matches()) {
                        if (matcher.groupCount() == 0) {
                            attributeMap.put(attributeName, matcher.group(0));
                        } else {
                            attributeMap.put(attributeName, matcher.group(1));
                        }
                    }
                }
            }
        }

        return attributeMap;
    }
}
