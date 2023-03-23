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
package org.apache.nifi.authorization.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.authorization.util.IdentityMapping.Transform;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.nifi.util.NiFiProperties.SECURITY_GROUP_MAPPING_PATTERN_PREFIX;
import static org.apache.nifi.util.NiFiProperties.SECURITY_GROUP_MAPPING_TRANSFORM_PREFIX;
import static org.apache.nifi.util.NiFiProperties.SECURITY_GROUP_MAPPING_VALUE_PREFIX;
import static org.apache.nifi.util.NiFiProperties.SECURITY_IDENTITY_MAPPING_PATTERN_PREFIX;
import static org.apache.nifi.util.NiFiProperties.SECURITY_IDENTITY_MAPPING_TRANSFORM_PREFIX;
import static org.apache.nifi.util.NiFiProperties.SECURITY_IDENTITY_MAPPING_VALUE_PREFIX;

public class IdentityMappingUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(IdentityMappingUtil.class);
    private static final Pattern backReferencePattern = Pattern.compile("\\$(\\d+)");

    /**
     * Builds the identity mappings from NiFiProperties.
     *
     * @param properties the NiFiProperties instance
     * @return a list of identity mappings
     */
    public static List<IdentityMapping> getIdentityMappings(final NiFiProperties properties) {
        return getMappings(
                properties,
                SECURITY_IDENTITY_MAPPING_PATTERN_PREFIX,
                SECURITY_IDENTITY_MAPPING_VALUE_PREFIX,
                SECURITY_IDENTITY_MAPPING_TRANSFORM_PREFIX,
                () -> "Identity");
    }

    /**
     * Buils the group mappings from NiFiProperties.
     *
     * @param properties the NiFiProperties instance
     * @return a list of group mappings
     */
    public static List<IdentityMapping> getGroupMappings(final NiFiProperties properties) {
        return getMappings(
                properties,
                SECURITY_GROUP_MAPPING_PATTERN_PREFIX,
                SECURITY_GROUP_MAPPING_VALUE_PREFIX,
                SECURITY_GROUP_MAPPING_TRANSFORM_PREFIX,
                () -> "Group");
    }

    private static List<IdentityMapping> getMappings(final NiFiProperties properties, final String patternPrefix,
            final String valuePrefix, final String transformPrefix, final Supplier<String> getSubject) {

        final List<IdentityMapping> mappings = new ArrayList<>();

        // go through each property
        for (String propertyName : properties.getPropertyKeys()) {
            if (StringUtils.startsWith(propertyName, patternPrefix)) {
                final String key = StringUtils.substringAfter(propertyName, patternPrefix);
                final String identityPattern = properties.getProperty(propertyName);

                if (StringUtils.isBlank(identityPattern)) {
                    LOGGER.warn("{} Mapping property {} was found, but was empty", new Object[] {getSubject.get(), propertyName});
                    continue;
                }

                final String identityValueProperty = valuePrefix + key;
                final String identityValue = properties.getProperty(identityValueProperty);

                if (StringUtils.isBlank(identityValue)) {
                    LOGGER.warn("{} Mapping property {} was found, but corresponding value {} was not found",
                            new Object[] {getSubject.get(), propertyName, identityValueProperty});
                    continue;
                }

                final String identityTransformProperty = transformPrefix + key;
                String rawIdentityTransform = properties.getProperty(identityTransformProperty);

                if (StringUtils.isBlank(rawIdentityTransform)) {
                    LOGGER.debug("{} Mapping property {} was found, but no transform was present. Using NONE.", new Object[] {getSubject.get(), propertyName});
                    rawIdentityTransform = Transform.NONE.name();
                }

                final Transform identityTransform;
                try {
                    identityTransform = Transform.valueOf(rawIdentityTransform);
                } catch (final IllegalArgumentException iae) {
                    LOGGER.warn("{} Mapping property {} was found, but corresponding transform {} was not valid. Allowed values {}",
                            new Object[] {getSubject.get(), propertyName, rawIdentityTransform, StringUtils.join(Transform.values(), ", ")});
                    continue;
                }

                final IdentityMapping identityMapping = new IdentityMapping(key, Pattern.compile(identityPattern), identityValue, identityTransform);
                mappings.add(identityMapping);

                LOGGER.debug("Found {} Mapping with key = {}, pattern = {}, value = {}, transform = {}",
                        new Object[] {getSubject.get(), key, identityPattern, identityValue, rawIdentityTransform});
            }
        }

        // sort the list by the key so users can control the ordering in nifi.properties
        Collections.sort(mappings, new Comparator<IdentityMapping>() {
            @Override
            public int compare(IdentityMapping m1, IdentityMapping m2) {
                return m1.getKey().compareTo(m2.getKey());
            }
        });

        return mappings;
    }

    /**
     * Checks the given identity against each provided mapping and performs the mapping using the first one that matches.
     * If none match then the identity is returned as is.
     *
     * @param identity the identity to map
     * @param mappings the mappings
     * @return the mapped identity, or the same identity if no mappings matched
     */
    public static String mapIdentity(final String identity, List<IdentityMapping> mappings) {
        for (IdentityMapping mapping : mappings) {
            Matcher m = mapping.getPattern().matcher(identity);
            if (m.matches()) {
                final String pattern = mapping.getPattern().pattern();
                final String replacementValue = escapeLiteralBackReferences(mapping.getReplacementValue(), m.groupCount());
                final String replacement = identity.replaceAll(pattern, replacementValue);

                if (Transform.UPPER.equals(mapping.getTransform())) {
                    return replacement.toUpperCase();
                } else if (Transform.LOWER.equals(mapping.getTransform())) {
                    return replacement.toLowerCase();
                } else {
                    return replacement;
                }
            }
        }

        return identity;
    }

    // If we find a back reference that is not valid, then we will treat it as a literal string. For example, if we have 3 capturing
    // groups and the Replacement Value has the value is "I owe $8 to him", then we want to treat the $8 as a literal "$8", rather
    // than attempting to use it as a back reference.
    private static String escapeLiteralBackReferences(final String unescaped, final int numCapturingGroups) {
        if (numCapturingGroups == 0) {
            return unescaped;
        }

        String value = unescaped;
        final Matcher backRefMatcher = backReferencePattern.matcher(value);
        while (backRefMatcher.find()) {
            final String backRefNum = backRefMatcher.group(1);
            if (backRefNum.startsWith("0")) {
                continue;
            }
            final int originalBackRefIndex = Integer.parseInt(backRefNum);
            int backRefIndex = originalBackRefIndex;

            // if we have a replacement value like $123, and we have less than 123 capturing groups, then
            // we want to truncate the 3 and use capturing group 12; if we have less than 12 capturing groups,
            // then we want to truncate the 2 and use capturing group 1; if we don't have a capturing group then
            // we want to truncate the 1 and get 0.
            while (backRefIndex > numCapturingGroups && backRefIndex >= 10) {
                backRefIndex /= 10;
            }

            if (backRefIndex > numCapturingGroups) {
                final StringBuilder sb = new StringBuilder(value.length() + 1);
                final int groupStart = backRefMatcher.start(1);

                sb.append(value.substring(0, groupStart - 1));
                sb.append("\\");
                sb.append(value.substring(groupStart - 1));
                value = sb.toString();
            }
        }

        return value;
    }

}
