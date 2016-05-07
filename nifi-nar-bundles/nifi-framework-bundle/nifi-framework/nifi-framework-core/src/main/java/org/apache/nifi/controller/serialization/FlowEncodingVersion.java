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

package org.apache.nifi.controller.serialization;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.w3c.dom.Element;

/**
 * Provides a mechanism for interpreting the version of the encoding scheme that was used to serialize
 * a NiFi Flow. The versioning scheme is made up of a major version and a minor version, both being
 * positive integers.
 */
public class FlowEncodingVersion {
    public static final String ENCODING_VERSION_ATTRIBUTE = "encoding-version";

    private static final Pattern VERSION_PATTERN = Pattern.compile("(\\d+)\\.(\\d)+");

    private final int majorVersion;
    private final int minorVersion;

    public FlowEncodingVersion(final int majorVersion, final int minorVersion) {
        if (majorVersion < 0) {
            throw new IllegalArgumentException("Invalid version: Major version cannot be less than 0 but was " + majorVersion);
        }
        if (minorVersion < 0) {
            throw new IllegalArgumentException("Invalid version: Minor version cannot be less than 0 but was " + minorVersion);
        }

        this.majorVersion = majorVersion;
        this.minorVersion = minorVersion;
    }

    /**
     * Parses the 'encoding-version' attribute of the given XML Element as FlowEncodingVersion.
     * The attribute value is expected to be in the format &lt;major version&gt;.&lt;minor version&lt;
     *
     * @param xmlElement the XML Element that contains an 'encoding-version' attribute
     * @return a FlowEncodingVersion that has the major and minor versions specified in the String, or <code>null</code> if the input is null or the input
     *         does not have an 'encoding-version' attribute
     *
     * @throws IllegalArgumentException if the value is not in the format &lt;major version&gt;.&lt;minor version&gt;, if either major version or minor
     *             version is not an integer, or if either the major or minor version is less than 0.
     */
    public static FlowEncodingVersion parse(final Element xmlElement) {
        if (xmlElement == null) {
            return null;
        }

        final String version = xmlElement.getAttribute(ENCODING_VERSION_ATTRIBUTE);
        if (version == null) {
            return null;
        }

        return parse(version);
    }

    /**
     * Parses the given String as FlowEncodingVersion. The String is expected to be in the format &lt;major version&gt;.&lt;minor version&lt;
     *
     * @param version the String representation of the encoding version
     * @return a FlowEncodingVersion that has the major and minor versions specified in the String, or <code>null</code> if the input is null
     *
     * @throws IllegalArgumentException if the value is not in the format &lt;major version&gt;.&lt;minor version&gt;, if either major version or minor
     *             version is not an integer, or if either the major or minor version is less than 0.
     */
    public static FlowEncodingVersion parse(final String version) {
        if (version == null || version.trim().isEmpty()) {
            return null;
        }

        final Matcher matcher = VERSION_PATTERN.matcher(version.trim());
        if (!matcher.matches()) {
            throw new IllegalArgumentException(version + " is not a valid version for Flow serialization. Should be in format <number>.<number>");
        }

        final int majorVersion = Integer.parseInt(matcher.group(1));
        final int minorVersion = Integer.parseInt(matcher.group(2));
        return new FlowEncodingVersion(majorVersion, minorVersion);
    }

    public int getMajorVersion() {
        return majorVersion;
    }

    public int getMinorVersion() {
        return minorVersion;
    }
}
