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
package org.apache.nifi.parameter;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility class for handling parameter context name patterns, particularly for
 * names with numeric suffixes like "Context (1)", "Context (2)", etc.
 *
 * <p>When importing versioned flows with the REPLACE parameter context strategy,
 * NiFi creates new parameter contexts with suffixed names if a context with the
 * same name already exists. This utility provides methods to parse and match
 * these naming patterns consistently across the codebase.</p>
 */
public final class ParameterContextNameUtils {

    private static final String PATTERN_GROUP_NAME = "name";
    private static final String PATTERN_GROUP_INDEX = "index";

    /**
     * Pattern that matches parameter context names, optionally with a numeric suffix.
     * Examples: "MyContext", "MyContext (1)", "MyContext (2)", "My Context (10)"
     */
    private static final String LINEAGE_FORMAT = "^(?<" + PATTERN_GROUP_NAME + ">.+?)( \\((?<" + PATTERN_GROUP_INDEX + ">[0-9]+)\\))?$";
    private static final Pattern LINEAGE_PATTERN = Pattern.compile(LINEAGE_FORMAT);

    /**
     * Format string for creating parameter context names with a numeric suffix.
     * Usage: String.format(NAME_FORMAT, baseName, index) produces "baseName (index)"
     */
    public static final String NAME_FORMAT = "%s (%d)";

    private ParameterContextNameUtils() {
        // Utility class - prevent instantiation
    }

    /**
     * Checks if the given context name matches a base name with a numeric suffix.
     * For example, "P (1)" or "P (2)" would match base name "P".
     *
     * @param contextName the actual parameter context name to check
     * @param baseName the base parameter context name (without suffix)
     * @return true if contextName equals baseName with a suffix like " (n)", false otherwise
     */
    public static boolean isNameWithSuffix(final String contextName, final String baseName) {
        if (contextName == null || baseName == null) {
            return false;
        }

        final Matcher matcher = LINEAGE_PATTERN.matcher(contextName);
        if (!matcher.matches()) {
            return false;
        }

        final String extractedBaseName = matcher.group(PATTERN_GROUP_NAME);
        final String indexGroup = matcher.group(PATTERN_GROUP_INDEX);

        // Must have a suffix (index) and the base name must match
        return indexGroup != null && baseName.equals(extractedBaseName);
    }

    /**
     * Extracts the base name from a parameter context name, removing any numeric suffix.
     * For example, "MyContext (1)" returns "MyContext", and "MyContext" returns "MyContext".
     *
     * @param name the parameter context name
     * @return the base name without the suffix, or the original name if no suffix exists
     * @throws IllegalArgumentException if the name cannot be parsed
     */
    public static String extractBaseName(final String name) {
        if (name == null) {
            throw new IllegalArgumentException("Parameter context name cannot be null");
        }

        final Matcher matcher = LINEAGE_PATTERN.matcher(name);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Parameter context name \"" + name + "\" cannot be parsed");
        }

        return matcher.group(PATTERN_GROUP_NAME);
    }

    /**
     * Extracts the suffix index from a parameter context name.
     * For example, "MyContext (3)" returns 3, and "MyContext" returns -1.
     *
     * @param name the parameter context name
     * @return the suffix index, or -1 if the name has no suffix
     * @throws IllegalArgumentException if the name cannot be parsed
     */
    public static int extractSuffixIndex(final String name) {
        if (name == null) {
            throw new IllegalArgumentException("Parameter context name cannot be null");
        }

        final Matcher matcher = LINEAGE_PATTERN.matcher(name);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Parameter context name \"" + name + "\" cannot be parsed");
        }

        final String indexGroup = matcher.group(PATTERN_GROUP_INDEX);
        return indexGroup == null ? -1 : Integer.parseInt(indexGroup);
    }

    /**
     * Creates a parameter context name with a numeric suffix.
     *
     * @param baseName the base name
     * @param index the suffix index
     * @return the formatted name, e.g., "baseName (index)"
     */
    public static String createNameWithSuffix(final String baseName, final int index) {
        return String.format(NAME_FORMAT, baseName, index);
    }

    /**
     * Returns the compiled pattern for matching parameter context names with optional suffixes.
     * This can be used for more complex matching scenarios.
     *
     * @return the compiled Pattern
     */
    public static Pattern getLineagePattern() {
        return LINEAGE_PATTERN;
    }
}
