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
package org.apache.nifi.ui.extension.contentviewer;

import java.util.Collection;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * Utility class for resolving content types to their appropriate display names and content viewers.
 * Supports both exact MIME type matching and pattern-based matching for structured suffix types
 * like application/fhir+xml, application/vnd.api+json, etc.
 */
public final class ContentTypeResolver {

    // Patterns for matching structured syntax suffixes (RFC 6839)
    private static final Pattern XML_SUFFIX_PATTERN = Pattern.compile("application/[a-zA-Z0-9.-]+\\+xml");
    private static final Pattern JSON_SUFFIX_PATTERN = Pattern.compile("application/[a-zA-Z0-9.-]+\\+json");
    private static final Pattern YAML_SUFFIX_PATTERN = Pattern.compile("application/[a-zA-Z0-9.-]+\\+yaml");

    private ContentTypeResolver() {
        // Utility class
    }

    /**
     * Resolves a content type to its display name by checking against the supported MIME types
     * of the provided content viewers. First attempts exact matching, then falls back to
     * pattern-based matching for structured suffix types.
     *
     * @param contentType the content type to resolve (e.g., "application/fhir+xml")
     * @param contentViewers the collection of available content viewers
     * @return an Optional containing the matching SupportedMimeTypes, or empty if no match found
     */
    public static Optional<ResolvedContentType> resolve(final String contentType, final Collection<ContentViewer> contentViewers) {
        if (contentType == null || contentViewers == null) {
            return Optional.empty();
        }

        // First, try exact matching
        for (final ContentViewer viewer : contentViewers) {
            for (final SupportedMimeTypes supportedMimeTypes : viewer.getSupportedMimeTypes()) {
                for (final String mimeType : supportedMimeTypes.getMimeTypes()) {
                    if (contentType.equals(mimeType)) {
                        return Optional.of(new ResolvedContentType(viewer, supportedMimeTypes));
                    }
                }
            }
        }

        // Then, try startsWith matching (for types like "text/plain; charset=UTF-8")
        for (final ContentViewer viewer : contentViewers) {
            for (final SupportedMimeTypes supportedMimeTypes : viewer.getSupportedMimeTypes()) {
                for (final String mimeType : supportedMimeTypes.getMimeTypes()) {
                    if (contentType.startsWith(mimeType)) {
                        return Optional.of(new ResolvedContentType(viewer, supportedMimeTypes));
                    }
                }
            }
        }

        // Finally, try pattern-based matching for structured suffix types
        final String suffixDisplayName = resolveSuffixPattern(contentType);
        if (suffixDisplayName != null) {
            for (final ContentViewer viewer : contentViewers) {
                for (final SupportedMimeTypes supportedMimeTypes : viewer.getSupportedMimeTypes()) {
                    if (suffixDisplayName.equals(supportedMimeTypes.getDisplayName())) {
                        return Optional.of(new ResolvedContentType(viewer, supportedMimeTypes));
                    }
                }
            }
        }

        return Optional.empty();
    }

    /**
     * Resolves a content type to its display name without requiring the viewer collection.
     * First checks exact matches, then falls back to pattern-based matching for structured suffix types.
     *
     * @param contentType the content type to resolve
     * @return the display name (xml, json, yaml, etc.) if found, null otherwise
     */
    public static String resolveDisplayName(final String contentType) {
        if (contentType == null) {
            return null;
        }

        // First check exact matches
        final String exactMatch = switch (contentType) {
            case "application/json" -> "json";
            case "application/xml", "text/xml" -> "xml";
            case "application/avro-binary", "avro/binary", "application/avro+binary" -> "avro";
            case "text/x-yaml", "text/yaml", "text/yml", "application/x-yaml", "application/x-yml", "application/yaml",
                 "application/yml" -> "yaml";
            case "text/plain" -> "text";
            case "text/csv" -> "csv";
            default -> null;
        };

        if (exactMatch != null) {
            return exactMatch;
        }

        // Fall back to pattern-based matching for structured suffix types (RFC 6839)
        return resolveSuffixPattern(contentType);
    }

    /**
     * Resolves a content type to its display name using pattern-based matching
     * for structured suffix types (RFC 6839).
     *
     * @param contentType the content type to resolve
     * @return the display name (xml, json, yaml) if pattern matches, null otherwise
     */
    public static String resolveSuffixPattern(final String contentType) {
        if (contentType == null) {
            return null;
        }

        if (XML_SUFFIX_PATTERN.matcher(contentType).matches()) {
            return "xml";
        }
        if (JSON_SUFFIX_PATTERN.matcher(contentType).matches()) {
            return "json";
        }
        if (YAML_SUFFIX_PATTERN.matcher(contentType).matches()) {
            return "yaml";
        }

        return null;
    }

    /**
     * Represents a resolved content type with its associated viewer and supported MIME types.
     */
    public static class ResolvedContentType {
        private final ContentViewer contentViewer;
        private final SupportedMimeTypes supportedMimeTypes;

        public ResolvedContentType(final ContentViewer contentViewer, final SupportedMimeTypes supportedMimeTypes) {
            this.contentViewer = contentViewer;
            this.supportedMimeTypes = supportedMimeTypes;
        }

        public ContentViewer getContentViewer() {
            return contentViewer;
        }

        public SupportedMimeTypes getSupportedMimeTypes() {
            return supportedMimeTypes;
        }

        public String getDisplayName() {
            return supportedMimeTypes.getDisplayName();
        }
    }
}

