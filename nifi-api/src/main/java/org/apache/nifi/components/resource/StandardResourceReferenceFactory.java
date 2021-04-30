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

package org.apache.nifi.components.resource;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class StandardResourceReferenceFactory implements ResourceReferenceFactory {

    public ResourceReferences createResourceReferences(final String value, final ResourceDefinition resourceDefinition) {
        if (value == null) {
            return new StandardResourceReferences(Collections.emptyList());
        }

        final String trimmed = value.trim();
        if (trimmed.isEmpty()) {
            return null;
        }

        if (resourceDefinition == null) {
            return null;
        }

        final List<ResourceReference> references;
        final List<String> locations = parseResourceLocations(value);
        references = new ArrayList<>(locations.size());
        locations.forEach(location -> references.add(createResourceReference(location, resourceDefinition)));

        return new StandardResourceReferences(references);
    }

    public ResourceReference createResourceReference(final String value, final ResourceDefinition resourceDefinition) {
        if (value == null) {
            return null;
        }

        final String trimmed = value.trim();
        if (trimmed.isEmpty()) {
            return null;
        }

        if (resourceDefinition == null) {
            return null;
        }

        final Set<ResourceType> allowedResourceTypes = resourceDefinition.getResourceTypes();
        if (allowedResourceTypes.contains(ResourceType.URL)) {
            try {
                if (trimmed.startsWith("http://") || trimmed.startsWith("https://")) {
                    return new URLResourceReference(new URL(trimmed));
                }

                if (trimmed.startsWith("file:")) {
                    final URL url = new URL(trimmed);
                    final String filename = url.getFile();
                    final File file = new File(filename);
                    return new FileResourceReference(file);
                }
            } catch (MalformedURLException e) {
                throw new IllegalArgumentException("Invalid URL: " + trimmed);
            }
        }

        final boolean fileAllowed = allowedResourceTypes.contains(ResourceType.FILE) || allowedResourceTypes.contains(ResourceType.DIRECTORY);
        final boolean textAllowed = allowedResourceTypes.contains(ResourceType.TEXT);

        if (fileAllowed && textAllowed) {
            // We have to make a determination whether this is a file or text. Eventually, it will be best if the user tells us explicitly.
            // For now, we will make a determination based on a couple of simple rules.
            final File file = new File(trimmed);
            if (file.isAbsolute() || file.exists()) {
                return new FileResourceReference(file);
            }

            if (trimmed.startsWith("./") || trimmed.startsWith(".\\")) {
                return new FileResourceReference(file);
            }

            return new Utf8TextResource(value); // Use explicit value, not trimmed value, as the white space may be important for textual content.
        }

        if (fileAllowed) {
            final File file = new File(trimmed);
            return new FileResourceReference(file);
        }

        if (textAllowed) {
            return new Utf8TextResource(value);
        }

        return null;
    }

    private List<String> parseResourceLocations(final String rawValue) {
        final List<String> resourceLocations = new ArrayList<>();
        final String[] splits = rawValue.split(",");
        for (final String split : splits) {
            final String trimmed = split.trim();
            if (trimmed.isEmpty()) {
                continue;
            }

            resourceLocations.add(trimmed);
        }

        return resourceLocations;
    }
}
