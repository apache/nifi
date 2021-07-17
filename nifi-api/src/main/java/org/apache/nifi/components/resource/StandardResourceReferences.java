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
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class StandardResourceReferences implements ResourceReferences {
    public List<ResourceReference> resourceReferences;

    public StandardResourceReferences(final List<ResourceReference> resourceReferences) {
        this.resourceReferences = Objects.requireNonNull(resourceReferences);
    }

    @Override
    public List<ResourceReference> asList() {
        return Collections.unmodifiableList(resourceReferences);
    }

    @Override
    public List<String> asLocations() {
        final List<String> locations = new ArrayList<>(resourceReferences.size());
        resourceReferences.forEach(ref -> locations.add(ref.getLocation()));
        return locations;
    }

    @Override
    public List<URL> asURLs() {
        final List<URL> locations = new ArrayList<>(resourceReferences.size());
        resourceReferences.forEach(ref -> locations.add(ref.asURL()));
        return locations;
    }

    @Override
    public int getCount() {
        return resourceReferences.size();
    }

    @Override
    public ResourceReferences flatten() {
        if (resourceReferences.isEmpty()) {
            return this;
        }

        final List<ResourceReference> flattened = new ArrayList<>();
        resourceReferences.forEach(reference -> {
            if (reference.getResourceType() == ResourceType.DIRECTORY) {
                addChildren(reference.asFile(), flattened);
            } else {
                flattened.add(reference);
            }
        });

        return new StandardResourceReferences(flattened);
    }

    private void addChildren(final File file, final List<ResourceReference> flattened) {
        if (file == null) {
            return;
        }

        if (file.isDirectory()) {
            final File[] children = file.listFiles();
            if (children != null) {
                for (final File child : children) {
                    if (child.isFile()) {
                        flattened.add(new FileResourceReference(child));
                    }
                }
            }
        } else {
            flattened.add(new FileResourceReference(file));
        }
    }


    @Override
    public ResourceReferences flattenRecursively() {
        if (resourceReferences.isEmpty()) {
            return this;
        }

        final List<ResourceReference> flattened = new ArrayList<>();
        resourceReferences.forEach(reference -> {
            if (reference.getResourceType() == ResourceType.DIRECTORY) {
                recurse(reference.asFile(), flattened);
            } else {
                flattened.add(reference);
            }
        });

        return new StandardResourceReferences(flattened);
    }

    private void recurse(final File file, final List<ResourceReference> flattened) {
        if (file == null) {
            return;
        }

        if (file.isDirectory()) {
            final File[] children = file.listFiles();
            if (children != null) {
                for (final File child : children) {
                    recurse(child, flattened);
                }
            }
        } else {
            flattened.add(new FileResourceReference(file));
        }
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final StandardResourceReferences that = (StandardResourceReferences) o;
        return Objects.equals(resourceReferences, that.resourceReferences);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resourceReferences);
    }

    @Override
    public String toString() {
        return "StandardResourceReferences[resources=" + resourceReferences + "]";
    }
}
