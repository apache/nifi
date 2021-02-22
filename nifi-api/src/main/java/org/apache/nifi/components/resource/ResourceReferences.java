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

import java.net.URL;
import java.util.List;

/**
 * A representation of zero or more {@link ResourceReference}s
 */
public interface ResourceReferences {

    /**
     * @return a List representation of all Resource References
     */
    List<ResourceReference> asList();

    /**
     * @return a list of all Resource References' locations
     */
    List<String> asLocations();

    /**
     * @return a list of all Resource References' URLs
     */
    List<URL> asURLs();

    /**
     * @return the number of Resource References held
     */
    int getCount();

    /**
     * Iterates through the Resource References and for any reference that may represent more than one
     * resource, flattens the resource into a List of single-entity references. For example, consider that this ResourceReferences
     * holds a single ResourceReference, of type DIRECTORY and the referenced directory contains 10 files. Calling {@link #asList()} would
     * return a single ResourceReference. But calling <code>flatten()</code> would return a new ResourceReferences type whose {@link #asList()}
     * method would return 10 ResourceReference objects, each with a ResourceType of FILE. The flatten operation is not recursive, meaning that if
     * a DIRECTORY is flattened, any sub-directories will be dropped. If the contents of the subdirectories are to be retained, use {@link #flattenRecursively()}
     * instead.
     *
     * @return a flattened ResourceReferences
     */
    ResourceReferences flatten();

    /**
     * Recursively iterates through the Resource References and for any reference that may represent more than one
     * resource, flattens the resource into a List of single-entity references. For example, consider that this ResourceReferences
     * holds a single ResourceReference, of type DIRECTORY and the referenced directory contains 10 files. Calling {@link #asList()} would
     * return a single ResourceReference. But calling <code>flatten()</code> would return a new ResourceReferences type whose {@link #asList()}
     * method would return 10 ResourceReference objects, each with a ResourceType of FILE. The flatten operation is recursive, meaning that if
     * a DIRECTORY is encountered, its reference will be replaced with a new reference for each file, even if that file exists 100 levels deep
     * in the directory structure.
     *
     * @return a flattened ResourceReferences
     */
    ResourceReferences flattenRecursively();
}
