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
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

/**
 * A reference to a Resource that is identified by a property value
 */
public interface ResourceReference {

    /**
     * @return a file representation of the resource, or <code>null</code> if the Resource cannot be represented as a File
     */
    File asFile();

    /**
     * @return a URL representation of the resource, or <code>null</code> if the Resource cannot be represented as a URL
     */
    URL asURL();

    /**
     * @return an InputStream to read the contents of the resource
     *
     * @throws IOException if unable to obtain an InputStream from the resource
     */
    InputStream read() throws IOException;

    /**
     * Indicates whether or not the resource is accessible. What it means for the resource to be accessible depends on the type of
     * resource. A File resource, for example, might be accessible only if the file exists and is readable, while a URL resource might
     * always be considered accessible, or might be accesssible only if the existence of the resource can be confirmed.
     *
     * @return <code>true</code> if the file can be accessed, <code>false</code> otherwise
     */
    boolean isAccessible();

    /**
     * @return a String representation of the location, or <code>null</code> for a Resource that does not have an external location.
     * For a File or a Directory, this will be the full path name; for a URL it will be the String form of the URL
     */
    String getLocation();

    /**
     * @return the type of resource that is being referenced
     */
    ResourceType getResourceType();
}
