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
package org.apache.nifi.registry.extension;

import org.apache.nifi.authorization.user.NiFiUser;

import java.io.IOException;
import java.io.InputStream;
import java.util.Set;

/**
 * Represents an extension registry that can be used to list/retrieve available bundles.
 *
 * @param <T> the type of bundle metadata returned from listing bundles
 */
public interface ExtensionRegistry<T extends ExtensionBundleMetadata> {

    /**
     * @return the identifier of the registry
     */
    String getIdentifier();

    /**
     * @return the description of the registry
     */
    String getDescription();

    /**
     * @param description the description of the registry
     */
    void setDescription(String description);

    /**
     * @return the url of the registry
     */
    String getURL();

    /**
     * @param url the url of the registry
     */
    void setURL(String url);

    /**
     * @return the name of the registry
     */
    String getName();

    /**
     * @param name the name of the registry
     */
    void setName(String name);

    /**
     * Retrieves a listing of all available bundles in the given registry.
     *
     * @param user an optional end user making the request
     * @return the set of bundle metadata for available bundles
     * @throws IOException if an I/O error occurs
     * @throws ExtensionRegistryException if a non I/O error occurs
     */
    Set<T> getExtensionBundleMetadata(NiFiUser user)
            throws IOException, ExtensionRegistryException;

    /**
     * Retrieves the content of a bundle specified by the given bundle metadata.
     *
     * @param user an optional end user making the request
     * @param bundleMetadata a bundle metadata specifying the bundle to retrieve
     * @return an InputStream to the content of the specified bundle
     * @throws IOException if an I/O error occurs
     * @throws ExtensionRegistryException if a non I/O error occurs
     */
    InputStream getExtensionBundleContent(NiFiUser user, T bundleMetadata)
            throws IOException, ExtensionRegistryException;

}


