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
package org.apache.nifi.registry.client;

import org.apache.nifi.registry.extension.component.ExtensionFilterParams;
import org.apache.nifi.registry.extension.component.ExtensionMetadataContainer;
import org.apache.nifi.registry.extension.component.TagCount;
import org.apache.nifi.registry.extension.component.manifest.ProvidedServiceAPI;

import java.io.IOException;
import java.util.List;

/**
 * Client for obtaining information about extensions.
 */
public interface ExtensionClient {

    /**
     * Retrieves extensions according to the given filter params.
     *
     * @param filterParams the filter params
     * @return the metadata for the extensions matching the filter params
     *
     * @throws IOException if an I/O error occurs
     * @throws NiFiRegistryException if an non I/O error occurs
     */
    ExtensionMetadataContainer findExtensions(ExtensionFilterParams filterParams) throws IOException, NiFiRegistryException;

    /**
     * Retrieves extensions that provide the given service API.
     *
     * @param providedServiceAPI the service API
     * @return the metadata for extensions that provided the service API
     *
     * @throws IOException if an I/O error occurs
     * @throws NiFiRegistryException if an non I/O error occurs
     */
    ExtensionMetadataContainer findExtensions(ProvidedServiceAPI providedServiceAPI) throws IOException, NiFiRegistryException;

    /**
     * @return all of the tags known the registry with their corresponding counts
     *
     * @throws IOException if an I/O error occurs
     * @throws NiFiRegistryException if an non I/O error occurs
     */
    List<TagCount> getTagCounts() throws IOException, NiFiRegistryException;

}
