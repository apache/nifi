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

import org.apache.nifi.registry.extension.bundle.Bundle;
import org.apache.nifi.registry.extension.bundle.BundleFilterParams;

import java.io.IOException;
import java.util.List;

/**
 * Client for interacting with extension bundles.
 */
public interface BundleClient {

    /**
     * Retrieves all extension bundles located in buckets the current user is authorized for.
     *
     * @return the list of extension bundles
     *
     * @throws IOException if an I/O error occurs
     * @throws NiFiRegistryException if an non I/O error occurs
     */
    List<Bundle> getAll() throws IOException, NiFiRegistryException;

    /**
     * Retrieves all extension bundles matching the specified filters, located in buckets the current user is authorized for.
     *
     * @param filterParams the filter params
     * @return the list of extension bundles
     *
     * @throws IOException if an I/O error occurs
     * @throws NiFiRegistryException if an non I/O error occurs
     */
    List<Bundle> getAll(BundleFilterParams filterParams) throws IOException, NiFiRegistryException;

    /**
     * Retrieves the extension bundles located in the given bucket.
     *
     * @param bucketId the bucket id
     * @return the list of bundles in the bucket
     *
     * @throws IOException if an I/O error occurs
     * @throws NiFiRegistryException if an non I/O error occurs
     */
    List<Bundle> getByBucket(String bucketId) throws IOException, NiFiRegistryException;

    /**
     * Retrieves the extension bundle with the given id.
     *
     * @param bundleId the id of the bundle
     * @return the bundle with the given id
     *
     * @throws IOException if an I/O error occurs
     * @throws NiFiRegistryException if an non I/O error occurs
     */
    Bundle get(String bundleId) throws IOException, NiFiRegistryException;

    /**
     * Deletes the extension bundle with the given id, and all of its versions.
     *
     * @param bundleId the bundle id
     * @return the deleted bundle
     *
     * @throws IOException if an I/O error occurs
     * @throws NiFiRegistryException if an non I/O error occurs
     */
    Bundle delete(String bundleId) throws IOException, NiFiRegistryException;

}
