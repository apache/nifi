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

import org.apache.nifi.registry.bucket.BucketItem;
import org.apache.nifi.registry.field.Fields;

import java.io.IOException;
import java.util.List;

/**
 * Client for interacting with bucket items.
 *
 * Bucket items contain the common fields across anything stored in the registry.
 *
 * Each item contains a type field and a link to the URI of the specific item.
 *
 * i.e. The link field of a flow item would contain the URI to the specific flow.
 */
public interface ItemsClient {

    /**
     * Gets all bucket items in the registry.
     *
     * @return the list of all bucket items
     * @throws NiFiRegistryException if an error is encountered other than IOException
     * @throws IOException if an I/O error is encountered
     */
    List<BucketItem> getAll() throws NiFiRegistryException, IOException;

    /**
     * Gets all bucket items for the given bucket.
     *
     * @param bucketId the bucket id
     * @return the list of items in the given bucket
     * @throws NiFiRegistryException if an error is encountered other than IOException
     * @throws IOException if an I/O error is encountered
     */
    List<BucketItem> getByBucket(String bucketId) throws NiFiRegistryException, IOException;

    /**
     * Gets the field info for bucket items.
     *
     * @return the list of field info
     * @throws NiFiRegistryException if an error is encountered other than IOException
     * @throws IOException if an I/O error is encountered
     */
    Fields getFields() throws NiFiRegistryException, IOException;

}
