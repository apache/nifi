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

import org.apache.nifi.registry.bucket.Bucket;
import org.apache.nifi.registry.field.Fields;
import org.apache.nifi.registry.revision.entity.RevisionInfo;

import java.io.IOException;
import java.util.List;

/**
 * Client for interacting with buckets.
 */
public interface BucketClient {

    /**
     * Creates the given bucket.
     *
     * @param bucket the bucket to create
     * @return the created bucket with containing identifier that was generated
     */
    Bucket create(Bucket bucket) throws NiFiRegistryException, IOException;

    /**
     * Gets the bucket with the given id.
     *
     * @param bucketId the id of the bucket to retrieve
     * @return the bucket with the given id
     */
    Bucket get(String bucketId) throws NiFiRegistryException, IOException;

    /**
     * Updates the given bucket. Only the name and description can be updated.
     *
     * @param bucket the bucket with updates, must contain the id
     * @return the updated bucket
     */
    Bucket update(Bucket bucket) throws NiFiRegistryException, IOException;

    /**
     * Deletes the bucket with the given id.
     *
     * @param bucketId the id of the bucket to delete
     * @return the deleted bucket
     */
    Bucket delete(String bucketId) throws NiFiRegistryException, IOException;

    /**
     * Deletes the bucket with the given id and revision
     *
     * @param bucketId the id of the bucket to delete
     * @param revision the revision info for the bucket being deleted
     * @return the deleted bucket
     */
    Bucket delete(String bucketId, RevisionInfo revision) throws NiFiRegistryException, IOException;

    /**
     * Gets the fields that can be used to sort/search buckets.
     *
     * @return the bucket fields
     */
    Fields getFields() throws NiFiRegistryException, IOException;

    /**
     * Gets all buckets.
     *
     * @return the list of all buckets
     */
    List<Bucket> getAll() throws NiFiRegistryException, IOException;

}
