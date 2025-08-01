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
package org.apache.nifi.couchbase;

import com.couchbase.client.java.Collection;
import org.apache.nifi.controller.ControllerService;

import com.couchbase.client.java.Bucket;

/**
 * Provides a connection to a Couchbase Server cluster throughout a NiFi Data
 * flow.
 */
public interface CouchbaseClusterControllerService extends ControllerService {

    /**
     * Open a bucket connection.
     * @param bucketName the bucket name to access
     * @return a connected bucket instance
     */
    Bucket openBucket(String bucketName);
    Collection openCollection(String bucketName, String scopeName, String collectionName);

    String connectionString();
}
