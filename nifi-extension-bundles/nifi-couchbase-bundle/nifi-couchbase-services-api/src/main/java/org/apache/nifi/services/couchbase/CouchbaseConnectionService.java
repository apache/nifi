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
package org.apache.nifi.services.couchbase;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.services.couchbase.exception.CouchbaseErrorHandler;
import org.apache.nifi.services.couchbase.exception.CouchbaseException;
import org.apache.nifi.services.couchbase.utils.CouchbaseGetResult;
import org.apache.nifi.services.couchbase.utils.CouchbasePutResult;

public interface CouchbaseConnectionService extends ControllerService {

    PropertyDescriptor COUCHBASE_CLUSTER_SERVICE = new PropertyDescriptor.Builder()
            .name("Couchbase Cluster Service")
            .description("A Couchbase Cluster Service which manages connections to a Couchbase cluster.")
            .required(true)
            .identifiesControllerService(CouchbaseClusterService.class)
            .build();

    CouchbaseGetResult getDocument(String documentId) throws CouchbaseException;

    CouchbasePutResult putDocument(String documentId, byte[] content) throws CouchbaseException;

    CouchbaseErrorHandler getErrorHandler();

    String createTransitUrl(String documentId);
}
