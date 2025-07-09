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

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.Scope;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;

import static org.apache.nifi.couchbase.CouchbaseConfigurationProperties.COUCHBASE_CLUSTER_SERVICE;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class AbstractCouchbaseNifiTest {
    protected static final String SERVICE_ID = "couchbaseClusterService";
    protected final String bucketName = "bucket-1";
    protected final String scopeName = "scope-1";
    protected final String collectionName = "collection-1";
    protected TestRunner testRunner;

    @BeforeEach
    public void init() throws Exception {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.nifi.processors.couchbase.GetCouchbaseKey", "debug");
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.nifi.processors.couchbase.TestGetCouchbaseKey", "debug");

        testRunner = TestRunners.newTestRunner(processor());
    }

    protected abstract Class<? extends Processor> processor();

    protected Collection setupMockCouchbase(String bucket, String scope, String collection) throws InitializationException {
        Bucket bucketMock = mock(Bucket.class);
        Scope scopeMock = mock(Scope.class);
        Collection collectionMock = mock(Collection.class);
        CouchbaseClusterControllerService service = mock(CouchbaseClusterControllerService.class);
        when(bucketMock.name()).thenReturn(bucket);
        when(service.getIdentifier()).thenReturn(SERVICE_ID);
        when(service.openBucket(bucket)).thenReturn(bucketMock);
        when(service.openCollection(bucket, scope, collection)).thenReturn(collectionMock);
        when(bucketMock.scope(anyString())).thenReturn(scopeMock);
        when(scopeMock.name()).thenReturn(scope);
        when(scopeMock.collection(anyString())).thenReturn(collectionMock);
        when(collectionMock.name()).thenReturn(collection);
        when(collectionMock.bucketName()).thenReturn(bucket);
        when(collectionMock.scopeName()).thenReturn(scope);
        testRunner.addControllerService(SERVICE_ID, service);
        testRunner.enableControllerService(service);
        testRunner.setProperty(COUCHBASE_CLUSTER_SERVICE, SERVICE_ID);
        return collectionMock;
    }
}
