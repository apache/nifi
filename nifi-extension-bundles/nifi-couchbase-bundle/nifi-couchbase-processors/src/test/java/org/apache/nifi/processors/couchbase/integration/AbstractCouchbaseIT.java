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
package org.apache.nifi.processors.couchbase.integration;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.services.couchbase.CouchbaseClient;
import org.apache.nifi.services.couchbase.StandardCouchbaseConnectionService;
import org.apache.nifi.services.couchbase.exception.CouchbaseException;
import org.apache.nifi.services.couchbase.utils.CouchbaseContext;
import org.apache.nifi.services.couchbase.utils.DocumentType;
import org.apache.nifi.util.TestRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.couchbase.BucketDefinition;
import org.testcontainers.couchbase.CouchbaseContainer;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.apache.nifi.processors.couchbase.utils.CouchbaseAttributes.DEFAULT_COLLECTION;
import static org.apache.nifi.processors.couchbase.utils.CouchbaseAttributes.DEFAULT_SCOPE;
import static org.apache.nifi.services.couchbase.StandardCouchbaseConnectionService.CONNECTION_STRING;
import static org.apache.nifi.services.couchbase.StandardCouchbaseConnectionService.PASSWORD;
import static org.apache.nifi.services.couchbase.StandardCouchbaseConnectionService.USERNAME;

public class AbstractCouchbaseIT {

    protected static final String TEST_BUCKET_NAME = "test_bucket";
    protected static final String COUCHBASE_IMAGE_COMMUNITY_RECENT = "couchbase/server:community-7.6.2";
    protected static final String SERVICE_ID = "couchbaseConnectionService";
    protected static final String TEST_DOCUMENT_ID = "test-document-id";
    private static final Duration CLIENT_READY_TIMEOUT = Duration.ofSeconds(60);
    private static final Duration CLIENT_READY_POLL_INTERVAL = Duration.ofMillis(100);

    protected static final String TEST_DATA = """
            {
                "last_name": "Doe",
                "first_name": "John",
                "age": "30"
            }""";

    protected static TestRunner runner;
    private StandardCouchbaseConnectionService connectionService;

    protected static CouchbaseContainer container = new CouchbaseContainer(COUCHBASE_IMAGE_COMMUNITY_RECENT).withBucket(new BucketDefinition(TEST_BUCKET_NAME));

    protected void initConnectionService() throws InitializationException {
        connectionService = new StandardCouchbaseConnectionService();
        runner.addControllerService(SERVICE_ID, connectionService);
        runner.setProperty(connectionService, CONNECTION_STRING, container.getConnectionString());
        runner.setProperty(connectionService, USERNAME, container.getUsername());
        runner.setProperty(connectionService, PASSWORD, container.getPassword());
        runner.setValidateExpressionUsage(false);
        runner.enableControllerService(connectionService);
        waitForClientReady();
    }

    @BeforeAll
    public static void start() {
        container.start();
    }

    @AfterEach
    public void disableConnectionService() {
        if (connectionService != null && runner.isControllerServiceEnabled(connectionService)) {
            runner.disableControllerService(connectionService);
        }
    }

    @AfterAll
    public static void stop() {
        container.stop();
    }

    private void waitForClientReady() {
        final CouchbaseContext context = new CouchbaseContext(TEST_BUCKET_NAME, DEFAULT_SCOPE, DEFAULT_COLLECTION, DocumentType.JSON);
        final CouchbaseClient client = connectionService.getClient(context);
        final long deadline = System.nanoTime() + CLIENT_READY_TIMEOUT.toNanos();
        CouchbaseException lastException;

        do {
            try {
                client.documentExists(TEST_DOCUMENT_ID);
                return;
            } catch (final CouchbaseException e) {
                lastException = e;
            }

            try {
                TimeUnit.NANOSECONDS.sleep(CLIENT_READY_POLL_INTERVAL.toNanos());
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("Interrupted while waiting for Couchbase client readiness", e);
            }
        } while (System.nanoTime() < deadline);

        throw new AssertionError("Couchbase client was not ready within %s".formatted(CLIENT_READY_TIMEOUT), lastException);
    }
}
