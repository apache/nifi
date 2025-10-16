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
import org.apache.nifi.services.couchbase.StandardCouchbaseConnectionService;
import org.apache.nifi.util.TestRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.couchbase.BucketDefinition;
import org.testcontainers.couchbase.CouchbaseContainer;

import static org.apache.nifi.services.couchbase.StandardCouchbaseConnectionService.CONNECTION_STRING;
import static org.apache.nifi.services.couchbase.StandardCouchbaseConnectionService.PASSWORD;
import static org.apache.nifi.services.couchbase.StandardCouchbaseConnectionService.USERNAME;

public class AbstractCouchbaseIT {

    protected static final String TEST_BUCKET_NAME = "test_bucket";
    protected static final String COUCHBASE_IMAGE_COMMUNITY_RECENT = "couchbase/server:community-7.6.2";
    protected static final String SERVICE_ID = "couchbaseConnectionService";
    protected static final String TEST_DOCUMENT_ID = "test-document-id";

    protected static final String TEST_DATA = """
            {
                "last_name": "Doe",
                "first_name": "John",
                "age": "30"
            }""";

    protected static TestRunner runner;

    protected static CouchbaseContainer container = new CouchbaseContainer(COUCHBASE_IMAGE_COMMUNITY_RECENT).withBucket(new BucketDefinition(TEST_BUCKET_NAME));

    protected void initConnectionService() throws InitializationException {
        final StandardCouchbaseConnectionService connectionService = new StandardCouchbaseConnectionService();
        runner.addControllerService(SERVICE_ID, connectionService);
        runner.setProperty(connectionService, CONNECTION_STRING, container.getConnectionString());
        runner.setProperty(connectionService, USERNAME, container.getUsername());
        runner.setProperty(connectionService, PASSWORD, container.getPassword());
        runner.setValidateExpressionUsage(false);
        runner.enableControllerService(connectionService);
    }

    @BeforeAll
    public static void start() {
        container.start();
    }

    @AfterAll
    public static void stop() {
        container.stop();
    }
}
