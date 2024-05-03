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
package org.apache.nifi.processors.salesforce.util;

import org.apache.nifi.oauth2.StandardOauth2AccessTokenProvider;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.salesforce.rest.SalesforceConfiguration;
import org.apache.nifi.processors.salesforce.rest.SalesforceRestClient;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Set constants in {@link SalesforceConfigAware}
 */
class SalesforceRestServiceIT implements SalesforceConfigAware {
    private TestRunner runner;
    private SalesforceRestClient testSubject;

    @BeforeEach
    void setUp() throws Exception {
        runner = TestRunners.newTestRunner(new AbstractSessionFactoryProcessor() {
            @Override
            public void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory) throws ProcessException {
            }
        });

        StandardOauth2AccessTokenProvider oauth2AccessTokenProvider = initOAuth2AccessTokenProvider(runner);
        SalesforceConfiguration configuration = SalesforceConfiguration.create(INSTANCE_URL, VERSION,
                () -> oauth2AccessTokenProvider.getAccessDetails().getAccessToken(), 5_000);
        testSubject = new SalesforceRestClient(configuration);
    }

    @AfterEach
    void tearDown() {
        runner.shutdown();
    }

    @Test
    void describeSObjectSucceeds() throws IOException {
        try (InputStream describeSObjectResultJson = testSubject.describeSObject("Account")) {
            assertNotNull(describeSObjectResultJson);
        }
    }

    @Test
    void querySucceeds() throws IOException {
        String query = "SELECT id,BillingAddress FROM Account";

        try (InputStream querySObjectRecordsResultJson = testSubject.query(query)) {
            assertNotNull(querySObjectRecordsResultJson);
        }
    }
}
