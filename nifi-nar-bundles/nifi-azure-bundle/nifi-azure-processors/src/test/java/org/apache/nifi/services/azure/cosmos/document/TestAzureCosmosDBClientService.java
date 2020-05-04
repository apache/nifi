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
package org.apache.nifi.services.azure.cosmos.document;

import org.apache.nifi.processors.azure.cosmos.document.AzureCosmosDBUtils;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

public class TestAzureCosmosDBClientService {

    private static final String MOCK_URI = "https://mockURI:443/";
    private static final String DB_ACCESS_KEY = "mockDB_ACCESS_KEY";

    private TestRunner runner;
    private AzureCosmosDBClientService service;

    @Before
    public void setUp() throws InitializationException {
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        service = new AzureCosmosDBClientService();
        runner.addControllerService("connService", service);
    }

    @Test
    public void testValidWithURIandDBAccessKey() {
        configureURI();
        configureDBAccessKey();

        runner.assertValid(service);
    }

    @Test
    public void testNotValidBecauseURIMissing() {
        configureDBAccessKey();

        runner.assertNotValid(service);
    }

    @Test
    public void testNotValidBecauseDBAccessKeyMissing() {
        configureURI();

        runner.assertNotValid(service);
    }

    private void configureURI() {
        runner.setProperty(service, AzureCosmosDBUtils.URI, MOCK_URI);
    }

    private void configureDBAccessKey() {
        runner.setProperty(service, AzureCosmosDBUtils.DB_ACCESS_KEY, DB_ACCESS_KEY);
    }

}
