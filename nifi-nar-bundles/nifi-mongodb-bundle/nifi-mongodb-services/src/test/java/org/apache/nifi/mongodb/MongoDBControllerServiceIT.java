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

package org.apache.nifi.mongodb;

import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.VerifiableControllerService;
import org.apache.nifi.util.MockConfigurationContext;
import org.apache.nifi.util.MockControllerServiceLookup;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MongoDBControllerServiceIT extends AbstractMongoIT {

    private static final String IDENTIFIER = "Client Service";

    private TestRunner runner;
    private MongoDBControllerService service;

    @BeforeEach
    public void before() throws Exception {
        runner = TestRunners.newTestRunner(TestControllerServiceProcessor.class);
        service = new MongoDBControllerService();
        runner.addControllerService(IDENTIFIER, service);
        runner.setProperty(service, MongoDBControllerService.URI, MONGO_CONTAINER.getConnectionString());
        runner.enableControllerService(service);
    }

    @AfterEach
    public void after() {
        service.onDisable();
    }

    @Test
    public void testInit() {
        runner.assertValid(service);
    }

    private Map<PropertyDescriptor, String> getClientServiceProperties() {
        return ((MockControllerServiceLookup) runner.getProcessContext().getControllerServiceLookup())
                .getControllerServices().get(IDENTIFIER).getProperties();
    }

    @Test
    public void testVerifyWithCorrectConnectionString() {
        final List<ConfigVerificationResult> results = ((VerifiableControllerService) service).verify(
                new MockConfigurationContext(service, getClientServiceProperties(), runner.getProcessContext().getControllerServiceLookup(), null),
                runner.getLogger(),
                Collections.emptyMap()
        );

        assertEquals(1, results.size());
        assertEquals(ConfigVerificationResult.Outcome.SUCCESSFUL, results.get(0).getOutcome());
    }

    @Test
    public void testVerifyWithIncorrectConnectionString() {
        runner.disableControllerService(service);
        runner.setProperty(service, MongoDBControllerService.URI, "mongodb://localhost:2701");
        final List<ConfigVerificationResult> results = ((VerifiableControllerService) service).verify(
                new MockConfigurationContext(service, getClientServiceProperties(), runner.getProcessContext().getControllerServiceLookup(), null),
                runner.getLogger(),
                Collections.emptyMap()
        );

        assertEquals(1, results.size());
        assertEquals(ConfigVerificationResult.Outcome.FAILED, results.get(0).getOutcome());
    }
}
