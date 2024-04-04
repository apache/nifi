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
package org.apache.nifi.processors.gcp.pubsub;

import org.apache.nifi.processors.gcp.credentials.service.GCPCredentialsControllerService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;

import java.util.HashMap;
import java.util.Map;

public class AbstractGCPubSubIT {

    protected static final String PROJECT_ID = "my-gcm-client";
    protected static final String CONTROLLER_SERVICE = "GCPCredentialsService";
    protected static TestRunner runner;

    protected TestRunner setCredentialsCS(TestRunner runner) throws InitializationException {
        final String serviceAccountJsonFilePath = "path/to/credentials/json";
        final Map<String, String> propertiesMap = new HashMap<>();
        final GCPCredentialsControllerService credentialsControllerService = new GCPCredentialsControllerService();

        propertiesMap.put("application-default-credentials", "false");
        propertiesMap.put("compute-engine-credentials", "false");
        propertiesMap.put("service-account-json-file", serviceAccountJsonFilePath);

        runner.addControllerService(CONTROLLER_SERVICE, credentialsControllerService, propertiesMap);
        runner.enableControllerService(credentialsControllerService);
        runner.assertValid(credentialsControllerService);

        return runner;
    }
}
