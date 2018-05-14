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
package com.marklogic.nifi.controller;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

public class DefaultMarkLogicDatabaseClientServiceIT {
    protected String hostName = "localhost";
    protected String port = "8000";
    protected String database = "Documents";
    protected String username = "admin";
    protected String password = "admin";
    protected String authentication= "DIGEST";
    @Before
    public void init() {

    }

    @Test
    public void testService() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final DefaultMarkLogicDatabaseClientService service = new DefaultMarkLogicDatabaseClientService();
        runner.addControllerService("test-good", service);
        runner.setProperty(service, DefaultMarkLogicDatabaseClientService.HOST, hostName);
        runner.setProperty(service, DefaultMarkLogicDatabaseClientService.PORT, port);runner.setProperty(service, DefaultMarkLogicDatabaseClientService.DATABASE, database);
        runner.setProperty(service, DefaultMarkLogicDatabaseClientService.USERNAME, username);
        runner.setProperty(service, DefaultMarkLogicDatabaseClientService.PASSWORD, password);
        runner.setProperty(service, DefaultMarkLogicDatabaseClientService.SECURITY_CONTEXT_TYPE, authentication);
        runner.enableControllerService(service);
        runner.assertValid(service);
    }

}
