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

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Calendar;

public class MongoDBControllerServiceIT {
    private static final String DB_NAME = String.format("nifi_test-%d", Calendar.getInstance().getTimeInMillis());
    private static final String COL_NAME = String.format("nifi_test-%d", Calendar.getInstance().getTimeInMillis());

    private TestRunner runner;
    private MongoDBControllerService service;

    @Before
    public void before() throws Exception {
        runner = TestRunners.newTestRunner(TestControllerServiceProcessor.class);
        service = new MongoDBControllerService();
        runner.addControllerService("Client Service", service);
        runner.setProperty(service, MongoDBControllerService.URI, "mongodb://localhost:27017");
        runner.enableControllerService(service);
    }

    @After
    public void after() throws Exception {
        service.onDisable();
    }

    @Test
    public void testInit() throws Exception {
        runner.assertValid(service);
    }
}
