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
package org.apache.nifi.processors.livy;

import org.apache.nifi.controller.livy.LivySessionController;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.web.util.TestServer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestExecuteSparkInteractive extends ExecuteSparkInteractiveTestBase {

    private static TestServer server;
    private static String url;

    @BeforeClass
    public static void beforeClass() throws Exception {
        // useful for verbose logging output
        // don't commit this with this property enabled, or any 'mvn test' will be really verbose
        // System.setProperty("org.slf4j.simpleLogger.log.nifi.processors.standard", "debug");

        // create a Jetty server on a random port
        server = createServer();
        server.startServer();

        // this is the base url with the random port
        url = server.getUrl();
    }

    @AfterClass
    public static void afterClass() throws Exception {
        server.shutdownServer();
    }

    @Before
    public void before() throws Exception {
        runner = TestRunners.newTestRunner(ExecuteSparkInteractive.class);
        LivySessionController livyControllerService = new LivySessionController();
        runner.addControllerService("livyCS", livyControllerService);
        runner.setProperty(livyControllerService, LivySessionController.LIVY_HOST, url.substring(url.indexOf("://") + 3, url.lastIndexOf(":")));
        runner.setProperty(livyControllerService, LivySessionController.LIVY_PORT, url.substring(url.lastIndexOf(":") + 1));
        runner.enableControllerService(livyControllerService);
        runner.setProperty(ExecuteSparkInteractive.LIVY_CONTROLLER_SERVICE, "livyCS");

        server.clearHandlers();
    }

    @After
    public void after() {
        runner.shutdown();
    }

    private static TestServer createServer() {
        return new TestServer();
    }

    @Test
    public void testSparkSession() throws Exception {
        testCode(server, "print \"hello world\"");
    }

    @Test
    public void testSparkSessionWithSpecialChars() throws Exception {
        testCode(server, "print \"/'?!<>[]{}()$&*=%;.|_-\\\"");
    }
}
