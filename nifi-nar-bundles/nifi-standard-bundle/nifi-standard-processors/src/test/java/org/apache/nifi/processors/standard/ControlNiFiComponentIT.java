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
package org.apache.nifi.processors.standard;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.ssl.StandardSSLContextService;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

/**
 * Integration tests for the ControlNiFiComponent processor.
 * The tests are supposed to be updated to match the environment you're running with.
 * The tests are not executed in a normal build of NiFi.
 */
public class ControlNiFiComponentIT {

    @Test
    public void testStartStopProcessorUnsecured() throws IOException {
        TestRunner runner = TestRunners.newTestRunner(ControlNiFiComponent.class);

        runner.setProperty(ControlNiFiComponent.NIFI_API_URL, "http://localhost:8080/nifi-api");
        runner.setProperty(ControlNiFiComponent.ACTION, ControlNiFiComponent.START_STOP);
        runner.setProperty(ControlNiFiComponent.SLEEP, "1 sec");
        runner.setProperty(ControlNiFiComponent.UUID, "${processor}");
        runner.setProperty("test", "${value}");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("processor", "cac3a577-015b-1000-b9c3-843741836f15");
        attributes.put("value", "test");
        runner.enqueue(new byte[0], attributes);

        runner.run();

        runner.assertTransferCount(ControlNiFiComponent.SUCCESS, 1);
    }

    @Test
    public void testStartStopProcessGroupUnsecured() throws IOException {
        TestRunner runner = TestRunners.newTestRunner(ControlNiFiComponent.class);

        runner.setProperty(ControlNiFiComponent.NIFI_API_URL, "http://localhost:8080/nifi-api");
        runner.setProperty(ControlNiFiComponent.ACTION, ControlNiFiComponent.START_STOP);
        runner.setProperty(ControlNiFiComponent.SLEEP, "5 sec");
        runner.setProperty(ControlNiFiComponent.COMPONENT_TYPE, ControlNiFiComponent.PROCESS_GROUP.getValue());
        runner.setProperty(ControlNiFiComponent.UUID, "${component}");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("component", "cac382a6-015b-1000-bc94-cbdb4bd0d0b9");
        runner.enqueue(new byte[0], attributes);

        runner.run();

        runner.assertTransferCount(ControlNiFiComponent.SUCCESS, 1);
    }

    @Test
    public void testStartStopReportingTaskUnsecured() throws IOException {
        TestRunner runner = TestRunners.newTestRunner(ControlNiFiComponent.class);

        runner.setProperty(ControlNiFiComponent.NIFI_API_URL, "http://localhost:8080/nifi-api");
        runner.setProperty(ControlNiFiComponent.ACTION, ControlNiFiComponent.START_STOP);
        runner.setProperty(ControlNiFiComponent.SLEEP, "5 sec");
        runner.setProperty(ControlNiFiComponent.COMPONENT_TYPE, ControlNiFiComponent.REPORTING_TASK.getValue());
        runner.setProperty(ControlNiFiComponent.UUID, "${component}");
        runner.setProperty("Directory Location", "/var"); // ReportingTask is MonitorDiskUsage

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("component", "f5c07838-015c-1000-f044-e13f54a42e68");
        runner.enqueue(new byte[0], attributes);

        runner.run();

        runner.assertTransferCount(ControlNiFiComponent.SUCCESS, 1);
    }

    @Test
    public void testStartStopControllerServiceUnsecured() throws IOException {
        TestRunner runner = TestRunners.newTestRunner(ControlNiFiComponent.class);

        runner.setProperty(ControlNiFiComponent.NIFI_API_URL, "http://localhost:8080/nifi-api");
        runner.setProperty(ControlNiFiComponent.ACTION, ControlNiFiComponent.START_STOP);
        runner.setProperty(ControlNiFiComponent.SLEEP, "5 sec");
        runner.setProperty(ControlNiFiComponent.COMPONENT_TYPE, ControlNiFiComponent.CONTROLLER_SERVICE.getValue());
        runner.setProperty(ControlNiFiComponent.UUID, "${component}");
        runner.setProperty("Maximum Outstanding Requests", "10000"); // StandardHttpContextMap

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("component", "fbec5165-015a-1000-ef40-26bc04ec2b63");
        runner.enqueue(new byte[0], attributes);

        runner.run();

        runner.assertTransferCount(ControlNiFiComponent.SUCCESS, 1);
    }

    @Test
    public void testStartStopProcessorSecuredLoginPassword() throws IOException, InitializationException {
        TestRunner runner = TestRunners.newTestRunner(ControlNiFiComponent.class);

        final Map<String, String> sslProperties = new HashMap<>();
        sslProperties.put(StandardSSLContextService.TRUSTSTORE.getName(), "/path/to/truststore.jks");
        sslProperties.put(StandardSSLContextService.TRUSTSTORE_PASSWORD.getName(), "password");
        sslProperties.put(StandardSSLContextService.TRUSTSTORE_TYPE.getName(), "JKS");

        final StandardSSLContextService sslService = new StandardSSLContextService();
        runner.addControllerService("ssl-context", sslService, sslProperties);
        runner.enableControllerService(sslService);

        runner.setProperty(ControlNiFiComponent.NIFI_API_URL, "https://localhost:8443/nifi-api");
        runner.setProperty(ControlNiFiComponent.ACTION, ControlNiFiComponent.START_STOP);
        runner.setProperty(ControlNiFiComponent.SLEEP, "1 sec");
        runner.setProperty(ControlNiFiComponent.UUID, "${processor}");
        runner.setProperty(ControlNiFiComponent.SSL_CONTEXT_SERVICE, "ssl-context");
        runner.setProperty(ControlNiFiComponent.USERNAME, "admin");
        runner.setProperty(ControlNiFiComponent.PASSWORD, "password");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("processor", "cac3a577-015b-1000-b9c3-843741836f15");
        runner.enqueue(new byte[0], attributes);

        runner.run();

        runner.assertTransferCount(ControlNiFiComponent.SUCCESS, 1);
    }

    @Test
    public void testStartStopProcessorSecuredCertificates() throws IOException, InitializationException {
        TestRunner runner = TestRunners.newTestRunner(ControlNiFiComponent.class);

        final Map<String, String> sslProperties = new HashMap<>();
        sslProperties.put(StandardSSLContextService.TRUSTSTORE.getName(), "/path/to/truststore.jks");
        sslProperties.put(StandardSSLContextService.TRUSTSTORE_PASSWORD.getName(), "password");
        sslProperties.put(StandardSSLContextService.TRUSTSTORE_TYPE.getName(), "JKS");
        sslProperties.put(StandardSSLContextService.KEYSTORE.getName(), "/path/to/keystore.jks");
        sslProperties.put(StandardSSLContextService.KEYSTORE_PASSWORD.getName(), "password");
        sslProperties.put(StandardSSLContextService.KEYSTORE_TYPE.getName(), "JKS");

        final StandardSSLContextService sslService = new StandardSSLContextService();
        runner.addControllerService("ssl-context", sslService, sslProperties);
        runner.enableControllerService(sslService);

        runner.setProperty(ControlNiFiComponent.NIFI_API_URL, "https://localhost:8443/nifi-api");
        runner.setProperty(ControlNiFiComponent.ACTION, ControlNiFiComponent.START_STOP);
        runner.setProperty(ControlNiFiComponent.SLEEP, "1 sec");
        runner.setProperty(ControlNiFiComponent.UUID, "${processor}");
        runner.setProperty(ControlNiFiComponent.SSL_CONTEXT_SERVICE, "ssl-context");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("processor", "cac3a577-015b-1000-b9c3-843741836f15");
        runner.enqueue(new byte[0], attributes);

        runner.run();

        runner.assertTransferCount(ControlNiFiComponent.SUCCESS, 1);
    }

}