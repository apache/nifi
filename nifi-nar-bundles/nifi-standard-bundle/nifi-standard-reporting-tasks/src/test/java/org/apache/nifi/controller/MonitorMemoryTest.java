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
package org.apache.nifi.controller;

import org.apache.commons.io.FileUtils;
import org.apache.nifi.admin.service.AuditService;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.controller.repository.FlowFileEventRepository;
import org.apache.nifi.provenance.MockProvenanceRepository;
import org.apache.nifi.registry.VariableRegistryUtils;
import org.apache.nifi.util.CapturingLogger;
import org.apache.nifi.util.NiFiProperties;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class MonitorMemoryTest {

    private FlowController fc;

    @Before
    public void before() throws Exception {
        System.setProperty("nifi.properties.file.path", "src/test/resources/nifi.properties");
        NiFiProperties.getInstance().setProperty(NiFiProperties.ADMINISTRATIVE_YIELD_DURATION, "1 sec");
        NiFiProperties.getInstance().setProperty(NiFiProperties.STATE_MANAGEMENT_CONFIG_FILE, "target/test-classes/state-management.xml");
        NiFiProperties.getInstance().setProperty(NiFiProperties.STATE_MANAGEMENT_LOCAL_PROVIDER_ID, "local-provider");
        fc = this.buildFlowControllerForTest();
    }

    @After
    public void after() throws Exception {
        fc.shutdown(true);
        FileUtils.deleteDirectory(new File("./target/test-repo"));
        FileUtils.deleteDirectory(new File("./target/content_repository"));
    }

    @Test(expected = IllegalStateException.class)
    public void validatevalidationKicksInOnWrongPoolNames() throws Exception {
        ReportingTaskNode reportingTask = fc.createReportingTask(MonitorMemory.class.getName());
        reportingTask.setProperty(MonitorMemory.MEMORY_POOL_PROPERTY.getName(), "foo");
        ProcessScheduler ps = fc.getProcessScheduler();
        ps.schedule(reportingTask);
    }

    @Test
    @Ignore // temporarily ignoring it since it fails intermittently due to
            // unpredictability during full build
            // still keeping it for local testing
    public void validateWarnWhenPercentThresholdReached() throws Exception {
        this.doValidate("10%");
    }

    /*
     * We're ignoring this tests as it is practically impossible to run it
     * reliably together with automated Maven build since we can't control the
     * state of the JVM on each machine during the build. However, you can run
     * it selectively for further validation
     */
    @Test
    @Ignore
    public void validateWarnWhenSizeThresholdReached() throws Exception {
        this.doValidate("10 MB");
    }

    public void doValidate(String threshold) throws Exception {
        CapturingLogger capturingLogger = this.wrapAndReturnCapturingLogger();
        ReportingTaskNode reportingTask = fc.createReportingTask(MonitorMemory.class.getName());
        reportingTask.setScheduldingPeriod("1 sec");
        reportingTask.setProperty(MonitorMemory.MEMORY_POOL_PROPERTY.getName(), "PS Old Gen");
        reportingTask.setProperty(MonitorMemory.REPORTING_INTERVAL.getName(), "100 millis");
        reportingTask.setProperty(MonitorMemory.THRESHOLD_PROPERTY.getName(), threshold);

        ProcessScheduler ps = fc.getProcessScheduler();
        ps.schedule(reportingTask);

        Thread.sleep(2000);
        // ensure no memory warning were issued
        assertTrue(capturingLogger.getWarnMessages().size() == 0);

        // throw something on the heap
        @SuppressWarnings("unused")
        byte[] b = new byte[Integer.MAX_VALUE / 3];
        Thread.sleep(200);
        assertTrue(capturingLogger.getWarnMessages().size() > 0);
        assertTrue(capturingLogger.getWarnMessages().get(0).getMsg()
                .startsWith("Memory Pool 'PS Old Gen' has exceeded the configured Threshold of " + threshold));

        // now try to clear the heap and see memory being reclaimed
        b = null;
        System.gc();
        Thread.sleep(1000);
        assertTrue(capturingLogger.getInfoMessages().get(0).getMsg().startsWith(
                "Memory Pool 'PS Old Gen' is no longer exceeding the configured Threshold of " + threshold));
    }

    private CapturingLogger wrapAndReturnCapturingLogger() throws Exception {
        Field loggerField = MonitorMemory.class.getDeclaredField("logger");
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(loggerField, loggerField.getModifiers() & ~Modifier.FINAL);

        loggerField.setAccessible(true);
        CapturingLogger capturingLogger = new CapturingLogger((Logger) loggerField.get(null));
        loggerField.set(null, capturingLogger);
        return capturingLogger;
    }

    private FlowController buildFlowControllerForTest() throws Exception {
        NiFiProperties properties = NiFiProperties.getInstance();

        properties.setProperty(NiFiProperties.PROVENANCE_REPO_IMPLEMENTATION_CLASS,
                MockProvenanceRepository.class.getName());
        properties.setProperty("nifi.remote.input.socket.port", "");
        properties.setProperty("nifi.remote.input.secure", "");

        return FlowController.createStandaloneInstance(mock(FlowFileEventRepository.class), properties,
                mock(Authorizer.class), mock(AuditService.class), null, null, VariableRegistryUtils.createCustomVariableRegistry(properties.getVariableRegistryPropertiesPaths()));
    }
}
