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
package org.apache.nifi.documentation.html;

import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.documentation.DocumentationWriter;
import org.apache.nifi.documentation.example.ControllerServiceWithLogger;
import org.apache.nifi.documentation.example.FullyDocumentedControllerService;
import org.apache.nifi.documentation.example.FullyDocumentedReportingTask;
import org.apache.nifi.documentation.example.ReportingTaskWithLogger;
import org.apache.nifi.init.ControllerServiceInitializer;
import org.apache.nifi.init.ReportingTaskingInitializer;
import org.apache.nifi.mock.MockControllerServiceInitializationContext;
import org.apache.nifi.mock.MockReportingInitializationContext;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.StandardExtensionDiscoveringManager;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.reporting.ReportingTask;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.apache.nifi.documentation.html.XmlValidator.assertContains;
import static org.junit.Assert.assertEquals;

public class HtmlDocumentationWriterTest {

    private ExtensionManager extensionManager;

    @Before
    public void setup() {
        extensionManager = new StandardExtensionDiscoveringManager();
    }

    @Test
    public void testJoin() {
        assertEquals("a, b, c", HtmlDocumentationWriter.join(new String[] { "a", "b", "c" }, ", "));
        assertEquals("a, b", HtmlDocumentationWriter.join(new String[] { "a", "b" }, ", "));
        assertEquals("a", HtmlDocumentationWriter.join(new String[] { "a" }, ", "));
    }

    @Test
    public void testDocumentControllerService() throws InitializationException, IOException {

        FullyDocumentedControllerService controllerService = new FullyDocumentedControllerService();
        ControllerServiceInitializer initializer = new ControllerServiceInitializer(extensionManager);
        initializer.initialize(controllerService);

        DocumentationWriter writer = new HtmlDocumentationWriter(extensionManager);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        writer.write(controllerService, baos, false);
        initializer.teardown(controllerService);

        String results = new String(baos.toByteArray());
        XmlValidator.assertXmlValid(results);

        // description
        assertContains(results, "A documented controller service that can help you do things");

        // tags
        assertContains(results, "one, two, three");

        // properties
        assertContains(results, "Keystore Filename");
        assertContains(results, "The fully-qualified filename of the Keystore");
        assertContains(results, "Keystore Type");
        assertContains(results, "JKS");
        assertContains(results, "PKCS12");
        assertContains(results, "Sensitive Property: true");

        // restricted
        assertContains(results, "controller service restriction description");

        // verify system resource considerations
        assertContains(results, SystemResource.CPU.name());
        assertContains(results, SystemResourceConsideration.DEFAULT_DESCRIPTION);
        assertContains(results, SystemResource.DISK.name());
        assertContains(results, "Customized disk usage description");
        assertContains(results, SystemResource.MEMORY.name());
        assertContains(results, "Not Specified");

        // verify the right OnRemoved and OnShutdown methods were called
        Assert.assertEquals(0, controllerService.getOnRemovedArgs());
        Assert.assertEquals(0, controllerService.getOnRemovedNoArgs());

        Assert.assertEquals(1, controllerService.getOnShutdownArgs());
        Assert.assertEquals(1, controllerService.getOnShutdownNoArgs());
    }

    @Test
    public void testDocumentReportingTask() throws InitializationException, IOException {

        FullyDocumentedReportingTask reportingTask = new FullyDocumentedReportingTask();
        ReportingTaskingInitializer initializer = new ReportingTaskingInitializer(extensionManager);
        initializer.initialize(reportingTask);

        DocumentationWriter writer = new HtmlDocumentationWriter(extensionManager);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        writer.write(reportingTask, baos, false);
        initializer.teardown(reportingTask);

        String results = new String(baos.toByteArray());
        XmlValidator.assertXmlValid(results);

        // description
        assertContains(results, "A helper reporting task to do...");

        // tags
        assertContains(results, "first, second, third");

        // properties
        assertContains(results, "Show Deltas");
        assertContains(results, "Specifies whether or not to show the difference in values between the current status and the previous status");
        assertContains(results, "true");
        assertContains(results, "false");

        // restricted
        assertContains(results, "reporting task restriction description");

        // verify system resource considerations
        assertContains(results, SystemResource.CPU.name());
        assertContains(results, SystemResourceConsideration.DEFAULT_DESCRIPTION);
        assertContains(results, SystemResource.DISK.name());
        assertContains(results, "Customized disk usage description");
        assertContains(results, SystemResource.MEMORY.name());
        assertContains(results, "Not Specified");

        // verify the right OnRemoved and OnShutdown methods were called
        Assert.assertEquals(0, reportingTask.getOnRemovedArgs());
        Assert.assertEquals(0, reportingTask.getOnRemovedNoArgs());

        Assert.assertEquals(1, reportingTask.getOnShutdownArgs());
        Assert.assertEquals(1, reportingTask.getOnShutdownNoArgs());
    }

    @Test
    public void testControllerServiceWithLogger() throws InitializationException, IOException {

        ControllerService controllerService = new ControllerServiceWithLogger();
        controllerService.initialize(new MockControllerServiceInitializationContext());

        DocumentationWriter writer = new HtmlDocumentationWriter(extensionManager);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        writer.write(controllerService, baos, false);

        String results = new String(baos.toByteArray());
        XmlValidator.assertXmlValid(results);
    }

    @Test
    public void testReportingTaskWithLogger() throws InitializationException, IOException {

        ReportingTask controllerService = new ReportingTaskWithLogger();
        controllerService.initialize(new MockReportingInitializationContext());

        DocumentationWriter writer = new HtmlDocumentationWriter(extensionManager);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        writer.write(controllerService, baos, false);

        String results = new String(baos.toByteArray());
        XmlValidator.assertXmlValid(results);
    }
}
