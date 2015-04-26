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

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.documentation.DocumentationWriter;
import org.apache.nifi.documentation.example.FullyDocumentedControllerService;
import org.apache.nifi.documentation.example.FullyDocumentedReportingTask;
import org.apache.nifi.documentation.mock.MockControllerServiceInitializationContext;
import org.apache.nifi.documentation.mock.MockReportingInitializationContext;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.reporting.ReportingTask;
import org.junit.Test;

import static org.apache.nifi.documentation.html.XmlValidator.assertContains;
import static org.junit.Assert.assertEquals;

public class HtmlDocumentationWriterTest {

    @Test
    public void testJoin() {
        assertEquals("a, b, c", HtmlDocumentationWriter.join(new String[]{"a", "b", "c"}, ", "));
        assertEquals("a, b", HtmlDocumentationWriter.join(new String[]{"a", "b"}, ", "));
        assertEquals("a", HtmlDocumentationWriter.join(new String[]{"a"}, ", "));
    }

    @Test
    public void testDocumentControllerService() throws InitializationException, IOException {

        ControllerService controllerService = new FullyDocumentedControllerService();
        controllerService.initialize(new MockControllerServiceInitializationContext());

        DocumentationWriter writer = new HtmlDocumentationWriter();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        writer.write(controllerService, baos, false);

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
    }

    @Test
    public void testDocumentReportingTask() throws InitializationException, IOException {

        ReportingTask reportingTask = new FullyDocumentedReportingTask();
        reportingTask.initialize(new MockReportingInitializationContext());

        DocumentationWriter writer = new HtmlDocumentationWriter();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        writer.write(reportingTask, baos, false);

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
    }
}
