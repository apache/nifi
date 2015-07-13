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

import static org.apache.nifi.documentation.html.XmlValidator.assertContains;
import static org.apache.nifi.documentation.html.XmlValidator.assertNotContains;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.documentation.DocumentationWriter;
import org.apache.nifi.documentation.example.FullyDocumentedProcessor;
import org.apache.nifi.documentation.example.NakedProcessor;
import org.apache.nifi.documentation.example.ProcessorWithLogger;
import org.apache.nifi.documentation.init.ProcessorInitializer;
import org.junit.Assert;
import org.junit.Test;

public class ProcessorDocumentationWriterTest {

    @Test
    public void testFullyDocumentedProcessor() throws IOException {
        FullyDocumentedProcessor processor = new FullyDocumentedProcessor();
        ProcessorInitializer initializer = new ProcessorInitializer();
        initializer.initialize(processor);

        DocumentationWriter writer = new HtmlProcessorDocumentationWriter();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        writer.write(processor, baos, false);
        initializer.teardown(processor);

        String results = new String(baos.toByteArray());
        XmlValidator.assertXmlValid(results);

        assertContains(results, FullyDocumentedProcessor.DIRECTORY.getDisplayName());
        assertContains(results, FullyDocumentedProcessor.DIRECTORY.getDescription());
        assertContains(results, FullyDocumentedProcessor.OPTIONAL_PROPERTY.getDisplayName());
        assertContains(results, FullyDocumentedProcessor.OPTIONAL_PROPERTY.getDescription());
        assertContains(results, FullyDocumentedProcessor.POLLING_INTERVAL.getDisplayName());
        assertContains(results, FullyDocumentedProcessor.POLLING_INTERVAL.getDescription());
        assertContains(results, FullyDocumentedProcessor.POLLING_INTERVAL.getDefaultValue());
        assertContains(results, FullyDocumentedProcessor.RECURSE.getDisplayName());
        assertContains(results, FullyDocumentedProcessor.RECURSE.getDescription());

        assertContains(results, FullyDocumentedProcessor.REL_SUCCESS.getName());
        assertContains(results, FullyDocumentedProcessor.REL_SUCCESS.getDescription());
        assertContains(results, FullyDocumentedProcessor.REL_FAILURE.getName());
        assertContains(results, FullyDocumentedProcessor.REL_FAILURE.getDescription());
        assertContains(results, "Controller Service API: ");
        assertContains(results, "SampleService");

        assertNotContains(results, "iconSecure.png");
        assertContains(results, FullyDocumentedProcessor.class.getAnnotation(CapabilityDescription.class)
                .value());
        assertNotContains(results, "This component has no required or optional properties.");
        assertNotContains(results, "No description provided.");
        assertNotContains(results, "No Tags provided.");
        assertNotContains(results, "Additional Details...");

        // verify the right OnRemoved and OnShutdown methods were called
        Assert.assertEquals(0, processor.getOnRemovedArgs());
        Assert.assertEquals(0, processor.getOnRemovedNoArgs());

        Assert.assertEquals(1, processor.getOnShutdownArgs());
        Assert.assertEquals(1, processor.getOnShutdownNoArgs());
    }

    @Test
    public void testNakedProcessor() throws IOException {
        NakedProcessor processor = new NakedProcessor();
        ProcessorInitializer initializer = new ProcessorInitializer();
        initializer.initialize(processor);

        DocumentationWriter writer = new HtmlProcessorDocumentationWriter();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        writer.write(processor, baos, false);
        initializer.teardown(processor);

        String results = new String(baos.toByteArray());
        XmlValidator.assertXmlValid(results);

        // no description
        assertContains(results, "No description provided.");

        // no tags
        assertContains(results, "None.");

        // properties
        assertContains(results, "This component has no required or optional properties.");

        // relationships
        assertContains(results, "This processor has no relationships.");

    }

    @Test
    public void testProcessorWithLoggerInitialization() throws IOException {
        ProcessorWithLogger processor = new ProcessorWithLogger();
        ProcessorInitializer initializer = new ProcessorInitializer();
        initializer.initialize(processor);

        DocumentationWriter writer = new HtmlProcessorDocumentationWriter();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        writer.write(processor, baos, false);
        initializer.teardown(processor);

        String results = new String(baos.toByteArray());
        XmlValidator.assertXmlValid(results);

    }
}
