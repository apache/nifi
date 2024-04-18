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
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.documentation.DocumentationWriter;
import org.apache.nifi.documentation.example.DeprecatedProcessor;
import org.apache.nifi.documentation.example.FullyDocumentedProcessor;
import org.apache.nifi.documentation.example.NakedProcessor;
import org.apache.nifi.documentation.example.ProcessorWithLogger;
import org.apache.nifi.init.ProcessorInitializer;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.StandardExtensionDiscoveringManager;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.apache.nifi.documentation.html.AbstractHtmlDocumentationWriter.NO_DESCRIPTION;
import static org.apache.nifi.documentation.html.AbstractHtmlDocumentationWriter.NO_PROPERTIES;
import static org.apache.nifi.documentation.html.AbstractHtmlDocumentationWriter.NO_TAGS;
import static org.apache.nifi.documentation.html.XmlValidator.assertContains;
import static org.apache.nifi.documentation.html.XmlValidator.assertNotContains;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ProcessorDocumentationWriterTest {

    @Test
    public void testFullyDocumentedProcessor() throws IOException {
        ExtensionManager extensionManager = new StandardExtensionDiscoveringManager();

        FullyDocumentedProcessor processor = new FullyDocumentedProcessor();
        ProcessorInitializer initializer = new ProcessorInitializer(extensionManager);
        initializer.initialize(processor);

        DocumentationWriter<ConfigurableComponent> writer = new HtmlProcessorDocumentationWriter(extensionManager);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        writer.write(processor, baos, false);
        initializer.teardown(processor);

        String results = baos.toString();
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

        assertContains(results, "CLUSTER, LOCAL");
        assertContains(results, "state management description");

        assertContains(results, "processor restriction description");
        assertContains(results, RequiredPermission.READ_FILESYSTEM.getPermissionLabel());
        assertContains(results, "Requires read filesystem permission");

        assertNotContains(results, "iconSecure.png");
        assertContains(results, FullyDocumentedProcessor.class.getAnnotation(CapabilityDescription.class)
                .value());
        assertNotContains(results, NO_PROPERTIES);
        assertNotContains(results, NO_DESCRIPTION);

        assertNotContains(results, NO_TAGS);
        assertNotContains(results, "Additional Details...");

        // check expression language scope
        assertContains(results, "Supports Expression Language: true (will be evaluated using Environment variables only)");

        // Check Property Values
        assertContains(results, "Enabled");
        assertContains(results, "0 sec");

        // verify dynamic properties
        assertContains(results, "Routes FlowFiles to relationships based on XPath");

        assertContains(results, "Supports Sensitive Dynamic Properties: <strong>No</strong>");

        // input requirement
        assertContains(results, "This component does not allow an incoming relationship.");

        // verify system resource considerations
        assertContains(results, SystemResource.CPU.name());
        assertContains(results, SystemResourceConsideration.DEFAULT_DESCRIPTION);
        assertContains(results, SystemResource.DISK.name());
        assertContains(results, "Customized disk usage description");
        assertContains(results, SystemResource.MEMORY.name());
        assertContains(results, "Not Specified");

        // verify the right OnRemoved and OnShutdown methods were called
        assertEquals(0, processor.getOnRemovedArgs());
        assertEquals(0, processor.getOnRemovedNoArgs());

        assertEquals(1, processor.getOnShutdownArgs());
        assertEquals(1, processor.getOnShutdownNoArgs());
    }

    @Test
    public void testNakedProcessor() throws IOException {
        ExtensionManager extensionManager = new StandardExtensionDiscoveringManager();

        NakedProcessor processor = new NakedProcessor();
        ProcessorInitializer initializer = new ProcessorInitializer(extensionManager);
        initializer.initialize(processor);

        DocumentationWriter<ConfigurableComponent> writer = new HtmlProcessorDocumentationWriter(extensionManager);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        writer.write(processor, baos, false);
        initializer.teardown(processor);

        String results = baos.toString();
        XmlValidator.assertXmlValid(results);

        // no description
        assertContains(results, NO_DESCRIPTION);

        // no tags
        assertContains(results, NO_TAGS);

        // properties
        assertContains(results, NO_PROPERTIES);

        // relationships
        assertContains(results, "This processor has no relationships.");

        // state management
        assertContains(results, "This component does not store state.");

        // state management
        assertContains(results, "This component is not restricted.");

        // input requirement
        assertNotContains(results, "Input requirement:");
    }

    @Test
    public void testProcessorWithLoggerInitialization() throws IOException {
        ExtensionManager extensionManager = new StandardExtensionDiscoveringManager();

        ProcessorWithLogger processor = new ProcessorWithLogger();
        ProcessorInitializer initializer = new ProcessorInitializer(extensionManager);
        initializer.initialize(processor);

        DocumentationWriter<ConfigurableComponent> writer = new HtmlProcessorDocumentationWriter(extensionManager);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        writer.write(processor, baos, false);
        initializer.teardown(processor);

        String results = baos.toString();
        XmlValidator.assertXmlValid(results);

    }

    @Test
    public void testDeprecatedProcessor() throws IOException {
        ExtensionManager extensionManager = new StandardExtensionDiscoveringManager();

        DeprecatedProcessor processor = new DeprecatedProcessor();
        ProcessorInitializer initializer = new ProcessorInitializer(extensionManager);
        initializer.initialize(processor);

        DocumentationWriter<ConfigurableComponent> writer = new HtmlProcessorDocumentationWriter(extensionManager);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        writer.write(processor, baos, false);
        initializer.teardown(processor);

        String results = baos.toString();
        XmlValidator.assertXmlValid(results);

        assertContains(results, DeprecatedProcessor.DIRECTORY.getDisplayName());
        assertContains(results, DeprecatedProcessor.DIRECTORY.getDescription());
        assertContains(results, DeprecatedProcessor.OPTIONAL_PROPERTY.getDisplayName());
        assertContains(results, DeprecatedProcessor.OPTIONAL_PROPERTY.getDescription());
        assertContains(results, DeprecatedProcessor.POLLING_INTERVAL.getDisplayName());
        assertContains(results, DeprecatedProcessor.POLLING_INTERVAL.getDescription());
        assertContains(results, DeprecatedProcessor.POLLING_INTERVAL.getDefaultValue());
        assertContains(results, DeprecatedProcessor.RECURSE.getDisplayName());
        assertContains(results, DeprecatedProcessor.RECURSE.getDescription());

        assertContains(results, DeprecatedProcessor.REL_SUCCESS.getName());
        assertContains(results, DeprecatedProcessor.REL_SUCCESS.getDescription());
        assertContains(results, DeprecatedProcessor.REL_FAILURE.getName());
        assertContains(results, DeprecatedProcessor.REL_FAILURE.getDescription());
        assertContains(results, "Controller Service API: ");
        assertContains(results, "SampleService");

        assertContains(results, "CLUSTER, LOCAL");
        assertContains(results, "state management description");

        assertContains(results, "processor restriction description");

        assertNotContains(results, "iconSecure.png");
        assertContains(results, DeprecatedProcessor.class.getAnnotation(CapabilityDescription.class)
                .value());

        // Check for the existence of deprecation notice
        assertContains(results, "Deprecation notice: ");
        // assertContains(results, DeprecatedProcessor.class.getAnnotation(DeprecationNotice.class.));

        assertNotContains(results, NO_PROPERTIES);
        assertNotContains(results, NO_DESCRIPTION);
        assertNotContains(results, NO_TAGS);
        assertNotContains(results, "Additional Details...");

        // verify the right OnRemoved and OnShutdown methods were called
        assertEquals(0, processor.getOnRemovedArgs());
        assertEquals(0, processor.getOnRemovedNoArgs());

        assertEquals(1, processor.getOnShutdownArgs());
        assertEquals(1, processor.getOnShutdownNoArgs());
    }
}
