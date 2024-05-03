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
package org.apache.nifi.documentation;

import org.apache.commons.io.FileUtils;
import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.bundle.BundleDetails;
import org.apache.nifi.documentation.example.ProcessorWithLogger;
import org.apache.nifi.nar.ExtensionDefinition;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.ExtensionMapping;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.util.NiFiProperties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class DocGeneratorTest {
    private static final Class<ProcessorWithLogger> PROCESSOR_CLASS = ProcessorWithLogger.class;

    private static final String[] HTML_EXTENSIONS = new String[]{"html"};

    private static final boolean RECURSIVE_ENABLED = true;

    @Mock
    ExtensionManager extensionManager;

    @Test
    void testGenerateExtensionsNotFound(@TempDir final File workingDirectory) {
        final NiFiProperties properties = getProperties(workingDirectory);
        final ExtensionMapping extensionMapping = new ExtensionMapping();

        DocGenerator.generate(properties, extensionManager, extensionMapping);

        final Collection<File> files = FileUtils.listFiles(workingDirectory, HTML_EXTENSIONS, RECURSIVE_ENABLED);
        assertTrue(files.isEmpty());
    }

    @Test
    void testGenerateProcessor(@TempDir final File workingDirectory) throws IOException {
        final NiFiProperties properties = getProperties(workingDirectory);
        final ExtensionMapping extensionMapping = new ExtensionMapping();

        final BundleCoordinate bundleCoordinate = BundleCoordinate.UNKNOWN_COORDINATE;
        final BundleDetails bundleDetails = new BundleDetails.Builder().workingDir(workingDirectory).coordinate(bundleCoordinate).build();
        final Bundle bundle = new Bundle(bundleDetails, getClass().getClassLoader());
        final ExtensionDefinition definition = new ExtensionDefinition.Builder()
            .bundle(bundle)
            .extensionType(Processor.class)
            .implementationClassName(PROCESSOR_CLASS.getName())
            .runtime(ExtensionDefinition.ExtensionRuntime.JAVA)
            .build();
        final Set<ExtensionDefinition> extensions = Collections.singleton(definition);
        when(extensionManager.getExtensions(eq(Processor.class))).thenReturn(extensions);
        doReturn(PROCESSOR_CLASS).when(extensionManager).getClass(eq(definition));

        final Processor processor = new ProcessorWithLogger();
        when(extensionManager.getTempComponent(eq(PROCESSOR_CLASS.getName()), eq(bundleCoordinate))).thenReturn(processor);

        DocGenerator.generate(properties, extensionManager, extensionMapping);

        final Collection<File> files = FileUtils.listFiles(workingDirectory, HTML_EXTENSIONS, RECURSIVE_ENABLED);
        assertFalse(files.isEmpty());

        final File file = files.iterator().next();
        final byte[] bytes = Files.readAllBytes(file.toPath());
        final String html = new String(bytes, StandardCharsets.UTF_8);

        assertTrue(html.contains(PROCESSOR_CLASS.getSimpleName()));
    }

    private NiFiProperties getProperties(final File workingDirectory) {
        final Properties properties = new Properties();
        properties.setProperty(NiFiProperties.COMPONENT_DOCS_DIRECTORY, workingDirectory.getAbsolutePath());
        return NiFiProperties.createBasicNiFiProperties(null, properties);
    }
}
