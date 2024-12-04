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
package org.apache.nifi.manifest;

import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.bundle.BundleDetails;
import org.apache.nifi.c2.protocol.component.api.ComponentManifest;
import org.apache.nifi.c2.protocol.component.api.ControllerServiceDefinition;
import org.apache.nifi.c2.protocol.component.api.ProcessorDefinition;
import org.apache.nifi.c2.protocol.component.api.RuntimeManifest;
import org.apache.nifi.extension.manifest.parser.ExtensionManifestParser;
import org.apache.nifi.extension.manifest.parser.jaxb.JAXBExtensionManifestParser;
import org.apache.nifi.nar.ExtensionDefinition;
import org.apache.nifi.nar.ExtensionDefinition.ExtensionRuntime;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.PythonBundle;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.python.PythonProcessorDetails;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.Collections.emptySet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StandardRuntimeManifestServiceTest {

    private Bundle frameworkBundle;
    private Bundle testComponentsBundle;
    private Bundle testPythonComponentsBundle;
    private ExtensionManager extensionManager;
    private ExtensionManifestParser extensionManifestParser;
    private RuntimeManifestService runtimeManifestService;

    @BeforeEach
    public void setup() {
        final BundleDetails frameworkBundleDetails = new BundleDetails.Builder()
                .coordinate(new BundleCoordinate("org.apache.nifi", "nifi-framework-nar", "1.16.0"))
                .buildJdk("1.8.0")
                .buildRevision("revision1")
                .buildTimestamp("2022-03-07T08:54:46Z")
                .workingDir(new File("src/test/resources/TestRuntimeManifest/nifi-framework-nar"))
                .build();

        frameworkBundle = new Bundle(frameworkBundleDetails, this.getClass().getClassLoader());

        final BundleDetails testComponentsBundleDetails = new BundleDetails.Builder()
                .coordinate(new BundleCoordinate("org.apache.nifi", "nifi-test-components-nar", "1.16.0"))
                .buildJdk("1.8.0")
                .buildRevision("revision1")
                .buildTimestamp("2022-03-07T08:54:46Z")
                .workingDir(new File("src/test/resources/TestRuntimeManifest/nifi-test-components-nar"))
                .build();

        testComponentsBundle = new Bundle(testComponentsBundleDetails, this.getClass().getClassLoader());

        final BundleDetails testPythonComponentsBundleDetails = new BundleDetails.Builder()
                .coordinate(PythonBundle.PYTHON_BUNDLE_COORDINATE)
                .workingDir(new File("src/test/resources/TestRuntimeManifest/nifi-test-python-components-nar"))
                .build();

        testPythonComponentsBundle = new Bundle(testPythonComponentsBundleDetails, this.getClass().getClassLoader());

        extensionManager = mock(ExtensionManager.class);
        extensionManifestParser = new JAXBExtensionManifestParser();
        runtimeManifestService = new TestableRuntimeManifestService(extensionManager, extensionManifestParser, frameworkBundle);
    }

    @Test
    public void testGetRuntimeManifest() {
        when(extensionManager.getAllBundles()).thenReturn(new HashSet<>(Arrays.asList(testComponentsBundle)));

        final RuntimeManifest runtimeManifest = runtimeManifestService.getManifest();
        assertNotNull(runtimeManifest);
        assertEquals(frameworkBundle.getBundleDetails().getCoordinate().getVersion(), runtimeManifest.getVersion());

        assertEquals(frameworkBundle.getBundleDetails().getCoordinate().getVersion(), runtimeManifest.getBuildInfo().getVersion());
        assertEquals(frameworkBundle.getBundleDetails().getBuildRevision(), runtimeManifest.getBuildInfo().getRevision());
        assertEquals(frameworkBundle.getBundleDetails().getBuildJdk(), runtimeManifest.getBuildInfo().getCompiler());

        final List<org.apache.nifi.c2.protocol.component.api.Bundle> bundles = runtimeManifest.getBundles();
        assertNotNull(bundles);
        assertEquals(1, bundles.size());

        final org.apache.nifi.c2.protocol.component.api.Bundle testComponentsManifestBundle = bundles.get(0);
        assertEquals(testComponentsBundle.getBundleDetails().getCoordinate().getGroup(), testComponentsManifestBundle.getGroup());
        assertEquals(testComponentsBundle.getBundleDetails().getCoordinate().getId(), testComponentsManifestBundle.getArtifact());
        assertEquals(testComponentsBundle.getBundleDetails().getCoordinate().getVersion(), testComponentsManifestBundle.getVersion());

        final ComponentManifest testComponentsManifest = testComponentsManifestBundle.getComponentManifest();
        assertNotNull(testComponentsManifest);

        final List<ProcessorDefinition> processorDefinitions = testComponentsManifest.getProcessors();
        assertEquals(3, processorDefinitions.size());

        final List<ControllerServiceDefinition> controllerServiceDefinitions = testComponentsManifest.getControllerServices();
        assertEquals(1, controllerServiceDefinitions.size());
    }

    @Test
    public void testGetPythonManifest() {
        final String processorClassName = "ClassName";
        final List<String> expectedTags = List.of("tag1", "tag2");

        when(extensionManager.getAllBundles()).thenReturn(emptySet());
        when(extensionManager.getExtensions(Processor.class)).thenReturn(Set.of(
                new ExtensionDefinition.Builder()
                        .implementationClassName(processorClassName)
                        .runtime(ExtensionRuntime.PYTHON)
                        .bundle(testPythonComponentsBundle)
                        .extensionType(Processor.class)
                        .tags(expectedTags)
                        .version(PythonBundle.VERSION)
                        .build()
        ));

        final PythonProcessorDetails pythonProcessorDetails = mock(PythonProcessorDetails.class);
        when(pythonProcessorDetails.getTags()).thenReturn(expectedTags);

        when(extensionManager.getPythonProcessorDetails(processorClassName, PythonBundle.VERSION)).thenReturn(pythonProcessorDetails);

        final List<org.apache.nifi.c2.protocol.component.api.Bundle> bundles = runtimeManifestService.getManifest().getBundles();
        assertNotNull(bundles);
        assertEquals(1, bundles.size());

        final org.apache.nifi.c2.protocol.component.api.Bundle testPythonComponentsManifestBundle = bundles.getFirst();
        assertEquals(PythonBundle.GROUP_ID, testPythonComponentsManifestBundle.getGroup());
        assertEquals(PythonBundle.ARTIFACT_ID, testPythonComponentsManifestBundle.getArtifact());
        assertEquals(PythonBundle.VERSION, testPythonComponentsManifestBundle.getVersion());

        final ComponentManifest testComponentsManifest = testPythonComponentsManifestBundle.getComponentManifest();
        assertNotNull(testComponentsManifest);

        final List<ProcessorDefinition> processorDefinitions = testComponentsManifest.getProcessors();
        assertEquals(1, processorDefinitions.size());

        assertEquals(new HashSet<>(expectedTags), processorDefinitions.getFirst().getTags());

        final List<ControllerServiceDefinition> controllerServiceDefinitions = testComponentsManifest.getControllerServices();
        assertEquals(0, controllerServiceDefinitions.size());
    }

    /**
     * Override getFrameworkBundle to provide a mocked Bundle.
     */
    private static class TestableRuntimeManifestService extends StandardRuntimeManifestService {

        private Bundle frameworkBundle;

        public TestableRuntimeManifestService(final ExtensionManager extensionManager, final ExtensionManifestParser extensionManifestParser, final Bundle frameworkBundle) {
            super(extensionManager, extensionManifestParser);
            this.frameworkBundle = frameworkBundle;
        }

        @Override
        Bundle getFrameworkBundle() {
            return frameworkBundle;
        }
    }
}
