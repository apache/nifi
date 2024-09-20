/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.nifi.nar;

import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.bundle.BundleDetails;
import org.apache.nifi.controller.ComponentNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ComponentNodeDefinitionPredicateTest {

    private static final String STANDARD_PROCESSOR_COMPONENT_TYPE = "MyProcessor";
    private static final String STANDARD_PROCESSOR_CLASS_NAME = "org.apache.nifi." + STANDARD_PROCESSOR_COMPONENT_TYPE;
    private static final BundleCoordinate STANDARD_BUNDLE_COORDINATE_V1 = new BundleCoordinate("org.apache.nifi", "my-processors-nar", "1.0.0");
    private static final BundleCoordinate STANDARD_BUNDLE_COORDINATE_V2 = new BundleCoordinate("org.apache.nifi", "my-processors-nar", "2.0.0");

    private static final String PYTHON_PROCESSOR_TYPE = "PythonProcessor";
    private static final String OTHER_PYTHON_PROCESSOR_TYPE = "OtherPythonProcessor";
    private static final BundleCoordinate PYTHON_BUNDLE_COORDINATE = new BundleCoordinate(PythonBundle.GROUP_ID, PythonBundle.ARTIFACT_ID, "0.0.1");

    private ExtensionDefinition standardProcessorDefinition;
    private ExtensionDefinition pythonProcessorDefinition;

    @BeforeEach
    public void setup() {
        final BundleDetails standardBundleDetails = mock(BundleDetails.class);
        when(standardBundleDetails.getCoordinate()).thenReturn(STANDARD_BUNDLE_COORDINATE_V1);

        final Bundle standardBundle = mock(Bundle.class);
        when(standardBundle.getBundleDetails()).thenReturn(standardBundleDetails);

        standardProcessorDefinition = mock(ExtensionDefinition.class);
        when(standardProcessorDefinition.getImplementationClassName()).thenReturn(STANDARD_PROCESSOR_CLASS_NAME);
        when(standardProcessorDefinition.getBundle()).thenReturn(standardBundle);

        final BundleDetails pythonBundleDetails = mock(BundleDetails.class);
        when(pythonBundleDetails.getCoordinate()).thenReturn(PYTHON_BUNDLE_COORDINATE);

        final Bundle pythonBundle = mock(Bundle.class);
        when(pythonBundle.getBundleDetails()).thenReturn(pythonBundleDetails);

        pythonProcessorDefinition = mock(ExtensionDefinition.class);
        when(pythonProcessorDefinition.getImplementationClassName()).thenReturn(PYTHON_PROCESSOR_TYPE);
        when(pythonProcessorDefinition.getBundle()).thenReturn(pythonBundle);
    }

    @Test
    public void testWhenComponentNodeMatchesDefinition() {
        final ComponentNode componentNode = mock(ComponentNode.class);
        when(componentNode.getCanonicalClassName()).thenReturn(STANDARD_PROCESSOR_CLASS_NAME);
        when(componentNode.getBundleCoordinate()).thenReturn(STANDARD_BUNDLE_COORDINATE_V1);

        final Predicate<ComponentNode> predicate = new ComponentNodeDefinitionPredicate(Set.of(standardProcessorDefinition));
        assertTrue(predicate.test(componentNode));
    }

    @Test
    public void testWhenComponentNodeTypeDoesNotMatchDefinition() {
        final ComponentNode componentNode = mock(ComponentNode.class);
        when(componentNode.getCanonicalClassName()).thenReturn("com.SomeOtherProcessor");
        when(componentNode.getBundleCoordinate()).thenReturn(STANDARD_BUNDLE_COORDINATE_V1);

        final Predicate<ComponentNode> predicate = new ComponentNodeDefinitionPredicate(Set.of(standardProcessorDefinition));
        assertFalse(predicate.test(componentNode));
    }

    @Test
    public void testWhenComponentNodeCoordinateDoesMatchDefinition() {
        final ComponentNode componentNode = mock(ComponentNode.class);
        when(componentNode.getCanonicalClassName()).thenReturn(STANDARD_PROCESSOR_CLASS_NAME);
        when(componentNode.getBundleCoordinate()).thenReturn(STANDARD_BUNDLE_COORDINATE_V2);

        final Predicate<ComponentNode> predicate = new ComponentNodeDefinitionPredicate(Set.of(standardProcessorDefinition));
        assertFalse(predicate.test(componentNode));
    }

    @Test
    public void testWhenComponentNodeIsMissingAndCompatibleBundle() {
        final ComponentNode componentNode = mock(ComponentNode.class);
        when(componentNode.getCanonicalClassName()).thenReturn(STANDARD_PROCESSOR_CLASS_NAME);
        when(componentNode.getBundleCoordinate()).thenReturn(STANDARD_BUNDLE_COORDINATE_V2);
        when(componentNode.isExtensionMissing()).thenReturn(true);

        final Predicate<ComponentNode> predicate = new ComponentNodeDefinitionPredicate(Set.of(standardProcessorDefinition));
        assertTrue(predicate.test(componentNode));
    }

    @Test
    public void testPythonComponentNodeMatchesPythonDefinition() {
        final ComponentNode componentNode = mock(ComponentNode.class);
        when(componentNode.getComponentType()).thenReturn(PYTHON_PROCESSOR_TYPE);
        when(componentNode.getBundleCoordinate()).thenReturn(PYTHON_BUNDLE_COORDINATE);

        final Predicate<ComponentNode> predicate = new ComponentNodeDefinitionPredicate(Set.of(pythonProcessorDefinition));
        assertTrue(predicate.test(componentNode));
    }

    @Test
    public void testPythonComponentNodeTypeDoesNotMatchPythonDefinition() {
        final ComponentNode componentNode = mock(ComponentNode.class);
        when(componentNode.getComponentType()).thenReturn(OTHER_PYTHON_PROCESSOR_TYPE);
        when(componentNode.getBundleCoordinate()).thenReturn(PYTHON_BUNDLE_COORDINATE);

        final Predicate<ComponentNode> predicate = new ComponentNodeDefinitionPredicate(Set.of(pythonProcessorDefinition));
        assertFalse(predicate.test(componentNode));
    }

    @Test
    public void testPythonComponentNodeTypeDoesNotMatchStandardDefinition() {
        final ComponentNode componentNode = mock(ComponentNode.class);
        when(componentNode.getComponentType()).thenReturn(PYTHON_PROCESSOR_TYPE);
        when(componentNode.getBundleCoordinate()).thenReturn(PYTHON_BUNDLE_COORDINATE);

        final Predicate<ComponentNode> predicate = new ComponentNodeDefinitionPredicate(Set.of(standardProcessorDefinition));
        assertFalse(predicate.test(componentNode));
    }
}
