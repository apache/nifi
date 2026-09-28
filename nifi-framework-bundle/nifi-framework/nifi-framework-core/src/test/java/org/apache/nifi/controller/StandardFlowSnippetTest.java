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

import org.apache.nifi.annotation.behavior.AllowsAutoScheduling;
import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.bundle.BundleDetails;
import org.apache.nifi.nar.ExtensionDefinition;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.web.api.dto.BundleDTO;
import org.apache.nifi.web.api.dto.FlowSnippetDTO;
import org.apache.nifi.web.api.dto.ProcessorConfigDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class StandardFlowSnippetTest {

    @Test
    void testUnsupportedAutomaticProcessorRejectedBeforeInstantiation() {
        final BundleCoordinate coordinate = new BundleCoordinate("group", "artifact", "version");
        final BundleDTO bundle = new BundleDTO();
        bundle.setGroup(coordinate.getGroup());
        bundle.setArtifact(coordinate.getId());
        bundle.setVersion(coordinate.getVersion());

        final ProcessorConfigDTO config = new ProcessorConfigDTO();
        config.setSchedulingStrategy(SchedulingStrategy.AUTO.name());
        final ProcessorDTO processor = new ProcessorDTO();
        processor.setId("processor-id");
        processor.setName("Unsupported Processor");
        processor.setType(UnsupportedAutomaticProcessor.class.getName());
        processor.setBundle(bundle);
        processor.setConfig(config);
        final FlowSnippetDTO snippet = new FlowSnippetDTO();
        snippet.setProcessors(Set.of(processor));

        final ExtensionDefinition definition = mock(ExtensionDefinition.class);
        when(definition.getImplementationClassName()).thenReturn(processor.getType());
        final BundleDetails bundleDetails = mock(BundleDetails.class);
        when(bundleDetails.getCoordinate()).thenReturn(coordinate);
        final Bundle installedBundle = mock(Bundle.class);
        when(installedBundle.getBundleDetails()).thenReturn(bundleDetails);
        final ExtensionManager extensionManager = mock(ExtensionManager.class);
        when(extensionManager.getExtensions(Processor.class)).thenReturn(Set.of(definition));
        when(extensionManager.getBundles(processor.getType())).thenReturn(List.of(installedBundle));

        final StandardFlowSnippet flowSnippet = new StandardFlowSnippet(snippet, extensionManager);
        final IllegalStateException unresolvedException = assertThrows(IllegalStateException.class, flowSnippet::verifyComponentTypesInSnippet);
        assertEquals("Processor Unsupported Processor [processor-id] cannot use scheduling strategy AUTO because its automatic scheduling capability could not be resolved",
                unresolvedException.getMessage());

        when(extensionManager.getTempComponent(processor.getType(), coordinate)).thenReturn(mock(UnsupportedAutomaticProcessor.class));
        final IllegalStateException exception = assertThrows(IllegalStateException.class, flowSnippet::verifyComponentTypesInSnippet);

        assertEquals("Processor Unsupported Processor [processor-id] cannot use scheduling strategy AUTO because its implementation disables automatic scheduling",
                exception.getMessage());
    }

    @AllowsAutoScheduling(false)
    private abstract static class UnsupportedAutomaticProcessor implements Processor {
    }
}
