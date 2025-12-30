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

package org.apache.nifi.minifi.commons.service;

import org.apache.nifi.controller.flow.VersionedDataflow;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class StandardFlowPropertyAssetReferenceResolverServiceTest {

    private Map<String, String> processorProperties;
    private Map<String, String> controllerServiceProperties;

    @Mock
    private Function<String, Optional<Path>> assetPathResolver;

    @InjectMocks
    private StandardFlowPropertyAssetReferenceResolverService victim;

    @Test
    public void testResolveAssetReferenceProperties() {
        initProperties();
        VersionedDataflow dataFlow = aVersionedDataflow();

        when(assetPathResolver.apply("asset1")).thenReturn(Optional.of(Path.of("asset1Path")));
        when(assetPathResolver.apply("asset2")).thenReturn(Optional.of(Path.of("asset2Path")));

        victim.resolveAssetReferenceProperties(dataFlow);

        verifyProperties();
    }

    @Test
    public void testResolveNestedAssetReferenceProperties() {
        initProperties();
        VersionedDataflow dataFlow = aVersionedDataflowWithNestedProcessGroup();

        when(assetPathResolver.apply("asset1")).thenReturn(Optional.of(Path.of("asset1Path")));
        when(assetPathResolver.apply("asset2")).thenReturn(Optional.of(Path.of("asset2Path")));

        victim.resolveAssetReferenceProperties(dataFlow);

        verifyProperties();
    }

    @Test
    public void testResolveAssetReferencePropertiesThrowIllegalStateException() {
        initProperties();
        VersionedDataflow dataFlow = aVersionedDataflow();

        when(assetPathResolver.apply("asset1")).thenReturn(Optional.empty());

        assertThrows(IllegalStateException.class, () -> {
            victim.resolveAssetReferenceProperties(dataFlow);
        });
    }

    private void initProperties() {
        processorProperties = new HashMap<>();
        processorProperties.put("assetReferenceProperty", "@{asset-id:asset1}");
        processorProperties.put("notAssetReferenceProperty", "some value1");

        controllerServiceProperties = new HashMap<>();
        controllerServiceProperties.put("assetReferenceProperty", "@{asset-id:asset2}");
        controllerServiceProperties.put("notAssetReferenceProperty", "some value2");
    }

    private void verifyProperties() {
        assertEquals(processorProperties.get("assetReferenceProperty"), "asset1Path");
        assertEquals(processorProperties.get("notAssetReferenceProperty"), "some value1");
        assertEquals(controllerServiceProperties.get("assetReferenceProperty"), "asset2Path");
        assertEquals(controllerServiceProperties.get("notAssetReferenceProperty"), "some value2");
    }

    private VersionedDataflow aVersionedDataflow() {
        VersionedDataflow versionedDataflow = new VersionedDataflow();
        versionedDataflow.setRootGroup(aVersionedProcessGroup());
        return versionedDataflow;
    }

    private VersionedDataflow aVersionedDataflowWithNestedProcessGroup() {
        VersionedDataflow versionedDataflow = new VersionedDataflow();
        VersionedProcessGroup versionedProcessGroup = new VersionedProcessGroup();
        versionedProcessGroup.setProcessGroups(Set.of(aVersionedProcessGroup()));
        versionedDataflow.setRootGroup(versionedProcessGroup);

        return versionedDataflow;
    }

    private VersionedProcessGroup aVersionedProcessGroup() {
        VersionedProcessGroup versionedProcessGroup = new VersionedProcessGroup();
        VersionedProcessor versionedProcessor = new VersionedProcessor();
        VersionedControllerService versionedControllerService = new VersionedControllerService();

        versionedControllerService.setProperties(controllerServiceProperties);
        versionedProcessor.setProperties(processorProperties);

        versionedProcessGroup.setProcessors(Set.of(versionedProcessor));
        versionedProcessGroup.setControllerServices(Set.of(versionedControllerService));

        return versionedProcessGroup;
    }
}
