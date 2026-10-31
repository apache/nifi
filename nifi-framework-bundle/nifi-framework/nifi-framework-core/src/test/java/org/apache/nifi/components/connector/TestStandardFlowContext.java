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

package org.apache.nifi.components.connector;

import org.apache.nifi.asset.Asset;
import org.apache.nifi.asset.AssetManager;
import org.apache.nifi.components.connector.components.FlowContextType;
import org.apache.nifi.components.connector.components.ParameterContextFacade;
import org.apache.nifi.components.connector.components.ParameterValue;
import org.apache.nifi.components.connector.components.ProcessGroupFacade;
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.VersionedAsset;
import org.apache.nifi.flow.VersionedParameter;
import org.apache.nifi.flow.VersionedParameterContext;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.parameter.ParameterContext;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.File;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestStandardFlowContext {

    @Test
    void testReloadReplacesStateAfterCreatingRootGroup() {
        final ProcessGroup managedProcessGroup = mock(ProcessGroup.class);
        final ProcessGroupFacadeFactory groupFacadeFactory = mock(ProcessGroupFacadeFactory.class);
        final ParameterContextFacadeFactory parameterContextFacadeFactory = mock(ParameterContextFacadeFactory.class);
        final ComponentLog originalLog = mock(ComponentLog.class);
        final ComponentLog replacementLog = mock(ComponentLog.class);
        final MutableConnectorConfigurationContext originalConfiguration = mock(MutableConnectorConfigurationContext.class);
        final MutableConnectorConfigurationContext replacementConfiguration = mock(MutableConnectorConfigurationContext.class);
        final MutableConnectorConfigurationContext failedConfiguration = mock(MutableConnectorConfigurationContext.class);
        final Bundle originalBundle = new Bundle("group", "artifact", "1.0.0");
        final Bundle replacementBundle = new Bundle("group", "artifact", "2.0.0");
        final Bundle failedBundle = new Bundle("group", "artifact", "3.0.0");
        final ProcessGroupFacade originalRootGroup = mock(ProcessGroupFacade.class);
        final ProcessGroupFacade replacementRootGroup = mock(ProcessGroupFacade.class);
        when(groupFacadeFactory.create(managedProcessGroup, originalLog)).thenReturn(originalRootGroup);
        when(groupFacadeFactory.create(managedProcessGroup, replacementLog)).thenReturn(replacementRootGroup);

        final StandardFlowContext flowContext = new StandardFlowContext(managedProcessGroup, originalConfiguration, groupFacadeFactory,
            parameterContextFacadeFactory, originalLog, FlowContextType.ACTIVE, originalBundle);

        flowContext.reload(replacementBundle, replacementLog, replacementConfiguration);

        assertSame(replacementBundle, flowContext.getBundle());
        assertSame(replacementConfiguration, flowContext.getConfigurationContext());
        assertSame(replacementRootGroup, flowContext.getRootGroup());

        final ComponentLog failedLog = mock(ComponentLog.class);
        when(groupFacadeFactory.create(managedProcessGroup, failedLog)).thenThrow(new IllegalStateException("Root group creation failed"));

        assertThrows(IllegalStateException.class, () -> flowContext.reload(failedBundle, failedLog, failedConfiguration));
        assertSame(replacementBundle, flowContext.getBundle());
        assertSame(replacementConfiguration, flowContext.getConfigurationContext());
        assertSame(replacementRootGroup, flowContext.getRootGroup());
    }

    @Test
    public void testRestorePersistedFlowResolvesParameterAssets() {
        final Asset asset = mock(Asset.class);
        when(asset.getFile()).thenReturn(new File("target/asset.txt"));
        final AssetManager assetManager = mock(AssetManager.class);
        when(assetManager.getAsset("asset-1")).thenReturn(Optional.of(asset));

        final ParameterContext managedParameterContext = mock(ParameterContext.class);
        when(managedParameterContext.getName()).thenReturn("managed-parameters");
        final ProcessGroup managedProcessGroup = mock(ProcessGroup.class);
        when(managedProcessGroup.getParameterContext()).thenReturn(managedParameterContext);

        final ParameterContextFacade parameterContextFacade = mock(ParameterContextFacade.class);
        final StandardFlowContext flowContext = new StandardFlowContext(managedProcessGroup,
            mock(MutableConnectorConfigurationContext.class), assetManager, mock(ProcessGroupFacadeFactory.class),
            group -> parameterContextFacade, mock(ComponentLog.class), FlowContextType.ACTIVE, new Bundle());

        final VersionedAsset versionedAsset = new VersionedAsset();
        versionedAsset.setIdentifier("asset-1");
        versionedAsset.setName("asset.txt");
        final VersionedParameter versionedParameter = new VersionedParameter();
        versionedParameter.setName("Asset Parameter");
        versionedParameter.setReferencedAssets(List.of(versionedAsset));
        final VersionedParameterContext versionedParameterContext = new VersionedParameterContext();
        versionedParameterContext.setIdentifier("parameter-context-1");
        versionedParameterContext.setName("managed-parameters");
        versionedParameterContext.setParameters(Set.of(versionedParameter));

        flowContext.restorePersistedFlow(new VersionedProcessGroup(), versionedParameterContext);

        final ArgumentCaptor<List<ParameterValue>> valuesCaptor = ArgumentCaptor.forClass(List.class);
        verify(parameterContextFacade).updateParameters(valuesCaptor.capture());
        final List<ParameterValue> restoredParameters = valuesCaptor.getValue();
        final ParameterValue restoredAssetParameter = restoredParameters.stream()
            .filter(parameter -> "Asset Parameter".equals(parameter.getName()))
            .findFirst()
            .orElseThrow();
        assertSame(asset, restoredAssetParameter.getAssets().getFirst());
    }
}
