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
package org.apache.nifi.tests.system.connectors;

import org.apache.nifi.components.connector.ConnectorState;
import org.apache.nifi.nar.NarState;
import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.tests.system.nar.NarUploadUtil;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.nifi.connectors.ChangeVersionConnector;
import org.apache.nifi.web.api.dto.ConfigurationStepConfigurationDTO;
import org.apache.nifi.web.api.dto.ConnectorConfigurationDTO;
import org.apache.nifi.web.api.dto.ConnectorValueReferenceDTO;
import org.apache.nifi.web.api.dto.NarSummaryDTO;
import org.apache.nifi.web.api.dto.PropertyGroupConfigurationDTO;
import org.apache.nifi.web.api.entity.ConnectorEntity;
import org.apache.nifi.web.api.entity.ProcessGroupFlowEntity;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConnectorChangeVersionIT extends NiFiSystemIT {

    private static final String NARS_LOCATION = "target/nifi-connector-change-version-nars";
    private static final String VERSION_ONE_NAR_ID = "nifi-connector-change-version-v1-nar";
    private static final String VERSION_TWO_NAR_ID = "nifi-connector-change-version-v2-nar";
    private static final String CONNECTOR_TYPE = "org.apache.nifi.connectors.tests.system.changeversion.ChangeVersionTestConnector";
    private static final String BUNDLE_GROUP = "org.apache.nifi";
    private static final String BUNDLE_ARTIFACT = "nifi-connector-change-version-nar";
    private static final String VERSION_ONE = "1.0.0";
    private static final String VERSION_TWO = "2.0.0";
    private static final String SETTINGS_STEP = "Settings";
    private static final String SHARED_PROPERTY = "Shared Property";
    private static final String NEW_PROPERTY = "New Property";
    private static final String CUSTOM_SHARED_VALUE = "kept-across-reload";
    private static final String VERSION_ONE_FLOW_NAME = "Change Version Flow v1";
    private static final String VERSION_TWO_DEFAULT = "version-two-default";

    @Override
    protected boolean isDestroyEnvironmentAfterEachTest() {
        return true;
    }

    @Test
    public void testChangeConnectorNarVersionPreservesManagedFlowAndOverlappingConfiguration() throws Exception {
        final NarUploadUtil narUploadUtil = new NarUploadUtil(getNifiClient());
        uploadNar(narUploadUtil, VERSION_ONE_NAR_ID);
        uploadNar(narUploadUtil, VERSION_TWO_NAR_ID);

        ConnectorEntity connector = getClientUtil().createConnector(CONNECTOR_TYPE, BUNDLE_GROUP, BUNDLE_ARTIFACT, VERSION_ONE);
        getClientUtil().waitForValidConnector(connector.getId());
        connector = getNifiClient().getConnectorClient().getConnector(connector.getId());
        assertEquals(VERSION_ONE, connector.getComponent().getBundle().getVersion());
        assertTrue(connector.getComponent().getMultipleVersionsAvailable());
        assertEquals(Boolean.FALSE, connector.getComponent().getExtensionMissing());

        getClientUtil().configureConnector(connector, SETTINGS_STEP, Map.of(SHARED_PROPERTY, CUSTOM_SHARED_VALUE));
        getClientUtil().applyConnectorUpdate(connector);
        connector = getNifiClient().getConnectorClient().getConnector(connector.getId());
        assertEquals(ConnectorState.STOPPED.name(), connector.getComponent().getState());

        final String managedProcessGroupId = connector.getComponent().getManagedProcessGroupId();
        final String managedProcessGroupName = getManagedProcessGroupName(connector);
        assertEquals(VERSION_ONE_FLOW_NAME, managedProcessGroupName);

        getClientUtil().waitForValidConnector(connector.getId());
        getClientUtil().startConnector(connector.getId());
        connector = getNifiClient().getConnectorClient().getConnector(connector.getId());
        assertEquals(ConnectorState.RUNNING.name(), connector.getComponent().getState());

        changeConnectorVersion(VERSION_ONE, VERSION_TWO);

        getClientUtil().waitForConnectorState(connector.getId(), ConnectorState.RUNNING);
        connector = getNifiClient().getConnectorClient().getConnector(connector.getId());
        assertEquals(VERSION_TWO, connector.getComponent().getBundle().getVersion());
        assertEquals(ConnectorState.RUNNING.name(), connector.getComponent().getState());
        assertEquals(Boolean.FALSE, connector.getComponent().getExtensionMissing());
        assertEquals(managedProcessGroupId, connector.getComponent().getManagedProcessGroupId());
        assertEquals(VERSION_ONE_FLOW_NAME, getManagedProcessGroupName(connector));
        assertEquals(CUSTOM_SHARED_VALUE, getPropertyValue(connector.getComponent().getActiveConfiguration(), SHARED_PROPERTY));
        assertEquals(VERSION_TWO_DEFAULT, getPropertyValue(connector.getComponent().getActiveConfiguration(), NEW_PROPERTY));

        getClientUtil().waitForValidConnector(connector.getId());
    }

    private NarSummaryDTO uploadNar(final NarUploadUtil narUploadUtil, final String narIdentifier) throws Exception {
        final File narsLocation = new File(NARS_LOCATION);
        if (!narsLocation.exists()) {
            throw new IllegalStateException("NARs location does not exist at: " + narsLocation.getAbsolutePath());
        }

        final NarSummaryDTO nar = narUploadUtil.uploadNar(narsLocation, narIdentifier);
        waitFor(narUploadUtil.getWaitForNarStateSupplier(nar.getIdentifier(), NarState.INSTALLED));
        return nar;
    }

    private void changeConnectorVersion(final String currentVersion, final String targetVersion) throws Exception {
        final Properties properties = new Properties();
        properties.setProperty(CommandOption.EXT_BUNDLE_GROUP.getLongName(), BUNDLE_GROUP);
        properties.setProperty(CommandOption.EXT_BUNDLE_ARTIFACT.getLongName(), BUNDLE_ARTIFACT);
        properties.setProperty(CommandOption.EXT_BUNDLE_VERSION.getLongName(), targetVersion);
        properties.setProperty(CommandOption.EXT_QUALIFIED_NAME.getLongName(), CONNECTOR_TYPE);
        properties.setProperty(CommandOption.EXT_BUNDLE_CURRENT_VERSION.getLongName(), currentVersion);

        new ChangeVersionConnector().doExecute(getNifiClient(), properties);
    }

    private String getManagedProcessGroupName(final ConnectorEntity connector) throws Exception {
        final ProcessGroupFlowEntity flowEntity = getNifiClient().getConnectorClient().getFlow(connector.getId());
        return flowEntity.getProcessGroupFlow().getBreadcrumb().getBreadcrumb().getName();
    }

    private String getPropertyValue(final ConnectorConfigurationDTO configuration, final String propertyName) {
        assertNotNull(configuration);
        assertNotNull(configuration.getConfigurationStepConfigurations());
        for (final ConfigurationStepConfigurationDTO stepConfiguration : configuration.getConfigurationStepConfigurations()) {
            if (!SETTINGS_STEP.equals(stepConfiguration.getConfigurationStepName())) {
                continue;
            }

            for (final PropertyGroupConfigurationDTO propertyGroup : stepConfiguration.getPropertyGroupConfigurations()) {
                final Map<String, ConnectorValueReferenceDTO> propertyValues = propertyGroup.getPropertyValues();
                if (propertyValues != null && propertyValues.containsKey(propertyName)) {
                    return propertyValues.get(propertyName).getValue();
                }
            }
        }

        throw new AssertionError("Property " + propertyName + " was not found");
    }
}
