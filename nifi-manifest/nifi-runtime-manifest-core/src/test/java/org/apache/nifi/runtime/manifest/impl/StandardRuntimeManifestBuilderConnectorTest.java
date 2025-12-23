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
package org.apache.nifi.runtime.manifest.impl;

import org.apache.nifi.c2.protocol.component.api.ConfigurationStep;
import org.apache.nifi.c2.protocol.component.api.ConfigurationStepDependency;
import org.apache.nifi.c2.protocol.component.api.ConnectorDefinition;
import org.apache.nifi.c2.protocol.component.api.ConnectorPropertyDescriptor;
import org.apache.nifi.c2.protocol.component.api.ConnectorPropertyGroup;
import org.apache.nifi.c2.protocol.component.api.ConnectorPropertyType;
import org.apache.nifi.c2.protocol.component.api.RuntimeManifest;
import org.apache.nifi.extension.manifest.AllowableValue;
import org.apache.nifi.extension.manifest.ConnectorProperty;
import org.apache.nifi.extension.manifest.ConnectorPropertyDependency;
import org.apache.nifi.extension.manifest.Extension;
import org.apache.nifi.extension.manifest.ExtensionManifest;
import org.apache.nifi.extension.manifest.ExtensionType;
import org.apache.nifi.extension.manifest.ParentNar;
import org.apache.nifi.runtime.manifest.ExtensionManifestContainer;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StandardRuntimeManifestBuilderConnectorTest {

    private static final String TEST_CONNECTOR_TYPE = "org.apache.nifi.connectors.TestConnector";
    private static final String TEST_GROUP = "org.apache.nifi";
    private static final String TEST_ARTIFACT = "nifi-test-nar";
    private static final String TEST_VERSION = "2.0.0";
    private static final String STEP_NAME = "Connection Settings";
    private static final String STEP_DESCRIPTION = "Configure connection parameters";
    private static final String GROUP_NAME = "Authentication";
    private static final String GROUP_DESCRIPTION = "Authentication settings";
    private static final String PROPERTY_NAME = "username";
    private static final String PROPERTY_DESCRIPTION = "The username for authentication";
    private static final String PROPERTY_DEFAULT = "admin";

    @Test
    void testConnectorDefinitionIsBuiltFromExtensionManifest() {
        final ExtensionManifest extensionManifest = createExtensionManifest();
        final Extension connectorExtension = createConnectorExtension();
        extensionManifest.setExtensions(List.of(connectorExtension));

        final ExtensionManifestContainer container = new ExtensionManifestContainer(extensionManifest, null);
        final RuntimeManifest runtimeManifest = new StandardRuntimeManifestBuilder()
                .addBundle(container)
                .build();

        assertNotNull(runtimeManifest);
        assertNotNull(runtimeManifest.getBundles());
        assertEquals(1, runtimeManifest.getBundles().size());

        final List<ConnectorDefinition> connectors = runtimeManifest.getBundles().get(0).getComponentManifest().getConnectors();
        assertNotNull(connectors);
        assertEquals(1, connectors.size());

        final ConnectorDefinition connector = connectors.get(0);
        assertEquals(TEST_CONNECTOR_TYPE, connector.getType());
    }

    @Test
    void testConfigurationStepsAreConverted() {
        final ExtensionManifest extensionManifest = createExtensionManifest();
        final Extension connectorExtension = createConnectorExtensionWithConfigurationSteps();
        extensionManifest.setExtensions(List.of(connectorExtension));

        final ExtensionManifestContainer container = new ExtensionManifestContainer(extensionManifest, null);
        final RuntimeManifest runtimeManifest = new StandardRuntimeManifestBuilder()
                .addBundle(container)
                .build();

        final ConnectorDefinition connector = runtimeManifest.getBundles().get(0).getComponentManifest().getConnectors().get(0);
        assertNotNull(connector.getConfigurationSteps());
        assertEquals(1, connector.getConfigurationSteps().size());

        final ConfigurationStep step = connector.getConfigurationSteps().get(0);
        assertEquals(STEP_NAME, step.getName());
        assertEquals(STEP_DESCRIPTION, step.getDescription());
    }

    @Test
    void testConfigurationStepDependenciesAreConverted() {
        final ExtensionManifest extensionManifest = createExtensionManifest();
        final Extension connectorExtension = createConnectorExtensionWithStepDependencies();
        extensionManifest.setExtensions(List.of(connectorExtension));

        final ExtensionManifestContainer container = new ExtensionManifestContainer(extensionManifest, null);
        final RuntimeManifest runtimeManifest = new StandardRuntimeManifestBuilder()
                .addBundle(container)
                .build();

        final ConfigurationStep step = runtimeManifest.getBundles().get(0).getComponentManifest().getConnectors().get(0).getConfigurationSteps().get(0);
        assertNotNull(step.getStepDependencies());
        assertEquals(1, step.getStepDependencies().size());

        final ConfigurationStepDependency dependency = step.getStepDependencies().get(0);
        assertEquals("previousStep", dependency.getStepName());
        assertEquals("connectionType", dependency.getPropertyName());
        assertEquals(List.of("advanced"), dependency.getDependentValues());
    }

    @Test
    void testPropertyGroupsAreConverted() {
        final ExtensionManifest extensionManifest = createExtensionManifest();
        final Extension connectorExtension = createConnectorExtensionWithPropertyGroups();
        extensionManifest.setExtensions(List.of(connectorExtension));

        final ExtensionManifestContainer container = new ExtensionManifestContainer(extensionManifest, null);
        final RuntimeManifest runtimeManifest = new StandardRuntimeManifestBuilder()
                .addBundle(container)
                .build();

        final ConfigurationStep step = runtimeManifest.getBundles().get(0).getComponentManifest().getConnectors().get(0).getConfigurationSteps().get(0);
        assertNotNull(step.getPropertyGroups());
        assertEquals(1, step.getPropertyGroups().size());

        final ConnectorPropertyGroup group = step.getPropertyGroups().get(0);
        assertEquals(GROUP_NAME, group.getName());
        assertEquals(GROUP_DESCRIPTION, group.getDescription());
    }

    @Test
    void testConnectorPropertiesAreConverted() {
        final ExtensionManifest extensionManifest = createExtensionManifest();
        final Extension connectorExtension = createConnectorExtensionWithProperties();
        extensionManifest.setExtensions(List.of(connectorExtension));

        final ExtensionManifestContainer container = new ExtensionManifestContainer(extensionManifest, null);
        final RuntimeManifest runtimeManifest = new StandardRuntimeManifestBuilder()
                .addBundle(container)
                .build();

        final ConnectorPropertyGroup group = runtimeManifest.getBundles().get(0).getComponentManifest().getConnectors().get(0)
                .getConfigurationSteps().get(0).getPropertyGroups().get(0);
        assertNotNull(group.getProperties());
        assertEquals(1, group.getProperties().size());

        final ConnectorPropertyDescriptor property = group.getProperties().get(0);
        assertEquals(PROPERTY_NAME, property.getName());
        assertEquals(PROPERTY_DESCRIPTION, property.getDescription());
        assertEquals(PROPERTY_DEFAULT, property.getDefaultValue());
        assertTrue(property.isRequired());
        assertFalse(property.isAllowableValuesFetchable());
        assertEquals(ConnectorPropertyType.STRING, property.getPropertyType());
    }

    @Test
    void testConnectorPropertyAllowableValuesAreConverted() {
        final ExtensionManifest extensionManifest = createExtensionManifest();
        final Extension connectorExtension = createConnectorExtensionWithAllowableValues();
        extensionManifest.setExtensions(List.of(connectorExtension));

        final ExtensionManifestContainer container = new ExtensionManifestContainer(extensionManifest, null);
        final RuntimeManifest runtimeManifest = new StandardRuntimeManifestBuilder()
                .addBundle(container)
                .build();

        final ConnectorPropertyDescriptor property = runtimeManifest.getBundles().get(0).getComponentManifest().getConnectors().get(0)
                .getConfigurationSteps().get(0).getPropertyGroups().get(0).getProperties().get(0);
        assertNotNull(property.getAllowableValues());
        assertEquals(2, property.getAllowableValues().size());
        assertEquals("option1", property.getAllowableValues().get(0).getValue());
        assertEquals("Option 1", property.getAllowableValues().get(0).getDisplayName());
    }

    @Test
    void testConnectorPropertyDependenciesAreConverted() {
        final ExtensionManifest extensionManifest = createExtensionManifest();
        final Extension connectorExtension = createConnectorExtensionWithPropertyDependencies();
        extensionManifest.setExtensions(List.of(connectorExtension));

        final ExtensionManifestContainer container = new ExtensionManifestContainer(extensionManifest, null);
        final RuntimeManifest runtimeManifest = new StandardRuntimeManifestBuilder()
                .addBundle(container)
                .build();

        final ConnectorPropertyDescriptor property = runtimeManifest.getBundles().get(0).getComponentManifest().getConnectors().get(0)
                .getConfigurationSteps().get(0).getPropertyGroups().get(0).getProperties().get(0);
        assertNotNull(property.getDependencies());
        assertEquals(1, property.getDependencies().size());
        assertEquals("authType", property.getDependencies().get(0).getPropertyName());
        assertEquals(List.of("basic", "oauth"), property.getDependencies().get(0).getDependentValues());
    }

    private ExtensionManifest createExtensionManifest() {
        final ExtensionManifest manifest = new ExtensionManifest();
        manifest.setGroupId(TEST_GROUP);
        manifest.setArtifactId(TEST_ARTIFACT);
        manifest.setVersion(TEST_VERSION);

        final ParentNar parentNar = new ParentNar();
        parentNar.setGroupId("org.apache.nifi");
        parentNar.setArtifactId("nifi-standard-services-api-nar");
        parentNar.setVersion(TEST_VERSION);
        manifest.setParentNar(parentNar);

        return manifest;
    }

    private Extension createConnectorExtension() {
        final Extension extension = new Extension();
        extension.setName(TEST_CONNECTOR_TYPE);
        extension.setType(ExtensionType.CONNECTOR);
        return extension;
    }

    private Extension createConnectorExtensionWithConfigurationSteps() {
        final Extension extension = createConnectorExtension();

        final org.apache.nifi.extension.manifest.ConfigurationStep step = new org.apache.nifi.extension.manifest.ConfigurationStep();
        step.setName(STEP_NAME);
        step.setDescription(STEP_DESCRIPTION);
        extension.setConfigurationSteps(List.of(step));

        return extension;
    }

    private Extension createConnectorExtensionWithStepDependencies() {
        final Extension extension = createConnectorExtension();

        final org.apache.nifi.extension.manifest.ConfigurationStepDependency dependency = new org.apache.nifi.extension.manifest.ConfigurationStepDependency();
        dependency.setStepName("previousStep");
        dependency.setPropertyName("connectionType");
        dependency.setDependentValues(List.of("advanced"));

        final org.apache.nifi.extension.manifest.ConfigurationStep step = new org.apache.nifi.extension.manifest.ConfigurationStep();
        step.setName(STEP_NAME);
        step.setDescription(STEP_DESCRIPTION);
        step.setStepDependencies(List.of(dependency));
        extension.setConfigurationSteps(List.of(step));

        return extension;
    }

    private Extension createConnectorExtensionWithPropertyGroups() {
        final Extension extension = createConnectorExtension();

        final org.apache.nifi.extension.manifest.ConnectorPropertyGroup group = new org.apache.nifi.extension.manifest.ConnectorPropertyGroup();
        group.setName(GROUP_NAME);
        group.setDescription(GROUP_DESCRIPTION);

        final org.apache.nifi.extension.manifest.ConfigurationStep step = new org.apache.nifi.extension.manifest.ConfigurationStep();
        step.setName(STEP_NAME);
        step.setDescription(STEP_DESCRIPTION);
        step.setPropertyGroups(List.of(group));
        extension.setConfigurationSteps(List.of(step));

        return extension;
    }

    private Extension createConnectorExtensionWithProperties() {
        final Extension extension = createConnectorExtension();

        final ConnectorProperty property = new ConnectorProperty();
        property.setName(PROPERTY_NAME);
        property.setDescription(PROPERTY_DESCRIPTION);
        property.setDefaultValue(PROPERTY_DEFAULT);
        property.setRequired(true);
        property.setAllowableValuesFetchable(false);
        property.setPropertyType(org.apache.nifi.extension.manifest.ConnectorPropertyType.STRING);

        final org.apache.nifi.extension.manifest.ConnectorPropertyGroup group = new org.apache.nifi.extension.manifest.ConnectorPropertyGroup();
        group.setName(GROUP_NAME);
        group.setDescription(GROUP_DESCRIPTION);
        group.setProperties(List.of(property));

        final org.apache.nifi.extension.manifest.ConfigurationStep step = new org.apache.nifi.extension.manifest.ConfigurationStep();
        step.setName(STEP_NAME);
        step.setDescription(STEP_DESCRIPTION);
        step.setPropertyGroups(List.of(group));
        extension.setConfigurationSteps(List.of(step));

        return extension;
    }

    private Extension createConnectorExtensionWithAllowableValues() {
        final Extension extension = createConnectorExtension();

        final AllowableValue value1 = new AllowableValue();
        value1.setValue("option1");
        value1.setDisplayName("Option 1");
        value1.setDescription("First option");

        final AllowableValue value2 = new AllowableValue();
        value2.setValue("option2");
        value2.setDisplayName("Option 2");
        value2.setDescription("Second option");

        final ConnectorProperty property = new ConnectorProperty();
        property.setName(PROPERTY_NAME);
        property.setDescription(PROPERTY_DESCRIPTION);
        property.setAllowableValues(List.of(value1, value2));

        final org.apache.nifi.extension.manifest.ConnectorPropertyGroup group = new org.apache.nifi.extension.manifest.ConnectorPropertyGroup();
        group.setName(GROUP_NAME);
        group.setProperties(List.of(property));

        final org.apache.nifi.extension.manifest.ConfigurationStep step = new org.apache.nifi.extension.manifest.ConfigurationStep();
        step.setName(STEP_NAME);
        step.setPropertyGroups(List.of(group));
        extension.setConfigurationSteps(List.of(step));

        return extension;
    }

    private Extension createConnectorExtensionWithPropertyDependencies() {
        final Extension extension = createConnectorExtension();

        final ConnectorPropertyDependency dependency = new ConnectorPropertyDependency();
        dependency.setPropertyName("authType");
        dependency.setDependentValues(List.of("basic", "oauth"));

        final ConnectorProperty property = new ConnectorProperty();
        property.setName(PROPERTY_NAME);
        property.setDescription(PROPERTY_DESCRIPTION);
        property.setDependencies(List.of(dependency));

        final org.apache.nifi.extension.manifest.ConnectorPropertyGroup group = new org.apache.nifi.extension.manifest.ConnectorPropertyGroup();
        group.setName(GROUP_NAME);
        group.setProperties(List.of(property));

        final org.apache.nifi.extension.manifest.ConfigurationStep step = new org.apache.nifi.extension.manifest.ConfigurationStep();
        step.setName(STEP_NAME);
        step.setPropertyGroups(List.of(group));
        extension.setConfigurationSteps(List.of(step));

        return extension;
    }
}

