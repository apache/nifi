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
package org.apache.nifi.authorization;

import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.components.connector.ConnectorValueType;
import org.apache.nifi.web.api.dto.AssetReferenceDTO;
import org.apache.nifi.web.api.dto.ConfigurationStepConfigurationDTO;
import org.apache.nifi.web.api.dto.ConnectorValueReferenceDTO;
import org.apache.nifi.web.api.dto.PropertyGroupConfigurationDTO;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AuthorizeConnectorConfigReferencesTest {

    private static final String CONNECTOR_ID = "connector-1";
    private static final String SECRET_PROVIDER_ID = "parameter-provider-1";
    private static final String SECRET_PROVIDER_NAME = "Vault Provider";
    private static final String SECRET_NAME = "api-key";
    private static final String FULLY_QUALIFIED_SECRET_NAME = SECRET_PROVIDER_NAME + "." + SECRET_NAME;
    private static final String ASSET_ID = "asset-1";
    private static final String PROPERTY_GROUP_NAME = "authentication";
    private static final String PROPERTY_NAME = "password";

    @Mock
    private Authorizer authorizer;

    @Mock
    private AuthorizableLookup lookup;

    @Mock
    private Authorizable connectorAuthorizable;

    @Mock
    private Authorizable secretProviderAuthorizable;

    @Mock
    private Authorizable assetAuthorizable;

    @Test
    void testAuthorizesConnectorWriteWithNoReferences() {
        when(lookup.getConnector(eq(CONNECTOR_ID))).thenReturn(connectorAuthorizable);

        AuthorizeConnectorConfigReferences.authorize(authorizer, lookup, CONNECTOR_ID, new ConfigurationStepConfigurationDTO());

        verify(connectorAuthorizable).authorize(eq(authorizer), eq(RequestAction.WRITE), any());
    }

    @Test
    void testAuthorizesSecretReferenceReadOnProvider() {
        when(lookup.getConnector(eq(CONNECTOR_ID))).thenReturn(connectorAuthorizable);
        when(lookup.getConnectorSecretProvider(eq(SECRET_PROVIDER_ID), eq(SECRET_PROVIDER_NAME), eq(FULLY_QUALIFIED_SECRET_NAME)))
                .thenReturn(secretProviderAuthorizable);

        final ConfigurationStepConfigurationDTO configurationStep = configurationStepWithValue(secretReference());
        AuthorizeConnectorConfigReferences.authorize(authorizer, lookup, CONNECTOR_ID, configurationStep);

        verify(connectorAuthorizable).authorize(eq(authorizer), eq(RequestAction.WRITE), any());
        verify(secretProviderAuthorizable).authorize(eq(authorizer), eq(RequestAction.READ), any());
    }

    @Test
    void testAuthorizesAssetReferenceReadOnOwningConnector() {
        when(lookup.getConnector(eq(CONNECTOR_ID))).thenReturn(connectorAuthorizable);
        when(lookup.getConnectorAsset(eq(CONNECTOR_ID), eq(ASSET_ID))).thenReturn(assetAuthorizable);

        final ConfigurationStepConfigurationDTO configurationStep = configurationStepWithValue(assetReference());
        AuthorizeConnectorConfigReferences.authorize(authorizer, lookup, CONNECTOR_ID, configurationStep);

        verify(connectorAuthorizable).authorize(eq(authorizer), eq(RequestAction.WRITE), any());
        verify(assetAuthorizable).authorize(eq(authorizer), eq(RequestAction.READ), any());
    }

    @Test
    void testIgnoresStringLiterals() {
        when(lookup.getConnector(eq(CONNECTOR_ID))).thenReturn(connectorAuthorizable);

        final ConnectorValueReferenceDTO stringLiteral = new ConnectorValueReferenceDTO();
        stringLiteral.setValueType(ConnectorValueType.STRING_LITERAL.toString());
        stringLiteral.setValue("plain-text");

        AuthorizeConnectorConfigReferences.authorize(authorizer, lookup, CONNECTOR_ID, configurationStepWithValue(stringLiteral));

        verify(connectorAuthorizable).authorize(eq(authorizer), eq(RequestAction.WRITE), any());
        verify(lookup, never()).getConnectorSecretProvider(any(), any(), any());
        verify(lookup, never()).getConnectorAsset(any(), any());
    }

    @Test
    void testConnectorWriteDeniedSkipsReferenceResolution() {
        when(lookup.getConnector(eq(CONNECTOR_ID))).thenReturn(connectorAuthorizable);
        doThrow(new AccessDeniedException("denied")).when(connectorAuthorizable).authorize(eq(authorizer), eq(RequestAction.WRITE), any());

        final ConfigurationStepConfigurationDTO configurationStep = configurationStepWithValue(secretReference());

        assertThrows(AccessDeniedException.class,
                () -> AuthorizeConnectorConfigReferences.authorize(authorizer, lookup, CONNECTOR_ID, configurationStep));

        verify(lookup, never()).getConnectorSecretProvider(any(), any(), any());
    }

    @Test
    void testSecretProviderReadDenied() {
        when(lookup.getConnector(eq(CONNECTOR_ID))).thenReturn(connectorAuthorizable);
        when(lookup.getConnectorSecretProvider(eq(SECRET_PROVIDER_ID), eq(SECRET_PROVIDER_NAME), eq(FULLY_QUALIFIED_SECRET_NAME)))
                .thenReturn(secretProviderAuthorizable);
        doThrow(new AccessDeniedException("denied")).when(secretProviderAuthorizable).authorize(eq(authorizer), eq(RequestAction.READ), any());

        final ConfigurationStepConfigurationDTO configurationStep = configurationStepWithValue(secretReference());

        assertThrows(AccessDeniedException.class,
                () -> AuthorizeConnectorConfigReferences.authorize(authorizer, lookup, CONNECTOR_ID, configurationStep));
    }

    @Test
    void testUnknownValueTypeRejected() {
        when(lookup.getConnector(eq(CONNECTOR_ID))).thenReturn(connectorAuthorizable);

        final ConnectorValueReferenceDTO unknown = new ConnectorValueReferenceDTO();
        unknown.setValueType("NOT_A_REAL_TYPE");

        final ConfigurationStepConfigurationDTO configurationStep = configurationStepWithValue(unknown);

        assertThrows(IllegalArgumentException.class,
                () -> AuthorizeConnectorConfigReferences.authorize(authorizer, lookup, CONNECTOR_ID, configurationStep));
    }

    @Test
    void testRejectsNullAssetReferences() {
        when(lookup.getConnector(eq(CONNECTOR_ID))).thenReturn(connectorAuthorizable);

        final ConnectorValueReferenceDTO valueReference = new ConnectorValueReferenceDTO();
        valueReference.setValueType(ConnectorValueType.ASSET_REFERENCE.toString());
        valueReference.setAssetReferences(null);

        assertThrows(IllegalArgumentException.class,
                () -> AuthorizeConnectorConfigReferences.authorize(authorizer, lookup, CONNECTOR_ID, configurationStepWithValue(valueReference)));

        verify(lookup, never()).getConnectorAsset(any(), any());
    }

    @Test
    void testRejectsEmptyAssetReferences() {
        when(lookup.getConnector(eq(CONNECTOR_ID))).thenReturn(connectorAuthorizable);

        final ConnectorValueReferenceDTO valueReference = new ConnectorValueReferenceDTO();
        valueReference.setValueType(ConnectorValueType.ASSET_REFERENCE.toString());
        valueReference.setAssetReferences(List.of());

        assertThrows(IllegalArgumentException.class,
                () -> AuthorizeConnectorConfigReferences.authorize(authorizer, lookup, CONNECTOR_ID, configurationStepWithValue(valueReference)));

        verify(lookup, never()).getConnectorAsset(any(), any());
    }

    @Test
    void testRejectsNullAssetReferenceId() {
        when(lookup.getConnector(eq(CONNECTOR_ID))).thenReturn(connectorAuthorizable);

        final ConnectorValueReferenceDTO valueReference = new ConnectorValueReferenceDTO();
        valueReference.setValueType(ConnectorValueType.ASSET_REFERENCE.toString());
        valueReference.setAssetReferences(List.of(new AssetReferenceDTO()));

        assertThrows(IllegalArgumentException.class,
                () -> AuthorizeConnectorConfigReferences.authorize(authorizer, lookup, CONNECTOR_ID, configurationStepWithValue(valueReference)));

        verify(lookup, never()).getConnectorAsset(any(), any());
    }

    private ConfigurationStepConfigurationDTO configurationStepWithValue(final ConnectorValueReferenceDTO valueReference) {
        final PropertyGroupConfigurationDTO propertyGroup = new PropertyGroupConfigurationDTO();
        propertyGroup.setPropertyGroupName(PROPERTY_GROUP_NAME);
        propertyGroup.setPropertyValues(Map.of(PROPERTY_NAME, valueReference));

        final ConfigurationStepConfigurationDTO configurationStep = new ConfigurationStepConfigurationDTO();
        configurationStep.setPropertyGroupConfigurations(List.of(propertyGroup));
        return configurationStep;
    }

    private ConnectorValueReferenceDTO secretReference() {
        final ConnectorValueReferenceDTO valueReference = new ConnectorValueReferenceDTO();
        valueReference.setValueType(ConnectorValueType.SECRET_REFERENCE.toString());
        valueReference.setSecretProviderId(SECRET_PROVIDER_ID);
        valueReference.setSecretProviderName(SECRET_PROVIDER_NAME);
        valueReference.setSecretName(SECRET_NAME);
        valueReference.setFullyQualifiedSecretName(FULLY_QUALIFIED_SECRET_NAME);
        return valueReference;
    }

    private ConnectorValueReferenceDTO assetReference() {
        final ConnectorValueReferenceDTO valueReference = new ConnectorValueReferenceDTO();
        valueReference.setValueType(ConnectorValueType.ASSET_REFERENCE.toString());
        valueReference.setAssetReferences(List.of(new AssetReferenceDTO(ASSET_ID)));
        return valueReference;
    }
}
