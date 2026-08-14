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
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.components.connector.ConnectorValueType;
import org.apache.nifi.web.api.dto.AssetReferenceDTO;
import org.apache.nifi.web.api.dto.ConfigurationStepConfigurationDTO;
import org.apache.nifi.web.api.dto.ConnectorValueReferenceDTO;
import org.apache.nifi.web.api.dto.PropertyGroupConfigurationDTO;

import java.util.List;
import java.util.Map;

/**
 * Authorizes a proposed Connector configuration step and the resources referenced by its property values.
 * Requires {@code WRITE} on the connector itself. In addition, referencing a Secret requires {@code READ} on
 * the backing Parameter Provider, and referencing an Asset requires {@code READ} on the connector that owns
 * the Asset, which enforces that the Asset belongs to the connector being configured.
 */
public final class AuthorizeConnectorConfigReferences {

    private AuthorizeConnectorConfigReferences() {
    }

    /**
     * Authorize a proposed Connector configuration step, including any resources it references.
     *
     * @param authorizer Authorizer used for determining results
     * @param lookup Authorizable Lookup used to resolve the connector and any referenced Secrets and Assets
     * @param connectorId the identifier of the connector whose configuration is being applied or verified
     * @param configurationStep the proposed configuration step, whose property values are inspected for references
     */
    public static void authorize(
            final Authorizer authorizer,
            final AuthorizableLookup lookup,
            final String connectorId,
            final ConfigurationStepConfigurationDTO configurationStep
    ) {
        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        lookup.getConnector(connectorId).authorize(authorizer, RequestAction.WRITE, user);

        if (configurationStep == null || configurationStep.getPropertyGroupConfigurations() == null) {
            return;
        }

        for (final PropertyGroupConfigurationDTO propertyGroup : configurationStep.getPropertyGroupConfigurations()) {
            final Map<String, ConnectorValueReferenceDTO> propertyValues = propertyGroup.getPropertyValues();
            if (propertyValues == null) {
                continue;
            }

            for (final ConnectorValueReferenceDTO valueReference : propertyValues.values()) {
                authorizeValueReference(authorizer, lookup, connectorId, valueReference, user);
            }
        }
    }

    private static void authorizeValueReference(
            final Authorizer authorizer,
            final AuthorizableLookup lookup,
            final String connectorId,
            final ConnectorValueReferenceDTO valueReference,
            final NiFiUser user
    ) {
        if (valueReference == null || valueReference.getValueType() == null) {
            return;
        }

        final ConnectorValueType valueType;
        try {
            valueType = ConnectorValueType.valueOf(valueReference.getValueType());
        } catch (final IllegalArgumentException e) {
            throw new IllegalArgumentException("Unknown Connector Value Type: " + valueReference.getValueType());
        }

        switch (valueType) {
            case STRING_LITERAL -> {
                // String literals do not reference any externally authorized resource.
            }
            case ASSET_REFERENCE -> authorizeAssetReferences(authorizer, lookup, connectorId, valueReference.getAssetReferences(), user);
            case SECRET_REFERENCE -> authorizeSecretReference(authorizer, lookup, valueReference, user);
        }
    }

    private static void authorizeAssetReferences(
            final Authorizer authorizer,
            final AuthorizableLookup lookup,
            final String connectorId,
            final List<AssetReferenceDTO> assetReferences,
            final NiFiUser user
    ) {
        if (assetReferences == null || assetReferences.isEmpty()) {
            throw new IllegalArgumentException("Asset references must be specified when value type is ASSET_REFERENCE");
        }

        for (final AssetReferenceDTO assetReference : assetReferences) {
            if (assetReference == null) {
                throw new IllegalArgumentException("Asset reference must be specified");
            }

            final String assetId = assetReference.getId();
            if (assetId == null || assetId.isEmpty()) {
                throw new IllegalArgumentException("Asset reference identifier must be specified");
            }

            final Authorizable assetAuthorizable = lookup.getConnectorAsset(connectorId, assetId);
            assetAuthorizable.authorize(authorizer, RequestAction.READ, user);
        }
    }

    private static void authorizeSecretReference(
            final Authorizer authorizer,
            final AuthorizableLookup lookup,
            final ConnectorValueReferenceDTO valueReference,
            final NiFiUser user
    ) {
        final Authorizable secretProvider = lookup.getConnectorSecretProvider(
                valueReference.getSecretProviderId(),
                valueReference.getSecretProviderName(),
                valueReference.getFullyQualifiedSecretName());
        secretProvider.authorize(authorizer, RequestAction.READ, user);
    }
}
