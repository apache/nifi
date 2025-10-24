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
package org.apache.nifi.web.api.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.xml.bind.annotation.XmlType;

/**
 * A reference to a connector property value, which includes the value and its type.
 * The value can be a string literal, a reference to an asset, or a reference to a secret.
 */
@XmlType(name = "connectorValueReference")
public class ConnectorValueReferenceDTO {

    private String valueType;
    private String value;
    private String assetIdentifier;
    private String secretProviderId;
    private String secretProviderName;
    private String secretName;

    /**
     * @return the type of value (STRING_LITERAL, ASSET_REFERENCE, or SECRET_REFERENCE)
     */
    @Schema(description = "The type of value (STRING_LITERAL, ASSET_REFERENCE, or SECRET_REFERENCE).")
    public String getValueType() {
        return valueType;
    }

    public void setValueType(final String valueType) {
        this.valueType = valueType;
    }

    /**
     * @return the string literal value when valueType is STRING_LITERAL
     */
    @Schema(description = "The string literal value. Applicable when valueType is STRING_LITERAL.")
    public String getValue() {
        return value;
    }

    public void setValue(final String value) {
        this.value = value;
    }

    /**
     * @return the asset identifier when valueType is ASSET_REFERENCE
     */
    @Schema(description = "The asset identifier. Applicable when valueType is ASSET_REFERENCE.")
    public String getAssetIdentifier() {
        return assetIdentifier;
    }

    public void setAssetIdentifier(final String assetIdentifier) {
        this.assetIdentifier = assetIdentifier;
    }

    /**
     * @return the secret provider identifier when valueType is SECRET_REFERENCE
     */
    @Schema(description = "The secret provider identifier. Applicable when valueType is SECRET_REFERENCE.")
    public String getSecretProviderId() {
        return secretProviderId;
    }

    public void setSecretProviderId(final String secretProviderId) {
        this.secretProviderId = secretProviderId;
    }

    /**
     * @return the secret provider name when valueType is SECRET_REFERENCE
     */
    @Schema(description = "The secret provider name. Applicable when valueType is SECRET_REFERENCE.")
    public String getSecretProviderName() {
        return secretProviderName;
    }

    public void setSecretProviderName(final String secretProviderName) {
        this.secretProviderName = secretProviderName;
    }

    /**
     * @return the secret name when valueType is SECRET_REFERENCE
     */
    @Schema(description = "The secret name. Applicable when valueType is SECRET_REFERENCE.")
    public String getSecretName() {
        return secretName;
    }

    public void setSecretName(final String secretName) {
        this.secretName = secretName;
    }
}

