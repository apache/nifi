/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import {
    AssetReference,
    buildSecretKey,
    ConnectorPropertyFormValue,
    ConnectorValueReference,
    PropertyType
} from '../types';

/**
 * Options for creating a secret value reference
 */
export interface SecretReferenceOptions {
    /** The secret name */
    secretName: string;
    /** The secret provider identifier */
    providerId: string;
    /** The secret provider name */
    providerName: string;
    /** The fully qualified secret name (includes group) */
    fullyQualifiedSecretName: string;
}

/**
 * Creates a ConnectorValueReference from a primitive form value.
 * Supports STRING_LITERAL, ASSET_REFERENCE, and SECRET_REFERENCE value types.
 *
 * For STRING_LIST properties, arrays are joined into comma-separated strings.
 * For ASSET properties, creates an ASSET_REFERENCE with the asset ID(s).
 * For ASSET_LIST properties, creates an ASSET_REFERENCE with multiple asset IDs.
 * For SECRET properties, use the secretOptions parameter to provide secret details.
 *
 * @param value The primitive value from the form
 * @param propertyType Optional property type - determines how to structure the value reference
 * @param secretOptions Optional secret details for SECRET_REFERENCE values
 * @returns A ConnectorValueReference suitable for the API
 */
export function toValueReference(
    value: ConnectorPropertyFormValue,
    propertyType?: PropertyType,
    secretOptions?: SecretReferenceOptions
): ConnectorValueReference {
    // For SECRET type with secret details, use SECRET_REFERENCE value type
    if (propertyType === 'SECRET' && secretOptions) {
        return {
            valueType: 'SECRET_REFERENCE',
            secretName: secretOptions.secretName,
            secretProviderId: secretOptions.providerId,
            secretProviderName: secretOptions.providerName,
            fullyQualifiedSecretName: secretOptions.fullyQualifiedSecretName
        };
    }

    // SECRET without secretOptions: clear if empty, preserve composite key if present
    if (propertyType === 'SECRET' && !secretOptions) {
        return {
            value: value != null && value !== '' ? value.toString() : null,
            valueType: 'STRING_LITERAL'
        };
    }

    // Handle ASSET type - single asset reference
    if (propertyType === 'ASSET') {
        if (value && typeof value === 'string') {
            return {
                valueType: 'ASSET_REFERENCE',
                assetReferences: [{ id: value }]
            };
        }
        return {
            value: null,
            valueType: 'STRING_LITERAL'
        };
    }

    // Handle ASSET_LIST type - multiple asset references
    if (propertyType === 'ASSET_LIST') {
        if (Array.isArray(value) && value.length > 0) {
            return {
                valueType: 'ASSET_REFERENCE',
                assetReferences: (value as string[]).map((id) => ({ id }))
            };
        }
        return {
            value: null,
            valueType: 'STRING_LITERAL'
        };
    }

    // Handle STRING_LIST - join array into comma-separated string
    // Null means "clear the property / revert to default" (distinct from empty string)
    let stringValue: string | null;
    if (propertyType === 'STRING_LIST' && Array.isArray(value)) {
        stringValue = value.length > 0 ? value.join(',') : null;
    } else {
        stringValue = value != null && value !== '' ? value.toString() : null;
    }

    return {
        value: stringValue,
        valueType: 'STRING_LITERAL'
    };
}

/**
 * Extracts the primitive value from a ConnectorValueReference.
 * Handles backward compatibility when the API returns a plain primitive value
 * instead of a ConnectorValueReference object.
 *
 * For STRING_LIST properties, comma-separated values are split into an array
 * for use with multi-select form controls.
 *
 * @param valueRef The ConnectorValueReference from the API, or a plain primitive value
 * @param propertyType Optional property type - when 'STRING_LIST', splits comma-separated values into array
 * @returns The primitive value suitable for display
 */
export function fromValueReference(
    valueRef: ConnectorValueReference | string | number | boolean | null | undefined,
    propertyType?: PropertyType
): ConnectorPropertyFormValue | undefined {
    if (valueRef === null || valueRef === undefined) {
        return undefined;
    }

    let rawValue: ConnectorPropertyFormValue | undefined;

    // Backward compatibility: if the API returns a plain primitive value,
    // return it directly without transformation
    if (typeof valueRef === 'number') {
        rawValue = String(valueRef);
    } else if (typeof valueRef === 'string' || typeof valueRef === 'boolean') {
        rawValue = valueRef;
    } else {
        // Handle ConnectorValueReference object
        switch (valueRef.valueType) {
            case 'STRING_LITERAL':
                rawValue = valueRef.value;
                break;
            case 'ASSET_REFERENCE': {
                const legacyId = (valueRef as ConnectorValueReference & { assetIdentifier?: string }).assetIdentifier;
                rawValue = valueRef.assetReferences ?? (legacyId ? legacyId : undefined);
                break;
            }
            case 'SECRET_REFERENCE':
                // Return composite key to uniquely identify the secret
                // Format: providerId::providerName::fullyQualifiedName
                rawValue = buildSecretKey(
                    valueRef.secretProviderId,
                    valueRef.secretProviderName,
                    valueRef.fullyQualifiedSecretName
                );
                break;
            default:
                // Fallback for unknown types - try to get value
                rawValue = valueRef.value;
        }
    }

    // For ASSET, return single reference or null
    if (propertyType === 'ASSET') {
        if (Array.isArray(rawValue) && rawValue.length > 0) {
            return rawValue[0];
        }
        // Handle legacy assetIdentifier format
        if (typeof rawValue === 'string') {
            return { id: rawValue } as AssetReference;
        }
        return null;
    }

    // For ASSET_LIST, return array (empty if none)
    if (propertyType === 'ASSET_LIST') {
        if (Array.isArray(rawValue)) {
            return rawValue;
        }
        // Handle legacy assetIdentifier format
        if (typeof rawValue === 'string') {
            return [{ id: rawValue }] as AssetReference[];
        }
        return [];
    }

    // For STRING_LIST, split comma-separated string into array for multi-select
    if (propertyType === 'STRING_LIST' && typeof rawValue === 'string') {
        return rawValue
            ? rawValue
                  .split(',')
                  .map((v) => v.trim())
                  .filter((v) => v !== '')
            : [];
    }

    return rawValue;
}
