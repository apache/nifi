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

import {
    ConfigurationStepConfiguration,
    ConnectorPropertyDescriptor,
    ConnectorValueReference,
    PropertyGroupConfiguration,
    PropertyType,
    buildSecretKey
} from '../types';

/**
 * Check if a property has a meaningful value set.
 */
export function hasPropertyValue(
    valueRef: ConnectorValueReference | undefined,
    propertyType: PropertyType,
    defaultValue?: any
): boolean {
    if (propertyType === 'ASSET' || propertyType === 'ASSET_LIST') {
        if (!valueRef || typeof valueRef !== 'object') {
            return false;
        }
        const refs = (valueRef as ConnectorValueReference).assetReferences;
        return Boolean(refs && refs.length > 0);
    }

    if (propertyType === 'SECRET') {
        if (!valueRef || typeof valueRef !== 'object') {
            return false;
        }
        if (valueRef.valueType !== 'SECRET_REFERENCE') {
            return false;
        }
        const secretKey = buildSecretKey(
            valueRef.secretProviderId,
            valueRef.secretProviderName,
            valueRef.fullyQualifiedSecretName
        );
        return secretKey !== '';
    }

    let value: any;

    if (valueRef && typeof valueRef === 'object') {
        value = valueRef.value;
    }

    if (value === null || value === undefined) {
        value = defaultValue;
    }

    if (value === null || value === undefined) {
        return false;
    }

    if (typeof value === 'string' && value === '') {
        return false;
    }

    return true;
}

export function assetReferencesHaveMissingContent(
    valueRef: ConnectorValueReference | undefined,
    propertyType: PropertyType
): boolean {
    if (propertyType !== 'ASSET' && propertyType !== 'ASSET_LIST') {
        return false;
    }
    if (!valueRef || typeof valueRef !== 'object') {
        return false;
    }
    const refs = valueRef.assetReferences;
    if (!refs || refs.length === 0) {
        return false;
    }
    return refs.some((ref) => ref.missingContent === true);
}

function findPropertyDescriptor(
    propertyName: string,
    propertyGroups: PropertyGroupConfiguration[]
): ConnectorPropertyDescriptor | undefined {
    for (const group of propertyGroups) {
        const descriptor = group.propertyDescriptors?.[propertyName];
        if (descriptor) {
            return descriptor;
        }
    }
    return undefined;
}

function findPropertyValue(propertyName: string, propertyGroups: PropertyGroupConfiguration[]): string | null {
    for (const group of propertyGroups) {
        const valueRef = group.propertyValues?.[propertyName];
        if (valueRef !== undefined) {
            if (typeof valueRef === 'string') {
                return valueRef;
            }
            if (valueRef && typeof valueRef === 'object' && 'value' in valueRef) {
                const val = valueRef.value;
                return val !== null && val !== undefined ? String(val) : null;
            }
            return null;
        }
    }
    return null;
}

function evaluatePropertyVisibility(
    descriptor: ConnectorPropertyDescriptor,
    propertyGroups: PropertyGroupConfiguration[]
): boolean {
    if (!descriptor.dependencies || descriptor.dependencies.length === 0) {
        return true;
    }

    return descriptor.dependencies.every((dependency) => {
        const dependentDescriptor = findPropertyDescriptor(dependency.propertyName, propertyGroups);
        if (dependentDescriptor && !evaluatePropertyVisibility(dependentDescriptor, propertyGroups)) {
            return false;
        }

        const dependentValue = findPropertyValue(dependency.propertyName, propertyGroups);

        if (dependency.dependentValues && dependency.dependentValues.length > 0) {
            return dependentValue !== null && dependency.dependentValues.includes(dependentValue);
        }
        return dependentValue !== null && dependentValue !== '';
    });
}

export function isPropertyVisible(propertyName: string, stepPropertyGroups: PropertyGroupConfiguration[]): boolean {
    const descriptor = findPropertyDescriptor(propertyName, stepPropertyGroups);

    if (!descriptor) {
        return true;
    }

    return evaluatePropertyVisibility(descriptor, stepPropertyGroups);
}

export function filterPropertyVisibility(steps: ConfigurationStepConfiguration[]): ConfigurationStepConfiguration[] {
    return steps.map((step) => {
        const stepGroups = step.propertyGroupConfigurations;
        return {
            ...step,
            propertyGroupConfigurations: stepGroups.map((group) => {
                const filteredDescriptors: { [propertyName: string]: ConnectorPropertyDescriptor } = {};
                for (const [name, descriptor] of Object.entries(group.propertyDescriptors)) {
                    if (isPropertyVisible(name, stepGroups)) {
                        filteredDescriptors[name] = descriptor;
                    }
                }
                return { ...group, propertyDescriptors: filteredDescriptors };
            })
        };
    });
}
