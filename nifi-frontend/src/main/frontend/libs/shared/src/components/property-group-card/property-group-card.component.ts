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

import { Component, computed, input } from '@angular/core';
import { MatCard } from '@angular/material/card';
import { MatError } from '@angular/material/form-field';
import {
    AssetReference,
    ConfigVerificationResult,
    ConnectorPropertyDescriptor,
    PropertyGroupConfiguration
} from '../../types';

/**
 * Read-only display card for a property group's configured values.
 * Used in the connector configuration summary step.
 */
@Component({
    selector: 'property-group-card',
    standalone: true,
    imports: [MatCard, MatError],
    template: `
        <mat-card appearance="outlined" class="p-4">
            @if (!hideGroupName()) {
                <h4 class="font-semibold mb-4">{{ propertyGroup().propertyGroupName }}</h4>
            }
            <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-x-6 gap-y-4">
                @for (propertyName of getPropertyNames(); track propertyName) {
                    <div class="flex flex-col gap-y-1">
                        <div class="flex items-center pt-1">
                            <span class="text-sm tertiary-color leading-5">{{ propertyName }}</span>
                            @if (isRequired(propertyName)) {
                                <span class="error-color ml-1 text-sm">*</span>
                            }
                        </div>
                        @if (hasValue(propertyName)) {
                            <span class="text-sm">{{ getDisplayValueForProperty(propertyName) }}</span>
                        } @else {
                            <span class="unset neutral-color text-sm">No value set</span>
                        }
                        @if (getFieldError(propertyName); as errorMessage) {
                            <mat-error class="error-color text-xs">{{ errorMessage }}</mat-error>
                        }
                    </div>
                }
            </div>
        </mat-card>
    `
})
export class PropertyGroupCard {
    readonly propertyGroup = input.required<PropertyGroupConfiguration>();
    readonly hideGroupName = input(false);
    readonly verificationErrors = input<ConfigVerificationResult[]>([]);

    readonly fieldErrors = computed(() => {
        const errors = this.verificationErrors();
        const group = this.propertyGroup();
        if (!errors.length || !group) return {} as Record<string, string>;
        const propertyNames = new Set(Object.keys(group.propertyDescriptors || {}));
        return errors
            .filter((e) => e.subject && propertyNames.has(e.subject))
            .reduce(
                (acc, e) => {
                    acc[e.subject!] = e.explanation;
                    return acc;
                },
                {} as Record<string, string>
            );
    });

    getPropertyNames(): string[] {
        return Object.keys(this.propertyGroup().propertyDescriptors || {});
    }

    getDescriptor(propertyName: string): ConnectorPropertyDescriptor | undefined {
        return this.propertyGroup().propertyDescriptors[propertyName];
    }

    isRequired(propertyName: string): boolean {
        return this.getDescriptor(propertyName)?.required ?? false;
    }

    getFieldError(propertyName: string): string | null {
        return this.fieldErrors()[propertyName] ?? null;
    }

    hasValue(propertyName: string): boolean {
        const valueRef = this.propertyGroup().propertyValues?.[propertyName];
        if (!valueRef) return false;
        if (valueRef.valueType === 'SECRET_REFERENCE') return true;
        if (valueRef.valueType === 'ASSET_REFERENCE') {
            return (valueRef.assetReferences?.length ?? 0) > 0;
        }
        return valueRef.value !== null && valueRef.value !== undefined && valueRef.value !== '';
    }

    getDisplayValueForProperty(propertyName: string): string {
        const valueRef = this.propertyGroup().propertyValues?.[propertyName];
        if (!valueRef) return '';
        if (valueRef.valueType === 'SECRET_REFERENCE') return '••••••••';
        if (valueRef.valueType === 'ASSET_REFERENCE') {
            const refs = valueRef.assetReferences;
            return refs?.map((r: AssetReference) => r.name || r.id).join(', ') || '';
        }
        return valueRef.value ?? '';
    }
}
