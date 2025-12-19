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

import { Component, Input, inject } from '@angular/core';
import { NiFiCommon, NifiTooltipDirective, TextTip } from '@nifi/shared';
import { ConnectorPropertyDescriptor, ConnectorPropertyDependency } from '../../../state/connector-definition';
import { PropertyAllowableValue } from '../../../state';

@Component({
    selector: 'connector-property-definition',
    imports: [NifiTooltipDirective],
    templateUrl: './connector-property-definition.component.html',
    styleUrl: './connector-property-definition.component.scss'
})
export class ConnectorPropertyDefinitionComponent {
    private nifiCommon = inject(NiFiCommon);

    @Input() propertyDescriptor!: ConnectorPropertyDescriptor;
    @Input() lookupProperty: ((name: string) => ConnectorPropertyDescriptor | undefined) | undefined;

    protected readonly TextTip = TextTip;

    formatDefaultValue(): string | undefined {
        if (!this.propertyDescriptor.defaultValue) {
            return undefined;
        }

        if (this.propertyDescriptor.allowableValues) {
            const defaultAllowableValue: PropertyAllowableValue | undefined =
                this.propertyDescriptor.allowableValues.find(
                    (allowableValue) => allowableValue.value === this.propertyDescriptor.defaultValue
                );

            if (defaultAllowableValue) {
                return defaultAllowableValue.displayName;
            }
        }

        return this.propertyDescriptor.defaultValue;
    }

    formatPropertyType(): string {
        return this.propertyDescriptor.propertyType || 'STRING';
    }

    sortDependencies(dependencies: ConnectorPropertyDependency[]): ConnectorPropertyDependency[] {
        return dependencies.slice().sort((a, b) => {
            return this.nifiCommon.compareString(a.propertyName, b.propertyName);
        });
    }

    formatDependentValue(dependency: ConnectorPropertyDependency): string {
        if (dependency.dependentValues) {
            if (this.lookupProperty) {
                const propertyDependency = this.lookupProperty(dependency.propertyName);

                if (propertyDependency?.allowableValues) {
                    return dependency.dependentValues
                        .map((dependentValue) => {
                            const dependentAllowableValue = propertyDependency.allowableValues?.find(
                                (allowableValue) => dependentValue === allowableValue.value
                            );
                            return dependentAllowableValue ? dependentAllowableValue.displayName : dependentValue;
                        })
                        .join(', ');
                }
            }

            return dependency.dependentValues.join(', ');
        }

        return '';
    }
}
