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

import { Component, Input } from '@angular/core';
import { NgxSkeletonLoaderModule } from 'ngx-skeleton-loader';
import { ComponentType, NiFiCommon, NifiTooltipDirective, TextTip } from '@nifi/shared';
import {
    DefinedType,
    PropertyDependency,
    PropertyDescriptor,
    PropertyResourceDefinition,
    ResourceCardinality
} from '../../../state';
import { MatTableModule } from '@angular/material/table';
import { AllowableValue, DocumentedType } from '../../../../../state/shared';
import { Observable } from 'rxjs';
import { AsyncPipe } from '@angular/common';
import { MatExpansionModule } from '@angular/material/expansion';
import { NiFiState } from '../../../../../state';
import { Store } from '@ngrx/store';
import { selectServiceImplementations } from '../../../../../state/extension-types/extension-types.selectors';
import { RouterLink } from '@angular/router';

@Component({
    selector: 'property-definition',
    imports: [NgxSkeletonLoaderModule, MatTableModule, NifiTooltipDirective, AsyncPipe, MatExpansionModule, RouterLink],
    templateUrl: './property-definition.component.html',
    styleUrl: './property-definition.component.scss'
})
export class PropertyDefinitionComponent {
    @Input() set propertyDescriptor(propertyDescriptor: PropertyDescriptor) {
        this.descriptor = propertyDescriptor;

        if (propertyDescriptor.typeProvidedByValue) {
            const serviceType: DefinedType = propertyDescriptor.typeProvidedByValue;

            this.serviceImplementations$ = this.store.select(
                selectServiceImplementations(serviceType.type, {
                    group: serviceType.group,
                    artifact: serviceType.artifact,
                    version: serviceType.version
                })
            );
        }
    }

    @Input() lookupProperty: ((name: string) => PropertyDescriptor | undefined) | undefined;

    descriptor: PropertyDescriptor | null = null;
    serviceImplementations$: Observable<DocumentedType[]> | null = null;

    constructor(
        private store: Store<NiFiState>,
        private nifiCommon: NiFiCommon
    ) {}

    formatDefaultValue(descriptor: PropertyDescriptor): string | undefined {
        if (descriptor.allowableValues) {
            const defaultAllowableValue: AllowableValue | undefined = descriptor.allowableValues.find(
                (allowableValue) => {
                    return allowableValue.value === descriptor.defaultValue;
                }
            );

            if (defaultAllowableValue) {
                return defaultAllowableValue.displayName;
            }
        }

        return descriptor.defaultValue;
    }

    formatServiceApi(serviceType: DefinedType): string {
        return this.nifiCommon.getComponentTypeLabel(serviceType.type);
    }

    formatImplementationName(service: DocumentedType): string {
        return this.nifiCommon.getComponentTypeLabel(service.type);
    }

    sortDependencies(dependencies: PropertyDependency[]): PropertyDependency[] {
        return dependencies.slice().sort((a, b) => {
            return this.nifiCommon.compareString(a.propertyDisplayName, b.propertyDisplayName);
        });
    }

    formatDependentValue(dependency: PropertyDependency): string {
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

    formatResources(resourceDefinition: PropertyResourceDefinition): string {
        return resourceDefinition.resourceTypes.join(', ');
    }

    protected readonly ComponentType = ComponentType;
    protected readonly TextTip = TextTip;
    protected readonly ResourceCardinality = ResourceCardinality;
}
