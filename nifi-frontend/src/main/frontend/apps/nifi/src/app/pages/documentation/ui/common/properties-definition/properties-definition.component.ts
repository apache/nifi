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

import { Component, Input, viewChild } from '@angular/core';
import { ConfigurableExtensionDefinition, PropertyDescriptor } from '../../../state';
import { MatAccordion, MatExpansionModule } from '@angular/material/expansion';
import { MatButtonModule } from '@angular/material/button';
import { NiFiCommon } from '@nifi/shared';
import { PropertyDefinitionComponent } from '../property-definition/property-definition.component';

@Component({
    selector: 'properties-definition',
    imports: [MatAccordion, MatExpansionModule, MatButtonModule, PropertyDefinitionComponent],
    templateUrl: './properties-definition.component.html',
    styleUrl: './properties-definition.component.scss'
})
export class PropertiesDefinitionComponent {
    @Input() set configurableExtensionDefinition(configurableExtensionDefinition: ConfigurableExtensionDefinition) {
        if (configurableExtensionDefinition.propertyDescriptors) {
            this.propertyDescriptors = Object.values(configurableExtensionDefinition.propertyDescriptors).sort(
                (a, b) => {
                    return this.nifiCommon.compareString(a.displayName, b.displayName);
                }
            );
        }
    }

    propertyDescriptors: PropertyDescriptor[] | null = null;
    propertiesAccordion = viewChild.required(MatAccordion);

    constructor(private nifiCommon: NiFiCommon) {}

    formatTitle(descriptor: PropertyDescriptor): string {
        if (descriptor.required) {
            return `${descriptor.displayName}*`;
        }

        return descriptor.displayName;
    }

    expand(): void {
        this.propertiesAccordion().openAll();
    }

    collapse(): void {
        this.propertiesAccordion().closeAll();
    }

    lookupProperty(name: string): PropertyDescriptor | undefined {
        return this.propertyDescriptors?.find((propertyDescriptor) => name === propertyDescriptor.name);
    }
}
