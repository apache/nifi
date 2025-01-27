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
import { NiFiCommon } from '@nifi/shared';
import { ConfigurableExtensionDefinition, Stateful } from '../../../state';
import { PropertiesDefinitionComponent } from '../properties-definition/properties-definition.component';
import { DynamicPropertiesDefinitionComponent } from '../dynamic-properties-definition/dynamic-properties-definition.component';
import { RestrictionsDefinitionComponent } from '../restrictions-definition/restrictions-definition.component';
import { ResourceConsiderationsComponent } from '../resource-considerations/resource-considerations.component';
import { SeeAlsoComponent } from '../see-also/see-also.component';
import { AdditionalDetailsComponent } from '../additional-details/additional-details.component';

@Component({
    selector: 'configurable-extension-definition',
    imports: [
        PropertiesDefinitionComponent,
        DynamicPropertiesDefinitionComponent,
        RestrictionsDefinitionComponent,
        ResourceConsiderationsComponent,
        SeeAlsoComponent,
        AdditionalDetailsComponent
    ],
    templateUrl: './configurable-extension-definition.component.html',
    styleUrl: './configurable-extension-definition.component.scss'
})
export class ConfigurableExtensionDefinitionComponent {
    @Input() configurableExtensionDefinition: ConfigurableExtensionDefinition | null = null;

    constructor(private nifiCommon: NiFiCommon) {}

    formatExtensionName(extensionType: string): string {
        return this.nifiCommon.getComponentTypeLabel(extensionType);
    }

    formatStatefulScope(stateful: Stateful): string {
        return stateful.scopes.join(', ');
    }
}
