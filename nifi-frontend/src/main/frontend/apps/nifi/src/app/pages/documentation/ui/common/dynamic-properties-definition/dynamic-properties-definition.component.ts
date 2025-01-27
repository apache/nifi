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
import { NiFiCommon } from '@nifi/shared';
import { ConfigurableExtensionDefinition, DynamicProperty, ExpressionLanguageScope } from '../../../state';
import { MatAccordion, MatExpansionModule } from '@angular/material/expansion';
import { MatIconButton } from '@angular/material/button';

@Component({
    selector: 'dynamic-properties-definition',
    imports: [MatAccordion, MatExpansionModule, MatIconButton],
    templateUrl: './dynamic-properties-definition.component.html',
    styleUrl: './dynamic-properties-definition.component.scss'
})
export class DynamicPropertiesDefinitionComponent {
    @Input() set configurableExtensionDefinition(configurableExtensionDefinition: ConfigurableExtensionDefinition) {
        if (configurableExtensionDefinition.dynamicProperties) {
            this.dynamicProperties = Object.values(configurableExtensionDefinition.dynamicProperties).sort((a, b) => {
                return this.nifiCommon.compareString(a.name, b.name);
            });

            this.supportsDynamicSensitiveProperties =
                configurableExtensionDefinition.supportsSensitiveDynamicProperties;
        } else {
            this.dynamicProperties = null;
            this.supportsDynamicSensitiveProperties = null;
        }
    }

    dynamicProperties: DynamicProperty[] | null = null;
    dynamicPropertiesAccordion = viewChild.required(MatAccordion);

    supportsDynamicSensitiveProperties: boolean | null = null;

    constructor(private nifiCommon: NiFiCommon) {}

    expand(): void {
        this.dynamicPropertiesAccordion().openAll();
    }

    collapse(): void {
        this.dynamicPropertiesAccordion().closeAll();
    }

    protected readonly ExpressionLanguageScope = ExpressionLanguageScope;
}
