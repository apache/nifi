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

import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ConfigurableExtensionDefinitionComponent } from './configurable-extension-definition.component';

import { ConfigurableExtensionDefinition } from '../../../state';

describe('ConfigurableExtensionDefinitionComponent', () => {
    let component: ConfigurableExtensionDefinitionComponent;
    let fixture: ComponentFixture<ConfigurableExtensionDefinitionComponent>;

    beforeEach(async () => {
        await TestBed.configureTestingModule({
            imports: [ConfigurableExtensionDefinitionComponent]
        }).compileComponents();

        fixture = TestBed.createComponent(ConfigurableExtensionDefinitionComponent);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });

    it('should display a deprecation notice when a custom reason is provided', () => {
        const definition: ConfigurableExtensionDefinition = {
            group: 'org.apache.nifi',
            artifact: 'nifi-standard-nar',
            version: '2.12.0',
            type: 'org.apache.nifi.processors.standard.GenerateFlowFile',
            typeDescription: 'Generate FlowFile processor',
            buildInfo: {
                revision: 'test'
            },
            additionalDetails: false,
            supportsDynamicProperties: false,
            supportsSensitiveDynamicProperties: false,
            deprecated: true,
            deprecationReason: 'This extension has been replaced.'
        };

        fixture.componentRef.setInput('configurableExtensionDefinition', definition);
        fixture.detectChanges();

        const element: HTMLElement = fixture.nativeElement;

        expect(element.textContent).toContain('Deprecation Notice');
        expect(element.textContent).toContain('This extension has been replaced.');
    });

    it('should display the default deprecation message when no reason is provided', () => {
        const definition: ConfigurableExtensionDefinition = {
            group: 'org.apache.nifi',
            artifact: 'nifi-standard-nar',
            version: '2.12.0',
            type: 'org.apache.nifi.processors.standard.GenerateFlowFile',
            typeDescription: 'Generate FlowFile processor',
            buildInfo: {
                revision: 'test'
            },
            additionalDetails: false,
            supportsDynamicProperties: false,
            supportsSensitiveDynamicProperties: false,
            deprecated: true
        };

        fixture.componentRef.setInput('configurableExtensionDefinition', definition);
        fixture.detectChanges();

        const element: HTMLElement = fixture.nativeElement;

        expect(element.textContent).toContain('Deprecation Notice');
        expect(element.textContent).toContain(
            'Please be aware this processor is deprecated and may be removed in the near future.'
        );
    });
});
