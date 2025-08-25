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

import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ComboEditor } from './combo-editor.component';
import { PropertyItem } from '../../property-item';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { Parameter } from '@nifi/shared';

describe('ComboEditor', () => {
    let component: ComboEditor;
    let fixture: ComponentFixture<ComboEditor>;

    let item: PropertyItem | null = null;
    const parameters: Parameter[] = [
        {
            name: 'one',
            description: 'Description for one.',
            sensitive: false,
            value: 'value',
            provided: false,
            referencingComponents: [],
            parameterContext: {
                id: '95d4f3d2-018b-1000-b7c7-b830c49a8026',
                permissions: {
                    canRead: true,
                    canWrite: true
                },
                component: {
                    id: '95d4f3d2-018b-1000-b7c7-b830c49a8026',
                    name: 'params 1'
                }
            },
            inherited: false
        },
        {
            name: 'two',
            description: 'Description for two.',
            sensitive: false,
            value: 'value',
            provided: false,
            referencingComponents: [],
            parameterContext: {
                id: '95d4f3d2-018b-1000-b7c7-b830c49a8026',
                permissions: {
                    canRead: true,
                    canWrite: true
                },
                component: {
                    id: '95d4f3d2-018b-1000-b7c7-b830c49a8026',
                    name: 'params 1'
                }
            },
            inherited: false
        }
    ];

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [ComboEditor, NoopAnimationsModule]
        });
        fixture = TestBed.createComponent(ComboEditor);
        component = fixture.componentInstance;

        // re-establish the item before each test execution
        item = {
            property: 'Destination',
            value: 'flowfile-attribute',
            descriptor: {
                name: 'Destination',
                displayName: 'Destination',
                description:
                    "Control if JSON value is written as a new flowfile attribute 'JSONAttributes' or written in the flowfile content. Writing to flowfile content will overwrite any existing flowfile content.",
                defaultValue: 'flowfile-attribute',
                allowableValues: [
                    {
                        allowableValue: {
                            displayName: 'flowfile-attribute',
                            value: 'flowfile-attribute'
                        },
                        canRead: true
                    },
                    {
                        allowableValue: {
                            displayName: 'flowfile-content',
                            value: 'flowfile-content'
                        },
                        canRead: true
                    }
                ],
                required: true,
                sensitive: false,
                dynamic: false,
                supportsEl: false,
                expressionLanguageScope: 'Not Supported',
                dependencies: []
            },
            id: 2,
            triggerEdit: false,
            deleted: false,
            added: false,
            dirty: false,
            savedValue: 'flowfile-attribute',
            type: 'required'
        };
    });

    it('should create', () => {
        if (item) {
            component.item = item;
            component.parameterConfig = {
                supportsParameters: false,
                parameters: null
            };

            fixture.detectChanges();

            expect(component).toBeTruthy();
        }
    });

    it('verify combo value', () => {
        if (item) {
            component.item = item;
            component.parameterConfig = {
                supportsParameters: false,
                parameters: null
            };

            fixture.detectChanges();

            const formValue = component.comboEditorForm.get('value')?.value;
            expect(component.itemLookup.get(formValue)?.value).toEqual(item.value);
            expect(component.comboEditorForm.get('parameterReference')).toBeNull();

            jest.spyOn(component.ok, 'next');
            component.okClicked();
            expect(component.ok.next).toHaveBeenCalledWith(item.value);
        }
    });

    it('verify combo not required with null value and default', () => {
        if (item) {
            item.value = null;
            item.descriptor.required = false;

            component.item = item;
            component.parameterConfig = {
                supportsParameters: false,
                parameters: null
            };

            fixture.detectChanges();

            const formValue = component.comboEditorForm.get('value')?.value;
            expect(component.itemLookup.get(formValue)?.value).toEqual(item.descriptor.defaultValue);
            expect(component.comboEditorForm.get('parameterReference')).toBeNull();

            jest.spyOn(component.ok, 'next');
            component.okClicked();
            expect(component.ok.next).toHaveBeenCalledWith(item.descriptor.defaultValue);
        }
    });

    it('verify combo not required with null value and no default', () => {
        if (item) {
            item.value = null;
            item.descriptor.required = false;
            item.descriptor.defaultValue = undefined;

            component.item = item;
            component.parameterConfig = {
                supportsParameters: false,
                parameters: null
            };

            fixture.detectChanges();

            const formValue = component.comboEditorForm.get('value')?.value;
            expect(component.itemLookup.get(formValue)?.value).toEqual(item.value);
            expect(component.comboEditorForm.get('parameterReference')).toBeNull();

            jest.spyOn(component.ok, 'next');
            component.okClicked();
            expect(component.ok.next).toHaveBeenCalledWith(item.value);
        }
    });

    it('verify combo with parameter reference', async () => {
        if (item) {
            item.value = '#{one}';

            component.item = item;
            component.parameterConfig = {
                supportsParameters: true,
                parameters
            };

            fixture.detectChanges();
            await fixture.whenStable();

            const formValue = component.comboEditorForm.get('value')?.value;
            expect(component.itemLookup.get(Number(formValue))?.value).toBeNull();
            expect(component.comboEditorForm.get('parameterReference')).toBeDefined();

            const parameterReferenceValue = component.comboEditorForm.get('parameterReference')?.value;
            expect(component.itemLookup.get(Number(parameterReferenceValue))?.value).toEqual(item.value);

            jest.spyOn(component.ok, 'next');
            component.okClicked();
            expect(component.ok.next).toHaveBeenCalledWith(item.value);
        }
    });

    it('verify combo with missing parameter reference', async () => {
        if (item) {
            item.value = '#{three}';

            component.item = item;
            component.parameterConfig = {
                supportsParameters: true,
                parameters
            };

            fixture.detectChanges();
            await fixture.whenStable();

            const formValue = component.comboEditorForm.get('value')?.value;
            expect(component.itemLookup.get(Number(formValue))?.value).toBeNull();
            expect(component.comboEditorForm.get('parameterReference')).toBeDefined();

            // since the value does not match any parameters it should match the first
            const firstParameterValue = '#{' + parameters[0].name + '}';

            const parameterReferenceValue = component.comboEditorForm.get('parameterReference')?.value;
            expect(component.itemLookup.get(Number(parameterReferenceValue))?.value).toEqual(firstParameterValue);

            jest.spyOn(component.ok, 'next');
            component.okClicked();
            expect(component.ok.next).toHaveBeenCalledWith(firstParameterValue);
        }
    });
});
