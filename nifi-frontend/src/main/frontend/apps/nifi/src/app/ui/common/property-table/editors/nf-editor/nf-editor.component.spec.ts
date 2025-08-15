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
import { MockComponent } from 'ng-mocks';
import { FormBuilder } from '@angular/forms';

import { NfEditor } from './nf-editor.component';
import { PropertyItem } from '../../property-item';
import { ElService, CodemirrorNifiLanguageService } from '@nifi/shared';
import { Codemirror } from '@nifi/shared';
import { of } from 'rxjs';

describe('NfEditor', () => {
    let component: NfEditor;
    let fixture: ComponentFixture<NfEditor>;
    let mockNifiLanguagePackage: any;

    beforeEach(() => {
        mockNifiLanguagePackage = {
            setLanguageOptions: jest.fn(),
            getLanguageSupport: jest.fn().mockReturnValue({}),
            isValidParameter: jest.fn().mockReturnValue(true),
            isValidElFunction: jest.fn().mockReturnValue(true)
        };

        TestBed.configureTestingModule({
            imports: [NfEditor],
            providers: [
                {
                    provide: FormBuilder,
                    useValue: {
                        group: () => ({
                            patchValue: jest.fn(),
                            get: jest.fn().mockReturnValue({
                                value: 'test',
                                setValue: jest.fn(),
                                disable: jest.fn(),
                                enable: jest.fn(),
                                addValidators: jest.fn(),
                                removeValidators: jest.fn(),
                                updateValueAndValidity: jest.fn()
                            }),
                            dirty: false,
                            valid: true
                        })
                    }
                },
                {
                    provide: ElService,
                    useValue: {
                        getELSpecification: () => of([])
                    }
                },
                {
                    provide: CodemirrorNifiLanguageService,
                    useValue: mockNifiLanguagePackage
                }
            ]
        }).overrideComponent(NfEditor, {
            remove: {
                imports: [Codemirror]
            },
            add: {
                imports: [MockComponent(Codemirror)]
            }
        });
        fixture = TestBed.createComponent(NfEditor);
        component = fixture.componentInstance;
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });

    it('should inject CodemirrorNifiLanguageService service', () => {
        expect(component['nifiLanguageService']).toBeDefined();
        expect(component['nifiLanguageService']).toBe(mockNifiLanguagePackage);
    });

    it('should configure plugins with validation service when item supports EL', () => {
        // Setup component with EL support
        const mockItem: PropertyItem = {
            property: 'test.property',
            descriptor: {
                name: 'test.property',
                displayName: 'Test Property',
                description: 'A test property',
                required: false,
                sensitive: false,
                dynamic: false,
                supportsEl: true,
                expressionLanguageScope: 'FLOWFILE_ATTRIBUTES',
                dependencies: []
            },
            value: 'test value',
            id: 1,
            triggerEdit: false,
            deleted: false,
            added: false,
            dirty: false,
            savedValue: 'test value',
            type: 'optional'
        };
        component.item = mockItem;
        component.parameterConfig = {
            parameters: [{ name: 'testParam', value: 'testValue', description: '', sensitive: false }],
            supportsParameters: true
        };

        // Verify that the validation service was configured
        expect(component.supportsEl).toBe(true);
        expect(mockNifiLanguagePackage.setLanguageOptions).toHaveBeenCalledWith({
            functionsEnabled: true,
            parametersEnabled: true,
            parameters: [{ name: 'testParam', value: 'testValue', description: '', sensitive: false }]
        });
    });

    it('should configure plugins with validation service when item has parameters', () => {
        // Setup component with parameters
        const mockItem: PropertyItem = {
            property: 'test.property',
            descriptor: {
                name: 'test.property',
                displayName: 'Test Property',
                description: 'A test property',
                required: false,
                sensitive: false,
                dynamic: false,
                supportsEl: false,
                expressionLanguageScope: 'NONE',
                dependencies: []
            },
            value: 'test value',
            id: 1,
            triggerEdit: false,
            deleted: false,
            added: false,
            dirty: false,
            savedValue: 'test value',
            type: 'optional'
        };
        component.item = mockItem;
        component.parameterConfig = {
            parameters: [{ name: 'testParam', value: 'testValue', description: '', sensitive: false }],
            supportsParameters: true
        };

        // Verify that parameters are available for validation
        expect(component.parameters).toBeDefined();
        expect(component.parameters?.length).toBe(1);
        expect(mockNifiLanguagePackage.setLanguageOptions).toHaveBeenCalledWith({
            functionsEnabled: false,
            parametersEnabled: true,
            parameters: [{ name: 'testParam', value: 'testValue', description: '', sensitive: false }]
        });
    });

    it('should validate parameters using the validation service', () => {
        // Test that the service methods are accessible
        const result1 = mockNifiLanguagePackage.isValidParameter('testParam');
        const result2 = mockNifiLanguagePackage.isValidElFunction('uuid');

        expect(result1).toBe(true);
        expect(result2).toBe(true);
        expect(mockNifiLanguagePackage.isValidParameter).toHaveBeenCalledWith('testParam');
        expect(mockNifiLanguagePackage.isValidElFunction).toHaveBeenCalledWith('uuid');
    });
});
