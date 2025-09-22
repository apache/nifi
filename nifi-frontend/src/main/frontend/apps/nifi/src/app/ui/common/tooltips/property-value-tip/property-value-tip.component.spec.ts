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
import { PropertyValueTip } from './property-value-tip.component';
import { PropertyValueTipInput } from '../../../../state/shared';

describe('PropertyValueTip', () => {
    let component: PropertyValueTip;
    let fixture: ComponentFixture<PropertyValueTip>;

    beforeEach(async () => {
        await TestBed.configureTestingModule({
            imports: [PropertyValueTip]
        }).compileComponents();

        fixture = TestBed.createComponent(PropertyValueTip);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });

    describe('extractParameterReferences', () => {
        function buildDescriptor(overrides: Partial<any> = {}) {
            return {
                name: 'prop',
                displayName: 'Prop',
                description: 'desc',
                required: false,
                sensitive: false,
                dynamic: false,
                supportsEl: true,
                expressionLanguageScope: '',
                dependencies: [],
                ...overrides
            };
        }

        it('should return early when property is sensitive', () => {
            const data: PropertyValueTipInput = {
                property: {
                    property: 'prop',
                    value: "#{'PARAM_A'}",
                    descriptor: buildDescriptor({ sensitive: true })
                },
                parameters: [{ parameter: { name: 'PARAM_A', description: '', sensitive: false, value: '1' } }]
            };

            component.data = data;
            fixture.detectChanges();

            expect(component.parameterReferences.length).toBe(0);
        });

        it('should match quoted and unquoted parameter references', () => {
            const data: PropertyValueTipInput = {
                property: {
                    property: 'prop',
                    value: 'start #{PARAM_A} mid #{\'PARAM_B\'} end #{"PARAM_C"}',
                    descriptor: buildDescriptor()
                },
                parameters: [
                    { parameter: { name: 'PARAM_A', description: '', sensitive: false, value: 'a' } },
                    { parameter: { name: 'PARAM_B', description: '', sensitive: false, value: 'b' } },
                    { parameter: { name: 'PARAM_C', description: '', sensitive: false, value: 'c' } }
                ]
            };

            component.data = data;
            fixture.detectChanges();

            const names = component.parameterReferences.map((p) => p.name);
            expect(names).toEqual(['PARAM_A', 'PARAM_B', 'PARAM_C']);
        });

        it('should ignore sensitive parameters in regex construction', () => {
            const data: PropertyValueTipInput = {
                property: {
                    property: 'prop',
                    value: '#{PARAM_A} #{PARAM_SEC}',
                    descriptor: buildDescriptor()
                },
                parameters: [
                    { parameter: { name: 'PARAM_A', description: '', sensitive: false, value: 'a' } },
                    { parameter: { name: 'PARAM_SEC', description: '', sensitive: true, value: 'secret' } }
                ]
            };

            component.data = data;
            fixture.detectChanges();

            const names = component.parameterReferences.map((p) => p.name);
            expect(names).toEqual(['PARAM_A']);
        });

        it('should deduplicate multiple occurrences of the same parameter', () => {
            const data: PropertyValueTipInput = {
                property: {
                    property: 'prop',
                    value: "#{PARAM_A} and again #{'PARAM_A'}",
                    descriptor: buildDescriptor()
                },
                parameters: [{ parameter: { name: 'PARAM_A', description: '', sensitive: false, value: 'a' } }]
            };

            component.data = data;
            fixture.detectChanges();

            expect(component.parameterReferences.length).toBe(1);
            expect(component.parameterReferences[0].name).toBe('PARAM_A');
        });

        it('should deduplicate parameters while preserving different parameter references', () => {
            const data: PropertyValueTipInput = {
                property: {
                    property: 'prop',
                    value: "#{PARAM_A} then #{PARAM_B} and #{PARAM_A} again and #{'PARAM_B'}",
                    descriptor: buildDescriptor()
                },
                parameters: [
                    { parameter: { name: 'PARAM_A', description: '', sensitive: false, value: 'a' } },
                    { parameter: { name: 'PARAM_B', description: '', sensitive: false, value: 'b' } }
                ]
            };

            component.data = data;
            fixture.detectChanges();

            expect(component.parameterReferences.length).toBe(2);
            const names = component.parameterReferences.map((p) => p.name).sort();
            expect(names).toEqual(['PARAM_A', 'PARAM_B']);
        });

        it('should handle null or empty property values', () => {
            const dataNull: PropertyValueTipInput = {
                property: {
                    property: 'prop',
                    value: null,
                    descriptor: buildDescriptor()
                },
                parameters: [{ parameter: { name: 'PARAM_A', description: '', sensitive: false, value: 'a' } }]
            };

            component.data = dataNull;
            fixture.detectChanges();
            expect(component.parameterReferences.length).toBe(0);

            const dataEmpty: PropertyValueTipInput = {
                property: {
                    property: 'prop',
                    value: '',
                    descriptor: buildDescriptor()
                },
                parameters: [{ parameter: { name: 'PARAM_A', description: '', sensitive: false, value: 'a' } }]
            };

            component.data = dataEmpty;
            fixture.detectChanges();
            expect(component.parameterReferences.length).toBe(0);
        });
    });
});
