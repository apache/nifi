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

import { ElementRef } from '@angular/core';
import { CdkOverlayOrigin } from '@angular/cdk/overlay';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { PropertyTable } from './property-table.component';
import { PropertyItem } from './property-item';
import { ParameterContextEntity } from '../../../state/shared';

describe('PropertyTable', () => {
    let component: PropertyTable;
    let fixture: ComponentFixture<PropertyTable>;

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [PropertyTable]
        });
        fixture = TestBed.createComponent(PropertyTable);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });

    describe('optional goToParameter callback', () => {
        const mockItem: PropertyItem = {
            property: 'group.id',
            value: '#{some-param}',
            descriptor: {
                name: 'group.id',
                displayName: 'Group ID',
                description: '',
                required: true,
                sensitive: false,
                dynamic: false,
                supportsEl: true,
                expressionLanguageScope: 'Environment variables defined at JVM level and system properties',
                dependencies: []
            },
            id: 3,
            triggerEdit: false,
            deleted: false,
            added: false,
            dirty: false,
            savedValue: '#{some-param}',
            type: 'required'
        };

        const readableParameterContext: ParameterContextEntity = {
            id: 'ctx-1',
            uri: '/parameter-contexts/ctx-1',
            revision: { version: 0 },
            permissions: { canRead: true, canWrite: false }
        };

        it('should no-op goToParameterClicked when goToParameter callback is not supplied', () => {
            expect(component.goToParameter).toBeUndefined();
            expect(() => component.goToParameterClicked(mockItem)).not.toThrow();
        });

        it('should invoke goToParameter callback when supplied and item.value is non-null', () => {
            const goToParameterSpy = vi.fn();
            component.goToParameter = goToParameterSpy;

            component.goToParameterClicked(mockItem);

            expect(goToParameterSpy).toHaveBeenCalledWith('#{some-param}');
        });

        it('should not invoke goToParameter callback when item.value is null', () => {
            const goToParameterSpy = vi.fn();
            component.goToParameter = goToParameterSpy;

            const itemWithoutValue: PropertyItem = { ...mockItem, value: null };
            component.goToParameterClicked(itemWithoutValue);

            expect(goToParameterSpy).not.toHaveBeenCalled();
        });

        it('canGoToParameter returns false when goToParameter callback is not supplied, even with a readable parameterContext and a matching item.value', () => {
            component.parameterContext = readableParameterContext;
            // goToParameter intentionally left unset — the connector canvas read-only
            // dialogs leave it undefined so the "Go to Parameter" affordance is hidden
            // while parameter values still render in the value tip.

            expect(component.goToParameter).toBeUndefined();
            expect(component.canGoToParameter(mockItem)).toBe(false);
        });

        it('canGoToParameter returns true when parameterContext, goToParameter, and a matching value are all present', () => {
            component.parameterContext = readableParameterContext;
            component.goToParameter = vi.fn();

            expect(component.canGoToParameter(mockItem)).toBe(true);
        });
    });

    describe('openEditor', () => {
        const mockItem: PropertyItem = {
            property: 'Service',
            value: null,
            descriptor: {
                name: 'Service',
                displayName: 'Service',
                description: '',
                required: false,
                sensitive: false,
                dynamic: false,
                supportsEl: false,
                expressionLanguageScope: 'Not Supported',
                dependencies: []
            },
            id: 4,
            triggerEdit: false,
            deleted: false,
            added: false,
            dirty: false,
            savedValue: null,
            type: 'optional'
        };

        function open(item: PropertyItem, cellWidth: number): void {
            const cell = document.createElement('td');
            const content = document.createElement('span');
            cell.appendChild(content);
            vi.spyOn(component['changeDetector'], 'detectChanges').mockImplementation(() => undefined);
            vi.spyOn(cell, 'getBoundingClientRect').mockReturnValue({ width: cellWidth } as DOMRect);

            component.openEditor({ elementRef: new ElementRef(cell) } as CdkOverlayOrigin, item, {
                target: content
            } as unknown as MouseEvent);
        }

        it('should use a minimum width for the combo editor when the value cell is narrow', () => {
            const comboItem: PropertyItem = {
                ...mockItem,
                descriptor: { ...mockItem.descriptor, allowableValues: [] }
            };

            open(comboItem, 300);

            expect(component.editorWidth).toBe(500);
        });

        it('should keep the value cell width plus padding for the combo editor when the value cell is wide', () => {
            const comboItem: PropertyItem = {
                ...mockItem,
                descriptor: { ...mockItem.descriptor, allowableValues: [] }
            };

            open(comboItem, 700);

            expect(component.editorWidth).toBe(750);
        });

        it('should not apply the combo editor minimum width to other editors', () => {
            open(mockItem, 300);

            expect(component.editorWidth).toBe(400);
        });
    });
});
