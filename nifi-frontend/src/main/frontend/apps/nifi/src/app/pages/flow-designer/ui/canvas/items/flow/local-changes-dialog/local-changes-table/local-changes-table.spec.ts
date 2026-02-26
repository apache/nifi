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

import { LocalChangesTable } from './local-changes-table';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { ComponentType } from '@nifi/shared';
import { ComponentDifference } from '../../../../../../state/flow';

describe('LocalChangesTable', () => {
    let component: LocalChangesTable;
    let fixture: ComponentFixture<LocalChangesTable>;

    const mixedDifferences: ComponentDifference[] = [
        {
            componentType: ComponentType.Processor,
            componentId: '1',
            processGroupId: 'pg-1',
            componentName: 'GenerateFlowFile',
            differences: [
                { differenceType: 'Property Value Changed', difference: 'Batch Size changed from 1 to 10' },
                {
                    differenceType: 'Component Bundle Changed',
                    difference: 'Bundle changed from 1.0 to 2.0',
                    environmental: true
                }
            ]
        },
        {
            componentType: ComponentType.ControllerService,
            componentId: '2',
            processGroupId: 'pg-1',
            componentName: 'DBCPService',
            differences: [
                {
                    differenceType: 'Component Bundle Changed',
                    difference: 'Bundle changed from 1.0 to 2.0',
                    environmental: false
                }
            ]
        }
    ];

    beforeEach(async () => {
        await TestBed.configureTestingModule({
            imports: [LocalChangesTable, NoopAnimationsModule]
        }).compileComponents();

        fixture = TestBed.createComponent(LocalChangesTable);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });

    describe('canGoTo', () => {
        it('should return true if can go to', () => {
            const localChange = {
                componentType: 'Processor',
                componentId: '123',
                componentName: 'Test Processor',
                processGroupId: '456',
                differenceType: 'Component Added',
                difference: 'Processor was added.'
            };
            expect(component.canGoTo(localChange)).toBe(true);
        });

        it('should return false if cannot go to', () => {
            const localChange = {
                componentType: 'Processor',
                componentId: '123',
                componentName: 'Test Processor',
                processGroupId: '456',
                differenceType: 'Component Removed',
                difference: 'Processor was removed.'
            };
            expect(component.canGoTo(localChange)).toBe(false);
        });
    });

    describe('goToClicked', () => {
        it('should next goToChange event on Processor added', () => {
            const localChange = {
                componentType: 'Processor',
                componentId: '123',
                componentName: 'Test Processor',
                processGroupId: '456',
                differenceType: 'Component Added',
                difference: 'Processor was added.'
            };
            jest.spyOn(component.goToChange, 'next');
            component.goToClicked(localChange);
            expect(component.goToChange.next).toHaveBeenCalledWith({
                id: localChange.componentId,
                type: ComponentType.Processor,
                processGroupId: localChange.processGroupId
            });
        });

        it('should next goToChange event on Controller Service added', () => {
            const localChange = {
                componentType: 'Controller Service',
                componentId: '123',
                componentName: 'Test Controller Service',
                processGroupId: '456',
                differenceType: 'Component Added',
                difference: 'Controller Service was added.'
            };
            jest.spyOn(component.goToChange, 'next');
            component.goToClicked(localChange);
            expect(component.goToChange.next).toHaveBeenCalledWith({
                id: localChange.componentId,
                type: ComponentType.ControllerService,
                processGroupId: localChange.processGroupId
            });
        });
    });

    describe('isEnvironmental', () => {
        it('should return true when environmental is true', () => {
            expect(
                component.isEnvironmental({
                    componentType: 'Processor',
                    componentId: '1',
                    componentName: 'P',
                    processGroupId: 'pg',
                    differenceType: 'Bundle Changed',
                    difference: 'diff',
                    environmental: true
                })
            ).toBe(true);
        });

        it('should return false when environmental is false', () => {
            expect(
                component.isEnvironmental({
                    componentType: 'Processor',
                    componentId: '1',
                    componentName: 'P',
                    processGroupId: 'pg',
                    differenceType: 'Bundle Changed',
                    difference: 'diff',
                    environmental: false
                })
            ).toBe(false);
        });

        it('should return false when environmental is undefined', () => {
            expect(
                component.isEnvironmental({
                    componentType: 'Processor',
                    componentId: '1',
                    componentName: 'P',
                    processGroupId: 'pg',
                    differenceType: 'Changed',
                    difference: 'diff'
                })
            ).toBe(false);
        });
    });

    describe('environmentalCount', () => {
        it('should count environmental changes from input differences', () => {
            component.differences = mixedDifferences;
            expect(component.environmentalCount).toBe(1);
        });
    });

    describe('SHOW mode filtering', () => {
        beforeEach(() => {
            component.mode = 'SHOW';
        });

        it('should hide environmental changes by default', () => {
            component.differences = mixedDifferences;
            expect(component.showEnvironmentalChanges).toBe(false);
            expect(component.dataSource.data.length).toBe(2);
            expect(component.dataSource.data.every((d) => d.environmental !== true)).toBe(true);
        });

        it('should include environmental changes when toggle is enabled', () => {
            component.differences = mixedDifferences;
            component.toggleEnvironmentalChanges();
            expect(component.showEnvironmentalChanges).toBe(true);
            expect(component.dataSource.data.length).toBe(3);
        });

        it('should set totalCount to all changes including environmental', () => {
            component.differences = mixedDifferences;
            expect(component.totalCount).toBe(3);
        });

        it('should set filteredCount to displayed changes only', () => {
            component.differences = mixedDifferences;
            expect(component.filteredCount).toBe(2);

            component.toggleEnvironmentalChanges();
            expect(component.filteredCount).toBe(3);
        });
    });

    describe('REVERT mode filtering', () => {
        beforeEach(() => {
            component.mode = 'REVERT';
        });

        it('should always filter out environmental changes', () => {
            component.differences = mixedDifferences;
            expect(component.dataSource.data.length).toBe(2);
            expect(component.dataSource.data.every((d) => d.environmental !== true)).toBe(true);
        });

        it('should not include environmental changes even after toggle', () => {
            component.differences = mixedDifferences;
            component.showEnvironmentalChanges = true;
            component.differences = mixedDifferences;
            expect(component.dataSource.data.length).toBe(2);
        });

        it('should set totalCount to non-environmental changes only', () => {
            component.differences = mixedDifferences;
            expect(component.totalCount).toBe(2);
        });

        it('should set filteredCount equal to totalCount', () => {
            component.differences = mixedDifferences;
            expect(component.filteredCount).toBe(2);
            expect(component.totalCount).toBe(component.filteredCount);
        });
    });

    describe('toggleEnvironmentalChanges', () => {
        it('should toggle the flag and re-filter', () => {
            component.mode = 'SHOW';
            component.differences = mixedDifferences;

            expect(component.showEnvironmentalChanges).toBe(false);
            expect(component.dataSource.data.length).toBe(2);

            component.toggleEnvironmentalChanges();
            expect(component.showEnvironmentalChanges).toBe(true);
            expect(component.dataSource.data.length).toBe(3);

            component.toggleEnvironmentalChanges();
            expect(component.showEnvironmentalChanges).toBe(false);
            expect(component.dataSource.data.length).toBe(2);
        });
    });
});
