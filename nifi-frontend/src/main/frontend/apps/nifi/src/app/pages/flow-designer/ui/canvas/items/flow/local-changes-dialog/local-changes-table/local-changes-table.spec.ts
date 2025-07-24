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

describe('LocalChangesTable', () => {
    let component: LocalChangesTable;
    let fixture: ComponentFixture<LocalChangesTable>;

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
});
