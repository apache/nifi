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
import { MatStep } from '@angular/material/stepper';
import { Wizard } from './wizard.component';

describe('Wizard', () => {
    let component: Wizard;
    let fixture: ComponentFixture<Wizard>;

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [Wizard]
        });
        fixture = TestBed.createComponent(Wizard);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });

    describe('onHeaderClick', () => {
        it('should call step.select() when no beforeStepChange listener is bound', () => {
            const mockStep = {
                select: vi.fn(),
                isNavigable: vi.fn().mockReturnValue(true)
            } as unknown as MatStep;

            component.onHeaderClick(mockStep);

            expect(mockStep.select).toHaveBeenCalled();
        });

        it('should not call step.select() when step is disabled', () => {
            component.linear = true;
            const mockStep = {
                select: vi.fn(),
                isNavigable: vi.fn().mockReturnValue(false)
            } as unknown as MatStep;

            component.onHeaderClick(mockStep);

            expect(mockStep.select).not.toHaveBeenCalled();
        });

        it('should emit beforeStepChange and not call step.select() when listener is bound', () => {
            const mockStep = {
                select: vi.fn(),
                isNavigable: vi.fn().mockReturnValue(true)
            } as unknown as MatStep;

            const beforeStepChangeSpy = vi.fn();
            component.beforeStepChange.subscribe(beforeStepChangeSpy);

            const mockSteps = [{ select: vi.fn() }, mockStep];
            vi.spyOn(component.steps, 'toArray').mockReturnValue(mockSteps as any);
            vi.spyOn(component, 'selectedIndex', 'get').mockReturnValue(0);

            component.onHeaderClick(mockStep);

            expect(mockStep.select).not.toHaveBeenCalled();
            expect(beforeStepChangeSpy).toHaveBeenCalledWith({
                currentIndex: 0,
                targetIndex: 1
            });
        });
    });

    describe('goToStep', () => {
        it('should select the step at the given index', () => {
            const mockStep0 = { select: vi.fn() };
            const mockStep1 = { select: vi.fn() };
            vi.spyOn(component.steps, 'toArray').mockReturnValue([mockStep0, mockStep1] as any);

            component.goToStep(1);

            expect(mockStep1.select).toHaveBeenCalled();
            expect(mockStep0.select).not.toHaveBeenCalled();
        });

        it('should not throw when index is out of bounds (negative)', () => {
            vi.spyOn(component.steps, 'toArray').mockReturnValue([{ select: vi.fn() }] as any);

            expect(() => component.goToStep(-1)).not.toThrow();
        });

        it('should not throw when index is out of bounds (too large)', () => {
            vi.spyOn(component.steps, 'toArray').mockReturnValue([{ select: vi.fn() }] as any);

            expect(() => component.goToStep(5)).not.toThrow();
        });
    });
});
