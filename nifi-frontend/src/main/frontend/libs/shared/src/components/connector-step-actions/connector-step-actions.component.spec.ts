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

import { TestBed } from '@angular/core/testing';
import { By } from '@angular/platform-browser';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { ConnectorStepActions } from './connector-step-actions.component';

describe('ConnectorStepActions', () => {
    interface SetupOptions {
        isSaving?: boolean;
        showNext?: boolean;
        showBack?: boolean;
        showSaveAndClose?: boolean;
        nextDisabled?: boolean;
        backDisabled?: boolean;
        saveAndCloseDisabled?: boolean;
        nextLabel?: string | null;
        backLabel?: string | null;
        saveAndCloseLabel?: string | null;
        savingLabel?: string | null;
        nextActionName?: string | null;
        backActionName?: string | null;
        saveAndCloseActionName?: string | null;
    }

    async function setup(options: SetupOptions = {}) {
        await TestBed.configureTestingModule({
            imports: [ConnectorStepActions, NoopAnimationsModule]
        }).compileComponents();

        const fixture = TestBed.createComponent(ConnectorStepActions);
        const component = fixture.componentInstance;

        if (options.isSaving !== undefined) fixture.componentRef.setInput('isSaving', options.isSaving);
        if (options.showNext !== undefined) fixture.componentRef.setInput('showNext', options.showNext);
        if (options.showBack !== undefined) fixture.componentRef.setInput('showBack', options.showBack);
        if (options.showSaveAndClose !== undefined)
            fixture.componentRef.setInput('showSaveAndClose', options.showSaveAndClose);
        if (options.nextDisabled !== undefined) fixture.componentRef.setInput('nextDisabled', options.nextDisabled);
        if (options.backDisabled !== undefined) fixture.componentRef.setInput('backDisabled', options.backDisabled);
        if (options.saveAndCloseDisabled !== undefined)
            fixture.componentRef.setInput('saveAndCloseDisabled', options.saveAndCloseDisabled);
        if (options.nextLabel !== undefined) fixture.componentRef.setInput('nextLabel', options.nextLabel);
        if (options.backLabel !== undefined) fixture.componentRef.setInput('backLabel', options.backLabel);
        if (options.saveAndCloseLabel !== undefined)
            fixture.componentRef.setInput('saveAndCloseLabel', options.saveAndCloseLabel);
        if (options.savingLabel !== undefined) fixture.componentRef.setInput('savingLabel', options.savingLabel);
        if (options.nextActionName !== undefined)
            fixture.componentRef.setInput('nextActionName', options.nextActionName);
        if (options.backActionName !== undefined)
            fixture.componentRef.setInput('backActionName', options.backActionName);
        if (options.saveAndCloseActionName !== undefined)
            fixture.componentRef.setInput('saveAndCloseActionName', options.saveAndCloseActionName);

        fixture.detectChanges();

        const queryButton = (qa: string) => fixture.debugElement.query(By.css(`[data-qa="${qa}"]`));

        return { fixture, component, queryButton };
    }

    beforeEach(() => {
        vi.clearAllMocks();
    });

    describe('Default rendering', () => {
        it('should create', async () => {
            const { component } = await setup();
            expect(component).toBeTruthy();
        });

        it('should render all three buttons by default', async () => {
            const { queryButton } = await setup();

            expect(queryButton('save-and-close-button')).toBeTruthy();
            expect(queryButton('back-button')).toBeTruthy();
            expect(queryButton('next-button')).toBeTruthy();
        });

        it('should render buttons with Angular Material button variants', async () => {
            const { queryButton } = await setup();

            const saveClasses = queryButton('save-and-close-button').nativeElement.className as string;
            const backClasses = queryButton('back-button').nativeElement.className as string;
            const nextClasses = queryButton('next-button').nativeElement.className as string;

            expect(saveClasses).toContain('mat-mdc-button');
            expect(backClasses).toContain('mat-mdc-button');
            expect(nextClasses).toContain('mat-mdc-unelevated-button');
        });

        it('should enable all buttons when not saving', async () => {
            const { queryButton } = await setup();

            expect(queryButton('save-and-close-button').nativeElement.disabled).toBe(false);
            expect(queryButton('back-button').nativeElement.disabled).toBe(false);
            expect(queryButton('next-button').nativeElement.disabled).toBe(false);
        });
    });

    describe('Visibility', () => {
        it('should hide Next button when showNext is false', async () => {
            const { queryButton } = await setup({ showNext: false });
            expect(queryButton('next-button')).toBeFalsy();
        });

        it('should hide Back button when showBack is false', async () => {
            const { queryButton } = await setup({ showBack: false });
            expect(queryButton('back-button')).toBeFalsy();
        });

        it('should hide Save and close button when showSaveAndClose is false', async () => {
            const { queryButton } = await setup({ showSaveAndClose: false });
            expect(queryButton('save-and-close-button')).toBeFalsy();
        });
    });

    describe('Disabled state - isSaving', () => {
        it('should disable all buttons when isSaving is true', async () => {
            const { queryButton } = await setup({ isSaving: true });

            expect(queryButton('save-and-close-button').nativeElement.disabled).toBe(true);
            expect(queryButton('back-button').nativeElement.disabled).toBe(true);
            expect(queryButton('next-button').nativeElement.disabled).toBe(true);
        });
    });

    describe('Disabled state - per-button', () => {
        it('should disable only Next when nextDisabled is true', async () => {
            const { queryButton } = await setup({ nextDisabled: true });

            expect(queryButton('next-button').nativeElement.disabled).toBe(true);
            expect(queryButton('back-button').nativeElement.disabled).toBe(false);
            expect(queryButton('save-and-close-button').nativeElement.disabled).toBe(false);
        });

        it('should disable only Back when backDisabled is true', async () => {
            const { queryButton } = await setup({ backDisabled: true });

            expect(queryButton('back-button').nativeElement.disabled).toBe(true);
            expect(queryButton('next-button').nativeElement.disabled).toBe(false);
            expect(queryButton('save-and-close-button').nativeElement.disabled).toBe(false);
        });

        it('should disable only Save and close when saveAndCloseDisabled is true', async () => {
            const { queryButton } = await setup({ saveAndCloseDisabled: true });

            expect(queryButton('save-and-close-button').nativeElement.disabled).toBe(true);
            expect(queryButton('next-button').nativeElement.disabled).toBe(false);
            expect(queryButton('back-button').nativeElement.disabled).toBe(false);
        });
    });

    describe('Events', () => {
        it('should emit next when Next button is clicked', async () => {
            const { component, queryButton } = await setup();
            const spy = vi.spyOn(component.next, 'emit');

            queryButton('next-button').nativeElement.click();

            expect(spy).toHaveBeenCalledTimes(1);
        });

        it('should emit back when Back button is clicked', async () => {
            const { component, queryButton } = await setup();
            const spy = vi.spyOn(component.back, 'emit');

            queryButton('back-button').nativeElement.click();

            expect(spy).toHaveBeenCalledTimes(1);
        });

        it('should emit saveAndClose when Save and close button is clicked', async () => {
            const { component, queryButton } = await setup();
            const spy = vi.spyOn(component.saveAndClose, 'emit');

            queryButton('save-and-close-button').nativeElement.click();

            expect(spy).toHaveBeenCalledTimes(1);
        });
    });

    describe('Spinner tracking', () => {
        it('should set saveTriggeredBy to next when Next is clicked', async () => {
            const { component, queryButton } = await setup();

            queryButton('next-button').nativeElement.click();

            expect(component.saveTriggeredBy()).toBe('next');
        });

        it('should set saveTriggeredBy to back when Back is clicked', async () => {
            const { component, queryButton } = await setup();

            queryButton('back-button').nativeElement.click();

            expect(component.saveTriggeredBy()).toBe('back');
        });

        it('should set saveTriggeredBy to saveAndClose when Save and close is clicked', async () => {
            const { component, queryButton } = await setup();

            queryButton('save-and-close-button').nativeElement.click();

            expect(component.saveTriggeredBy()).toBe('saveAndClose');
        });

        it('should report isButtonSaving true only for the triggered button', async () => {
            const { component, fixture } = await setup();

            component.saveTriggeredBy.set('next');
            fixture.componentRef.setInput('isSaving', true);
            fixture.detectChanges();

            expect(component.isButtonSaving('next')).toBe(true);
            expect(component.isButtonSaving('back')).toBe(false);
            expect(component.isButtonSaving('saveAndClose')).toBe(false);
        });

        it('should report isButtonSaving false for all when not saving', async () => {
            const { component, fixture } = await setup();

            component.saveTriggeredBy.set('next');
            fixture.componentRef.setInput('isSaving', false);
            fixture.detectChanges();

            expect(component.isButtonSaving('next')).toBe(false);
            expect(component.isButtonSaving('back')).toBe(false);
            expect(component.isButtonSaving('saveAndClose')).toBe(false);
        });

        it('should clear saveTriggeredBy when isSaving transitions to false', async () => {
            const { component, fixture } = await setup({ isSaving: true });

            component.saveTriggeredBy.set('next');
            fixture.detectChanges();
            expect(component.saveTriggeredBy()).toBe('next');

            fixture.componentRef.setInput('isSaving', false);
            fixture.detectChanges();

            expect(component.saveTriggeredBy()).toBeNull();
        });

        it('should not show spinner when save is triggered externally without a button click', async () => {
            const { component, fixture } = await setup();

            fixture.componentRef.setInput('isSaving', true);
            fixture.detectChanges();

            expect(component.saveTriggeredBy()).toBeNull();
            expect(component.isButtonSaving('next')).toBe(false);
            expect(component.isButtonSaving('back')).toBe(false);
            expect(component.isButtonSaving('saveAndClose')).toBe(false);
        });
    });

    describe('Custom labels', () => {
        it('should render custom nextLabel when provided', async () => {
            const { queryButton } = await setup({ nextLabel: 'Continue' });
            expect(queryButton('next-button').nativeElement.textContent.trim()).toBe('Continue');
        });

        it('should render custom backLabel when provided', async () => {
            const { queryButton } = await setup({ backLabel: 'Previous' });
            expect(queryButton('back-button').nativeElement.textContent.trim()).toBe('Previous');
        });

        it('should render custom saveAndCloseLabel when provided', async () => {
            const { queryButton } = await setup({ saveAndCloseLabel: 'Close wizard' });
            expect(queryButton('save-and-close-button').nativeElement.textContent.trim()).toBe('Close wizard');
        });
    });

    describe('Accessibility', () => {
        it('should have aria-label on all buttons', async () => {
            const { queryButton } = await setup();

            expect(queryButton('save-and-close-button').nativeElement.hasAttribute('aria-label')).toBe(true);
            expect(queryButton('back-button').nativeElement.hasAttribute('aria-label')).toBe(true);
            expect(queryButton('next-button').nativeElement.hasAttribute('aria-label')).toBe(true);
        });
    });

    describe('Action name telemetry', () => {
        it('should not render data-action-name when action name inputs are not provided', async () => {
            const { queryButton } = await setup();

            expect(queryButton('next-button').nativeElement.hasAttribute('data-action-name')).toBe(false);
            expect(queryButton('back-button').nativeElement.hasAttribute('data-action-name')).toBe(false);
            expect(queryButton('save-and-close-button').nativeElement.hasAttribute('data-action-name')).toBe(false);
        });

        it('should render data-action-name when action name inputs are provided', async () => {
            const { queryButton } = await setup({
                nextActionName: 'connector-wizard-next',
                backActionName: 'connector-wizard-back',
                saveAndCloseActionName: 'connector-wizard-save-close'
            });

            expect(queryButton('next-button').nativeElement.getAttribute('data-action-name')).toBe(
                'connector-wizard-next'
            );
            expect(queryButton('back-button').nativeElement.getAttribute('data-action-name')).toBe(
                'connector-wizard-back'
            );
            expect(queryButton('save-and-close-button').nativeElement.getAttribute('data-action-name')).toBe(
                'connector-wizard-save-close'
            );
        });

        it('should render data-action-name only on buttons that have it set', async () => {
            const { queryButton } = await setup({
                nextActionName: 'my-next-action'
            });

            expect(queryButton('next-button').nativeElement.getAttribute('data-action-name')).toBe('my-next-action');
            expect(queryButton('back-button').nativeElement.hasAttribute('data-action-name')).toBe(false);
            expect(queryButton('save-and-close-button').nativeElement.hasAttribute('data-action-name')).toBe(false);
        });
    });
});
