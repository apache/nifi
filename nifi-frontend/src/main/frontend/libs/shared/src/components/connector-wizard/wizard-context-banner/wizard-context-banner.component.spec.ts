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

import { signal } from '@angular/core';
import { TestBed } from '@angular/core/testing';
import { By } from '@angular/platform-browser';
import { ConnectorWizardStore } from '../connector-wizard.store';
import { Banner } from '../../banner/banner.component';
import { StatusBanner } from '../../status-banner/status-banner.component';
import { WizardContextBanner } from './wizard-context-banner.component';

function createMockStore(initialErrors: string[] = []) {
    const bannerErrors = signal<string[]>(initialErrors);
    return {
        bannerErrors,
        clearBannerErrors: vi.fn(() => bannerErrors.set([]))
    };
}

interface SetupOptions {
    initialErrors?: string[];
    variant?: 'critical' | 'caution' | 'info' | 'success';
    persistOnDestroy?: boolean;
}

async function setup(options: SetupOptions = {}) {
    const mockStore = createMockStore(options.initialErrors ?? []);

    await TestBed.configureTestingModule({
        imports: [WizardContextBanner],
        providers: [{ provide: ConnectorWizardStore, useValue: mockStore }]
    }).compileComponents();

    const fixture = TestBed.createComponent(WizardContextBanner);
    const component = fixture.componentInstance;

    if (options.variant) {
        fixture.componentRef.setInput('variant', options.variant);
    }
    if (options.persistOnDestroy !== undefined) {
        fixture.componentRef.setInput('persistOnDestroy', options.persistOnDestroy);
    }

    fixture.detectChanges();

    return { fixture, component, mockStore };
}

describe('WizardContextBanner', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    describe('messages computed signal', () => {
        it('returns null when store has no banner errors', async () => {
            const { component } = await setup({ initialErrors: [] });
            expect(component.messages()).toBeNull();
        });

        it('returns the error array when store has banner errors', async () => {
            const { component } = await setup({ initialErrors: ['Error one', 'Error two'] });
            expect(component.messages()).toEqual(['Error one', 'Error two']);
        });

        it('updates reactively when errors are added to the store', async () => {
            const { component, mockStore } = await setup();
            expect(component.messages()).toBeNull();

            mockStore.bannerErrors.set(['New error']);

            expect(component.messages()).toEqual(['New error']);
        });

        it('returns null after errors are cleared', async () => {
            const { component, mockStore } = await setup({ initialErrors: ['Error'] });
            mockStore.bannerErrors.set([]);
            expect(component.messages()).toBeNull();
        });
    });

    describe('dismiss()', () => {
        it('calls clearBannerErrors on the store', async () => {
            const { component, mockStore } = await setup({ initialErrors: ['Error'] });
            component.dismiss();
            expect(mockStore.clearBannerErrors).toHaveBeenCalledTimes(1);
        });

        it('clears the messages signal after dismiss', async () => {
            const { component } = await setup({ initialErrors: ['Error'] });
            component.dismiss();
            expect(component.messages()).toBeNull();
        });
    });

    describe('ngOnDestroy', () => {
        it('clears banner errors by default (persistOnDestroy = false)', async () => {
            const { fixture, mockStore } = await setup({ initialErrors: ['Error'] });
            fixture.destroy();
            expect(mockStore.clearBannerErrors).toHaveBeenCalledTimes(1);
        });

        it('does not clear errors when persistOnDestroy is true', async () => {
            const { fixture, mockStore } = await setup({
                initialErrors: ['Error'],
                persistOnDestroy: true
            });
            fixture.destroy();
            expect(mockStore.clearBannerErrors).not.toHaveBeenCalled();
        });
    });

    describe('inputs', () => {
        it('defaults variant to critical', async () => {
            const { component } = await setup();
            expect(component.variant()).toBe('critical');
        });

        it('accepts caution variant', async () => {
            const { component } = await setup({ variant: 'caution' });
            expect(component.variant()).toBe('caution');
        });
    });

    describe('presentation', () => {
        it('renders Banner with StatusBanner when store has errors', async () => {
            const { fixture } = await setup({ initialErrors: ['Save failed'], variant: 'critical' });
            expect(fixture.debugElement.query(By.directive(Banner))).toBeTruthy();
            const statusBanner = fixture.debugElement.query(By.directive(StatusBanner));
            expect(statusBanner).toBeTruthy();
            expect(statusBanner.componentInstance.variant).toBe('critical');
            expect(fixture.nativeElement.textContent).toContain('Save failed');
        });

        it('does not render visible banner content when store has no errors', async () => {
            const { fixture } = await setup();
            expect(fixture.nativeElement.querySelector('.banner-container')).toBeNull();
            expect(fixture.nativeElement.querySelector('.status-banner-container')).toBeNull();
        });
    });
});
