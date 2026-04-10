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

import { NO_ERRORS_SCHEMA, signal, Signal } from '@angular/core';
import { TestBed } from '@angular/core/testing';
import { StepDocumentationState } from '../../../types';
import { ConnectorWizardStore } from '../connector-wizard.store';
import { WizardStepDocumentationPanel } from './wizard-step-documentation-panel.component';

function createMockStore(docState: StepDocumentationState | null = null) {
    const stepDocSignal = signal<StepDocumentationState | null>(docState);
    return {
        documentationForStep: vi.fn().mockReturnValue(stepDocSignal as Signal<StepDocumentationState | null>),
        loadStepDocumentation: vi.fn(),
        toggleDocumentationPanel: vi.fn(),
        stepDocSignal
    };
}

interface SetupOptions {
    stepName?: string;
    isOpen?: boolean;
}

async function setup(options: SetupOptions = {}) {
    const mockStore = createMockStore();

    await TestBed.configureTestingModule({
        imports: [WizardStepDocumentationPanel],
        providers: [{ provide: ConnectorWizardStore, useValue: mockStore }],
        schemas: [NO_ERRORS_SCHEMA]
    }).compileComponents();

    const fixture = TestBed.createComponent(WizardStepDocumentationPanel);
    const component = fixture.componentInstance;

    fixture.componentRef.setInput('stepName', options.stepName ?? 'Test Step');
    fixture.componentRef.setInput('isOpen', options.isOpen ?? false);

    fixture.detectChanges();

    return { fixture, component, mockStore };
}

describe('WizardStepDocumentationPanel', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    describe('creation', () => {
        it('should create', async () => {
            const { component } = await setup();
            expect(component).toBeTruthy();
        });
    });

    describe('effect-based loading', () => {
        it('does not call loadStepDocumentation when isOpen is false', async () => {
            const { mockStore } = await setup({ stepName: 'Step A', isOpen: false });
            expect(mockStore.loadStepDocumentation).not.toHaveBeenCalled();
        });

        it('calls loadStepDocumentation when isOpen is true and no doc exists', async () => {
            const { mockStore } = await setup({ stepName: 'Step A', isOpen: true });
            expect(mockStore.loadStepDocumentation).toHaveBeenCalledWith('Step A');
        });

        it('does not re-load documentation for the same stepName after a successful load', async () => {
            const { fixture, mockStore } = await setup({ stepName: 'Step A', isOpen: true });
            expect(mockStore.loadStepDocumentation).toHaveBeenCalledTimes(1);
            mockStore.loadStepDocumentation.mockClear();

            mockStore.stepDocSignal.set({
                loaded: true,
                loading: false,
                stepDocumentation: null,
                error: null
            });

            fixture.componentRef.setInput('isOpen', false);
            fixture.detectChanges();
            fixture.componentRef.setInput('isOpen', true);
            fixture.detectChanges();

            expect(mockStore.loadStepDocumentation).not.toHaveBeenCalled();
        });

        it('retries loading when the previous attempt failed for the same step', async () => {
            const { fixture, mockStore } = await setup({ stepName: 'Step A', isOpen: true });
            expect(mockStore.loadStepDocumentation).toHaveBeenCalledTimes(1);
            mockStore.loadStepDocumentation.mockClear();

            mockStore.stepDocSignal.set({
                loaded: false,
                loading: false,
                stepDocumentation: null,
                error: 'Network error'
            });

            fixture.componentRef.setInput('isOpen', false);
            fixture.detectChanges();
            fixture.componentRef.setInput('isOpen', true);
            fixture.detectChanges();

            expect(mockStore.loadStepDocumentation).toHaveBeenCalledWith('Step A');
        });

        it('loads documentation for a new stepName after the step changes', async () => {
            const { fixture, mockStore } = await setup({ stepName: 'Step A', isOpen: true });
            mockStore.loadStepDocumentation.mockClear();

            fixture.componentRef.setInput('stepName', 'Step B');
            fixture.detectChanges();

            expect(mockStore.loadStepDocumentation).toHaveBeenCalledWith('Step B');
        });

        it('does not load when documentation is already loaded', async () => {
            const { fixture, mockStore } = await setup({ stepName: 'Step A', isOpen: false });

            mockStore.stepDocSignal.set({
                loaded: true,
                loading: false,
                stepDocumentation: null,
                error: null
            });

            fixture.componentRef.setInput('isOpen', true);
            fixture.detectChanges();

            expect(mockStore.loadStepDocumentation).not.toHaveBeenCalled();
        });
    });

    describe('derived accessors', () => {
        it('isLoadingDoc returns true when documentation is loading', async () => {
            const { component, mockStore } = await setup();
            mockStore.stepDocSignal.set({ loading: true, loaded: false, stepDocumentation: null, error: null });
            expect(component.isLoadingDoc()).toBe(true);
        });

        it('isLoadingDoc returns false when not loading', async () => {
            const { component } = await setup();
            expect(component.isLoadingDoc()).toBe(false);
        });

        it('getStepDocumentation returns markdown content when loaded', async () => {
            const { component, mockStore } = await setup();
            mockStore.stepDocSignal.set({
                loaded: true,
                loading: false,
                stepDocumentation: '## Documentation',
                error: null
            });
            expect(component.getStepDocumentation()).toBe('## Documentation');
        });

        it('getStepDocumentation returns null when no doc available', async () => {
            const { component } = await setup();
            expect(component.getStepDocumentation()).toBeNull();
        });

        it('getErrorMessage returns error string when load failed', async () => {
            const { component, mockStore } = await setup();
            mockStore.stepDocSignal.set({
                loaded: false,
                loading: false,
                stepDocumentation: null,
                error: 'Failed to load'
            });
            expect(component.getErrorMessage()).toBe('Failed to load');
        });

        it('getErrorMessage returns null when no error', async () => {
            const { component } = await setup();
            expect(component.getErrorMessage()).toBeNull();
        });

        it('documentationState updates reactively when signal value changes', async () => {
            const { component, mockStore } = await setup();
            expect(component.getStepDocumentation()).toBeNull();

            mockStore.stepDocSignal.set({
                loaded: true,
                loading: false,
                stepDocumentation: '## Live content',
                error: null
            });

            expect(component.getStepDocumentation()).toBe('## Live content');
        });
    });

    describe('resolvedPanelTitle', () => {
        it('defaults to Documentation when panelTitle is not set', async () => {
            const { component } = await setup();
            expect(component.resolvedPanelTitle()).toBe('Documentation');
        });

        it('uses panelTitle when provided', async () => {
            const { fixture, component } = await setup();
            fixture.componentRef.setInput('panelTitle', 'Help');
            fixture.detectChanges();
            expect(component.resolvedPanelTitle()).toBe('Help');
        });
    });

    describe('onClose()', () => {
        it('calls toggleDocumentationPanel(false) on the store', async () => {
            const { component, mockStore } = await setup();
            component.onClose();
            expect(mockStore.toggleDocumentationPanel).toHaveBeenCalledWith(false);
        });

        it('emits the closePanel event', async () => {
            const { component } = await setup();
            const closeSpy = vi.fn();
            component.closePanel.subscribe(closeSpy);
            component.onClose();
            expect(closeSpy).toHaveBeenCalledTimes(1);
        });
    });
});
