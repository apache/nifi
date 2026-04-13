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

import { NO_ERRORS_SCHEMA, signal } from '@angular/core';
import { StepperSelectionEvent } from '@angular/cdk/stepper';
import { TestBed } from '@angular/core/testing';
import { ConnectorWizard } from './connector-wizard.component';
import { ConnectorWizardStore } from './connector-wizard.store';
import { CONNECTOR_WIZARD_CONFIG } from './connector-wizard.types';
import { ConnectorEntity } from '../../types';

// --------------- Mock store factory ---------------

interface MockStoreState {
    status?: 'pending' | 'configuring' | 'saving' | 'complete' | 'error';
    error?: string | null;
    stepNames?: string[];
    loading?: boolean;
}

function createMockStore(state: MockStoreState = {}) {
    const status = signal<'pending' | 'configuring' | 'saving' | 'complete' | 'error'>(state.status ?? 'pending');
    const visibleStepNames = signal<string[]>(state.stepNames ?? []);
    const completedSteps = signal<Record<string, boolean>>({});
    const loading = signal<boolean>(state.loading ?? false);
    const pendingStepAdvancement = signal<boolean>(false);

    return {
        status,
        visibleStepNames,
        completedSteps,
        loading,
        pendingStepAdvancement,
        error: signal<string | null>(state.error ?? null),
        applying: signal<boolean>(false),
        workingConfiguration: signal<null>(null),
        allStepsVerifying: signal<boolean>(false),
        verificationPassed: signal<boolean | null>(null),
        currentVerifyingStepName: signal<string | null>(null),
        stepVerificationResults: signal<Record<string, unknown>>({}),
        verifyAllError: signal<string | null>(null),
        bannerErrors: signal<string[]>([]),
        initializeWithConnector: vi.fn(),
        loadSecrets: vi.fn(),
        resetState: vi.fn(),
        changeStep: vi.fn(),
        enterSummaryStep: vi.fn(),
        refreshConnectorForSummary: vi.fn(),
        saveStep: vi.fn(),
        clearPendingStepAdvancement: vi.fn(),
        verifyAllSteps: vi.fn(),
        applyConfiguration: vi.fn()
    };
}

// --------------- Fixtures ---------------

function makeConnector(id = 'c-1'): ConnectorEntity {
    return {
        id,
        uri: `/connectors/${id}`,
        permissions: { canRead: true, canWrite: true },
        bulletins: [],
        status: { runStatus: 'RUNNING', validationStatus: 'VALID' },
        component: {
            id,
            name: 'Test Connector',
            type: 'TestConnector',
            state: 'RUNNING',
            bundle: { group: 'org.test', artifact: 'test', version: '1.0' },
            managedProcessGroupId: 'pg-1',
            availableActions: [],
            workingConfiguration: { configurationStepConfigurations: [] }
        },
        revision: { version: 1 }
    };
}

// --------------- Setup ---------------

interface SetupOptions {
    storeState?: MockStoreState;
    connector?: ConnectorEntity | null;
    summaryLabel?: string;
}

async function setup(options: SetupOptions = {}) {
    const mockStore = createMockStore(options.storeState ?? {});

    await TestBed.configureTestingModule({
        imports: [ConnectorWizard],
        providers: [
            { provide: ConnectorWizardStore, useValue: mockStore },
            { provide: CONNECTOR_WIZARD_CONFIG, useValue: null }
        ],
        schemas: [NO_ERRORS_SCHEMA]
    }).compileComponents();

    const fixture = TestBed.createComponent(ConnectorWizard);
    const component = fixture.componentInstance;

    if (options.connector !== undefined) {
        fixture.componentRef.setInput('connector', options.connector);
    }
    if (options.summaryLabel) {
        fixture.componentRef.setInput('summaryLabel', options.summaryLabel);
    }

    fixture.detectChanges();

    return { fixture, component, mockStore };
}

// --------------- Tests ---------------

describe('ConnectorWizard', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    describe('creation', () => {
        it('should create', async () => {
            const { component } = await setup();
            expect(component).toBeTruthy();
        });
    });

    describe('ngOnInit', () => {
        it('calls initializeWithConnector when connector input is provided', async () => {
            const connector = makeConnector();
            const { mockStore } = await setup({ connector });
            expect(mockStore.initializeWithConnector).toHaveBeenCalledWith(connector);
        });

        it('does not call initializeWithConnector when no connector input', async () => {
            const { mockStore } = await setup({ connector: null });
            expect(mockStore.initializeWithConnector).not.toHaveBeenCalled();
        });

        it('loads secrets when connector input is provided', async () => {
            const connector = makeConnector('c-2');
            const { mockStore } = await setup({ connector });
            expect(mockStore.loadSecrets).toHaveBeenCalledWith('c-2');
        });

        it('does not load secrets when no connector input', async () => {
            const { mockStore } = await setup({ connector: null });
            expect(mockStore.loadSecrets).not.toHaveBeenCalled();
        });

        it('clears pendingTargetIndex when banner errors appear', async () => {
            const connector = makeConnector();
            const { component, mockStore, fixture } = await setup({ connector });

            // Simulate a pending target from a header-click save
            (component as any).pendingTargetIndex = 3;

            // Simulate save failure adding a banner error
            mockStore.bannerErrors.set(['Save failed']);
            fixture.detectChanges();

            // Allow the toObservable emission to propagate
            await fixture.whenStable();
            expect((component as any).pendingTargetIndex).toBeNull();
        });
    });

    describe('ngOnDestroy', () => {
        it('calls resetState on the store', async () => {
            const { fixture, mockStore } = await setup();
            fixture.destroy();
            expect(mockStore.resetState).toHaveBeenCalledTimes(1);
        });
    });

    describe('error state rendering', () => {
        it('shows error container when status is error', async () => {
            const { fixture } = await setup({ storeState: { status: 'error', error: 'Load failed' } });
            const errorContainer = fixture.nativeElement.querySelector('[data-qa="error-container"]');
            expect(errorContainer).toBeTruthy();
        });

        it('shows the error message text', async () => {
            const { fixture } = await setup({ storeState: { status: 'error', error: 'Something went wrong' } });
            const errorMessage = fixture.nativeElement.querySelector('[data-qa="error-message"]');
            expect(errorMessage?.textContent?.trim()).toBe('Something went wrong');
        });

        it('does not show error container when status is pending', async () => {
            const { fixture } = await setup({ storeState: { status: 'pending' } });
            const errorContainer = fixture.nativeElement.querySelector('[data-qa="error-container"]');
            expect(errorContainer).toBeNull();
        });
    });

    describe('no-steps state rendering', () => {
        it('shows no-steps-container when steps array is empty (non-error)', async () => {
            const { fixture } = await setup({ storeState: { status: 'configuring', stepNames: [] } });
            const noSteps = fixture.nativeElement.querySelector('[data-qa="no-steps-container"]');
            expect(noSteps).toBeTruthy();
        });

        it('does not show wizard when steps are empty', async () => {
            const { fixture } = await setup({ storeState: { status: 'configuring', stepNames: [] } });
            const wizard = fixture.nativeElement.querySelector('[data-qa="configuration-wizard"]');
            expect(wizard).toBeNull();
        });
    });

    describe('navigateBack output', () => {
        it('emits navigateBack when return button is clicked in error state', async () => {
            const { fixture, component } = await setup({
                storeState: { status: 'error', error: 'Error' }
            });
            const backSpy = vi.fn();
            component.navigateBack.subscribe(backSpy);

            const returnButton = fixture.nativeElement.querySelector('[data-qa="return-button"]');
            returnButton?.click();
            fixture.detectChanges();

            expect(backSpy).toHaveBeenCalledTimes(1);
        });
    });

    describe('onStepChange', () => {
        it('calls changeStep with the selected index', async () => {
            const { component, mockStore } = await setup();
            component.onStepChange({ selectedIndex: 2, previouslySelectedIndex: 1 } as StepperSelectionEvent, [
                'A',
                'B'
            ]);
            expect(mockStore.changeStep).toHaveBeenCalledWith(2);
        });

        it('calls enterSummaryStep and refreshConnectorForSummary when last step selected', async () => {
            const { component, mockStore } = await setup();
            component.onStepChange({ selectedIndex: 2, previouslySelectedIndex: 1 } as StepperSelectionEvent, [
                'A',
                'B'
            ]);
            expect(mockStore.enterSummaryStep).toHaveBeenCalled();
            expect(mockStore.refreshConnectorForSummary).toHaveBeenCalled();
        });

        it('does not call enterSummaryStep for intermediate steps', async () => {
            const { component, mockStore } = await setup();
            component.onStepChange({ selectedIndex: 0, previouslySelectedIndex: 0 } as StepperSelectionEvent, [
                'A',
                'B',
                'C'
            ]);
            expect(mockStore.enterSummaryStep).not.toHaveBeenCalled();
        });
    });

    describe('getCustomStepTemplate', () => {
        it('returns null when no custom step directives are registered', async () => {
            const { component } = await setup();
            expect(component.getCustomStepTemplate('Any Step')).toBeNull();
        });
    });
});
