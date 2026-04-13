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

import { TestBed, fakeAsync, tick } from '@angular/core/testing';
import { Subject } from 'rxjs';
import { StandardConnectorWizardStore, ConnectorWizardStore } from './connector-wizard.store';
import { CONNECTOR_WIZARD_CONFIG, ConnectorWizardConfig, createPropertyKey } from './connector-wizard.types';
import { ConnectorConfigurationService } from '../../services/connector-configuration.service';
import { MatDialog } from '@angular/material/dialog';
import { MatSnackBar } from '@angular/material/snack-bar';
import {
    AllowableValue,
    ConnectorEntity,
    ConfigurationStepConfiguration,
    ConfigurationStepEntity,
    ConnectorConfigStepVerificationRequestEntity,
    ConfigVerificationResult
} from '../../types';
import { of, throwError } from 'rxjs';

// --------------- Factories ---------------

function makeConnector(overrides: Partial<ConnectorEntity> = {}): ConnectorEntity {
    return {
        id: 'connector-1',
        uri: '/connectors/connector-1',
        permissions: { canRead: true, canWrite: true },
        bulletins: [],
        status: { runStatus: 'RUNNING', validationStatus: 'VALID' },
        component: {
            id: 'connector-1',
            name: 'Test Connector',
            type: 'TestConnector',
            state: 'RUNNING',
            bundle: { group: 'org.test', artifact: 'test', version: '1.0' },
            managedProcessGroupId: 'pg-1',
            availableActions: [],
            workingConfiguration: {
                configurationStepConfigurations: [makeStep('Step A'), makeStep('Step B')]
            }
        },
        revision: { version: 1 },
        ...overrides
    };
}

function makeStep(name: string): ConfigurationStepConfiguration {
    return {
        configurationStepName: name,
        propertyGroupConfigurations: [],
        dependencies: []
    };
}

function makeStepEntity(stepName: string, revision = 2): ConfigurationStepEntity {
    return {
        configurationStep: makeStep(stepName),
        parentConnectorId: 'connector-1',
        parentConnectorRevision: { version: revision }
    };
}

function makeVerificationEntity(
    requestId: string,
    complete: boolean,
    results: ConfigVerificationResult[] | null = null
): ConnectorConfigStepVerificationRequestEntity {
    return {
        request: {
            requestId,
            uri: `/verify/${requestId}`,
            connectorId: 'connector-1',
            configurationStepName: 'Step A',
            configurationStep: makeStep('Step A'),
            complete,
            percentCompleted: complete ? 100 : 0,
            state: complete ? 'COMPLETE' : 'RUNNING',
            failureReason: null,
            results
        }
    };
}

function makeVerificationResult(outcome: 'SUCCESSFUL' | 'FAILED', subject?: string): ConfigVerificationResult {
    return {
        outcome,
        verificationStepName: 'connectivity',
        explanation: outcome === 'FAILED' ? 'Connection refused' : 'OK',
        ...(subject ? { subject } : {})
    };
}

// --------------- Mock dialog ref factory ---------------

function createMockDialogRef() {
    const afterClosed$ = new Subject<void>();
    const stopVerification$ = new Subject<void>();
    return {
        afterClosed: vi.fn().mockReturnValue(afterClosed$.asObservable()),
        close: vi.fn().mockImplementation(() => {
            afterClosed$.next();
            afterClosed$.complete();
        }),
        componentInstance: {
            stepName: '',
            verificationRequest$: of(),
            stopVerification: stopVerification$.asObservable()
        }
    };
}

// --------------- Providers ---------------

const mockConnectorConfigService = {
    getConnector: vi.fn(),
    getSecrets: vi.fn(),
    updateConfigurationStep: vi.fn(),
    applyConnectorUpdate: vi.fn(),
    getStepDocumentation: vi.fn(),
    submitVerificationRequest: vi.fn(),
    getVerificationRequest: vi.fn(),
    deleteVerificationRequest: vi.fn(),
    getPropertyAllowableValues: vi.fn()
};

const mockSnackBar = { open: vi.fn(), openFromComponent: vi.fn() };

interface SetupOptions {
    config?: ConnectorWizardConfig | null;
}

function setup(options: SetupOptions = {}) {
    const mockDialog = {
        open: vi.fn().mockReturnValue(createMockDialogRef())
    };

    TestBed.configureTestingModule({
        providers: [
            StandardConnectorWizardStore,
            { provide: ConnectorWizardStore, useExisting: StandardConnectorWizardStore },
            { provide: ConnectorConfigurationService, useValue: mockConnectorConfigService },
            { provide: MatDialog, useValue: mockDialog },
            { provide: MatSnackBar, useValue: mockSnackBar },
            { provide: CONNECTOR_WIZARD_CONFIG, useValue: options.config ?? null }
        ]
    });

    const store = TestBed.inject(StandardConnectorWizardStore);
    return { store, mockDialog };
}

// Helper: initialize store with a connector ready for subsequent operations
function initConnector(store: InstanceType<typeof StandardConnectorWizardStore>, connector = makeConnector()) {
    store.initializeWithConnector(connector);
    return connector;
}

// --------------- Tests ---------------

describe('StandardConnectorWizardStore', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    // ═══════════════════════════════════════════════════════
    // Initial state
    // ═══════════════════════════════════════════════════════

    describe('initial state', () => {
        it('should create', () => {
            const { store } = setup();
            expect(store).toBeTruthy();
        });

        it('exposes correct initial signal values', () => {
            const { store } = setup();
            expect(store.connectorId()).toBeNull();
            expect(store.loading()).toBe(false);
            expect(store.status()).toBe('pending');
            expect(store.error()).toBeNull();
            expect(store.bannerErrors()).toEqual([]);
            expect(store.visibleStepNames()).toEqual([]);
        });
    });

    // ═══════════════════════════════════════════════════════
    // initializeWithConnector
    // ═══════════════════════════════════════════════════════

    describe('initializeWithConnector', () => {
        it('populates connector, stepNames, stepConfigurations, and status', () => {
            const { store } = setup();
            store.initializeWithConnector(makeConnector());
            expect(store.connectorId()).toBe('connector-1');
            expect(store.stepNames()).toEqual(['Step A', 'Step B']);
            expect(store.status()).toBe('configuring');
        });

        it('sets visibleStepNames from step names', () => {
            const { store } = setup();
            store.initializeWithConnector(makeConnector());
            expect(store.visibleStepNames()).toEqual(['Step A', 'Step B']);
        });
    });

    // ═══════════════════════════════════════════════════════
    // Banner errors
    // ═══════════════════════════════════════════════════════

    describe('banner error management', () => {
        it('addBannerError appends to bannerErrors', () => {
            const { store } = setup();
            store.addBannerError('Error one');
            store.addBannerError('Error two');
            expect(store.bannerErrors()).toEqual(['Error one', 'Error two']);
        });

        it('clearBannerErrors empties bannerErrors', () => {
            const { store } = setup();
            store.addBannerError('Some error');
            store.clearBannerErrors();
            expect(store.bannerErrors()).toEqual([]);
        });

        it('changeStep clears banner errors', () => {
            const { store } = setup();
            store.addBannerError('banner message');
            store.changeStep(1);
            expect(store.bannerErrors()).toEqual([]);
        });
    });

    // ═══════════════════════════════════════════════════════
    // Unsaved step values
    // ═══════════════════════════════════════════════════════

    describe('unsaved step values', () => {
        it('updateUnsavedStepValues stores values per step', () => {
            const { store } = setup();
            store.updateUnsavedStepValues({ stepName: 'Step A', values: { host: 'localhost' } });
            expect(store.unsavedStepValues()['Step A']).toEqual({ host: 'localhost' });
        });

        it('clearUnsavedStepValues removes step entry', () => {
            const { store } = setup();
            store.updateUnsavedStepValues({ stepName: 'Step A', values: { host: 'localhost' } });
            store.clearUnsavedStepValues('Step A');
            expect(store.unsavedStepValues()['Step A']).toBeUndefined();
        });
    });

    // ═══════════════════════════════════════════════════════
    // Per-step signal factories
    // ═══════════════════════════════════════════════════════

    describe('per-step signal factories', () => {
        it('stepConfiguration returns the config for a known step', () => {
            const { store } = setup();
            store.initializeWithConnector(makeConnector());
            expect(store.stepConfiguration('Step A')()?.configurationStepName).toBe('Step A');
        });

        it('stepConfiguration returns null for an unknown step', () => {
            const { store } = setup();
            expect(store.stepConfiguration('Unknown')()).toBeNull();
        });

        it('stepSaving returns false by default', () => {
            const { store } = setup();
            expect(store.stepSaving('Step A')()).toBe(false);
        });

        it('documentationForStep returns null by default', () => {
            const { store } = setup();
            expect(store.documentationForStep('Step A')()).toBeNull();
        });
    });

    // ═══════════════════════════════════════════════════════
    // Synchronous state mutations
    // ═══════════════════════════════════════════════════════

    describe('synchronous state mutations', () => {
        it('toggleDocumentationPanel updates documentationPanelOpen', () => {
            const { store } = setup();
            store.toggleDocumentationPanel(false);
            expect(store.documentationPanelOpen()).toBe(false);
        });

        it('initializeAssets sets assetsByProperty', () => {
            const { store } = setup();
            const asset = { id: 'asset-1', name: 'file.csv', contentType: 'text/csv', size: 100 };
            store.initializeAssets({ myProp: [asset] });
            expect(store.assetsByProperty()['myProp']).toHaveLength(1);
        });

        it('resetState reverts all signals to initial values', () => {
            const { store } = setup();
            store.initializeWithConnector(makeConnector());
            store.addBannerError('test');
            store.resetState();
            expect(store.connectorId()).toBeNull();
            expect(store.status()).toBe('pending');
            expect(store.bannerErrors()).toEqual([]);
        });
    });

    // ═══════════════════════════════════════════════════════
    // loadConnector
    // ═══════════════════════════════════════════════════════

    describe('loadConnector', () => {
        it('calls the service and populates state on success', () => {
            const { store } = setup();
            const connector = makeConnector();
            mockConnectorConfigService.getConnector.mockReturnValue(of(connector));
            mockConnectorConfigService.getSecrets.mockReturnValue(of({ secrets: [] }));

            store.loadConnector('connector-1');

            expect(mockConnectorConfigService.getConnector).toHaveBeenCalledWith('connector-1');
            expect(store.connectorId()).toBe('connector-1');
            expect(store.status()).toBe('configuring');
        });

        it('automatically loads secrets after loading the connector', () => {
            const { store } = setup();
            const secrets = [{ id: 'secret-1', name: 'DB Password' }];
            mockConnectorConfigService.getConnector.mockReturnValue(of(makeConnector()));
            mockConnectorConfigService.getSecrets.mockReturnValue(of({ secrets }));

            store.loadConnector('connector-1');

            expect(mockConnectorConfigService.getSecrets).toHaveBeenCalledWith('connector-1');
            expect(store.availableSecrets()).toEqual(secrets);
            expect(store.secretsLoading()).toBe(false);
        });

        it('sets status to error when the service call fails', () => {
            const { store } = setup();
            mockConnectorConfigService.getConnector.mockReturnValue(
                throwError(() => ({ error: { message: 'Not found' }, message: 'Not found' }))
            );

            store.loadConnector('bad-id');

            expect(store.status()).toBe('error');
        });
    });

    // ═══════════════════════════════════════════════════════
    // loadSecrets
    // ═══════════════════════════════════════════════════════

    describe('loadSecrets', () => {
        it('populates availableSecrets on success', () => {
            const { store } = setup();
            const secrets = [{ id: 'secret-1', name: 'My Secret' }];
            mockConnectorConfigService.getSecrets.mockReturnValue(of({ secrets }));

            store.loadSecrets('connector-1');

            expect(mockConnectorConfigService.getSecrets).toHaveBeenCalledWith('connector-1');
            expect(store.availableSecrets()).toEqual(secrets);
            expect(store.secretsLoading()).toBe(false);
            expect(store.secretsError()).toBeNull();
        });

        it('sets secretsError on failure', () => {
            const { store } = setup();
            mockConnectorConfigService.getSecrets.mockReturnValue(
                throwError(() => ({ error: { message: 'Secrets unavailable' }, message: 'Secrets unavailable' }))
            );

            store.loadSecrets('connector-1');

            expect(store.secretsLoading()).toBe(false);
            expect(store.secretsError()).toBe('Secrets unavailable');
        });
    });

    // ═══════════════════════════════════════════════════════
    // saveStep
    // ═══════════════════════════════════════════════════════

    describe('saveStep', () => {
        it('saves the step and marks it complete on success', () => {
            const { store } = setup();
            initConnector(store);
            mockConnectorConfigService.updateConfigurationStep.mockReturnValue(of(makeStepEntity('Step A')));

            store.saveStep({ stepName: 'Step A', stepConfiguration: makeStep('Step A') });

            expect(mockConnectorConfigService.updateConfigurationStep).toHaveBeenCalled();
            expect(store.completedSteps()['Step A']).toBe(true);
            expect(store.stepSaving('Step A')()).toBe(false);
            expect(store.status()).toBe('configuring');
            expect(store.bannerErrors()).toEqual([]);
        });

        it('updates parentConnectorRevision from the response', () => {
            const { store } = setup();
            initConnector(store);
            mockConnectorConfigService.updateConfigurationStep.mockReturnValue(of(makeStepEntity('Step A', 5)));

            store.saveStep({ stepName: 'Step A', stepConfiguration: makeStep('Step A') });

            expect(store.parentConnectorRevision()).toEqual({ version: 5 });
        });

        it('clears unsaved values for the saved step on success', () => {
            const { store } = setup();
            initConnector(store);
            store.updateUnsavedStepValues({ stepName: 'Step A', values: { host: 'localhost' } });
            mockConnectorConfigService.updateConfigurationStep.mockReturnValue(of(makeStepEntity('Step A')));

            store.saveStep({ stepName: 'Step A', stepConfiguration: makeStep('Step A') });

            expect(store.unsavedStepValues()['Step A']).toBeUndefined();
        });

        it('adds a banner error on failure', () => {
            const { store } = setup();
            initConnector(store);
            mockConnectorConfigService.updateConfigurationStep.mockReturnValue(
                throwError(() => ({ error: { message: 'Save failed' }, message: 'Save failed' }))
            );

            store.saveStep({ stepName: 'Step A', stepConfiguration: makeStep('Step A') });

            expect(store.bannerErrors()).toContain('Save failed');
            expect(store.stepSaving('Step A')()).toBe(false);
            expect(store.status()).toBe('configuring');
        });

        it('does nothing when connectorId is missing', () => {
            const { store } = setup();
            // No initializeWithConnector — connectorId is null

            store.saveStep({ stepName: 'Step A', stepConfiguration: makeStep('Step A') });

            expect(mockConnectorConfigService.updateConfigurationStep).not.toHaveBeenCalled();
        });

        it('sets status to saving while the request is in-flight', () => {
            const { store } = setup();
            initConnector(store);
            // Block the response so we can observe in-flight state
            const subject = new Subject<ConfigurationStepEntity>();
            mockConnectorConfigService.updateConfigurationStep.mockReturnValue(subject.asObservable());

            store.saveStep({ stepName: 'Step A', stepConfiguration: makeStep('Step A') });

            expect(store.status()).toBe('saving');
            expect(store.stepSaving('Step A')()).toBe(true);

            subject.next(makeStepEntity('Step A'));
            subject.complete();

            expect(store.status()).toBe('configuring');
        });
    });

    // ═══════════════════════════════════════════════════════
    // applyConfiguration
    // ═══════════════════════════════════════════════════════

    describe('applyConfiguration', () => {
        it('sets status to complete on success', () => {
            const { store } = setup();
            const connector = initConnector(store);
            const updatedConnector = makeConnector({ revision: { version: 2 } });
            mockConnectorConfigService.applyConnectorUpdate.mockReturnValue(of(updatedConnector));

            store.applyConfiguration();

            expect(mockConnectorConfigService.applyConnectorUpdate).toHaveBeenCalledWith(
                connector.id,
                connector.revision,
                false
            );
            expect(store.status()).toBe('complete');
        });

        it('updates connector from the response', () => {
            const { store } = setup();
            initConnector(store);
            const updatedConnector = makeConnector({ revision: { version: 9 } });
            mockConnectorConfigService.applyConnectorUpdate.mockReturnValue(of(updatedConnector));

            store.applyConfiguration();

            expect(store.connector()).toEqual(updatedConnector);
        });

        it('sets status to error on failure', () => {
            const { store } = setup();
            initConnector(store);
            mockConnectorConfigService.applyConnectorUpdate.mockReturnValue(
                throwError(() => ({ error: { message: 'Apply failed' }, message: 'Apply failed' }))
            );

            store.applyConfiguration();

            expect(store.status()).toBe('error');
            expect(store.error()).toBe('Apply failed');
        });

        it('does nothing when no connector is loaded', () => {
            const { store } = setup();
            store.applyConfiguration();
            expect(mockConnectorConfigService.applyConnectorUpdate).not.toHaveBeenCalled();
        });

        it('calls onApplySuccess config callback on success', () => {
            const onApplySuccess = vi.fn();
            const { store } = setup({ config: { onApplySuccess } as unknown as ConnectorWizardConfig });
            initConnector(store);
            mockConnectorConfigService.applyConnectorUpdate.mockReturnValue(of(makeConnector()));

            store.applyConfiguration();

            expect(onApplySuccess).toHaveBeenCalledWith('connector-1');
        });
    });

    // ═══════════════════════════════════════════════════════
    // refreshConnectorForSummary
    // ═══════════════════════════════════════════════════════

    describe('refreshConnectorForSummary', () => {
        it('updates the connector from the service response', () => {
            const { store } = setup();
            initConnector(store);
            const refreshed = makeConnector({ revision: { version: 3 } });
            mockConnectorConfigService.getConnector.mockReturnValue(of(refreshed));

            store.refreshConnectorForSummary();

            expect(store.connector()).toEqual(refreshed);
            expect(store.loading()).toBe(false);
        });

        it('sets error state when refresh fails', () => {
            const { store } = setup();
            initConnector(store);
            mockConnectorConfigService.getConnector.mockReturnValue(
                throwError(() => ({ error: { message: 'Refresh failed' }, message: 'Refresh failed' }))
            );

            store.refreshConnectorForSummary();

            expect(store.status()).toBe('error');
            expect(store.error()).toBe('Refresh failed');
        });
    });

    // ═══════════════════════════════════════════════════════
    // loadStepDocumentation
    // ═══════════════════════════════════════════════════════

    describe('loadStepDocumentation', () => {
        it('loads documentation and marks it as loaded on success', () => {
            const config = {
                connectorMetadata: { group: 'org.test', artifact: 'test', version: '1.0', type: 'TestConnector' }
            } as unknown as ConnectorWizardConfig;
            const { store } = setup({ config });
            initConnector(store);
            mockConnectorConfigService.getStepDocumentation.mockReturnValue(
                of({ stepDocumentation: '## Step A\nDo this.' })
            );

            store.loadStepDocumentation('Step A');

            const doc = store.documentationForStep('Step A')();
            expect(doc?.stepDocumentation).toBe('## Step A\nDo this.');
            expect(doc?.loaded).toBe(true);
            expect(doc?.loading).toBe(false);
            expect(doc?.error).toBeNull();
        });

        it('sets error state when documentation fetch fails', () => {
            const config = {
                connectorMetadata: { group: 'org.test', artifact: 'test', version: '1.0', type: 'TestConnector' }
            } as unknown as ConnectorWizardConfig;
            const { store } = setup({ config });
            initConnector(store);
            mockConnectorConfigService.getStepDocumentation.mockReturnValue(
                throwError(() => ({ error: { message: 'Docs not found' }, message: 'Docs not found' }))
            );

            store.loadStepDocumentation('Step A');

            const doc = store.documentationForStep('Step A')();
            expect(doc?.error).toBe('Docs not found');
            expect(doc?.loading).toBe(false);
        });

        it('falls back to connector bundle metadata when connectorMetadata config is absent', () => {
            const { store } = setup();
            initConnector(store);
            mockConnectorConfigService.getStepDocumentation.mockReturnValue(of({ stepDocumentation: '## fallback' }));

            store.loadStepDocumentation('Step A');

            expect(mockConnectorConfigService.getStepDocumentation).toHaveBeenCalledWith(
                'org.test',
                'test',
                '1.0',
                'TestConnector',
                'Step A'
            );
        });

        it('does not call service when documentation is already loaded', () => {
            const config = {
                connectorMetadata: { group: 'org.test', artifact: 'test', version: '1.0', type: 'TestConnector' }
            } as unknown as ConnectorWizardConfig;
            const { store } = setup({ config });
            initConnector(store);
            mockConnectorConfigService.getStepDocumentation.mockReturnValue(of({ stepDocumentation: '## cached' }));

            store.loadStepDocumentation('Step A');
            mockConnectorConfigService.getStepDocumentation.mockClear();
            store.loadStepDocumentation('Step A');

            expect(mockConnectorConfigService.getStepDocumentation).not.toHaveBeenCalled();
        });
    });

    // ═══════════════════════════════════════════════════════
    // fetchPropertyAllowableValues
    // ═══════════════════════════════════════════════════════

    describe('fetchPropertyAllowableValues', () => {
        it('populates dynamic allowable values on success', () => {
            const { store } = setup();
            initConnector(store);
            const allowableValues = [{ value: 'option1', displayName: 'Option 1' }];
            mockConnectorConfigService.getPropertyAllowableValues.mockReturnValue(of({ allowableValues }));

            store.fetchPropertyAllowableValues({
                stepName: 'Step A',
                propertyGroupName: 'Basic',
                propertyName: 'mode'
            });

            const key = createPropertyKey('Step A', 'Basic', 'mode');
            const state = store.dynamicAllowableValues()[key];
            expect(state?.values).toEqual(allowableValues);
            expect(state?.loading).toBe(false);
        });

        it('sets loading state while the request is in-flight', () => {
            const { store } = setup();
            initConnector(store);
            const subject = new Subject<{ allowableValues: AllowableValue[] }>();
            mockConnectorConfigService.getPropertyAllowableValues.mockReturnValue(subject.asObservable());

            store.fetchPropertyAllowableValues({
                stepName: 'Step A',
                propertyGroupName: 'Basic',
                propertyName: 'mode'
            });

            const key = createPropertyKey('Step A', 'Basic', 'mode');
            expect(store.dynamicAllowableValues()[key]?.loading).toBe(true);

            subject.next({ allowableValues: [] });
            subject.complete();

            expect(store.dynamicAllowableValues()[key]?.loading).toBe(false);
        });

        it('sets error state on failure', () => {
            const { store } = setup();
            initConnector(store);
            mockConnectorConfigService.getPropertyAllowableValues.mockReturnValue(
                throwError(() => ({ error: { message: 'Values unavailable' }, message: 'Values unavailable' }))
            );

            store.fetchPropertyAllowableValues({
                stepName: 'Step A',
                propertyGroupName: 'Basic',
                propertyName: 'mode'
            });

            const key = createPropertyKey('Step A', 'Basic', 'mode');
            expect(store.dynamicAllowableValues()[key]?.error).toBe('Values unavailable');
        });

        it('passes the optional filter parameter to the service', () => {
            const { store } = setup();
            initConnector(store);
            mockConnectorConfigService.getPropertyAllowableValues.mockReturnValue(of({ allowableValues: [] }));

            store.fetchPropertyAllowableValues({
                stepName: 'Step A',
                propertyGroupName: 'Basic',
                propertyName: 'mode',
                filter: 'search-term'
            });

            expect(mockConnectorConfigService.getPropertyAllowableValues).toHaveBeenCalledWith(
                'connector-1',
                'Step A',
                'Basic',
                'mode',
                'search-term'
            );
        });
    });

    // ═══════════════════════════════════════════════════════
    // verifyAllSteps
    // ═══════════════════════════════════════════════════════

    describe('verifyAllSteps', () => {
        // For all verification tests we use `complete: true` in the submit response so that
        // we skip the polling path (interval-based) and call deleteVerificationRequest directly.

        it('sets verificationPassed to true when all steps succeed', () => {
            const { store } = setup();
            initConnector(store);
            mockConnectorConfigService.submitVerificationRequest.mockReturnValue(
                of(makeVerificationEntity('req-1', true))
            );
            mockConnectorConfigService.deleteVerificationRequest.mockReturnValue(
                of(makeVerificationEntity('req-1', true, [makeVerificationResult('SUCCESSFUL')]))
            );

            store.verifyAllSteps();

            expect(store.verificationPassed()).toBe(true);
            expect(store.allStepsVerifying()).toBe(false);
        });

        it('sets verificationPassed to false when a step fails', () => {
            const { store } = setup();
            initConnector(store);
            mockConnectorConfigService.submitVerificationRequest.mockReturnValue(
                of(makeVerificationEntity('req-1', true))
            );
            mockConnectorConfigService.deleteVerificationRequest.mockReturnValue(
                of(makeVerificationEntity('req-1', true, [makeVerificationResult('FAILED')]))
            );

            store.verifyAllSteps();

            expect(store.verificationPassed()).toBe(false);
            expect(store.allStepsVerifying()).toBe(false);
        });

        it('sets subject verification errors for failures with a subject', () => {
            const { store } = setup();
            const singleStepConnector = makeConnector({
                component: {
                    ...makeConnector().component,
                    workingConfiguration: {
                        configurationStepConfigurations: [makeStep('Step A')]
                    }
                }
            } as any);
            initConnector(store, singleStepConnector);
            mockConnectorConfigService.submitVerificationRequest.mockReturnValue(
                of(makeVerificationEntity('req-1', true))
            );
            const failedResult = makeVerificationResult('FAILED', 'prop.host');
            mockConnectorConfigService.deleteVerificationRequest.mockReturnValue(
                of(makeVerificationEntity('req-1', true, [failedResult]))
            );

            store.verifyAllSteps();

            expect(store.subjectVerificationErrors()['prop.host']).toBe('Connection refused');
        });

        it('sets allStepsVerification.error on HTTP failure', () => {
            const { store } = setup();
            initConnector(store);
            mockConnectorConfigService.submitVerificationRequest.mockReturnValue(
                throwError(() => ({ error: { message: 'Service error' }, message: 'Service error' }))
            );

            store.verifyAllSteps();

            expect(store.verifyAllError()).toBe('Service error');
            expect(store.allStepsVerifying()).toBe(false);
        });

        it('clears banner errors and verification errors before starting', () => {
            const { store } = setup();
            initConnector(store);
            store.addBannerError('old error');
            mockConnectorConfigService.submitVerificationRequest.mockReturnValue(
                of(makeVerificationEntity('req-1', true))
            );
            mockConnectorConfigService.deleteVerificationRequest.mockReturnValue(
                of(makeVerificationEntity('req-1', true, [makeVerificationResult('SUCCESSFUL')]))
            );

            store.verifyAllSteps();

            // bannerErrors cleared at the start of verification
            expect(store.bannerErrors()).toEqual([]);
        });

        it('verifies all visible steps in sequence', () => {
            const { store } = setup();
            const connector = makeConnector({
                component: {
                    ...makeConnector().component,
                    workingConfiguration: {
                        configurationStepConfigurations: [makeStep('Step A'), makeStep('Step B')]
                    }
                }
            } as any);
            initConnector(store, connector);

            mockConnectorConfigService.submitVerificationRequest.mockReturnValue(
                of(makeVerificationEntity('req-1', true))
            );
            mockConnectorConfigService.deleteVerificationRequest.mockReturnValue(
                of(makeVerificationEntity('req-1', true, [makeVerificationResult('SUCCESSFUL')]))
            );

            store.verifyAllSteps();

            expect(mockConnectorConfigService.submitVerificationRequest).toHaveBeenCalledTimes(2);
            expect(mockConnectorConfigService.submitVerificationRequest).toHaveBeenCalledWith(
                'connector-1',
                'Step A',
                expect.anything()
            );
            expect(mockConnectorConfigService.submitVerificationRequest).toHaveBeenCalledWith(
                'connector-1',
                'Step B',
                expect.anything()
            );
        });

        it('does nothing when no connector is loaded', () => {
            const { store } = setup();
            store.verifyAllSteps();
            expect(mockConnectorConfigService.submitVerificationRequest).not.toHaveBeenCalled();
        });

        it('polls getVerificationRequest until the request completes', fakeAsync(() => {
            const { store } = setup();
            const singleStepConnector = makeConnector({
                component: {
                    ...makeConnector().component,
                    workingConfiguration: {
                        configurationStepConfigurations: [makeStep('Step A')]
                    }
                }
            } as any);
            initConnector(store, singleStepConnector);

            // Initial submit returns incomplete (triggers the polling branch)
            mockConnectorConfigService.submitVerificationRequest.mockReturnValue(
                of(makeVerificationEntity('req-1', false))
            );
            // First poll: still running; second poll: complete
            mockConnectorConfigService.getVerificationRequest
                .mockReturnValueOnce(of(makeVerificationEntity('req-1', false)))
                .mockReturnValueOnce(of(makeVerificationEntity('req-1', true)));
            mockConnectorConfigService.deleteVerificationRequest.mockReturnValue(
                of(makeVerificationEntity('req-1', true, [makeVerificationResult('SUCCESSFUL')]))
            );

            store.verifyAllSteps();

            // Advance past first poll interval — request still incomplete
            tick(2000);
            expect(mockConnectorConfigService.getVerificationRequest).toHaveBeenCalledTimes(1);
            expect(store.allStepsVerifying()).toBe(true);

            // Advance past second poll interval — request now complete
            tick(2000);

            expect(mockConnectorConfigService.getVerificationRequest).toHaveBeenCalledTimes(2);
            expect(mockConnectorConfigService.deleteVerificationRequest).toHaveBeenCalledTimes(1);
            expect(store.verificationPassed()).toBe(true);
            expect(store.allStepsVerifying()).toBe(false);
        }));
    });

    // ═══════════════════════════════════════════════════════
    // saveAndClose
    // ═══════════════════════════════════════════════════════

    describe('saveAndClose', () => {
        it('saves the step, clears savingSteps, and calls onNavigateBack on success', () => {
            const onNavigateBack = vi.fn();
            const { store } = setup({ config: { onNavigateBack } as unknown as ConnectorWizardConfig });
            initConnector(store);
            mockConnectorConfigService.updateConfigurationStep.mockReturnValue(of(makeStepEntity('Step A')));

            store.saveAndClose({ stepName: 'Step A', stepConfiguration: makeStep('Step A') });

            expect(mockConnectorConfigService.updateConfigurationStep).toHaveBeenCalled();
            expect(store.stepSaving('Step A')()).toBe(false);
            expect(onNavigateBack).toHaveBeenCalledWith('connector-1');
        });

        it('adds a banner error and clears savingSteps on failure', () => {
            const onNavigateBack = vi.fn();
            const { store } = setup({ config: { onNavigateBack } as unknown as ConnectorWizardConfig });
            initConnector(store);
            mockConnectorConfigService.updateConfigurationStep.mockReturnValue(
                throwError(() => ({ error: { message: 'Save failed' }, message: 'Save failed' }))
            );

            store.saveAndClose({ stepName: 'Step A', stepConfiguration: makeStep('Step A') });

            expect(store.bannerErrors()).toContain('Save failed');
            expect(store.stepSaving('Step A')()).toBe(false);
            expect(onNavigateBack).not.toHaveBeenCalled();
        });

        it('navigates immediately without API call when stepConfiguration is null', () => {
            const onNavigateBack = vi.fn();
            const { store } = setup({ config: { onNavigateBack } as unknown as ConnectorWizardConfig });
            initConnector(store);

            store.saveAndClose({ stepName: 'Step A', stepConfiguration: null });

            expect(mockConnectorConfigService.updateConfigurationStep).not.toHaveBeenCalled();
            expect(onNavigateBack).toHaveBeenCalledWith('connector-1');
        });

        it('sets savingSteps to true while the request is in-flight', () => {
            const { store } = setup();
            initConnector(store);
            const subject = new Subject<ConfigurationStepEntity>();
            mockConnectorConfigService.updateConfigurationStep.mockReturnValue(subject.asObservable());

            store.saveAndClose({ stepName: 'Step A', stepConfiguration: makeStep('Step A') });

            expect(store.stepSaving('Step A')()).toBe(true);

            subject.next(makeStepEntity('Step A'));
            subject.complete();

            expect(store.stepSaving('Step A')()).toBe(false);
        });
    });

    // ═══════════════════════════════════════════════════════
    // Assets management
    // ═══════════════════════════════════════════════════════

    describe('assets management', () => {
        it('initializeAssets sets assetsByProperty', () => {
            const { store } = setup();
            const asset = { id: 'asset-1', name: 'file.csv', contentType: 'text/csv', size: 100 };
            store.initializeAssets({ myProp: [asset] });
            expect(store.assetsByProperty()['myProp']).toHaveLength(1);
        });
    });
});
