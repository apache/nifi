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

import { Component, forwardRef, input, NO_ERRORS_SCHEMA, output, signal } from '@angular/core';
import { TestBed } from '@angular/core/testing';
import { ControlValueAccessor, NG_VALUE_ACCESSOR } from '@angular/forms';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { SharedConnectorConfigurationStep } from './connector-configuration-step.component';
import { ConnectorPropertyInput } from '../../connector-property-input/connector-property-input.component';
import { ConnectorWizardStore } from '../connector-wizard.store';
import { CONNECTOR_WIZARD_CONFIG } from '../connector-wizard.types';
import { ConnectorConfigurationService } from '../../../services/connector-configuration.service';
import { UploadService } from '../../../services/upload.service';
import { ActiveStepService } from '../../../services/active-step.service';
import {
    AssetInfo,
    ConfigurationStepConfiguration,
    ConnectorPropertyDescriptor,
    ConnectorValueReference,
    PropertyAllowableValuesState,
    Secret,
    UploadProgressInfo
} from '../../../types';

// --------------- Property/Step helpers ---------------

function makeProp(name: string, overrides: Partial<ConnectorPropertyDescriptor> = {}): ConnectorPropertyDescriptor {
    return {
        name,
        type: 'STRING',
        required: false,
        dependencies: [],
        ...overrides
    };
}

function makeStepConfig(
    stepName: string,
    properties: ConnectorPropertyDescriptor[] = [],
    propertyValues: Record<string, ConnectorValueReference> = {}
): ConfigurationStepConfiguration {
    const propertyDescriptors: Record<string, ConnectorPropertyDescriptor> = {};
    properties.forEach((p) => (propertyDescriptors[p.name] = p));

    return {
        configurationStepName: stepName,
        propertyGroupConfigurations: [
            {
                propertyGroupName: 'Basic',
                propertyDescriptors,
                propertyValues
            }
        ],
        dependencies: []
    };
}

// --------------- Mock store factory ---------------
// bannerErrors / subjectVerificationErrors / assetsByProperty MUST be real Angular Signals
// because the component passes them to toObservable().

function createMockStore(
    options: {
        stepConfig?: ConfigurationStepConfiguration | null;
        unsavedValues?: Record<string, unknown>;
        connectorId?: string | null;
    } = {}
) {
    const stepConfigSignal = signal<ConfigurationStepConfiguration | null>(options.stepConfig ?? null);
    return {
        // Real signals (required by toObservable)
        bannerErrors: signal<string[]>([]),
        subjectVerificationErrors: signal<Record<string, string>>({}),
        assetsByProperty: signal<Record<string, unknown[]>>({}),

        // Reactive step config
        stepConfiguration: vi.fn().mockReturnValue(stepConfigSignal),

        // Other plain callables
        connectorId: vi.fn().mockReturnValue(options.connectorId ?? null),
        documentationPanelOpen: vi.fn().mockReturnValue(false),
        availableSecrets: vi.fn().mockReturnValue(null),
        secretsLoading: vi.fn().mockReturnValue(false),
        secretsError: vi.fn().mockReturnValue(null),
        verifying: signal(false),
        allStepsVerifying: signal(false),
        unsavedStepValues: vi.fn().mockReturnValue(options.unsavedValues ? { 'test-step': options.unsavedValues } : {}),
        dynamicAllowableValues: vi.fn().mockReturnValue({}),

        // Per-step signal factories
        stepSaving: vi.fn().mockReturnValue(vi.fn().mockReturnValue(false)),
        generalVerificationErrorsForStep: vi.fn().mockReturnValue(vi.fn().mockReturnValue([])),
        documentationForStep: vi.fn().mockReturnValue(vi.fn().mockReturnValue(null)),

        // Methods
        saveStep: vi.fn(),
        saveAndClose: vi.fn(),
        advanceWithoutSaving: vi.fn(),
        verifyStep: vi.fn(),
        addBannerError: vi.fn(),
        clearBannerErrors: vi.fn(),
        toggleDocumentationPanel: vi.fn(),
        initializeAssets: vi.fn(),
        fetchPropertyAllowableValues: vi.fn(),
        clearSubjectVerificationError: vi.fn(),
        clearAssetsForProperty: vi.fn(),
        addAsset: vi.fn(),
        removeAsset: vi.fn(),
        updateUnsavedStepValues: vi.fn(),
        markStepDirty: vi.fn(),
        setStepVerificationResults: vi.fn(),

        // Expose to allow direct signal manipulation in tests
        _stepConfigSignal: stepConfigSignal
    };
}

// --------------- Mock services ---------------

const mockConnectorConfigService = {
    getConnector: vi.fn(),
    getSecrets: vi.fn(),
    updateConfigurationStep: vi.fn(),
    applyConnectorUpdate: vi.fn(),
    getStepDocumentation: vi.fn(),
    verifyConfigurationStep: vi.fn(),
    getPropertyAllowableValues: vi.fn()
};
const mockUploadService = { upload: vi.fn() };
const mockActiveStepService = {
    activeStep: null,
    register: vi.fn(),
    unregister: vi.fn()
};

/**
 * Minimal CVA stub with the same inputs/outputs as ConnectorPropertyInput — avoids NG0200
 * circular DI in the real component under TestBed.
 */
@Component({
    selector: 'connector-property-input',
    standalone: true,
    template: '',
    providers: [
        {
            provide: NG_VALUE_ACCESSOR,
            useExisting: forwardRef(() => StubConnectorPropertyInput),
            multi: true
        }
    ]
})
class StubConnectorPropertyInput implements ControlValueAccessor {
    readonly property = input.required<ConnectorPropertyDescriptor>();
    readonly dynamicAllowableValuesState = input<PropertyAllowableValuesState | null>(null);
    readonly currentAssets = input<AssetInfo[]>([]);
    readonly assetUploadProgress = input<UploadProgressInfo[]>([]);
    readonly availableSecrets = input<Secret[] | null>(null);
    readonly secretsLoading = input(false);
    readonly secretsError = input<string | null>(null);
    readonly requestAllowableValues = output<void>();
    readonly assetFilesSelected = output<File[]>();
    readonly assetDeleteRequested = output<AssetInfo>();
    readonly dismissFailedUploadRequested = output<UploadProgressInfo>();

    writeValue(): void {
        /* stub */
    }
    registerOnChange(): void {
        /* stub */
    }
    registerOnTouched(): void {
        /* stub */
    }
    setDisabledState(): void {
        /* stub */
    }
}

// --------------- Setup ---------------

interface SetupOptions {
    stepName?: string;
    isFirstStep?: boolean;
    stepConfig?: ConfigurationStepConfiguration | null;
    unsavedValues?: Record<string, unknown>;
    connectorId?: string | null;
}

async function setup(options: SetupOptions = {}) {
    const mockStore = createMockStore({
        stepConfig: options.stepConfig,
        unsavedValues: options.unsavedValues,
        connectorId: options.connectorId
    });

    await TestBed.configureTestingModule({
        imports: [SharedConnectorConfigurationStep, NoopAnimationsModule],
        providers: [
            { provide: ConnectorWizardStore, useValue: mockStore },
            { provide: ConnectorConfigurationService, useValue: mockConnectorConfigService },
            { provide: UploadService, useValue: mockUploadService },
            { provide: ActiveStepService, useValue: mockActiveStepService },
            { provide: CONNECTOR_WIZARD_CONFIG, useValue: null }
        ],
        schemas: [NO_ERRORS_SCHEMA]
    })
        .overrideComponent(SharedConnectorConfigurationStep, {
            remove: { imports: [ConnectorPropertyInput] },
            add: { imports: [StubConnectorPropertyInput] }
        })
        .compileComponents();

    const fixture = TestBed.createComponent(SharedConnectorConfigurationStep);
    const component = fixture.componentInstance;

    fixture.componentRef.setInput('stepName', options.stepName ?? 'test-step');
    fixture.componentRef.setInput('isFirstStep', options.isFirstStep ?? false);

    fixture.detectChanges();

    return { fixture, component, mockStore };
}

// --------------- Tests ---------------

describe('SharedConnectorConfigurationStep', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    // ═══════════════════════════════════════════════════════
    // Creation
    // ═══════════════════════════════════════════════════════

    describe('creation', () => {
        it('should create', async () => {
            const { component } = await setup();
            expect(component).toBeTruthy();
        });
    });

    // ═══════════════════════════════════════════════════════
    // ngOnInit / ActiveStepService
    // ═══════════════════════════════════════════════════════

    describe('ngOnInit', () => {
        it('registers with ActiveStepService', async () => {
            await setup();
            expect(mockActiveStepService.register).toHaveBeenCalledTimes(1);
        });
    });

    describe('ngOnDestroy', () => {
        it('unregisters from ActiveStepService on destroy', async () => {
            const { fixture, component } = await setup();
            fixture.destroy();
            expect(mockActiveStepService.unregister).toHaveBeenCalledWith(component);
        });
    });

    // ═══════════════════════════════════════════════════════
    // Form initialization
    // ═══════════════════════════════════════════════════════

    describe('form initialization', () => {
        it('creates an empty form when no step configuration is available', async () => {
            const { component } = await setup({ stepConfig: null });
            expect(Object.keys(component.stepForm.controls)).toHaveLength(0);
            expect(component.formReady).toBe(true);
        });

        it('creates controls for each property in the step config', async () => {
            const stepConfig = makeStepConfig('test-step', [makeProp('host'), makeProp('port')]);
            const { component } = await setup({ stepConfig });
            expect(component.stepForm.contains('host')).toBe(true);
            expect(component.stepForm.contains('port')).toBe(true);
            expect(component.formReady).toBe(true);
        });

        it('initializes form controls from the stored API values', async () => {
            const stepConfig = makeStepConfig('test-step', [makeProp('host')], {
                host: { valueType: 'STRING_LITERAL' as const, value: 'db.example.com' }
            });
            const { component } = await setup({ stepConfig });
            expect(component.stepForm.get('host')?.value).toBe('db.example.com');
        });

        it('unsaved values override API values', async () => {
            const stepConfig = makeStepConfig('test-step', [makeProp('host')], {
                host: { valueType: 'STRING_LITERAL' as const, value: 'old.host.com' }
            });
            const { component } = await setup({ stepConfig, unsavedValues: { host: 'new.host.com' } });
            expect(component.stepForm.get('host')?.value).toBe('new.host.com');
        });

        it('marks the form dirty when unsaved values are present', async () => {
            const stepConfig = makeStepConfig('test-step', [makeProp('host')]);
            const { component } = await setup({ stepConfig, unsavedValues: { host: 'unsaved' } });
            expect(component.stepForm.dirty).toBe(true);
        });

        it('calls markStepDirty when unsaved values are present', async () => {
            const stepConfig = makeStepConfig('test-step', [makeProp('host')]);
            const { mockStore } = await setup({ stepConfig, unsavedValues: { host: 'unsaved' } });
            expect(mockStore.markStepDirty).toHaveBeenCalledWith({ stepName: 'test-step', isDirty: true });
        });

        it('defaults BOOLEAN properties to false', async () => {
            const stepConfig = makeStepConfig('test-step', [makeProp('enabled', { type: 'BOOLEAN' })]);
            const { component } = await setup({ stepConfig });
            expect(component.stepForm.get('enabled')?.value).toBe(false);
        });

        it('defaults STRING_LIST properties to empty array', async () => {
            const stepConfig = makeStepConfig('test-step', [makeProp('tags', { type: 'STRING_LIST' })]);
            const { component } = await setup({ stepConfig });
            expect(component.stepForm.get('tags')?.value).toEqual([]);
        });
    });

    // ═══════════════════════════════════════════════════════
    // Form validators
    // ═══════════════════════════════════════════════════════

    describe('form validators', () => {
        it('applies required validator to required properties', async () => {
            const stepConfig = makeStepConfig('test-step', [makeProp('host', { required: true })]);
            const { component } = await setup({ stepConfig });
            const control = component.stepForm.get('host');
            control?.setValue('');
            expect(control?.hasError('required')).toBe(true);
        });

        it('does not apply required validator to optional properties', async () => {
            const stepConfig = makeStepConfig('test-step', [makeProp('description', { required: false })]);
            const { component } = await setup({ stepConfig });
            const control = component.stepForm.get('description');
            control?.setValue('');
            expect(control?.hasError('required')).toBe(false);
        });

        it('applies pattern validator to INTEGER properties', async () => {
            const stepConfig = makeStepConfig('test-step', [makeProp('port', { type: 'INTEGER' })]);
            const { component } = await setup({ stepConfig });
            const control = component.stepForm.get('port');
            control?.setValue('3.14');
            expect(control?.hasError('pattern')).toBe(true);
            control?.setValue('5432');
            expect(control?.hasError('pattern')).toBe(false);
        });
    });

    // ═══════════════════════════════════════════════════════
    // Property visibility / dependencies
    // ═══════════════════════════════════════════════════════

    describe('property visibility', () => {
        it('shows all properties that have no dependencies', async () => {
            const stepConfig = makeStepConfig('test-step', [makeProp('host'), makeProp('port')]);
            const { component } = await setup({ stepConfig });
            expect(component.isPropertyVisible(makeProp('host'))).toBe(true);
            expect(component.isPropertyVisible(makeProp('port'))).toBe(true);
        });

        it('hides a property when its dependency is not satisfied', async () => {
            const stepConfig = makeStepConfig('test-step', [
                makeProp('mode'),
                makeProp('advanced-setting', {
                    dependencies: [{ propertyName: 'mode', dependentValues: ['advanced'] }]
                })
            ]);
            const { component } = await setup({ stepConfig });

            // mode = '' (empty) → advanced-setting should be hidden
            expect(component.stepForm.get('advanced-setting')?.disabled).toBe(true);
            expect(component.isPropertyVisible(makeProp('advanced-setting'))).toBe(false);
        });

        it('shows a property when its dependency is satisfied', async () => {
            const stepConfig = makeStepConfig('test-step', [
                makeProp('mode'),
                makeProp('advanced-setting', {
                    dependencies: [{ propertyName: 'mode', dependentValues: ['advanced'] }]
                })
            ]);
            const { component } = await setup({ stepConfig });

            component.setPropertyValue('mode', 'advanced');
            component['computeAllPropertyVisibility']();

            expect(component.stepForm.get('advanced-setting')?.enabled).toBe(true);
            expect(component.isPropertyVisible(makeProp('advanced-setting'))).toBe(true);
        });

        it('hides a property when its dependency has no dependentValues and the parent is empty', async () => {
            const stepConfig = makeStepConfig('test-step', [
                makeProp('optional-host'),
                makeProp('host-port', { dependencies: [{ propertyName: 'optional-host' }] })
            ]);
            const { component } = await setup({ stepConfig });

            // optional-host = '' → host-port hidden
            expect(component.stepForm.get('host-port')?.disabled).toBe(true);
        });

        it('shows a property when its dependency has no dependentValues and the parent has a value', async () => {
            const stepConfig = makeStepConfig('test-step', [
                makeProp('optional-host'),
                makeProp('host-port', { dependencies: [{ propertyName: 'optional-host' }] })
            ]);
            const { component } = await setup({ stepConfig });

            component.setPropertyValue('optional-host', 'myhost.com');
            component['computeAllPropertyVisibility']();

            expect(component.stepForm.get('host-port')?.enabled).toBe(true);
        });
    });

    // ═══════════════════════════════════════════════════════
    // STRING_LIST unsaved value handling
    // ═══════════════════════════════════════════════════════

    describe('STRING_LIST unsaved value handling', () => {
        it('converts a comma-separated string unsaved value to an array', async () => {
            const stepConfig = makeStepConfig('test-step', [makeProp('tags', { type: 'STRING_LIST' })]);
            const { component } = await setup({ stepConfig, unsavedValues: { tags: 'a, b, c' } });
            expect(component.stepForm.get('tags')?.value).toEqual(['a', 'b', 'c']);
        });

        it('converts an empty string unsaved value to an empty array', async () => {
            const stepConfig = makeStepConfig('test-step', [makeProp('tags', { type: 'STRING_LIST' })]);
            const { component } = await setup({ stepConfig, unsavedValues: { tags: '' } });
            expect(component.stepForm.get('tags')?.value).toEqual([]);
        });

        it('preserves an already-array unsaved value', async () => {
            const stepConfig = makeStepConfig('test-step', [makeProp('tags', { type: 'STRING_LIST' })]);
            const { component } = await setup({ stepConfig, unsavedValues: { tags: ['x', 'y'] } });
            expect(component.stepForm.get('tags')?.value).toEqual(['x', 'y']);
        });
    });

    // ═══════════════════════════════════════════════════════
    // Back navigation
    // ═══════════════════════════════════════════════════════

    describe('onBack()', () => {
        it('emits back with null configuration when form is pristine', async () => {
            const stepConfig = makeStepConfig('test-step', [makeProp('host')]);
            const { component } = await setup({ stepConfig });
            const backSpy = vi.fn();
            component.back.subscribe(backSpy);

            component.onBack();

            expect(backSpy).toHaveBeenCalledWith({ stepName: 'test-step', configuration: null });
        });

        it('emits back with configuration when form is dirty', async () => {
            const stepConfig = makeStepConfig('test-step', [makeProp('host')]);
            const { component } = await setup({ stepConfig });
            const backSpy = vi.fn();
            component.back.subscribe(backSpy);

            component.stepForm.get('host')?.setValue('new-value');
            component.stepForm.markAsDirty();
            component.onBack();

            const emitted = backSpy.mock.calls[0][0];
            expect(emitted.stepName).toBe('test-step');
            expect(emitted.configuration).not.toBeNull();
        });

        it('emits back with null when stepConfiguration is null (no step data)', async () => {
            const { component } = await setup({ stepConfig: null });
            const backSpy = vi.fn();
            component.back.subscribe(backSpy);

            component.onBack();

            expect(backSpy).toHaveBeenCalledWith({ stepName: 'test-step', configuration: null });
        });
    });

    // ═══════════════════════════════════════════════════════
    // getConfigurationForSave
    // ═══════════════════════════════════════════════════════

    describe('getConfigurationForSave()', () => {
        it('returns isDirty=false and null configuration when form is pristine', async () => {
            const stepConfig = makeStepConfig('test-step', [makeProp('host')]);
            const { component } = await setup({ stepConfig });
            const result = component.getConfigurationForSave();
            expect(result.isDirty).toBe(false);
            expect(result.configuration).toBeNull();
            expect(result.stepName).toBe('test-step');
        });

        it('returns isDirty=true and configuration when form is dirty', async () => {
            const stepConfig = makeStepConfig('test-step', [makeProp('host')]);
            const { component } = await setup({ stepConfig });

            component.stepForm.get('host')?.setValue('new-host');
            component.stepForm.markAsDirty();

            const result = component.getConfigurationForSave();
            expect(result.isDirty).toBe(true);
            expect(result.configuration).not.toBeNull();
            expect(result.configuration?.configurationStepName).toBe('test-step');
        });

        it('returns isDirty=false when form is dirty but stepData is null', async () => {
            const { component } = await setup({ stepConfig: null });
            component.stepForm.markAsDirty();
            const result = component.getConfigurationForSave();
            expect(result.isDirty).toBe(false);
        });
    });

    // ═══════════════════════════════════════════════════════
    // Form changes → store updates
    // ═══════════════════════════════════════════════════════

    describe('form changes', () => {
        it('calls updateUnsavedStepValues and markStepDirty when a form value changes', async () => {
            const stepConfig = makeStepConfig('test-step', [makeProp('host')]);
            const { component, mockStore, fixture } = await setup({ stepConfig });

            // setupFormSubscription is deferred with Promise.resolve()
            await fixture.whenStable();

            component.stepForm.get('host')?.setValue('changed-value');

            expect(mockStore.updateUnsavedStepValues).toHaveBeenCalledWith({
                stepName: 'test-step',
                values: expect.objectContaining({ host: 'changed-value' })
            });
            expect(mockStore.markStepDirty).toHaveBeenCalledWith({ stepName: 'test-step', isDirty: true });
        });
    });

    // ═══════════════════════════════════════════════════════
    // Store signal delegation
    // ═══════════════════════════════════════════════════════

    describe('store signal references', () => {
        it('availableSecrets delegates to the wizard store', async () => {
            const { component, mockStore } = await setup();
            expect(component.availableSecrets).toBe(mockStore.availableSecrets);
        });

        it('isVerifying reflects verifying state from the wizard store', async () => {
            const { component, mockStore } = await setup();
            expect(component.isVerifying()).toBe(false);
            mockStore.verifying.set(true);
            expect(component.isVerifying()).toBe(true);
            mockStore.verifying.set(false);
            mockStore.allStepsVerifying.set(true);
            expect(component.isVerifying()).toBe(true);
        });

        it('stepConfiguration is initialized for the given stepName', async () => {
            const { mockStore } = await setup({ stepName: 'my-step' });
            expect(mockStore.stepConfiguration).toHaveBeenCalledWith('my-step');
        });
    });
});
