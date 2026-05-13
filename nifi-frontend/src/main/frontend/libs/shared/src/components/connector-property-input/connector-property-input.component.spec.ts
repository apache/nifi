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

import { Component, signal, WritableSignal } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { FormControl, ReactiveFormsModule } from '@angular/forms';
import { By } from '@angular/platform-browser';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { MatIconTestingModule } from '@angular/material/icon/testing';

import { ConnectorPropertyInput } from './connector-property-input.component';
import {
    AllowableValue,
    AssetInfo,
    buildSecretKey,
    ConnectorPropertyDescriptor,
    PropertyAllowableValuesState,
    Secret,
    UploadProgressInfo
} from '../../types';

function makeProp(overrides: Partial<ConnectorPropertyDescriptor> = {}): ConnectorPropertyDescriptor {
    return {
        name: 'my-prop',
        type: 'STRING',
        required: false,
        dependencies: [],
        ...overrides
    };
}

function makeAllowable(value: string, displayName: string = value): AllowableValue {
    return {
        allowableValue: { value, displayName },
        canRead: true
    };
}

function makeAsset(overrides: Partial<AssetInfo> = {}): AssetInfo {
    return {
        id: 'asset-1',
        name: 'asset-1.jar',
        ...overrides
    };
}

function makeProgress(overrides: Partial<UploadProgressInfo> = {}): UploadProgressInfo {
    return {
        filename: 'in-flight.jar',
        percentComplete: 30,
        status: 'active',
        ...overrides
    };
}

function makeSecret(overrides: Partial<Secret> = {}): Secret {
    return {
        name: 'my-secret',
        fullyQualifiedName: 'group-a.my-secret',
        providerId: 'provider-1',
        providerName: 'Vault',
        groupName: 'group-a',
        description: 'A secret',
        ...overrides
    };
}

/**
 * Host fixture that owns the parent FormControl and reactively passes signal inputs
 * to ConnectorPropertyInput. Use setters on the returned harness to drive updates.
 */
@Component({
    standalone: true,
    imports: [ReactiveFormsModule, ConnectorPropertyInput],
    template: `
        <connector-property-input
            [formControl]="control"
            [property]="property()"
            [dynamicAllowableValuesState]="dynamicAllowableValuesState()"
            [currentAssets]="currentAssets()"
            [assetUploadProgress]="assetUploadProgress()"
            [availableSecrets]="availableSecrets()"
            [secretsLoading]="secretsLoading()"
            [secretsError]="secretsError()"
            (requestAllowableValues)="onRequestAllowableValues()"
            (assetFilesSelected)="onAssetFilesSelected($event)"
            (assetDeleteRequested)="onAssetDeleteRequested($event)"
            (dismissFailedUploadRequested)="onDismissFailedUploadRequested($event)">
        </connector-property-input>
    `
})
class HostComponent {
    control = new FormControl<string | string[] | boolean | null>(null);
    property: WritableSignal<ConnectorPropertyDescriptor> = signal(makeProp());
    dynamicAllowableValuesState: WritableSignal<PropertyAllowableValuesState | null> = signal(null);
    currentAssets: WritableSignal<AssetInfo[]> = signal([]);
    assetUploadProgress: WritableSignal<UploadProgressInfo[]> = signal([]);
    availableSecrets: WritableSignal<Secret[] | null> = signal(null);
    secretsLoading: WritableSignal<boolean> = signal(false);
    secretsError: WritableSignal<string | null> = signal(null);
    requestSpy = vi.fn();
    assetFilesSelectedSpy = vi.fn();
    assetDeleteRequestedSpy = vi.fn();
    dismissFailedUploadRequestedSpy = vi.fn();

    onRequestAllowableValues(): void {
        this.requestSpy();
    }

    onAssetFilesSelected(files: File[]): void {
        this.assetFilesSelectedSpy(files);
    }

    onAssetDeleteRequested(asset: AssetInfo): void {
        this.assetDeleteRequestedSpy(asset);
    }

    onDismissFailedUploadRequested(progress: UploadProgressInfo): void {
        this.dismissFailedUploadRequestedSpy(progress);
    }
}

class MockResizeObserver {
    disconnect = vi.fn();
    observe = vi.fn();
    unobserve = vi.fn();
    constructor(_callback: ResizeObserverCallback) {
        /* noop */
    }
}

async function setup(
    options: {
        property?: ConnectorPropertyDescriptor;
        dynamicState?: PropertyAllowableValuesState | null;
        initialValue?: string | string[] | boolean | null;
        currentAssets?: AssetInfo[];
        assetUploadProgress?: UploadProgressInfo[];
        availableSecrets?: Secret[] | null;
        secretsLoading?: boolean;
        secretsError?: string | null;
    } = {}
) {
    await TestBed.configureTestingModule({
        imports: [HostComponent, NoopAnimationsModule, MatIconTestingModule]
    }).compileComponents();

    const fixture: ComponentFixture<HostComponent> = TestBed.createComponent(HostComponent);
    const host = fixture.componentInstance;

    if (options.property) {
        host.property.set(options.property);
    }
    if (options.dynamicState !== undefined) {
        host.dynamicAllowableValuesState.set(options.dynamicState);
    }
    if (options.initialValue !== undefined) {
        host.control.setValue(options.initialValue);
    }
    if (options.currentAssets !== undefined) {
        host.currentAssets.set(options.currentAssets);
    }
    if (options.assetUploadProgress !== undefined) {
        host.assetUploadProgress.set(options.assetUploadProgress);
    }
    if (options.availableSecrets !== undefined) {
        host.availableSecrets.set(options.availableSecrets);
    }
    if (options.secretsLoading !== undefined) {
        host.secretsLoading.set(options.secretsLoading);
    }
    if (options.secretsError !== undefined) {
        host.secretsError.set(options.secretsError);
    }

    fixture.detectChanges();
    await fixture.whenStable();
    // Skip checkNoChanges on the post-stable CD so EllipsisTooltipDirective's deferred
    // overflow evaluation (which intentionally mutates MatTooltip.disabled in a microtask)
    // does not trigger NG0100 in dev mode. happy-dom returns offsetWidth=0/scrollWidth=0,
    // which causes the directive to flip MatTooltip.disabled from false to true.
    fixture.detectChanges(false);

    const inputDebug = fixture.debugElement.query(By.directive(ConnectorPropertyInput));
    const inputComponent = inputDebug.componentInstance as ConnectorPropertyInput;

    return { fixture, host, inputComponent };
}

describe('ConnectorPropertyInput', () => {
    const originalResizeObserver = (globalThis as { ResizeObserver?: typeof ResizeObserver }).ResizeObserver;

    beforeAll(() => {
        (globalThis as { ResizeObserver?: typeof ResizeObserver }).ResizeObserver =
            MockResizeObserver as unknown as typeof ResizeObserver;
    });

    afterAll(() => {
        if (originalResizeObserver) {
            (globalThis as { ResizeObserver?: typeof ResizeObserver }).ResizeObserver = originalResizeObserver;
        } else {
            delete (globalThis as { ResizeObserver?: typeof ResizeObserver }).ResizeObserver;
        }
    });

    beforeEach(() => {
        vi.clearAllMocks();
    });

    describe('requestAllowableValues emission', () => {
        it('emits once on init for a fetchable property with no static values', async () => {
            const { host } = await setup({
                property: makeProp({ allowableValuesFetchable: true })
            });
            expect(host.requestSpy).toHaveBeenCalledTimes(1);
        });

        it('does not emit when the property has static allowable values', async () => {
            const { host } = await setup({
                property: makeProp({
                    allowableValuesFetchable: true,
                    allowableValues: [makeAllowable('a'), makeAllowable('b')]
                })
            });
            expect(host.requestSpy).not.toHaveBeenCalled();
        });

        it('does not emit when the property is not fetchable', async () => {
            const { host } = await setup({
                property: makeProp({ allowableValuesFetchable: false })
            });
            expect(host.requestSpy).not.toHaveBeenCalled();
        });

        it('does not emit a second time when the dynamic values arrive', async () => {
            const { host, fixture } = await setup({
                property: makeProp({ allowableValuesFetchable: true })
            });
            expect(host.requestSpy).toHaveBeenCalledTimes(1);

            host.dynamicAllowableValuesState.set({ loading: false, error: null, values: [makeAllowable('a')] });
            fixture.detectChanges();
            await fixture.whenStable();
            fixture.detectChanges();

            expect(host.requestSpy).toHaveBeenCalledTimes(1);
        });
    });

    describe('select rendering', () => {
        it('renders a searchable-select when the property has static allowable values', async () => {
            const { fixture } = await setup({
                property: makeProp({
                    allowableValues: [makeAllowable('a'), makeAllowable('b')]
                })
            });

            const select = fixture.debugElement.query(By.css('[data-qa="property-input-select"]'));
            expect(select).toBeTruthy();
            expect(select.nativeElement.tagName.toLowerCase()).toBe('searchable-select');
        });

        it('populates searchable-select options from the static allowable values', async () => {
            const { inputComponent } = await setup({
                property: makeProp({
                    allowableValues: [makeAllowable('v1', 'Value One'), makeAllowable('v2', 'Value Two')]
                })
            });

            expect(inputComponent.selectOptions).toEqual([
                { value: 'v1', label: 'Value One' },
                { value: 'v2', label: 'Value Two' }
            ]);
        });

        it('populates searchable-select options from dynamic allowable values when available', async () => {
            const { inputComponent, host, fixture } = await setup({
                property: makeProp({ allowableValuesFetchable: true })
            });

            host.dynamicAllowableValuesState.set({
                loading: false,
                error: null,
                values: [makeAllowable('dyn-1', 'Dynamic 1'), makeAllowable('dyn-2', 'Dynamic 2')]
            });
            fixture.detectChanges();
            await fixture.whenStable();
            fixture.detectChanges();

            expect(inputComponent.selectOptions).toEqual([
                { value: 'dyn-1', label: 'Dynamic 1' },
                { value: 'dyn-2', label: 'Dynamic 2' }
            ]);
        });

        it('shows a loading spinner while a dynamic fetch is in flight', async () => {
            const { fixture } = await setup({
                property: makeProp({ allowableValuesFetchable: true }),
                dynamicState: { loading: true, error: null, values: null }
            });

            const spinner = fixture.debugElement.query(By.css('[data-qa="property-input-loading"]'));
            expect(spinner).toBeTruthy();
        });
    });

    describe('fallback to text input', () => {
        it('falls back to a text input when the dynamic fetch reports an error', async () => {
            const { fixture } = await setup({
                property: makeProp({ allowableValuesFetchable: true }),
                dynamicState: { loading: false, error: 'boom', values: null }
            });

            const select = fixture.debugElement.query(By.css('[data-qa="property-input-select"]'));
            const textInput = fixture.debugElement.query(By.css('[data-qa="property-input-text"]'));
            const errorHint = fixture.debugElement.query(By.css('[data-qa="property-input-fetch-error-hint"]'));

            expect(select).toBeNull();
            expect(textInput).toBeTruthy();
            expect(errorHint).toBeTruthy();
        });

        it('falls back to a text input when the dynamic fetch returns an empty list', async () => {
            const { fixture } = await setup({
                property: makeProp({ allowableValuesFetchable: true }),
                dynamicState: { loading: false, error: null, values: [] }
            });

            const select = fixture.debugElement.query(By.css('[data-qa="property-input-select"]'));
            const textInput = fixture.debugElement.query(By.css('[data-qa="property-input-text"]'));
            const emptyHint = fixture.debugElement.query(By.css('[data-qa="property-input-fetch-empty-hint"]'));

            expect(select).toBeNull();
            expect(textInput).toBeTruthy();
            expect(emptyHint).toBeTruthy();
        });
    });

    describe('text input rendering', () => {
        it('renders a text input for a plain STRING property with no allowable values', async () => {
            const { fixture } = await setup({
                property: makeProp()
            });

            const textInput = fixture.debugElement.query(By.css('[data-qa="property-input-text"]'));
            expect(textInput).toBeTruthy();
        });
    });

    describe('boolean rendering', () => {
        it('renders a mat-slide-toggle (not a checkbox) for BOOLEAN properties', async () => {
            const { fixture } = await setup({
                property: makeProp({ type: 'BOOLEAN' })
            });

            const toggle = fixture.debugElement.query(By.css('[data-qa="property-input-boolean"]'));
            const checkbox = fixture.debugElement.query(By.css('mat-checkbox'));

            expect(toggle).toBeTruthy();
            expect(toggle.nativeElement.tagName.toLowerCase()).toBe('mat-slide-toggle');
            expect(checkbox).toBeNull();
        });

        it('reflects the form value in the toggle checked state', async () => {
            const { fixture, host } = await setup({
                property: makeProp({ type: 'BOOLEAN' }),
                initialValue: true
            });

            const toggle = fixture.debugElement.query(By.css('[data-qa="property-input-boolean"]'));
            expect(toggle.componentInstance.checked).toBe(true);

            host.control.setValue(false);
            fixture.detectChanges();
            await fixture.whenStable();
            fixture.detectChanges(false);

            expect(toggle.componentInstance.checked).toBe(false);
        });

        it('propagates the form value through the [formControl] binding (parent <-> toggle round-trip)', async () => {
            const { fixture, host } = await setup({
                property: makeProp({ type: 'BOOLEAN' })
            });

            const toggleDebug = fixture.debugElement.query(By.css('[data-qa="property-input-boolean"]'));

            // Parent -> child
            host.control.setValue(true);
            fixture.detectChanges();
            await fixture.whenStable();
            fixture.detectChanges(false);
            expect(toggleDebug.componentInstance.checked).toBe(true);

            // Toggle -> parent: invoke the underlying input element's click,
            // which is the public surface MatSlideToggle exposes for user-driven changes.
            const input: HTMLInputElement | null = (toggleDebug.nativeElement as HTMLElement).querySelector(
                'button[role="switch"], input[type="checkbox"]'
            );
            expect(input).not.toBeNull();
            input!.click();
            fixture.detectChanges();
            await fixture.whenStable();
            fixture.detectChanges(false);

            expect(host.control.value).toBe(false);
        });

        it('coerces string "true" to true via writeValue', async () => {
            const { fixture, host } = await setup({
                property: makeProp({ type: 'BOOLEAN' })
            });

            host.control.setValue('true' as unknown as boolean);
            fixture.detectChanges();
            await fixture.whenStable();
            fixture.detectChanges(false);

            const toggle = fixture.debugElement.query(By.css('[data-qa="property-input-boolean"]'));
            expect(toggle.componentInstance.checked).toBe(true);
        });

        it('coerces non-true values to false via writeValue', async () => {
            const { fixture, host } = await setup({
                property: makeProp({ type: 'BOOLEAN' })
            });

            host.control.setValue('false' as unknown as boolean);
            fixture.detectChanges();
            await fixture.whenStable();
            fixture.detectChanges(false);

            const toggle = fixture.debugElement.query(By.css('[data-qa="property-input-boolean"]'));
            expect(toggle.componentInstance.checked).toBe(false);
        });
    });

    describe('STRING_LIST rendering', () => {
        it('renders a textarea for a STRING_LIST property with no allowable values', async () => {
            const { fixture } = await setup({
                property: makeProp({ type: 'STRING_LIST' })
            });

            const textarea = fixture.debugElement.query(By.css('[data-qa="property-input-textarea"]'));
            const select = fixture.debugElement.query(By.css('[data-qa="property-input-select"]'));

            expect(textarea).toBeTruthy();
            expect(textarea.nativeElement.tagName.toLowerCase()).toBe('textarea');
            expect(select).toBeNull();
        });

        it('renders a multi-select for a STRING_LIST property with static allowable values', async () => {
            const { fixture, inputComponent } = await setup({
                property: makeProp({
                    type: 'STRING_LIST',
                    allowableValues: [makeAllowable('a'), makeAllowable('b')]
                })
            });

            const select = fixture.debugElement.query(By.css('[data-qa="property-input-select"]'));
            const textarea = fixture.debugElement.query(By.css('[data-qa="property-input-textarea"]'));

            expect(select).toBeTruthy();
            expect(textarea).toBeNull();
            expect(inputComponent.isMultiSelect()).toBe(true);
            expect(select.componentInstance.multiple()).toBe(true);
        });

        it('renders a multi-select for a STRING_LIST property with fetched dynamic values', async () => {
            const { fixture, host, inputComponent } = await setup({
                property: makeProp({ type: 'STRING_LIST', allowableValuesFetchable: true })
            });

            host.dynamicAllowableValuesState.set({
                loading: false,
                error: null,
                values: [makeAllowable('topic-1'), makeAllowable('topic-2')]
            });
            fixture.detectChanges();
            await fixture.whenStable();
            fixture.detectChanges();

            const select = fixture.debugElement.query(By.css('[data-qa="property-input-select"]'));
            expect(select).toBeTruthy();
            expect(inputComponent.isMultiSelect()).toBe(true);
            expect(select.componentInstance.multiple()).toBe(true);
        });

        it('falls back to a textarea with an error hint when the dynamic fetch fails', async () => {
            const { fixture } = await setup({
                property: makeProp({ type: 'STRING_LIST', allowableValuesFetchable: true }),
                dynamicState: { loading: false, error: 'boom', values: null }
            });

            const textarea = fixture.debugElement.query(By.css('[data-qa="property-input-textarea"]'));
            const select = fixture.debugElement.query(By.css('[data-qa="property-input-select"]'));
            const errorHint = fixture.debugElement.query(
                By.css('[data-qa="property-input-textarea-fetch-error-hint"]')
            );

            expect(select).toBeNull();
            expect(textarea).toBeTruthy();
            expect(errorHint).toBeTruthy();
        });

        it('falls back to a textarea with an empty hint when the dynamic fetch returns an empty list', async () => {
            const { fixture } = await setup({
                property: makeProp({ type: 'STRING_LIST', allowableValuesFetchable: true }),
                dynamicState: { loading: false, error: null, values: [] }
            });

            const textarea = fixture.debugElement.query(By.css('[data-qa="property-input-textarea"]'));
            const select = fixture.debugElement.query(By.css('[data-qa="property-input-select"]'));
            const emptyHint = fixture.debugElement.query(
                By.css('[data-qa="property-input-textarea-fetch-empty-hint"]')
            );

            expect(select).toBeNull();
            expect(textarea).toBeTruthy();
            expect(emptyHint).toBeTruthy();
        });
    });

    describe('already-hydrated dynamicAllowableValuesState', () => {
        it('still emits requestAllowableValues exactly once when values are hydrated before first paint', async () => {
            const { host } = await setup({
                property: makeProp({ allowableValuesFetchable: true }),
                dynamicState: {
                    loading: false,
                    error: null,
                    values: [makeAllowable('pre-1'), makeAllowable('pre-2')]
                }
            });

            expect(host.requestSpy).toHaveBeenCalledTimes(1);
        });

        it('renders the searchable-select immediately when values are hydrated before first paint', async () => {
            const { fixture, inputComponent } = await setup({
                property: makeProp({ allowableValuesFetchable: true }),
                dynamicState: {
                    loading: false,
                    error: null,
                    values: [makeAllowable('pre-1', 'Pre One'), makeAllowable('pre-2', 'Pre Two')]
                }
            });

            const select = fixture.debugElement.query(By.css('[data-qa="property-input-select"]'));
            expect(select).toBeTruthy();
            expect(inputComponent.selectOptions).toEqual([
                { value: 'pre-1', label: 'Pre One' },
                { value: 'pre-2', label: 'Pre Two' }
            ]);
        });
    });

    describe('property descriptor rebinding', () => {
        it('re-emits requestAllowableValues when the descriptor changes to a different fetchable property', async () => {
            const { host, fixture } = await setup({
                property: makeProp({ name: 'first-prop', allowableValuesFetchable: true })
            });
            expect(host.requestSpy).toHaveBeenCalledTimes(1);

            host.property.set(makeProp({ name: 'second-prop', allowableValuesFetchable: true }));
            fixture.detectChanges();
            await fixture.whenStable();
            fixture.detectChanges();

            expect(host.requestSpy).toHaveBeenCalledTimes(2);
        });

        it('does not re-emit requestAllowableValues when the descriptor reference changes but the name stays the same', async () => {
            const { host, fixture } = await setup({
                property: makeProp({ name: 'same-prop', allowableValuesFetchable: true })
            });
            expect(host.requestSpy).toHaveBeenCalledTimes(1);

            host.property.set(makeProp({ name: 'same-prop', allowableValuesFetchable: true, description: 'updated' }));
            fixture.detectChanges();
            await fixture.whenStable();
            fixture.detectChanges();

            expect(host.requestSpy).toHaveBeenCalledTimes(1);
        });
    });

    describe('SECRET rendering', () => {
        it('renders a searchable-select for a SECRET property even when no secrets are loaded yet', async () => {
            const { fixture, inputComponent } = await setup({
                property: makeProp({ type: 'SECRET' })
            });

            const select = fixture.debugElement.query(By.css('[data-qa="property-input-select"]'));
            expect(select).toBeTruthy();
            expect(inputComponent.shouldUseSelect()).toBe(true);
        });

        it('builds options from availableSecrets using the composite key as the option value', async () => {
            const secret = makeSecret({
                name: 'Prod DB Password',
                fullyQualifiedName: 'group-a.prod-db',
                providerId: 'vault-1',
                providerName: 'Vault',
                groupName: 'group-a',
                description: 'Production DB password'
            });
            const { inputComponent } = await setup({
                property: makeProp({ type: 'SECRET' }),
                availableSecrets: [secret]
            });

            expect(inputComponent.selectOptions).toEqual([
                {
                    value: buildSecretKey('vault-1', 'Vault', 'group-a.prod-db'),
                    label: 'Prod DB Password',
                    description: 'Production DB password',
                    group: 'Vault'
                }
            ]);
        });

        it('uses the bare provider name as the group when a provider owns a single group', async () => {
            const secrets = [
                makeSecret({ name: 'secret-1', providerName: 'Vault', groupName: 'group-a' }),
                makeSecret({
                    name: 'secret-2',
                    fullyQualifiedName: 'group-a.secret-2',
                    providerName: 'Vault',
                    groupName: 'group-a'
                })
            ];
            const { inputComponent } = await setup({
                property: makeProp({ type: 'SECRET' }),
                availableSecrets: secrets
            });

            expect(inputComponent.selectOptions.map((o) => o.group)).toEqual(['Vault', 'Vault']);
        });

        it('formats the group as "Provider - Group" when a provider owns multiple groups', async () => {
            const secrets = [
                makeSecret({ name: 'secret-1', providerName: 'Vault', groupName: 'group-a' }),
                makeSecret({
                    name: 'secret-2',
                    fullyQualifiedName: 'group-b.secret-2',
                    providerName: 'Vault',
                    groupName: 'group-b'
                })
            ];
            const { inputComponent } = await setup({
                property: makeProp({ type: 'SECRET' }),
                availableSecrets: secrets
            });

            expect(inputComponent.selectOptions.map((o) => o.group)).toEqual(['Vault - group-a', 'Vault - group-b']);
        });

        it('appends a disabled "(no longer available)" option when the saved value is missing from the loaded secrets', async () => {
            const orphanKey = buildSecretKey('old-provider', 'OldVault', 'group-x.gone');
            const { inputComponent } = await setup({
                property: makeProp({ type: 'SECRET' }),
                initialValue: orphanKey,
                availableSecrets: [makeSecret()]
            });

            const orphan = inputComponent.selectOptions.find((o) => o.value === orphanKey);
            expect(orphan).toBeTruthy();
            expect(orphan?.disabled).toBe(true);
            expect(orphan?.label).toBe('group-x.gone (no longer available)');
            expect(orphan?.group).toBe('OldVault');
        });

        it('renders the saved value as an orphan placeholder while secrets are still loading', async () => {
            const orphanKey = buildSecretKey('provider-1', 'Vault', 'group-a.secret-1');
            const { inputComponent } = await setup({
                property: makeProp({ type: 'SECRET' }),
                initialValue: orphanKey,
                availableSecrets: null,
                secretsLoading: true
            });

            const orphan = inputComponent.selectOptions.find((o) => o.value === orphanKey);
            expect(orphan).toBeTruthy();
            expect(orphan?.disabled).toBe(true);
            expect(orphan?.label).toBe('group-a.secret-1 (no longer available)');
            expect(orphan?.group).toBe('Vault');
        });

        it('rewrites the form value to the current composite key after a provider rename', async () => {
            const savedKey = buildSecretKey('provider-1', 'OldVault', 'group-a.secret-1');
            const renamedSecret = makeSecret({
                providerId: 'provider-1',
                providerName: 'NewVault',
                fullyQualifiedName: 'group-a.secret-1',
                groupName: 'group-a',
                name: 'secret-1'
            });
            const expectedKey = buildSecretKey('provider-1', 'NewVault', 'group-a.secret-1');

            const { fixture, host } = await setup({
                property: makeProp({ type: 'SECRET' }),
                initialValue: savedKey,
                availableSecrets: [renamedSecret]
            });

            // afterNextRender defers the setValue to the next render; flush via detectChanges + whenStable.
            fixture.detectChanges();
            await fixture.whenStable();
            fixture.detectChanges(false);

            expect(host.control.value).toBe(expectedKey);
        });

        it('flips the searchable-select into loadError mode and suppresses the description hint when secretsError is set', async () => {
            const { fixture, inputComponent } = await setup({
                property: makeProp({ type: 'SECRET', description: 'A description' }),
                secretsError: 'Backend rejected the secrets request'
            });

            const select = fixture.debugElement.query(By.css('[data-qa="property-input-select"]'));
            expect(select.componentInstance.loadError()).toBe(true);
            expect(select.componentInstance.loadErrorMessage()).toBe('Failed to load secrets');
            expect(select.componentInstance.showHint()).toBe(false);
            expect(inputComponent.hasSecretsError()).toBe(true);
        });

        it('does not flip into loadError mode when secretsError is the empty string', async () => {
            const { fixture } = await setup({
                property: makeProp({ type: 'SECRET' }),
                secretsError: ''
            });

            const select = fixture.debugElement.query(By.css('[data-qa="property-input-select"]'));
            expect(select.componentInstance.loadError()).toBe(false);
            expect(select.componentInstance.loadErrorMessage()).toBe('Failed to load secrets');
        });

        it('shows the inline loading spinner with "Loading secrets..." while secretsLoading is true', async () => {
            const { fixture } = await setup({
                property: makeProp({ type: 'SECRET' }),
                secretsLoading: true
            });

            const spinner = fixture.debugElement.query(By.css('[data-qa="property-input-loading"]'));
            expect(spinner).toBeTruthy();
            expect((spinner.nativeElement as HTMLElement).textContent).toContain('Loading secrets...');
        });

        it('hides the loading spinner once secretsLoading flips back to false', async () => {
            const { fixture, host } = await setup({
                property: makeProp({ type: 'SECRET' }),
                secretsLoading: true
            });

            expect(fixture.debugElement.query(By.css('[data-qa="property-input-loading"]'))).toBeTruthy();

            host.secretsLoading.set(false);
            fixture.detectChanges();
            await fixture.whenStable();
            fixture.detectChanges(false);

            expect(fixture.debugElement.query(By.css('[data-qa="property-input-loading"]'))).toBeNull();
        });

        it('returns the right placeholder text for each SECRET state', async () => {
            const { inputComponent, host, fixture } = await setup({
                property: makeProp({ type: 'SECRET' }),
                secretsLoading: true
            });
            expect(inputComponent.getSelectPlaceholder()).toBe('Loading secrets...');

            host.secretsLoading.set(false);
            host.secretsError.set('boom');
            fixture.detectChanges();
            await fixture.whenStable();
            fixture.detectChanges(false);
            expect(inputComponent.getSelectPlaceholder()).toBe('Failed to load secrets');

            host.secretsError.set(null);
            host.availableSecrets.set([]);
            fixture.detectChanges();
            await fixture.whenStable();
            fixture.detectChanges(false);
            expect(inputComponent.getSelectPlaceholder()).toBe('No secrets available');

            host.availableSecrets.set([makeSecret()]);
            fixture.detectChanges();
            await fixture.whenStable();
            fixture.detectChanges(false);
            expect(inputComponent.getSelectPlaceholder()).toBe('Select a secret');
        });
    });

    describe('asset rendering', () => {
        it('renders an asset-upload (single) for an ASSET property and hides the default text input', async () => {
            const { fixture, inputComponent } = await setup({
                property: makeProp({ type: 'ASSET' })
            });

            const block = fixture.debugElement.query(By.css('[data-qa="property-input-asset-upload-block"]'));
            const upload = fixture.debugElement.query(By.css('[data-qa="property-input-asset-upload"]'));
            const textInput = fixture.debugElement.query(By.css('[data-qa="property-input-text"]'));
            const select = fixture.debugElement.query(By.css('[data-qa="property-input-select"]'));

            expect(block).toBeTruthy();
            expect(upload).toBeTruthy();
            expect(upload.nativeElement.tagName.toLowerCase()).toBe('asset-upload');
            expect(textInput).toBeNull();
            expect(select).toBeNull();
            expect(inputComponent.shouldUseAssetUpload()).toBe(true);
            expect(inputComponent.isMultipleAssets()).toBe(false);
        });

        it('renders an asset-upload (multi) for an ASSET_LIST property', async () => {
            const { fixture, inputComponent } = await setup({
                property: makeProp({ type: 'ASSET_LIST' })
            });

            const upload = fixture.debugElement.query(By.css('[data-qa="property-input-asset-upload"]'));
            const textarea = fixture.debugElement.query(By.css('[data-qa="property-input-textarea"]'));

            expect(upload).toBeTruthy();
            expect(textarea).toBeNull();
            expect(inputComponent.shouldUseAssetUpload()).toBe(true);
            expect(inputComponent.isMultipleAssets()).toBe(true);
            expect(upload.componentInstance.multiple).toBe(true);
        });

        it('renders the property name as the inline label inside the asset block', async () => {
            const { fixture } = await setup({
                property: makeProp({ name: 'truststore', type: 'ASSET' })
            });

            const block: HTMLElement = fixture.debugElement.query(
                By.css('[data-qa="property-input-asset-upload-block"]')
            ).nativeElement;
            expect(block.textContent).toContain('truststore');
        });

        it('passes the current assets and upload progress to the child', async () => {
            const asset = makeAsset({ id: 'a-1', name: 'file-a.jar' });
            const progress = makeProgress({ filename: 'file-b.jar', percentComplete: 60 });
            const { fixture } = await setup({
                property: makeProp({ type: 'ASSET' }),
                currentAssets: [asset],
                assetUploadProgress: [progress]
            });

            const upload = fixture.debugElement.query(By.css('[data-qa="property-input-asset-upload"]'));
            expect(upload.componentInstance.assets).toEqual([asset]);
            expect(upload.componentInstance.uploadProgress).toEqual([progress]);
        });

        it('forwards filesSelected from the child up through the wrapper output', async () => {
            const { fixture, host } = await setup({
                property: makeProp({ type: 'ASSET' })
            });

            const upload = fixture.debugElement.query(By.css('[data-qa="property-input-asset-upload"]'));
            const file = new File(['x'], 'creds.json', { type: 'application/json' });
            upload.componentInstance.filesSelected.emit([file]);

            expect(host.assetFilesSelectedSpy).toHaveBeenCalledWith([file]);
        });

        it('forwards deleteAsset from the child up through the wrapper output', async () => {
            const asset = makeAsset();
            const { fixture, host } = await setup({
                property: makeProp({ type: 'ASSET' }),
                currentAssets: [asset]
            });

            const upload = fixture.debugElement.query(By.css('[data-qa="property-input-asset-upload"]'));
            upload.componentInstance.deleteAsset.emit(asset);

            expect(host.assetDeleteRequestedSpy).toHaveBeenCalledWith(asset);
        });

        it('forwards dismissFailedUpload from the child up through the wrapper output', async () => {
            const progress = makeProgress({ status: 'error', error: 'boom' });
            const { fixture, host } = await setup({
                property: makeProp({ type: 'ASSET' }),
                assetUploadProgress: [progress]
            });

            const upload = fixture.debugElement.query(By.css('[data-qa="property-input-asset-upload"]'));
            upload.componentInstance.dismissFailedUpload.emit(progress);

            expect(host.dismissFailedUploadRequestedSpy).toHaveBeenCalledWith(progress);
        });

        it('shows a required error inside the asset block when the parent control is required and touched', async () => {
            const { fixture, host } = await setup({
                property: makeProp({ type: 'ASSET', required: true })
            });

            host.control.setErrors({ required: true });
            host.control.markAsTouched();
            fixture.detectChanges();
            await fixture.whenStable();
            fixture.detectChanges();

            const requiredError = fixture.debugElement.query(
                By.css('mat-error[data-qa="property-input-asset-required-error"]')
            );
            expect(requiredError).toBeTruthy();
            expect(requiredError.nativeElement.textContent.trim()).toBe('This field is required');
        });

        it('shows an assetContentMissing error inside the asset block when the parent reports it', async () => {
            const { fixture, host } = await setup({
                property: makeProp({ type: 'ASSET' })
            });

            host.control.setErrors({ assetContentMissing: true });
            host.control.markAsTouched();
            fixture.detectChanges();
            await fixture.whenStable();
            fixture.detectChanges();

            const missingError = fixture.debugElement.query(
                By.css('mat-error[data-qa="property-input-asset-missing-error"]')
            );
            expect(missingError).toBeTruthy();
            expect(missingError.nativeElement.textContent.trim()).toBe('Asset content is missing');
        });

        it('shows a verificationError inside the asset block when the parent reports it', async () => {
            const { fixture, host } = await setup({
                property: makeProp({ type: 'ASSET' })
            });

            host.control.setErrors({ verificationError: 'Backend rejected asset' });
            host.control.markAsTouched();
            fixture.detectChanges();
            await fixture.whenStable();
            fixture.detectChanges();

            const verificationError = fixture.debugElement.query(
                By.css('mat-error[data-qa="property-input-asset-verification-error"]')
            );
            expect(verificationError).toBeTruthy();
            expect(verificationError.nativeElement.textContent.trim()).toBe('Backend rejected asset');
        });

        it('does not render the asset-upload for a non-asset property (regression guard)', async () => {
            const { fixture } = await setup({
                property: makeProp({ type: 'STRING' })
            });

            const upload = fixture.debugElement.query(By.css('[data-qa="property-input-asset-upload"]'));
            const textInput = fixture.debugElement.query(By.css('[data-qa="property-input-text"]'));

            expect(upload).toBeNull();
            expect(textInput).toBeTruthy();
        });
    });

    describe('textarea validation parity', () => {
        async function setupStringList(errors: Record<string, unknown>): Promise<{
            fixture: ComponentFixture<HostComponent>;
            host: HostComponent;
        }> {
            const { fixture, host } = await setup({
                property: makeProp({ type: 'STRING_LIST' })
            });

            host.control.setErrors(errors);
            host.control.markAsTouched();
            fixture.detectChanges();
            await fixture.whenStable();
            fixture.detectChanges();

            return { fixture, host };
        }

        it('shows an "Invalid format" error on the textarea when the parent has a pattern error', async () => {
            const { fixture } = await setupStringList({ pattern: { requiredPattern: '^\\w+$', actualValue: '!!!' } });

            const errors = fixture.debugElement.queryAll(By.css('mat-error'));
            const errorTexts = errors.map((el) => el.nativeElement.textContent.trim());
            expect(errorTexts).toContain('Invalid format');
        });

        it('shows the verification error on the textarea when the parent carries a verificationError', async () => {
            const { fixture } = await setupStringList({ verificationError: 'Backend rejected list' });

            const errorEl = fixture.debugElement.query(By.css('[data-qa="verification-error"]'));
            expect(errorEl).toBeTruthy();
            expect(errorEl.nativeElement.textContent.trim()).toBe('Backend rejected list');
        });
    });
});
