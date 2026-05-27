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

import {
    afterNextRender,
    Component,
    DestroyRef,
    DoCheck,
    effect,
    inject,
    Injector,
    input,
    OnInit,
    output,
    runInInjectionContext
} from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { ControlValueAccessor, FormControl, NgControl, ReactiveFormsModule } from '@angular/forms';
import { MatError, MatFormField, MatHint, MatLabel } from '@angular/material/form-field';
import { MatInput } from '@angular/material/input';
import { MatProgressSpinner } from '@angular/material/progress-spinner';
import { MatSlideToggle } from '@angular/material/slide-toggle';
import {
    AllowableValue,
    AssetInfo,
    buildSecretKey,
    ConnectorPropertyDescriptor,
    parseSecretKey,
    PropertyAllowableValuesState,
    SearchableSelectOption,
    Secret,
    UploadProgressInfo
} from '../../types';
import { SearchableSelect } from '../searchable-select/searchable-select.component';
import { AssetUpload } from '../asset-upload/asset-upload.component';
import { StringListOrphansStrippedEvent } from './connector-property-input.types';

/**
 * Form control for a single connector property.
 * Renders different input types based on the property descriptor:
 * STRING/INTEGER/DOUBLE/FLOAT -> text input, BOOLEAN -> slide toggle,
 * STRING_LIST without allowable values -> textarea (comma-separated),
 * allowable values (static or fetched) -> searchable-select
 * (multi-select when the property type is STRING_LIST),
 * ASSET / ASSET_LIST -> asset-upload (drop zone + uploaded list + progress),
 * SECRET -> searchable-select populated from availableSecrets (composite key
 * value, provider/group grouping, orphan handling, provider-rename auto-fix).
 *
 * Uses an internal FormControl bound to the actual input elements so that
 * mat-form-field can detect error state. Validation state is synced from
 * the parent FormControl (via NgControl) using ngDoCheck.
 */
@Component({
    selector: 'connector-property-input',
    standalone: true,
    imports: [
        ReactiveFormsModule,
        MatFormField,
        MatError,
        MatLabel,
        MatHint,
        MatInput,
        MatProgressSpinner,
        MatSlideToggle,
        SearchableSelect,
        AssetUpload
    ],
    templateUrl: './connector-property-input.component.html'
})
export class ConnectorPropertyInput implements ControlValueAccessor, DoCheck, OnInit {
    private ngControl = inject(NgControl, { optional: true, self: true });
    private destroyRef = inject(DestroyRef);
    private injector = inject(Injector);

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

    /**
     * Emitted once per strip operation after STRING_LIST multi-select values that
     * are no longer in the loaded allowable-values list are removed from the form
     * control (see `computeSelectOptions`). Coalesces multiple `computeSelectOptions`
     * passes into a single emission via `stringListStripScheduled`.
     */
    readonly stringListOrphansStripped = output<StringListOrphansStrippedEvent>();

    formControl = new FormControl();

    // Cached options to avoid recomputing on every change detection cycle
    selectOptions: SearchableSelectOption<string>[] = [];

    // One-shot guard so we only emit requestAllowableValues once per control instance
    // per property descriptor. Reset when the descriptor's property name changes so a
    // host that rebinds [property] to a different descriptor re-issues the fetch.
    private hasFetchedDynamicValues = false;
    private lastSeenPropertyName: string | null = null;

    /**
     * Coalesces multiple `computeSelectOptions` passes before a scheduled
     * STRING_LIST orphan strip runs, so the form control is only rewritten and
     * `stringListOrphansStripped` is only emitted once per strip pass.
     */
    private stringListStripScheduled = false;

    get parentControl(): FormControl | null {
        return this.ngControl?.control as FormControl | null;
    }

    private onTouched: () => void = () => {
        /* noop until registerOnTouched */
    };
    private valueChangesSubscribed = false;

    constructor() {
        if (this.ngControl) {
            this.ngControl.valueAccessor = this;
        }

        // Recompute select options whenever the property descriptor or the dynamic
        // allowable values state changes. The initial emission of requestAllowableValues
        // happens in ngOnInit; if the descriptor identity changes later (different
        // property name) we reset the guard and re-issue the fetch from here.
        effect(() => {
            const prop = this.property();
            this.dynamicAllowableValuesState();
            this.availableSecrets();
            this.secretsLoading();
            this.secretsError();
            this.selectOptions = this.computeSelectOptions();

            const currentName = prop?.name ?? null;
            if (this.lastSeenPropertyName !== null && currentName !== this.lastSeenPropertyName) {
                this.hasFetchedDynamicValues = false;
                this.triggerDynamicValuesFetch();
            }
            this.lastSeenPropertyName = currentName;
        });

        // Reconcile form value with current secret identity after secrets load.
        // If the saved composite key matches a current secret by providerId + fullyQualifiedName
        // but the providerName has changed, rewrite the form value to the current key.
        // afterNextRender defers the setValue past the current change-detection cycle.
        // Multiple rapid re-runs are safe: the rewrite is idempotent.
        effect(() => {
            const secrets = this.availableSecrets();
            if (this.property()?.type !== 'SECRET' || !secrets) return;
            const currentValue = this.formControl.value as string | null;
            if (!currentValue) return;
            const parsed = parseSecretKey(currentValue);
            const matching = secrets.find(
                (s) => s.providerId === parsed.providerId && s.fullyQualifiedName === parsed.fullyQualifiedName
            );
            if (!matching) return;
            const expected = buildSecretKey(matching.providerId, matching.providerName, matching.fullyQualifiedName);
            if (currentValue !== expected) {
                runInInjectionContext(this.injector, () => {
                    afterNextRender(() => this.formControl.setValue(expected, { emitEvent: true }));
                });
            }
        });
    }

    ngOnInit(): void {
        this.lastSeenPropertyName = this.property()?.name ?? null;
        this.triggerDynamicValuesFetch();
    }

    writeValue(value: unknown): void {
        let normalized = value;
        if (this.property()?.type === 'BOOLEAN') {
            normalized = value === true || value === 'true';
        }
        this.formControl.setValue(normalized, { emitEvent: false });
        if (this.property()?.type === 'SECRET') {
            this.selectOptions = this.computeSelectOptions();
        }
    }

    registerOnChange(fn: (value: unknown) => void): void {
        if (!this.valueChangesSubscribed) {
            this.valueChangesSubscribed = true;
            this.formControl.valueChanges.pipe(takeUntilDestroyed(this.destroyRef)).subscribe((value) => {
                fn(value);
                if (this.property()?.type === 'SECRET') {
                    this.selectOptions = this.computeSelectOptions();
                }
            });
        }
    }

    registerOnTouched(fn: () => void): void {
        this.onTouched = fn;
    }

    setDisabledState(isDisabled: boolean): void {
        if (isDisabled) {
            this.formControl.disable({ emitEvent: false });
        } else {
            this.formControl.enable({ emitEvent: false });
        }
    }

    markAsTouched(): void {
        if (this.parentControl) {
            this.parentControl.markAsTouched();
            this.syncValidationState();
        }
        this.onTouched();
    }

    /**
     * Sync validation state from parent FormControl to internal formControl
     * so mat-form-field can detect and display errors.
     */
    ngDoCheck(): void {
        if (!this.parentControl) {
            return;
        }

        if (!this.parentControl.touched && this.formControl.touched) {
            this.formControl.markAsUntouched({ onlySelf: true });
            this.formControl.setErrors(null);
        }

        if (this.parentControl.dirty || this.parentControl.touched) {
            if (this.parentControl.touched && !this.formControl.touched) {
                this.formControl.markAsTouched({ onlySelf: true });
            }

            const parentErrors = this.parentControl.errors;
            const internalErrors = this.formControl.errors;
            if (JSON.stringify(parentErrors) !== JSON.stringify(internalErrors)) {
                this.formControl.setErrors(parentErrors);
            }
        }
    }

    getInputType(propertyType: string): string {
        switch (propertyType) {
            case 'INTEGER':
            case 'DOUBLE':
            case 'FLOAT':
                return 'number';
            default:
                return 'text';
        }
    }

    // ========================================================================================
    // Allowable-values helpers (PR1 subset: STRING + fetchable dynamic)
    // ========================================================================================

    /**
     * True when the descriptor carries a non-empty static allowableValues array.
     */
    hasStaticAllowableValues(): boolean {
        const prop = this.property();
        if (!prop) {
            return false;
        }
        return prop.allowableValues !== undefined && prop.allowableValues !== null && prop.allowableValues.length > 0;
    }

    /**
     * True when a dynamic fetch is currently in flight for this property.
     */
    isDynamicValuesLoading(): boolean {
        return this.dynamicAllowableValuesState()?.loading === true;
    }

    /**
     * True when a dynamic fetch completed with an error.
     */
    isDynamicValuesFetchFailed(): boolean {
        const state = this.dynamicAllowableValuesState();
        return state?.error !== null && state?.error !== undefined;
    }

    /**
     * True when a dynamic fetch completed successfully but returned an empty list.
     * In that case we fall back to a text input so the user can still enter a value.
     */
    isDynamicValuesFetchEmpty(): boolean {
        const prop = this.property();
        if (!prop?.allowableValuesFetchable) {
            return false;
        }
        const state = this.dynamicAllowableValuesState();
        return (
            state !== null &&
            state !== undefined &&
            !state.loading &&
            !state.error &&
            state.values !== null &&
            state.values.length === 0
        );
    }

    /**
     * Whether the property should be rendered as a searchable-select dropdown.
     *
     * Returns true when:
     * - Has static allowable values, OR
     * - Has dynamic allowable values already fetched, OR
     * - Is fetchable and still loading (or has not yet started fetching).
     *
     * Returns false when a dynamic fetch failed or came back empty: in both
     * cases we fall back to a plain text input so the field remains editable.
     */
    shouldUseSelect(): boolean {
        const prop = this.property();
        if (!prop || prop.type === 'BOOLEAN') {
            return false;
        }

        if (this.shouldUseAssetUpload()) {
            return false;
        }

        // SECRET always uses the select. Loading shows an inline spinner; load errors
        // disable the select via the searchable-select loadError input.
        if (prop.type === 'SECRET') {
            return true;
        }

        if (this.isDynamicValuesFetchFailed() || this.isDynamicValuesFetchEmpty()) {
            return false;
        }

        if (this.hasStaticAllowableValues()) {
            return true;
        }

        const state = this.dynamicAllowableValuesState();
        if (state?.values && state.values.length > 0) {
            return true;
        }

        if (prop.allowableValuesFetchable && this.isDynamicValuesLoading()) {
            return true;
        }

        if (prop.allowableValuesFetchable && !state) {
            return true;
        }

        return false;
    }

    /**
     * Whether the property should be rendered using the asset-upload component.
     * True for ASSET (single upload) and ASSET_LIST (multi-file upload).
     */
    shouldUseAssetUpload(): boolean {
        const type = this.property()?.type;
        return type === 'ASSET' || type === 'ASSET_LIST';
    }

    /**
     * Whether the asset-upload should accept multiple files (ASSET_LIST only).
     */
    isMultipleAssets(): boolean {
        return this.property()?.type === 'ASSET_LIST';
    }

    /**
     * Whether the property should be rendered as a multi-line textarea.
     * Used for STRING_LIST properties when no allowable values are available
     * (either the descriptor has none and is not fetchable, or the dynamic
     * fetch came back empty or failed). The user enters a comma-separated list.
     */
    shouldUseTextarea(): boolean {
        const prop = this.property();
        if (!prop || prop.type !== 'STRING_LIST') {
            return false;
        }
        return !this.shouldUseSelect();
    }

    /**
     * Whether the searchable-select should be rendered in multi-select mode.
     * True when the property is STRING_LIST and we have allowable values to
     * pick from (static or successfully fetched).
     */
    isMultiSelect(): boolean {
        return this.property()?.type === 'STRING_LIST' && this.shouldUseSelect();
    }

    /**
     * True when a SECRET property's secrets list is currently being loaded.
     */
    isSecretsLoading(): boolean {
        return this.property()?.type === 'SECRET' && this.secretsLoading();
    }

    /**
     * True when a SECRET property's secrets fetch reported an error.
     */
    hasSecretsError(): boolean {
        return this.property()?.type === 'SECRET' && !!this.secretsError();
    }

    /**
     * Placeholder text for the searchable-select dropdown.
     */
    getSelectPlaceholder(): string {
        const prop = this.property();
        if (prop?.type === 'SECRET') {
            if (this.secretsLoading()) {
                return 'Loading secrets...';
            }
            if (this.secretsError()) {
                return 'Failed to load secrets';
            }
            const list = this.availableSecrets();
            if (list && list.length === 0) {
                return 'No secrets available';
            }
            return 'Select a secret';
        }
        if (this.isDynamicValuesLoading()) {
            return 'Loading values...';
        }
        return 'Select a value';
    }

    /**
     * Returns a validation error message to forward to searchable-select when
     * the parent control reports a known error. Keeps the searchable-select
     * in charge of displaying its own mat-error.
     */
    getValidationErrorMessage(): string {
        const parent = this.parentControl;
        if (!parent || !parent.touched) {
            return '';
        }
        if (parent.hasError('required')) {
            return 'This field is required';
        }
        if (parent.hasError('verificationError')) {
            return parent.getError('verificationError');
        }
        if (parent.hasError('pattern')) {
            return 'Invalid format';
        }
        return '';
    }

    // ========================================================================================
    // The parent connector-configuration-step listens on the
    // component's existing assetFilesSelected / assetDeleteRequested / dismissFailedUploadRequested
    // outputs and drives the upload service + wizard store from there.
    // ========================================================================================

    onAssetFilesSelected(files: File[]): void {
        this.assetFilesSelected.emit(files);
    }

    onAssetDeleteRequested(asset: AssetInfo): void {
        this.assetDeleteRequested.emit(asset);
    }

    onDismissFailedUpload(progress: UploadProgressInfo): void {
        this.dismissFailedUploadRequested.emit(progress);
    }

    /**
     * Emits requestAllowableValues exactly once per fetchable property instance
     * when the descriptor is fetchable and has no static allowable values.
     */
    private triggerDynamicValuesFetch(): void {
        const prop = this.property();
        if (prop?.allowableValuesFetchable && !this.hasFetchedDynamicValues && !this.hasStaticAllowableValues()) {
            this.hasFetchedDynamicValues = true;
            this.requestAllowableValues.emit();
        }
    }

    /**
     * Builds SearchableSelectOptions from whichever source is available.
     *
     * For STRING_LIST multi-select, may schedule a deferred strip (via
     * `afterNextRender`) when saved values are not in the loaded allowable list;
     * that pass coalesces with `stringListStripScheduled` so the form control is
     * rewritten and `stringListOrphansStripped` is emitted at most once per strip.
     * Provider-rename rewrite and other reactive updates use separate effects.
     *
     * - SECRET: options come from availableSecrets, valued by a composite key
     *   (providerId::providerName::fullyQualifiedName) and grouped by provider
     *   (or "Provider - Group" when a provider owns multiple groups). Saved
     *   values that no longer match an available secret are surfaced as disabled
     *   options; the "(no longer available)" suffix is suppressed while secrets
     *   are still loading so as not to alarm the user prematurely.
     * - Otherwise: dynamic values when successfully fetched, falling back to
     *   the descriptor's static allowableValues.
     */
    private computeSelectOptions(): SearchableSelectOption<string>[] {
        const prop = this.property();
        if (!prop || prop.type === 'BOOLEAN') {
            return [];
        }

        if (prop.type === 'SECRET') {
            return this.computeSecretSelectOptions();
        }

        let allowableValues: AllowableValue[] = [];

        const state = this.dynamicAllowableValuesState();
        if (state?.values && state.values.length > 0) {
            allowableValues = state.values;
        } else if (this.hasStaticAllowableValues()) {
            allowableValues = prop.allowableValues || [];
        }

        const options: SearchableSelectOption<string>[] = allowableValues.map((av) => ({
            value: av.allowableValue.value,
            label: av.allowableValue.displayName,
            description: av.allowableValue.description
        }));

        // Surface saved values that are no longer in the loaded allowable list
        // as disabled "(no longer available)" options so the selection is visible
        // to the user. For STRING_LIST (multi-select) we additionally strip those
        // values from the form control on the next render and emit
        // stringListOrphansStripped so the host can banner the removal -- single
        // selects keep the orphan option around because the searchable-select
        // already shows the stale value and the user can pick a replacement.
        const currentValue = this.formControl.value;
        if (allowableValues.length > 0 && currentValue !== null && currentValue !== undefined) {
            const existingOptionValues = new Set(options.map((o) => o.value));
            const isMultiple = Array.isArray(currentValue);
            const valuesToCheck = isMultiple ? (currentValue as string[]) : [currentValue as string];
            const orphanedValues: string[] = [];

            for (const val of valuesToCheck) {
                if (val && !existingOptionValues.has(val)) {
                    orphanedValues.push(val);
                    options.unshift({
                        value: val,
                        label: `${val} (no longer available)`,
                        disabled: true
                    });
                }
            }

            if (isMultiple && orphanedValues.length > 0 && !this.stringListStripScheduled) {
                this.stringListStripScheduled = true;
                const validValues = (currentValue as string[]).filter((v) => !orphanedValues.includes(v));
                const removedSnapshot = [...orphanedValues];
                const propertyName = prop.name;
                runInInjectionContext(this.injector, () => {
                    afterNextRender(() => {
                        this.stringListStripScheduled = false;
                        this.formControl.setValue(validValues, { emitEvent: true });
                        this.stringListOrphansStripped.emit({
                            propertyName,
                            removed: removedSnapshot
                        });
                    });
                });
            }
        }

        return options;
    }

    private computeSecretSelectOptions(): SearchableSelectOption<string>[] {
        const options: SearchableSelectOption<string>[] = [];
        const secrets: Secret[] | null = this.availableSecrets();

        if (secrets && secrets.length > 0) {
            const providerGroups = new Map<string, Set<string>>();
            for (const s of secrets) {
                if (!providerGroups.has(s.providerName)) {
                    providerGroups.set(s.providerName, new Set());
                }
                providerGroups.get(s.providerName)!.add(s.groupName);
            }

            for (const s of secrets) {
                const groupCount = providerGroups.get(s.providerName)?.size ?? 1;
                const group = groupCount > 1 ? `${s.providerName} - ${s.groupName}` : s.providerName;
                options.push({
                    value: buildSecretKey(s.providerId, s.providerName, s.fullyQualifiedName),
                    label: s.name,
                    description: s.description,
                    group
                });
            }
        }

        const currentValue = this.formControl.value as string | null;
        if (currentValue && secrets) {
            const parsed = parseSecretKey(currentValue);
            const matching = secrets.find(
                (s) => s.providerId === parsed.providerId && s.fullyQualifiedName === parsed.fullyQualifiedName
            );
            if (!matching) {
                options.push({
                    value: currentValue,
                    label: `${parsed.fullyQualifiedName} (no longer available)`,
                    disabled: true,
                    group: parsed.providerName
                });
            }
        } else if (currentValue) {
            const parsed = parseSecretKey(currentValue);
            const loadCompleted = !this.secretsLoading() && !this.secretsError();
            options.push({
                value: currentValue,
                label: loadCompleted ? `${parsed.fullyQualifiedName} (no longer available)` : parsed.fullyQualifiedName,
                disabled: true,
                group: parsed.providerName
            });
        }

        return options;
    }

    private syncValidationState(): void {
        if (!this.parentControl) {
            return;
        }

        if (this.parentControl.touched && !this.formControl.touched) {
            this.formControl.markAsTouched({ onlySelf: true });
        }
        if (!this.parentControl.touched && this.formControl.touched) {
            this.formControl.markAsUntouched({ onlySelf: true });
        }
        if (this.parentControl.dirty && !this.formControl.dirty) {
            this.formControl.markAsDirty({ onlySelf: true });
        }

        if (this.parentControl.dirty || this.parentControl.touched) {
            this.formControl.setErrors(this.parentControl.errors);
        } else {
            this.formControl.setErrors(null);
        }
    }
}
