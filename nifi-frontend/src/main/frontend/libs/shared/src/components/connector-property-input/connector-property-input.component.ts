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

import { Component, DestroyRef, DoCheck, effect, inject, input, OnInit, output } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { ControlValueAccessor, FormControl, NgControl, ReactiveFormsModule } from '@angular/forms';
import { MatError, MatFormField, MatHint, MatLabel } from '@angular/material/form-field';
import { MatInput } from '@angular/material/input';
import { MatCheckbox } from '@angular/material/checkbox';
import { MatProgressSpinner } from '@angular/material/progress-spinner';
import {
    AllowableValue,
    AssetInfo,
    ConnectorPropertyDescriptor,
    PropertyAllowableValuesState,
    SearchableSelectOption,
    Secret,
    UploadProgressInfo
} from '../../types';
import { SearchableSelect } from '../searchable-select/searchable-select.component';

/**
 * Form control for a single connector property.
 * Renders different input types based on the property descriptor:
 * STRING/INTEGER/DOUBLE/FLOAT -> text input, BOOLEAN -> checkbox,
 * STRING_LIST without allowable values -> textarea (comma-separated),
 * allowable values (static or fetched) -> searchable-select
 * (multi-select when the property type is STRING_LIST).
 * SECRET, ASSET, and ASSET_LIST handling is deferred to follow-up PRs.
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
        MatCheckbox,
        MatProgressSpinner,
        SearchableSelect
    ],
    templateUrl: './connector-property-input.component.html'
})
export class ConnectorPropertyInput implements ControlValueAccessor, DoCheck, OnInit {
    private ngControl = inject(NgControl, { optional: true, self: true });
    private destroyRef = inject(DestroyRef);

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

    formControl = new FormControl();

    // Cached options to avoid recomputing on every change detection cycle
    selectOptions: SearchableSelectOption<string>[] = [];

    // One-shot guard so we only emit requestAllowableValues once per control instance
    // per property descriptor. Reset when the descriptor's property name changes so a
    // host that rebinds [property] to a different descriptor re-issues the fetch.
    private hasFetchedDynamicValues = false;
    private lastSeenPropertyName: string | null = null;

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
            this.selectOptions = this.computeSelectOptions();

            const currentName = prop?.name ?? null;
            if (this.lastSeenPropertyName !== null && currentName !== this.lastSeenPropertyName) {
                this.hasFetchedDynamicValues = false;
                this.triggerDynamicValuesFetch();
            }
            this.lastSeenPropertyName = currentName;
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
    }

    registerOnChange(fn: (value: unknown) => void): void {
        if (!this.valueChangesSubscribed) {
            this.valueChangesSubscribed = true;
            this.formControl.valueChanges.pipe(takeUntilDestroyed(this.destroyRef)).subscribe((value) => {
                fn(value);
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
     * Whether the property should be rendered as a plain text input.
     * Used for STRING/INTEGER/DOUBLE/FLOAT when a select is not appropriate.
     * STRING_LIST falls into the textarea branch instead.
     */
    shouldUseTextInput(): boolean {
        const prop = this.property();
        if (!prop) {
            return false;
        }
        if (prop.type === 'BOOLEAN' || prop.type === 'STRING_LIST') {
            return false;
        }
        return !this.shouldUseSelect();
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
     * Placeholder text for the searchable-select dropdown.
     */
    getSelectPlaceholder(): string {
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
     * Compute SearchableSelectOptions from whichever source is available:
     * dynamic values when successfully fetched, otherwise static values.
     */
    private computeSelectOptions(): SearchableSelectOption<string>[] {
        const prop = this.property();
        if (!prop || prop.type === 'BOOLEAN') {
            return [];
        }

        let allowableValues: AllowableValue[] = [];

        const state = this.dynamicAllowableValuesState();
        if (state?.values && state.values.length > 0) {
            allowableValues = state.values;
        } else if (this.hasStaticAllowableValues()) {
            allowableValues = prop.allowableValues || [];
        }

        return allowableValues.map((av) => ({
            value: av.allowableValue.value,
            label: av.allowableValue.displayName
        }));
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
