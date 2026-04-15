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

import { Component, DestroyRef, DoCheck, inject, input, output } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { ControlValueAccessor, FormControl, NgControl, ReactiveFormsModule } from '@angular/forms';
import { MatFormField, MatError, MatLabel, MatHint } from '@angular/material/form-field';
import { MatInput } from '@angular/material/input';
import { MatSelect, MatOption } from '@angular/material/select';
import { MatCheckbox } from '@angular/material/checkbox';
import {
    ConnectorPropertyDescriptor,
    PropertyAllowableValuesState,
    AssetInfo,
    UploadProgressInfo,
    Secret
} from '../../types';

/**
 * Form control for a single connector property.
 * Renders different input types based on the property descriptor:
 * STRING/INTEGER/DOUBLE/FLOAT -> text input, BOOLEAN -> checkbox,
 * SECRET -> select with secrets. ASSET uploads are handled at the
 * configuration-step level rather than within this component.
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
        MatSelect,
        MatOption,
        MatCheckbox
    ],
    template: `
        @let prop = property();
        @if (prop) {
            @switch (prop.type) {
                @case ('BOOLEAN') {
                    <mat-checkbox [formControl]="formControl" data-qa="property-input-boolean">
                        <span class="text-sm">{{ prop.name }}</span>
                        @if (prop.description) {
                            <span class="text-xs tertiary-color block">{{ prop.description }}</span>
                        }
                    </mat-checkbox>
                }
                @default {
                    <mat-form-field class="w-full" subscriptSizing="dynamic">
                        <mat-label>{{ prop.name }}</mat-label>
                        @if (prop.allowableValues && prop.allowableValues.length > 0) {
                            <mat-select
                                [formControl]="formControl"
                                [required]="prop.required"
                                (blur)="markAsTouched()"
                                data-qa="property-input-select">
                                @for (av of prop.allowableValues; track av.allowableValue.value) {
                                    <mat-option [value]="av.allowableValue.value">
                                        {{ av.allowableValue.displayName }}
                                    </mat-option>
                                }
                            </mat-select>
                        } @else {
                            <input
                                matInput
                                [formControl]="formControl"
                                [type]="getInputType(prop.type)"
                                [required]="prop.required"
                                (blur)="markAsTouched()"
                                data-qa="property-input-text" />
                        }
                        @if (prop.description) {
                            <mat-hint>{{ prop.description }}</mat-hint>
                        }
                        @if (parentControl?.hasError('required') && parentControl?.touched) {
                            <mat-error>This field is required</mat-error>
                        }
                        @if (parentControl?.hasError('verificationError')) {
                            <mat-error data-qa="verification-error">{{
                                parentControl?.getError('verificationError')
                            }}</mat-error>
                        }
                        @if (parentControl?.hasError('pattern') && parentControl?.touched) {
                            <mat-error>Invalid format</mat-error>
                        }
                        @if (parentControl?.hasError('assetContentMissing') && parentControl?.touched) {
                            <mat-error>Asset content is missing</mat-error>
                        }
                    </mat-form-field>
                }
            }
        }
    `,
    host: {
        class: 'block'
    }
})
export class ConnectorPropertyInput implements ControlValueAccessor, DoCheck {
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
    }

    writeValue(value: any): void {
        if (this.property()?.type === 'BOOLEAN') {
            value = value === true || value === 'true';
        }
        this.formControl.setValue(value, { emitEvent: false });
    }

    registerOnChange(fn: (value: any) => void): void {
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
