/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the 'License'); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an 'AS IS' BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { js_beautify } from 'js-beautify';
import { Component, OnDestroy } from '@angular/core';
import { Store } from '@ngrx/store';
import { NiFiJoltTransformJsonUiState } from '../../../state';
import { TextTip, isDefinedAndNotNull } from '@nifi/shared';
import {
    selectClientIdFromRoute,
    selectDisconnectedNodeAcknowledgedFromRoute,
    selectEditableFromRoute,
    selectJoltTransformJsonUiState,
    selectProcessorDetails,
    selectProcessorIdFromRoute,
    selectRevisionFromRoute
} from '../state/jolt-transform-json-ui/jolt-transform-json-ui.selectors';
import { tap } from 'rxjs';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import {
    loadProcessorDetails,
    resetJoltTransformJsonUiState,
    resetValidateJoltSpecState,
    saveProperties,
    transformJoltSpec,
    validateJoltSpec
} from '../state/jolt-transform-json-ui/jolt-transform-json-ui.actions';
import { SavePropertiesRequest, ValidateJoltSpecRequest } from '../state/jolt-transform-json-ui';
import { FormBuilder, FormControl, FormGroup, Validators } from '@angular/forms';

const JS_BEAUTIFY_OPTIONS = {
    indent_size: 2,
    indent_char: ' '
};

@Component({
    selector: 'jolt-transform-json-ui',
    templateUrl: './jolt-transform-json-ui.component.html',
    styleUrls: ['./jolt-transform-json-ui.component.scss']
})
export class JoltTransformJsonUi implements OnDestroy {
    private processorId$ = this.store.selectSignal(selectProcessorIdFromRoute);
    private revision$ = this.store.selectSignal(selectRevisionFromRoute);
    private clientId$ = this.store.selectSignal(selectClientIdFromRoute);
    private disconnectedNodeAcknowledged$ = this.store.selectSignal(selectDisconnectedNodeAcknowledgedFromRoute);

    protected readonly TextTip = TextTip;

    editJoltTransformJSONProcessorForm: FormGroup;
    step = 0;
    joltState = this.store.selectSignal(selectJoltTransformJsonUiState);
    processorDetails$ = this.store.select(selectProcessorDetails);
    editable: boolean = false;

    constructor(
        private formBuilder: FormBuilder,
        private store: Store<NiFiJoltTransformJsonUiState>
    ) {
        // Select the processor id from the query params and GET processor details
        this.store
            .select(selectProcessorIdFromRoute)
            .pipe(
                isDefinedAndNotNull(),
                tap((processorId: string) => {
                    this.store.dispatch(
                        loadProcessorDetails({
                            processorId
                        })
                    );
                }),
                takeUntilDestroyed()
            )
            .subscribe();

        this.store
            .select(selectEditableFromRoute)
            .pipe(
                isDefinedAndNotNull(),
                tap((editable: string) => {
                    this.editable = editable === 'true';
                }),
                takeUntilDestroyed()
            )
            .subscribe();

        // set processor details values in the form
        this.processorDetails$.pipe(isDefinedAndNotNull(), takeUntilDestroyed()).subscribe({
            next: (processorDetails: any) => {
                this.editJoltTransformJSONProcessorForm.setValue({
                    input: '',
                    specification: processorDetails.properties['Jolt Specification'],
                    transform: processorDetails.properties['Jolt Transform'],
                    customClass: processorDetails.properties['Custom Transformation Class Name'],
                    expressionLanguageAttributes: {},
                    modules: processorDetails.properties['Custom Module Directory']
                });

                if (
                    processorDetails.properties['Jolt Specification'] &&
                    processorDetails.properties['Jolt Transform']
                ) {
                    this.validateJoltSpec();
                }
            }
        });

        // build the form
        this.editJoltTransformJSONProcessorForm = this.formBuilder.group({
            input: new FormControl('', Validators.required),
            specification: new FormControl('', Validators.required),
            transform: new FormControl('', Validators.required),
            customClass: new FormControl(''),
            expressionLanguageAttributes: new FormControl({}), // TODO: Attributes
            modules: new FormControl('')
        });

        // listen to value changes
        this.editJoltTransformJSONProcessorForm.controls['specification'].valueChanges.subscribe(() => {
            this.store.dispatch(resetValidateJoltSpecState());
        });

        this.editJoltTransformJSONProcessorForm.controls['transform'].valueChanges.subscribe(() => {
            this.store.dispatch(resetValidateJoltSpecState());
        });
    }

    ngOnDestroy(): void {
        this.store.dispatch(resetJoltTransformJsonUiState());
    }

    getJoltSpecOptions(): any {
        return {
            theme: 'nifi',
            lineNumbers: true,
            gutters: ['CodeMirror-lint-markers'],
            mode: 'application/json',
            lint: true,
            extraKeys: {
                'Shift-Ctrl-F': () => {
                    this.formatSpecification();
                }
            }
        };
    }

    getExampleDataOptions(): any {
        return {
            theme: 'nifi',
            lineNumbers: true,
            gutters: ['CodeMirror-lint-markers'],
            mode: 'application/json',
            lint: true,
            extraKeys: {
                'Shift-Ctrl-F': () => {
                    this.formatInput();
                }
            }
        };
    }

    getOutputOptions(): any {
        return {
            theme: 'nifi',
            lineNumbers: true,
            gutters: ['CodeMirror-lint-markers'],
            mode: 'application/json',
            lint: false,
            readOnly: true
        };
    }

    preventDrag(event: MouseEvent): void {
        event.stopPropagation();
    }

    setStep(index: number) {
        this.step = index;
    }

    isTransformDisabled(): boolean {
        return (
            this.editJoltTransformJSONProcessorForm.get('input')?.value === '' ||
            (this.editJoltTransformJSONProcessorForm.get('specification')?.value === '' &&
                this.editJoltTransformJSONProcessorForm.get('transform')?.value !== 'jolt-transform-sort')
        );
    }

    isSaveDisabled(): boolean {
        return !(
            ((this.editJoltTransformJSONProcessorForm.get('specification')?.dirty ||
                this.editJoltTransformJSONProcessorForm.get('transform')?.dirty) &&
                this.editJoltTransformJSONProcessorForm.get('specification')?.value !== '' &&
                this.editJoltTransformJSONProcessorForm.get('transform')?.value !== 'jolt-transform-sort') ||
            !this.editable
        );
    }

    isValidateDisabled(): boolean {
        return this.editJoltTransformJSONProcessorForm.get('specification')?.value === '';
    }

    saveProperties() {
        const payload: SavePropertiesRequest = {
            'Jolt Specification': this.editJoltTransformJSONProcessorForm.get('specification')?.value,
            'Jolt Transform': this.editJoltTransformJSONProcessorForm.get('transform')?.value,
            processorId: this.processorId$(),
            clientId: this.clientId$(),
            revision: this.revision$(),
            disconnectedNodeAcknowledged: this.disconnectedNodeAcknowledged$()
        };

        // reset the form to the new values
        this.editJoltTransformJSONProcessorForm.reset(this.editJoltTransformJSONProcessorForm.value);

        this.store.dispatch(
            saveProperties({
                request: payload
            })
        );
    }

    validateJoltSpec() {
        const payload: ValidateJoltSpecRequest = {
            customClass: this.editJoltTransformJSONProcessorForm.get('customClass')?.value,
            expressionLanguageAttributes: {}, // TODO: Attributes
            input: this.editJoltTransformJSONProcessorForm.get('input')?.value,
            modules: this.editJoltTransformJSONProcessorForm.get('modules')?.value,
            specification: this.editJoltTransformJSONProcessorForm.get('specification')?.value,
            transform: this.editJoltTransformJSONProcessorForm.get('transform')?.value
        };

        this.store.dispatch(
            validateJoltSpec({
                request: payload
            })
        );
    }

    transformJoltSpec() {
        const payload: ValidateJoltSpecRequest = {
            customClass: this.editJoltTransformJSONProcessorForm.get('customClass')?.value,
            expressionLanguageAttributes: {}, // TODO: Attributes
            input: this.editJoltTransformJSONProcessorForm.get('input')?.value,
            modules: this.editJoltTransformJSONProcessorForm.get('modules')?.value,
            specification: this.editJoltTransformJSONProcessorForm.get('specification')?.value,
            transform: this.editJoltTransformJSONProcessorForm.get('transform')?.value
        };

        this.store.dispatch(
            transformJoltSpec({
                request: payload
            })
        );
    }

    formatInput() {
        if (this.editJoltTransformJSONProcessorForm.get('input')) {
            const jsonValue = js_beautify(
                this.editJoltTransformJSONProcessorForm.get('input')?.value,
                JS_BEAUTIFY_OPTIONS
            );
            this.editJoltTransformJSONProcessorForm.setValue({
                customClass: this.editJoltTransformJSONProcessorForm.get('customClass')?.value,
                expressionLanguageAttributes: {},
                input: jsonValue,
                modules: this.editJoltTransformJSONProcessorForm.get('modules')?.value,
                specification: this.editJoltTransformJSONProcessorForm.get('specification')?.value,
                transform: this.editJoltTransformJSONProcessorForm.get('transform')?.value
            });
        }
    }

    formatSpecification() {
        if (this.editJoltTransformJSONProcessorForm.get('specification')) {
            const jsonValue = js_beautify(
                this.editJoltTransformJSONProcessorForm.get('specification')?.value,
                JS_BEAUTIFY_OPTIONS
            );
            this.editJoltTransformJSONProcessorForm.setValue({
                customClass: this.editJoltTransformJSONProcessorForm.get('customClass')?.value,
                expressionLanguageAttributes: {},
                input: this.editJoltTransformJSONProcessorForm.get('input')?.value,
                modules: this.editJoltTransformJSONProcessorForm.get('modules')?.value,
                specification: jsonValue,
                transform: this.editJoltTransformJSONProcessorForm.get('transform')?.value
            });
        }
    }
}
