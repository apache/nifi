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
import { Component, OnDestroy, Renderer2 } from '@angular/core';
import { Store } from '@ngrx/store';
import { NiFiJoltTransformJsonUiState } from '../../../state';
import { TextTip, isDefinedAndNotNull, MapTableHelperService, MapTableEntry } from '@nifi/shared';
import {
    selectClientIdFromRoute,
    selectDisconnectedNodeAcknowledgedFromRoute,
    selectEditableFromRoute,
    selectJoltTransformJsonProcessorDetailsState,
    selectProcessorDetails,
    selectProcessorDetailsError,
    selectProcessorDetailsLoading,
    selectProcessorIdFromRoute,
    selectRevisionFromRoute
} from '../state/jolt-transform-json-processor-details/jolt-transform-json-processor-details.selectors';
import { Observable, tap } from 'rxjs';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import {
    loadProcessorDetails,
    resetJoltTransformJsonProcessorDetailsState
} from '../state/jolt-transform-json-processor-details/jolt-transform-json-processor-details.actions';
import { FormBuilder, FormControl, FormGroup, Validators } from '@angular/forms';
import { Editor, EditorChange, EditorFromTextArea } from 'codemirror';
import { CodemirrorComponent } from '@ctrl/ngx-codemirror/codemirror.component';
import {
    resetJoltTransformJsonTransformState,
    transformJoltSpec
} from '../state/jolt-transform-json-transform/jolt-transform-json-transform.actions';
import {
    resetJoltTransformJsonValidateState,
    resetValidateJoltSpecState,
    validateJoltSpec
} from '../state/jolt-transform-json-validate/jolt-transform-json-validate.actions';
import {
    resetJoltTransformJsonPropertyState,
    saveProperties
} from '../state/jolt-transform-json-property/jolt-transform-json-property.actions';
import { SavePropertiesRequest } from '../state/jolt-transform-json-property';
import { ValidateJoltSpecRequest } from '../state/jolt-transform-json-validate';
import { selectJoltTransformJsonPropertyState } from '../state/jolt-transform-json-property/jolt-transform-json-property.selectors';
import { selectJoltTransformJsonValidateState } from '../state/jolt-transform-json-validate/jolt-transform-json-validate.selectors';
import { selectJoltTransformJsonTransformState } from '../state/jolt-transform-json-transform/jolt-transform-json-transform.selectors';

const JS_BEAUTIFY_OPTIONS = {
    indent_size: 2,
    indent_char: ' '
};

@Component({
    selector: 'jolt-transform-json-ui',
    templateUrl: './jolt-transform-json-ui.component.html',
    styleUrls: ['./jolt-transform-json-ui.component.scss'],
    standalone: false
})
export class JoltTransformJsonUi implements OnDestroy {
    private processorId$ = this.store.selectSignal(selectProcessorIdFromRoute);
    private revision$ = this.store.selectSignal(selectRevisionFromRoute);
    private clientId$ = this.store.selectSignal(selectClientIdFromRoute);
    private disconnectedNodeAcknowledged$ = this.store.selectSignal(selectDisconnectedNodeAcknowledgedFromRoute);
    private exampleDataOptions = {
        theme: 'nifi',
        lineNumbers: true,
        gutters: ['CodeMirror-lint-markers'],
        mode: 'application/json',
        lint: false,
        extraKeys: {
            'Shift-Ctrl-F': () => {
                this.formatInput();
            }
        }
    };

    protected readonly TextTip = TextTip;

    editJoltTransformJSONProcessorForm: FormGroup;
    step = 0;
    joltState = {
        processorDetails: this.store.selectSignal(selectJoltTransformJsonProcessorDetailsState),
        properties: this.store.selectSignal(selectJoltTransformJsonPropertyState),
        validate: this.store.selectSignal(selectJoltTransformJsonValidateState),
        transform: this.store.selectSignal(selectJoltTransformJsonTransformState)
    };
    processorDetails$ = this.store.select(selectProcessorDetails);
    processorDetailsLoading$ = this.store.select(selectProcessorDetailsLoading);
    processorDetailsError = this.store.selectSignal(selectProcessorDetailsError);
    editable: boolean = false;
    createNew: (existingEntries: string[]) => Observable<MapTableEntry> =
        this.mapTableHelperService.createNewEntry('Attribute');

    constructor(
        private formBuilder: FormBuilder,
        private store: Store<NiFiJoltTransformJsonUiState>,
        private mapTableHelperService: MapTableHelperService,
        private renderer: Renderer2
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
                    expressionLanguageAttributes: [],
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
            input: new FormControl(''),
            specification: new FormControl('', Validators.required),
            transform: new FormControl('', Validators.required),
            customClass: new FormControl(''),
            expressionLanguageAttributes: new FormControl([]),
            modules: new FormControl('')
        });
    }

    ngOnDestroy(): void {
        this.store.dispatch(resetJoltTransformJsonTransformState());
        this.store.dispatch(resetJoltTransformJsonValidateState());
        this.store.dispatch(resetJoltTransformJsonProcessorDetailsState());
        this.store.dispatch(resetJoltTransformJsonPropertyState());
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
        return this.exampleDataOptions;
    }

    getOutputOptions(): any {
        return {
            theme: 'nifi',
            lineNumbers: true,
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
            expressionLanguageAttributes: this.mapExpressionLanguageAttributes(
                this.editJoltTransformJSONProcessorForm.get('expressionLanguageAttributes')?.value
            ),
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
            expressionLanguageAttributes: this.mapExpressionLanguageAttributes(
                this.editJoltTransformJSONProcessorForm.get('expressionLanguageAttributes')?.value
            ),
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
                expressionLanguageAttributes:
                    this.editJoltTransformJSONProcessorForm.get('expressionLanguageAttributes')?.value,
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
                expressionLanguageAttributes:
                    this.editJoltTransformJSONProcessorForm.get('expressionLanguageAttributes')?.value,
                input: this.editJoltTransformJSONProcessorForm.get('input')?.value,
                modules: this.editJoltTransformJSONProcessorForm.get('modules')?.value,
                specification: jsonValue,
                transform: this.editJoltTransformJSONProcessorForm.get('transform')?.value
            });
        }
    }

    isEmpty() {
        const attributes = this.editJoltTransformJSONProcessorForm.get('expressionLanguageAttributes')?.value || [];
        return attributes.length === 0;
    }

    clearAttributesClicked() {
        this.editJoltTransformJSONProcessorForm.get('expressionLanguageAttributes')?.setValue([]);
    }

    private mapExpressionLanguageAttributes(attributeArray: MapTableEntry[]) {
        const result: { [key: string]: string | null } = {};
        if (attributeArray) {
            attributeArray.forEach((attriubte) => (result[attriubte.name] = attriubte.value));
        }
        return result;
    }

    initSpecEditor(codeEditor: CodemirrorComponent): void {
        if (codeEditor.codeMirror) {
            codeEditor.codeMirror.on('change', (cm: Editor, changeObj: EditorChange) => {
                const transform = this.editJoltTransformJSONProcessorForm.get('transform')?.value;

                if (!(transform == 'jolt-transform-sort' && changeObj.text.toString() == '')) {
                    this.clearMessages();
                }
            });

            // listen to value changes
            this.editJoltTransformJSONProcessorForm.controls['specification'].valueChanges.subscribe(() => {
                this.toggleSpecEditorEnabled(codeEditor.codeMirror);
            });

            this.editJoltTransformJSONProcessorForm.controls['transform'].valueChanges.subscribe(() => {
                this.toggleSpecEditorEnabled(codeEditor.codeMirror);
            });
        }
    }

    initInputEditor(codeEditor: any): void {
        codeEditor.codeMirror.on('change', () => {
            this.clearMessages();
        });

        this.editJoltTransformJSONProcessorForm.controls['input'].valueChanges.subscribe(() => {
            this.exampleDataOptions.lint = true;
        });
    }

    private clearMessages() {
        this.store.dispatch(resetValidateJoltSpecState());
    }

    private toggleSpecEditorEnabled(specEditor: EditorFromTextArea | undefined) {
        if (specEditor) {
            const transform = this.editJoltTransformJSONProcessorForm.get('transform')?.value;
            const display: HTMLElement = specEditor.getWrapperElement();

            if (transform == 'jolt-transform-sort') {
                specEditor.setOption('readOnly', 'nocursor');
                this.renderer.addClass(display, 'disabled');
                this.toggleDisplayEditorErrors(specEditor, true);
            } else {
                specEditor.setOption('readOnly', false);
                this.renderer.removeClass(display, 'disabled');
                this.toggleDisplayEditorErrors(specEditor);
            }

            this.clearMessages();
        }
    }

    private toggleDisplayEditorErrors(specEditor: EditorFromTextArea | undefined, hideErrors: boolean = false) {
        if (specEditor) {
            const display: HTMLElement = specEditor.getWrapperElement();
            const errors: Element[] = Array.from(display.getElementsByClassName('CodeMirror-lint-marker-error'));

            if (hideErrors) {
                errors.forEach((error: Element) => {
                    this.renderer.addClass(error, 'hidden');
                });

                const markErrors: Element[] = Array.from(display.getElementsByClassName('CodeMirror-lint-mark-error'));
                markErrors.forEach((markError: Element) => {
                    this.renderer.addClass(markError, 'CodeMirror-lint-mark-error-hide');
                    this.renderer.removeClass(markError, 'CodeMirror-lint-mark-error');
                });
            } else {
                errors.forEach((error: Element) => {
                    this.renderer.removeClass(error, 'hidden');
                });

                const markErrors: Element[] = Array.from(
                    display.getElementsByClassName('CodeMirror-lint-mark-error-hide')
                );
                markErrors.forEach((markError: Element) => {
                    this.renderer.addClass(markError, 'CodeMirror-lint-mark-error');
                    this.renderer.removeClass(markError, 'CodeMirror-lint-mark-error-hide');
                });
            }
        }
    }

    getFormatTooltip() {
        const isMac = navigator.platform.toUpperCase().indexOf('MAC') >= 0;
        return 'Format JSON:    ⇧' + (isMac ? '⌘' : '⌃') + 'F';
    }
}
