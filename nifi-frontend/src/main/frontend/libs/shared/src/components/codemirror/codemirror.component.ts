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
    ChangeDetectionStrategy,
    Component,
    ElementRef,
    EventEmitter,
    Input,
    OnChanges,
    OnDestroy,
    OnInit,
    Output,
    SimpleChanges,
    ViewEncapsulation,
    forwardRef
} from '@angular/core';
import { ControlValueAccessor, NG_VALUE_ACCESSOR } from '@angular/forms';
import { Annotation, Compartment, EditorState, Extension, StateEffect } from '@codemirror/state';
import { EditorView } from '@codemirror/view';
import { defaultTheme } from './themes/defaultTheme';
import { languages } from '@codemirror/language-data';
import { LanguageDescription } from '@codemirror/language';

export const External = Annotation.define<boolean>();

export interface CodeMirrorConfig {
    appearance?: Extension;
    focusOnInit?: boolean;
    content?: string;
    disabled?: boolean;
    readOnly?: boolean;
    plugins?: Extension[];
    syntaxModes?: LanguageDescription[];
    syntaxMode?: string;
}

@Component({
    selector: 'codemirror',
    standalone: true,
    templateUrl: './codemirror.component.html',
    styleUrls: ['./codemirror.component.scss'],
    host: {
        class: 'cm-s-nifi'
    },
    encapsulation: ViewEncapsulation.None,
    changeDetection: ChangeDetectionStrategy.OnPush,
    providers: [
        {
            provide: NG_VALUE_ACCESSOR,
            useExisting: forwardRef(() => Codemirror),
            multi: true
        }
    ]
})
export class Codemirror implements OnChanges, OnInit, OnDestroy, ControlValueAccessor {
    @Input() documentRoot?: Document | ShadowRoot;
    @Input() settings: CodeMirrorConfig = {
        appearance: defaultTheme,
        focusOnInit: false,
        content: '',
        disabled: false,
        readOnly: false,
        plugins: [],
        syntaxModes: languages,
        syntaxMode: ''
    };

    @Output() contentChange = new EventEmitter<string>();
    @Output() ready = new EventEmitter<Codemirror>();
    @Output() focused = new EventEmitter<void>();
    @Output() blurred = new EventEmitter<void>();

    view: EditorView | null = null;
    isInitialized = false;
    onTouched: (() => void) | null = null;
    onChange: ((value: string) => void) | null = null;

    private editableCompartment = new Compartment();
    private readonlyCompartment = new Compartment();
    private themeCompartment = new Compartment();
    private languageCompartment = new Compartment();

    private updateListener = EditorView.updateListener.of((viewUpdate) => {
        if (viewUpdate.docChanged && !viewUpdate.transactions.some((tr) => tr.annotation(External))) {
            const currentValue = viewUpdate.state.doc.toString();
            this.onChange?.(currentValue);
            this.contentChange.emit(currentValue);
        }
    });

    constructor(private elementRef: ElementRef<Element>) {}

    get theme(): Extension {
        return this.settings.appearance ?? defaultTheme;
    }

    get autoFocus(): boolean {
        return this.settings.focusOnInit ?? false;
    }

    get value(): string {
        return this.settings.content ?? '';
    }

    get viewDisabled(): boolean {
        return this.settings.disabled ?? false;
    }

    get readonly(): boolean {
        return this.settings.readOnly ?? false;
    }

    get extensions(): Extension[] {
        return this.settings.plugins ?? [];
    }

    get languages(): LanguageDescription[] {
        return this.settings.syntaxModes ?? languages;
    }

    get language(): string {
        return this.settings.syntaxMode ?? '';
    }

    ngOnInit(): void {
        this.initializeEditor();
        this.setupEventListeners();
        this.applyInitialConfiguration();
        this.isInitialized = true;
        this.ready.emit(this);
    }

    ngOnChanges(changes: SimpleChanges): void {
        if (!this.isInitialized || !changes['settings']) {
            return;
        }

        const configChange = changes['settings'];
        if (!configChange.currentValue) {
            return;
        }

        const previousConfig = configChange.previousValue || {};
        const currentConfig = configChange.currentValue;

        this.processConfigChanges(previousConfig, currentConfig);
    }

    ngOnDestroy(): void {
        if (this.view) {
            this.view.destroy();
            this.view = null;
            this.isInitialized = false;
        }
    }

    writeValue(value: string): void {
        if (this.view && value !== undefined && value !== null) {
            this.setValue(value);
        }
    }

    registerOnChange(onChange: (value: string) => void): void {
        this.onChange = onChange;
    }

    registerOnTouched(onTouch: () => void): void {
        this.onTouched = onTouch;
    }

    setDisabledState(isDisabled: boolean): void {
        this.settings = { ...this.settings, disabled: isDisabled };
        if (this.isInitialized) {
            this.setEditable(!isDisabled);
        }
    }

    addEventListener(type: string, listener: EventListenerOrEventListenerObject): void {
        if (this.view?.contentDOM) {
            this.view.contentDOM.addEventListener(type, listener);
        }
    }

    private initializeEditor(): void {
        if (!this.elementRef.nativeElement) {
            return;
        }

        this.view = new EditorView({
            root: this.documentRoot,
            parent: this.elementRef.nativeElement,
            state: EditorState.create({
                doc: this.value,
                extensions: this.getAllExtensions()
            })
        });

        if (this.autoFocus && this.view) {
            this.view.focus();
        }
    }

    private setupEventListeners(): void {
        if (!this.view?.contentDOM) {
            return;
        }

        const contentDOM = this.view.contentDOM;

        contentDOM.addEventListener('focus', () => {
            this.onTouched?.();
            this.focused.emit();
        });

        contentDOM.addEventListener('blur', () => {
            this.onTouched?.();
            this.blurred.emit();
        });
    }

    private applyInitialConfiguration(): void {
        if (!this.view) {
            return;
        }

        this.setEditable(!this.viewDisabled);
        this.setReadonly(this.readonly);
        this.setTheme(this.theme);

        if (this.language && this.languages.length > 0) {
            this.setLanguage(this.language);
        }
    }

    private processConfigChanges(previousConfig: CodeMirrorConfig, currentConfig: CodeMirrorConfig): void {
        if (currentConfig.content !== previousConfig.content) {
            this.setValue(currentConfig.content || '');
        }
        if (currentConfig.disabled !== previousConfig.disabled) {
            this.setEditable(!(currentConfig.disabled ?? false));
        }
        if (currentConfig.readOnly !== previousConfig.readOnly) {
            this.setReadonly(currentConfig.readOnly ?? false);
        }
        if (currentConfig.appearance !== previousConfig.appearance) {
            this.setTheme(currentConfig.appearance ?? defaultTheme);
        }
        if (currentConfig.plugins !== previousConfig.plugins) {
            this.setExtensions(this.getAllExtensions());
        }
        if (currentConfig.syntaxMode !== previousConfig.syntaxMode) {
            this.setLanguage(currentConfig.syntaxMode || '');
        }
    }

    private getAllExtensions(): Extension[] {
        return [
            this.updateListener,
            this.editableCompartment.of([]),
            this.readonlyCompartment.of([]),
            this.themeCompartment.of([]),
            this.languageCompartment.of([]),
            ...this.extensions
        ];
    }

    private selectAll(): void {
        if (!this.view) {
            return;
        }

        this.view.dispatch({
            selection: { anchor: 0, head: this.view.state.doc.length }
        });
    }

    private setTheme(themeExtension: Extension): void {
        if (!this.view) {
            return;
        }

        this.view.dispatch({
            effects: this.themeCompartment.reconfigure(themeExtension)
        });
    }

    private setValue(newValue: string): void {
        if (!this.view) {
            return;
        }

        this.view.dispatch({
            changes: {
                from: 0,
                to: this.view.state.doc.length,
                insert: newValue
            },
            annotations: [External.of(true)]
        });

        if (!this.readonly) {
            this.selectAll();
        }
    }

    private setReadonly(isReadonly: boolean): void {
        if (!this.view) {
            return;
        }

        this.view.dispatch({
            effects: this.readonlyCompartment.reconfigure(EditorState.readOnly.of(isReadonly))
        });

        this.toggleCSSClass('editor-readonly', isReadonly);

        if (!isReadonly) {
            this.selectAll();
        }
    }

    private setEditable(isEditable: boolean): void {
        if (!this.view) {
            return;
        }

        this.view.dispatch({
            effects: this.editableCompartment.reconfigure(EditorView.editable.of(isEditable))
        });

        this.toggleCSSClass('editor-disabled', !isEditable);

        if (isEditable) {
            this.selectAll();
        }
    }

    private setExtensions(extensionArray: Extension[]): void {
        if (!this.view) {
            return;
        }

        this.view.dispatch({
            effects: StateEffect.reconfigure.of(extensionArray)
        });
    }

    private setLanguage(languageName: string): void {
        if (!this.view) {
            return;
        }

        if (!languageName || languageName === 'plaintext') {
            this.clearLanguage();
            return;
        }

        const languagesArray = this.settings.syntaxModes || [];
        if (languagesArray.length === 0) {
            return;
        }

        const languageDescription = this.findLanguageDescription(languageName);
        if (languageDescription) {
            this.loadAndApplyLanguage(languageDescription);
        }
    }

    private clearLanguage(): void {
        if (!this.view) {
            return;
        }

        this.view.dispatch({
            effects: this.languageCompartment.reconfigure([])
        });
    }

    private findLanguageDescription(languageName: string): LanguageDescription | null {
        const normalizedName = languageName.toLowerCase();

        for (const languageDesc of this.settings.syntaxModes || []) {
            const allNames = [languageDesc.name, ...languageDesc.alias];
            const hasMatch = allNames.some((name) => name.toLowerCase() === normalizedName);

            if (hasMatch) {
                return languageDesc;
            }
        }

        return null;
    }

    private async loadAndApplyLanguage(languageDescription: LanguageDescription): Promise<void> {
        const languageSupport = await languageDescription.load();
        if (this.view) {
            this.view.dispatch({
                effects: this.languageCompartment.reconfigure([languageSupport])
            });
        }
    }

    private toggleCSSClass(className: string, shouldAdd: boolean): void {
        const element = this.elementRef.nativeElement;
        if (!element) {
            return;
        }

        if (shouldAdd) {
            element.classList.add(className);
        } else {
            element.classList.remove(className);
        }
    }
}
