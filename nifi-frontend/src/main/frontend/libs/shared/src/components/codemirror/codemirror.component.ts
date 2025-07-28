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
    /** The editor's theme. */
    theme?: Extension;
    /** Whether focus on the editor after init. Don't support change dynamically! */
    autoFocus?: boolean;
    /** The editor's value. */
    value?: string;
    /** Whether the editor is disabled. */
    viewDisabled?: boolean;
    /** Whether the editor is readonly. */
    readonly?: boolean;
    /** Extensions to be appended to the root extensions. */
    extensions?: Extension[];
    /**
     * An array of language descriptions for known language-data.
     * Don't support change dynamically!
     */
    languages?: LanguageDescription[];
    /** The editor's language. You should set the languages prop at first. */
    language?: string;
}

type ConfigPropertyKey = keyof CodeMirrorConfig;
type EventListenerCallback = (() => void) | null;
type ChangeCallback = ((value: string) => void) | null;

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
    /**
     * EditorView's [root](https://codemirror.net/docs/ref/#view.EditorView.root).
     *
     * Don't support change dynamically!
     */
    @Input() root?: Document | ShadowRoot;

    /** CodeMirror configuration object */
    @Input() config: CodeMirrorConfig = {
        theme: defaultTheme,
        autoFocus: false,
        value: '',
        viewDisabled: false,
        readonly: false,
        extensions: [],
        languages: languages,
        language: ''
    };

    /** Event emitted when the editor's value changes. */
    @Output() valueChange = new EventEmitter<string>();

    /** Event emitted after the editor's view has been created. */
    @Output() loaded = new EventEmitter<Codemirror>();

    /** Event emitted when focus on the editor. */
    @Output() editorFocus = new EventEmitter<void>();

    /** Event emitted when the editor has lost focus. */
    @Output() editorBlur = new EventEmitter<void>();

    onTouched: EventListenerCallback = null;
    onChange: ChangeCallback = null;

    // Private properties
    private view: EditorView | null = null;
    private isInitialized = false;
    private readonly CSS_CLASSES = {
        READONLY: 'editor-readonly',
        DISABLED: 'editor-disabled'
    } as const;

    // Extension compartments for dynamic configuration
    private readonly editableCompartment = new Compartment();
    private readonly readonlyCompartment = new Compartment();
    private readonly themeCompartment = new Compartment();
    private readonly languageCompartment = new Compartment();

    // Update listener for tracking document changes
    private readonly updateListener = EditorView.updateListener.of((viewUpdate) => {
        if (viewUpdate.docChanged && !viewUpdate.transactions.some((tr) => tr.annotation(External))) {
            const currentValue = viewUpdate.state.doc.toString();
            this.onChange?.(currentValue);
            this.valueChange.emit(currentValue);
        }
    });

    constructor(private readonly elementRef: ElementRef<Element>) {}

    // Computed properties with better performance and null safety
    get theme(): Extension {
        return this.config.theme ?? defaultTheme;
    }

    get autoFocus(): boolean {
        return this.config.autoFocus ?? false;
    }

    get value(): string {
        return this.config.value ?? '';
    }

    get viewDisabled(): boolean {
        return this.config.viewDisabled ?? false;
    }

    get readonly(): boolean {
        return this.config.readonly ?? false;
    }

    get extensions(): Extension[] {
        return this.config.extensions ?? [];
    }

    get languages(): LanguageDescription[] {
        return this.config.languages ?? languages;
    }

    get language(): string {
        return this.config.language ?? '';
    }

    ngOnChanges(changes: SimpleChanges): void {
        if (!this.isInitialized || !changes['config']) {
            return;
        }

        const configChange = changes['config'];
        if (!configChange.currentValue) {
            return;
        }

        const previousConfig = configChange.previousValue || {};
        const currentConfig = configChange.currentValue;

        // Process config changes efficiently
        this.processConfigChanges(previousConfig, currentConfig);
    }

    ngOnInit(): void {
        this.initializeEditor();
        this.setupEventListeners();
        this.applyInitialConfiguration();
        this.isInitialized = true;
        this.loaded.emit(this);
    }

    ngOnDestroy(): void {
        this.cleanupEditor();
    }

    // ControlValueAccessor implementation
    writeValue(value: string): void {
        if (this.view && value !== undefined && value !== null) {
            this.setValue(value);
        }
    }

    registerOnChange(onChange: ChangeCallback): void {
        this.onChange = onChange;
    }

    registerOnTouched(onTouch: EventListenerCallback): void {
        this.onTouched = onTouch;
    }

    setDisabledState(isDisabled: boolean): void {
        this.config = { ...this.config, viewDisabled: isDisabled };
        if (this.isInitialized) {
            this.setEditable(!isDisabled);
        }
    }

    addEventListener(type: string, listener: EventListenerOrEventListenerObject): void {
        if (this.view?.contentDOM) {
            this.view.contentDOM.addEventListener(type, listener);
        }
    }

    // Private methods for better organization
    private initializeEditor(): void {
        if (!this.elementRef.nativeElement) {
            return;
        }

        this.view = new EditorView({
            root: this.root,
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
            this.editorFocus.emit();
        });

        contentDOM.addEventListener('blur', () => {
            this.onTouched?.();
            this.editorBlur.emit();
        });
    }

    private applyInitialConfiguration(): void {
        if (!this.view) {
            return;
        }

        this.setEditable(!this.viewDisabled);
        this.setReadonly(this.readonly);
        this.setTheme(this.theme);

        // Only set language if we have a language specified and languages available
        if (this.language && this.languages.length > 0) {
            this.setLanguage(this.language);
        }
    }

    private processConfigChanges(previousConfig: CodeMirrorConfig, currentConfig: CodeMirrorConfig): void {
        const changedProperties: Array<{
            key: ConfigPropertyKey;
            handler: (value: any) => void;
        }> = [
            { key: 'value', handler: (value) => this.setValue(value) },
            { key: 'viewDisabled', handler: (disabled) => this.setEditable(!disabled) },
            { key: 'readonly', handler: (readonly) => this.setReadonly(readonly) },
            { key: 'theme', handler: (theme) => this.setTheme(theme) },
            { key: 'extensions', handler: () => this.setExtensions(this.getAllExtensions()) },
            { key: 'language', handler: (language) => this.setLanguage(language) }
        ];

        changedProperties.forEach(({ key, handler }) => {
            if (currentConfig[key] !== previousConfig[key]) {
                handler(currentConfig[key]);
            }
        });
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

    private cleanupEditor(): void {
        if (this.view) {
            this.view.destroy();
            this.view = null;
            this.isInitialized = false;
        }
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

        this.toggleCSSClass(this.CSS_CLASSES.READONLY, isReadonly);

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

        this.toggleCSSClass(this.CSS_CLASSES.DISABLED, !isEditable);

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

        // Only attempt to set language if languages are available
        const languagesArray = this.config.languages || [];
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

        for (const languageDesc of this.config.languages || []) {
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
