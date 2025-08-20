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
import { Annotation, Compartment, EditorState, Extension } from '@codemirror/state';
import { EditorView } from '@codemirror/view';
import { defaultTheme } from './themes/defaultTheme';

export interface CodeMirrorConfig {
    appearance?: Extension;
    focusOnInit?: boolean;
    content?: string;
    disabled?: boolean;
    readOnly?: boolean;
    plugins?: Extension[];
}

/**
 * Annotation to mark editor changes that come from external sources (not user input).
 * Used to prevent infinite loops in two-way data binding by:
 * 1. Marking programmatic changes with this annotation
 * 2. Not emitting change events for changes that have this annotation
 *
 * @example
 * // Mark a change as external
 * dispatch({
 *   changes: { from: 0, to: 10, insert: "new text" },
 *   annotations: [ExternalChange.of(true)]
 * });
 *
 * // Check if a change is external
 * if (!transaction.annotation(ExternalChange)) {
 *   // Handle user input...
 * }
 */
export const ExternalChange = Annotation.define<boolean>();

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
    @Input() config: CodeMirrorConfig = {
        appearance: defaultTheme,
        focusOnInit: false,
        content: '',
        disabled: false,
        readOnly: false,
        plugins: []
    };

    private readonly defaultConfig: CodeMirrorConfig = {
        appearance: defaultTheme,
        focusOnInit: false,
        content: '',
        disabled: false,
        readOnly: false,
        plugins: []
    };

    @Output() contentChange = new EventEmitter<string>();
    @Output() ready = new EventEmitter<Codemirror>();
    @Output() focused = new EventEmitter<void>();
    @Output() blurred = new EventEmitter<void>();

    private view: EditorView | null = null;
    private isInitialized = false;
    private themeCompartment = new Compartment();
    private editableCompartment = new Compartment();
    private readOnlyCompartment = new Compartment();
    private pluginCompartment = new Compartment();

    constructor(private elementRef: ElementRef<Element>) {}

    get settings(): CodeMirrorConfig {
        return { ...this.defaultConfig, ...this.config };
    }

    ngOnInit(): void {
        this.initializeEditor();
        this.setupEventListeners();
        this.applyInitialConfiguration();
        this.isInitialized = true;
        this.ready.emit(this);
    }

    ngOnChanges(changes: SimpleChanges): void {
        if (!this.isInitialized || !changes['config']) {
            return;
        }

        const previousConfig = { ...this.defaultConfig, ...changes['config'].previousValue };
        const currentConfig = { ...this.defaultConfig, ...changes['config'].currentValue };

        this.processConfigChanges(previousConfig, currentConfig);
    }

    ngOnDestroy(): void {
        this.view?.destroy();
        this.view = null;
    }

    // ControlValueAccessor implementation
    writeValue(value: string): void {
        if (this.view && value !== this.view.state.doc.toString()) {
            this.setValue(value);
        }
    }

    registerOnChange(fn: (value: string) => void): void {
        this.onChange = fn;
    }

    registerOnTouched(fn: () => void): void {
        this.onTouched = fn;
    }

    setDisabledState(isDisabled: boolean): void {
        this.setEditable(!isDisabled);
    }

    /**
     * Add an event listener to the editor's content DOM
     * @param type Event type (e.g., 'keydown', 'click', 'focus')
     * @param listener Event listener function
     */
    addEventListener(type: string, listener: EventListenerOrEventListenerObject): void {
        if (this.view?.contentDOM) {
            this.view.contentDOM.addEventListener(type, listener);
        }
    }

    /**
     * Remove an event listener from the editor's content DOM
     * @param type Event type
     * @param listener Event listener function
     */
    removeEventListener(type: string, listener: EventListenerOrEventListenerObject): void {
        if (this.view?.contentDOM) {
            this.view.contentDOM.removeEventListener(type, listener);
        }
    }

    /**
     * Focus the editor
     */
    focus(): void {
        if (this.view) {
            this.view.focus();
        }
    }

    /**
     * Get the current value of the editor
     * @returns Current editor content as string
     */
    getValue(): string {
        return this.view?.state.doc.toString() || '';
    }

    /**
     * Set the editor value programmatically
     * @param value New content for the editor
     */
    setEditorValue(value: string): void {
        this.setValue(value);
    }

    /**
     * Select all text in the editor
     */
    selectAll(): void {
        if (this.view) {
            this.view.dispatch({
                selection: { anchor: 0, head: this.view.state.doc.length }
            });
        }
    }

    /**
     * Get the current selection range
     * @returns Object with 'from' and 'to' positions
     */
    getSelection(): { from: number; to: number } | null {
        if (this.view) {
            const selection = this.view.state.selection.main;
            return { from: selection.from, to: selection.to };
        }
        return null;
    }

    /**
     * Set the selection range
     * @param from Start position
     * @param to End position (optional, defaults to from for cursor placement)
     */
    setSelection(from: number, to?: number): void {
        if (this.view) {
            this.view.dispatch({
                selection: { anchor: from, head: to ?? from }
            });
        }
    }

    /**
     * Insert text at the current cursor position
     * @param text Text to insert
     */
    insertText(text: string): void {
        if (this.view) {
            const selection = this.view.state.selection.main;
            this.view.dispatch({
                changes: {
                    from: selection.from,
                    to: selection.to,
                    insert: text
                }
            });
        }
    }

    /**
     * Replace text in a specific range
     * @param from Start position
     * @param to End position
     * @param text Replacement text
     */
    replaceRange(from: number, to: number, text: string): void {
        if (this.view) {
            this.view.dispatch({
                changes: { from, to, insert: text }
            });
        }
    }

    /**
     * Check if the editor is focused
     * @returns True if editor has focus
     */
    hasFocus(): boolean {
        return this.view?.hasFocus || false;
    }

    /**
     * Get the current line count
     * @returns Number of lines in the editor
     */
    getLineCount(): number {
        return this.view?.state.doc.lines || 0;
    }

    /**
     * Get text content of a specific line
     * @param lineNumber Line number (1-based)
     * @returns Line content or empty string if line doesn't exist
     */
    getLine(lineNumber: number): string {
        if (this.view && lineNumber > 0 && lineNumber <= this.view.state.doc.lines) {
            return this.view.state.doc.line(lineNumber).text;
        }
        return '';
    }

    private onChange?: (value: string) => void;
    private onTouched?: () => void;

    private initializeEditor(): void {
        const state = EditorState.create({
            doc: this.settings.content || '',
            extensions: [
                this.themeCompartment.of(this.settings.appearance || defaultTheme),
                this.editableCompartment.of(EditorView.editable.of(!this.settings.disabled)),
                this.readOnlyCompartment.of(EditorState.readOnly.of(this.settings.readOnly || false)),
                this.pluginCompartment.of(this.settings.plugins || []),
                EditorView.updateListener.of((update) => {
                    if (update.docChanged && !update.transactions.some((tr) => tr.annotation(ExternalChange))) {
                        const newValue = update.state.doc.toString();
                        this.contentChange.emit(newValue);
                        this.onChange?.(newValue);
                    }
                })
            ]
        });

        this.view = new EditorView({
            state,
            parent: this.elementRef.nativeElement,
            root: this.documentRoot
        });

        if (this.settings.focusOnInit) {
            this.view.focus();
        }
    }

    private setupEventListeners(): void {
        if (!this.view) return;

        this.view.contentDOM.addEventListener('focus', () => {
            this.focused.emit();
            this.onTouched?.();
        });

        this.view.contentDOM.addEventListener('blur', () => {
            this.blurred.emit();
        });
    }

    private applyInitialConfiguration(): void {
        this.setTheme(this.settings.appearance || defaultTheme);
        this.setEditable(!this.settings.disabled);
        this.setReadonly(this.settings.readOnly || false);
        this.setPlugins(this.settings.plugins || []);
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
            this.setPlugins(currentConfig.plugins ?? []);
        }
    }

    private setValue(content: string): void {
        if (!this.view) return;

        const transaction = this.view.state.update({
            changes: {
                from: 0,
                to: this.view.state.doc.length,
                insert: content
            },
            annotations: [ExternalChange.of(true)]
        });

        this.view.dispatch(transaction);

        if (!this.settings.readOnly) {
            this.selectAll();
        }
    }

    private setTheme(theme: Extension): void {
        if (!this.view) return;

        this.view.dispatch({
            effects: this.themeCompartment.reconfigure(theme)
        });
    }

    private setEditable(editable: boolean): void {
        if (!this.view) return;

        this.view.dispatch({
            effects: this.editableCompartment.reconfigure(EditorView.editable.of(editable))
        });

        if (editable) {
            this.selectAll();
        }
    }

    private setReadonly(readonly: boolean): void {
        if (!this.view) return;

        this.view.dispatch({
            effects: this.readOnlyCompartment.reconfigure(EditorState.readOnly.of(readonly))
        });

        if (!readonly) {
            this.selectAll();
        }
    }

    private setPlugins(plugins: Extension[]): void {
        if (!this.view) return;

        this.view.dispatch({
            effects: this.pluginCompartment.reconfigure(plugins)
        });
    }
}
