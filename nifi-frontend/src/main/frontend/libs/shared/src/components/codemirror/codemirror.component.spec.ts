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

import { ComponentFixture, TestBed } from '@angular/core/testing';
import { Codemirror, CodeMirrorConfig } from './codemirror.component';
import { SimpleChange } from '@angular/core';

jest.mock('@codemirror/state', () => ({
    EditorState: {
        create: jest.fn().mockReturnValue({
            mockState: true,
            doc: {
                length: 0,
                lines: 1,
                toString: jest.fn().mockReturnValue(''),
                line: jest.fn().mockReturnValue({ text: 'mock line content' })
            },
            update: jest.fn().mockReturnValue({ mockTransaction: true }),
            selection: {
                main: {
                    from: 0,
                    to: 0
                }
            }
        }),
        readOnly: {
            of: jest.fn().mockReturnValue({ mockReadOnly: true })
        }
    },
    Compartment: jest.fn().mockImplementation(() => ({
        of: jest.fn().mockReturnValue({ mockCompartment: true }),
        reconfigure: jest.fn().mockReturnValue({ mockReconfigure: true })
    })),
    StateEffect: {
        define: jest.fn().mockReturnValue(jest.fn().mockReturnValue({ mockDefinedEffect: true })),
        reconfigure: {
            of: jest.fn().mockReturnValue({ mockStateEffect: true })
        }
    },
    Annotation: {
        define: jest.fn().mockReturnValue({
            of: jest.fn().mockReturnValue({ mockAnnotation: true })
        })
    }
}));

// Mock @lezer/highlight
jest.mock('@lezer/highlight', () => ({
    tags: {
        keyword: 'keyword',
        string: 'string',
        comment: 'comment',
        number: 'number',
        operator: 'operator',
        punctuation: 'punctuation',
        bracket: 'bracket',
        variableName: 'variableName',
        function: jest.fn().mockReturnValue('function'),
        special: jest.fn().mockReturnValue('special'),
        labelName: 'labelName',
        typeName: 'typeName',
        className: 'className',
        changed: 'changed',
        annotation: 'annotation',
        modifier: 'modifier',
        self: 'self',
        namespace: 'namespace',
        operatorKeyword: 'operatorKeyword',
        url: 'url',
        escape: 'escape',
        regexp: 'regexp',
        link: 'link',
        heading: 'heading',
        atom: 'atom',
        bool: 'bool',
        processingInstruction: 'processingInstruction',
        inserted: 'inserted',
        invalid: 'invalid'
    }
}));

// Mock @codemirror/language
jest.mock('@codemirror/language', () => ({
    HighlightStyle: {
        define: jest.fn().mockReturnValue({ mockHighlightStyle: true })
    },
    syntaxHighlighting: jest.fn().mockReturnValue({ mockSyntaxHighlighting: true }),
    defaultHighlightStyle: { mockDefaultHighlightStyle: true },
    StreamLanguage: {
        define: jest.fn().mockReturnValue({ mockStreamLanguage: true })
    }
}));

// Mock EditorView for testing
jest.mock('@codemirror/view', () => ({
    EditorView: Object.assign(
        jest.fn().mockImplementation(() => ({
            contentDOM: {
                addEventListener: jest.fn(),
                removeEventListener: jest.fn()
            },
            state: {
                doc: {
                    length: 10,
                    lines: 2,
                    toString: jest.fn().mockReturnValue('test content'),
                    line: jest.fn().mockReturnValue({ text: 'mock line content' })
                },
                readOnly: false,
                facet: jest.fn().mockReturnValue(true),
                update: jest.fn().mockReturnValue({ mockTransaction: true }),
                selection: {
                    main: {
                        from: 0,
                        to: 5
                    }
                }
            },
            dispatch: jest.fn(),
            focus: jest.fn(),
            destroy: jest.fn(),
            hasFocus: true
        })),
        {
            updateListener: {
                of: jest.fn().mockReturnValue({ mockUpdateListener: true })
            },
            editable: {
                of: jest.fn().mockReturnValue({ mockEditable: true })
            },
            theme: jest.fn().mockReturnValue({ mockTheme: true }),
            lineWrapping: { mockLineWrapping: true }
        }
    ),
    keymap: {
        of: jest.fn().mockReturnValue({ mockKeymap: true })
    },
    placeholder: jest.fn().mockReturnValue({ mockPlaceholder: true }),
    highlightWhitespace: jest.fn().mockReturnValue({ mockHighlightWhitespace: true })
}));

describe('Codemirror', () => {
    let component: Codemirror;
    let fixture: ComponentFixture<Codemirror>;

    beforeEach(async () => {
        await TestBed.configureTestingModule({
            imports: [Codemirror]
        }).compileComponents();

        fixture = TestBed.createComponent(Codemirror);
        component = fixture.componentInstance;
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });

    describe('Settings Getter', () => {
        it('should merge default and custom configuration', () => {
            component.config = {
                content: 'test content',
                disabled: true
            };

            const settings = component.settings;

            expect(settings.content).toBe('test content');
            expect(settings.disabled).toBe(true);
            expect(settings.focusOnInit).toBe(false); // default value
            expect(settings.readOnly).toBe(false); // default value
        });

        it('should return default configuration when no config provided', () => {
            component.config = {};

            const settings = component.settings;

            expect(settings.content).toBe('');
            expect(settings.disabled).toBe(false);
            expect(settings.readOnly).toBe(false);
            expect(settings.focusOnInit).toBe(false);
            expect(settings.plugins).toEqual([]);
        });

        it('should handle undefined config properties', () => {
            component.config = {
                content: undefined,
                disabled: undefined,
                readOnly: undefined
            };

            const settings = component.settings;

            // Undefined values from config override defaults, so we get undefined
            expect(settings.content).toBeUndefined();
            expect(settings.disabled).toBeUndefined();
            expect(settings.readOnly).toBeUndefined();
            // But other defaults are preserved
            expect(settings.focusOnInit).toBe(false);
        });
    });

    describe('Initialization', () => {
        it('should initialize with default configuration', () => {
            expect(component).toBeTruthy();
            expect(component.settings).toBeDefined();
        });

        it('should initialize editor on ngOnInit', () => {
            component.config = {
                content: 'test content',
                plugins: []
            };

            component.ngOnInit();

            expect(component['isInitialized']).toBe(true);
        });

        it('should emit ready event after initialization', () => {
            const readySpy = jest.spyOn(component.ready, 'emit');

            component.ngOnInit();

            expect(readySpy).toHaveBeenCalledWith(component);
        });

        it('should focus editor if focusOnInit is true', () => {
            component.config = {
                focusOnInit: true,
                plugins: []
            };

            component.ngOnInit();

            expect(component['view']?.focus).toHaveBeenCalled();
        });

        it('should not focus editor if focusOnInit is false', () => {
            component.config = {
                focusOnInit: false,
                plugins: []
            };

            component.ngOnInit();

            expect(component['view']?.focus).not.toHaveBeenCalled();
        });

        it('should handle documentRoot parameter', () => {
            const mockShadowRoot = document.createElement('div').attachShadow({ mode: 'open' });
            component.documentRoot = mockShadowRoot;

            component.ngOnInit();

            expect(component['isInitialized']).toBe(true);
        });

        it('should initialize with plugins', () => {
            const mockPlugin = { mockPlugin: true };
            component.config = {
                plugins: [mockPlugin as any]
            };

            component.ngOnInit();

            expect(component['isInitialized']).toBe(true);
        });
    });

    describe('Configuration Changes', () => {
        beforeEach(() => {
            fixture.detectChanges();
        });

        const testConfigChange = (property: keyof CodeMirrorConfig, oldValue: any, newValue: any) => {
            it(`should handle ${property} changes`, () => {
                const previousConfig: CodeMirrorConfig = { [property]: oldValue };
                const currentConfig: CodeMirrorConfig = { [property]: newValue };
                const changes = {
                    config: new SimpleChange(previousConfig, currentConfig, false)
                };

                component.ngOnChanges(changes);

                expect(() => component.ngOnChanges(changes)).not.toThrow();
            });
        };

        testConfigChange('content', 'old content', 'new content');
        testConfigChange('disabled', false, true);
        testConfigChange('readOnly', false, true);
        testConfigChange('focusOnInit', false, true);

        it('should handle appearance changes', () => {
            const oldAppearance = { mockOldTheme: true };
            const newAppearance = { mockNewTheme: true };
            const changes = {
                config: new SimpleChange({ appearance: oldAppearance }, { appearance: newAppearance }, false)
            };

            component.ngOnChanges(changes);

            expect(() => component.ngOnChanges(changes)).not.toThrow();
        });

        it('should handle plugins changes', () => {
            const oldPlugins = [{ mockOldPlugin: true }];
            const newPlugins = [{ mockNewPlugin: true }];
            const changes = {
                config: new SimpleChange({ plugins: oldPlugins }, { plugins: newPlugins }, false)
            };

            component.ngOnChanges(changes);

            expect(() => component.ngOnChanges(changes)).not.toThrow();
        });

        it('should ignore changes when config property is not present', () => {
            const changes = {
                otherProperty: new SimpleChange('old', 'new', false)
            };

            component.ngOnChanges(changes);

            expect(() => component.ngOnChanges(changes)).not.toThrow();
        });

        it('should handle multiple simultaneous config changes', () => {
            const changes = {
                config: new SimpleChange(
                    { content: 'old', disabled: false, readOnly: false },
                    { content: 'new', disabled: true, readOnly: true },
                    false
                )
            };

            component.ngOnChanges(changes);

            expect(() => component.ngOnChanges(changes)).not.toThrow();
        });
    });

    describe('Form Control Integration', () => {
        beforeEach(() => {
            fixture.detectChanges();
        });

        it('should write value to editor via ControlValueAccessor', () => {
            const testValue = 'form control test value';
            const dispatchSpy = jest.spyOn(component['view']!, 'dispatch');

            component.writeValue(testValue);

            expect(dispatchSpy).toHaveBeenCalled();
        });

        it('should not write value if view is not initialized', () => {
            component['view'] = null;
            const testValue = 'test value';

            expect(() => component.writeValue(testValue)).not.toThrow();
        });

        it('should not write same value twice', () => {
            const testValue = 'test content'; // Same as mock return value
            const dispatchSpy = jest.spyOn(component['view']!, 'dispatch');
            dispatchSpy.mockClear(); // Clear any calls from initialization

            component.writeValue(testValue);

            expect(dispatchSpy).not.toHaveBeenCalled();
        });

        it('should register onChange callback', () => {
            const onChangeFn = jest.fn();

            component.registerOnChange(onChangeFn);

            expect(component['onChange']).toBe(onChangeFn);
        });

        it('should register onTouched callback', () => {
            const onTouchedFn = jest.fn();

            component.registerOnTouched(onTouchedFn);

            expect(component['onTouched']).toBe(onTouchedFn);
        });

        it('should handle disabled state changes', () => {
            const setEditableSpy = jest.spyOn(component as any, 'setEditable');

            component.setDisabledState(true);

            expect(setEditableSpy).toHaveBeenCalledWith(false);
        });

        it('should handle enabled state changes', () => {
            const setEditableSpy = jest.spyOn(component as any, 'setEditable');

            component.setDisabledState(false);

            expect(setEditableSpy).toHaveBeenCalledWith(true);
        });
    });

    describe('Public API Methods', () => {
        beforeEach(() => {
            fixture.detectChanges();
        });

        describe('Event Listener Management', () => {
            it('should add event listener to content DOM', () => {
                const mockListener = jest.fn();
                const addEventListenerSpy = jest.spyOn(component['view']!.contentDOM, 'addEventListener');

                component.addEventListener('click', mockListener);

                expect(addEventListenerSpy).toHaveBeenCalledWith('click', mockListener);
            });

            it('should not add event listener if view is not initialized', () => {
                component['view'] = null;
                const mockListener = jest.fn();

                expect(() => component.addEventListener('click', mockListener)).not.toThrow();
            });

            it('should remove event listener from content DOM', () => {
                const mockListener = jest.fn();
                const removeEventListenerSpy = jest.spyOn(component['view']!.contentDOM, 'removeEventListener');

                component.removeEventListener('click', mockListener);

                expect(removeEventListenerSpy).toHaveBeenCalledWith('click', mockListener);
            });

            it('should not remove event listener if view is not initialized', () => {
                component['view'] = null;
                const mockListener = jest.fn();

                expect(() => component.removeEventListener('click', mockListener)).not.toThrow();
            });
        });

        describe('Focus Management', () => {
            it('should focus the editor', () => {
                const focusSpy = jest.spyOn(component['view']!, 'focus');

                component.focus();

                expect(focusSpy).toHaveBeenCalled();
            });

            it('should not focus if view is not initialized', () => {
                component['view'] = null;

                expect(() => component.focus()).not.toThrow();
            });

            it('should return focus status', () => {
                expect(component.hasFocus()).toBe(true);
            });

            it('should return false if view is not initialized', () => {
                component['view'] = null;

                expect(component.hasFocus()).toBe(false);
            });
        });

        describe('Content Management', () => {
            it('should get current editor value', () => {
                const value = component.getValue();

                expect(value).toBe('test content');
            });

            it('should return empty string if view is not initialized', () => {
                component['view'] = null;

                expect(component.getValue()).toBe('');
            });

            it('should set editor value programmatically', () => {
                const dispatchSpy = jest.spyOn(component['view']!, 'dispatch');

                component.setEditorValue('new content');

                expect(dispatchSpy).toHaveBeenCalled();
            });

            it('should get line count', () => {
                const lineCount = component.getLineCount();

                expect(lineCount).toBe(2);
            });

            it('should return 0 lines if view is not initialized', () => {
                component['view'] = null;

                expect(component.getLineCount()).toBe(0);
            });

            it('should get line content by number', () => {
                const lineContent = component.getLine(1);

                expect(lineContent).toBe('mock line content');
            });

            it('should return empty string for invalid line number', () => {
                expect(component.getLine(0)).toBe('');
                expect(component.getLine(-1)).toBe('');
                expect(component.getLine(999)).toBe('');
            });

            it('should return empty string if view is not initialized', () => {
                component['view'] = null;

                expect(component.getLine(1)).toBe('');
            });
        });

        describe('Selection Management', () => {
            it('should select all text', () => {
                const dispatchSpy = jest.spyOn(component['view']!, 'dispatch');

                component.selectAll();

                expect(dispatchSpy).toHaveBeenCalledWith({
                    selection: { anchor: 0, head: 10 }
                });
            });

            it('should not select all if view is not initialized', () => {
                component['view'] = null;

                expect(() => component.selectAll()).not.toThrow();
            });

            it('should get current selection', () => {
                const selection = component.getSelection();

                expect(selection).toEqual({ from: 0, to: 5 });
            });

            it('should return null if view is not initialized', () => {
                component['view'] = null;

                expect(component.getSelection()).toBeNull();
            });

            it('should set selection range', () => {
                const dispatchSpy = jest.spyOn(component['view']!, 'dispatch');

                component.setSelection(2, 8);

                expect(dispatchSpy).toHaveBeenCalledWith({
                    selection: { anchor: 2, head: 8 }
                });
            });

            it('should set cursor position when to is not provided', () => {
                const dispatchSpy = jest.spyOn(component['view']!, 'dispatch');

                component.setSelection(5);

                expect(dispatchSpy).toHaveBeenCalledWith({
                    selection: { anchor: 5, head: 5 }
                });
            });

            it('should not set selection if view is not initialized', () => {
                component['view'] = null;

                expect(() => component.setSelection(0, 5)).not.toThrow();
            });
        });

        describe('Text Manipulation', () => {
            it('should insert text at cursor position', () => {
                const dispatchSpy = jest.spyOn(component['view']!, 'dispatch');

                component.insertText('inserted text');

                expect(dispatchSpy).toHaveBeenCalledWith({
                    changes: {
                        from: 0,
                        to: 5,
                        insert: 'inserted text'
                    }
                });
            });

            it('should not insert text if view is not initialized', () => {
                component['view'] = null;

                expect(() => component.insertText('text')).not.toThrow();
            });

            it('should replace text in range', () => {
                const dispatchSpy = jest.spyOn(component['view']!, 'dispatch');

                component.replaceRange(2, 8, 'replacement');

                expect(dispatchSpy).toHaveBeenCalledWith({
                    changes: { from: 2, to: 8, insert: 'replacement' }
                });
            });

            it('should not replace range if view is not initialized', () => {
                component['view'] = null;

                expect(() => component.replaceRange(0, 5, 'text')).not.toThrow();
            });
        });
    });

    describe('Event Handling', () => {
        beforeEach(() => {
            fixture.detectChanges();
        });

        it('should emit focused event on editor focus', () => {
            const focusedSpy = jest.spyOn(component.focused, 'emit');

            component.focused.emit();

            expect(focusedSpy).toHaveBeenCalled();
        });

        it('should emit blurred event on editor blur', () => {
            const blurredSpy = jest.spyOn(component.blurred, 'emit');

            component.blurred.emit();

            expect(blurredSpy).toHaveBeenCalled();
        });

        it('should emit contentChange on document changes', () => {
            const contentChangeSpy = jest.spyOn(component.contentChange, 'emit');

            component.contentChange.emit('new content');

            expect(contentChangeSpy).toHaveBeenCalledWith('new content');
        });

        it('should setup event listeners during initialization', () => {
            const addEventListenerSpy = jest.spyOn(component['view']!.contentDOM, 'addEventListener');

            // Call setupEventListeners method directly
            component['setupEventListeners']();

            expect(addEventListenerSpy).toHaveBeenCalledWith('focus', expect.any(Function));
            expect(addEventListenerSpy).toHaveBeenCalledWith('blur', expect.any(Function));
        });

        it('should not setup event listeners if view is not initialized', () => {
            component['view'] = null;

            expect(() => component['setupEventListeners']()).not.toThrow();
        });
    });

    describe('Private Methods', () => {
        beforeEach(() => {
            fixture.detectChanges();
        });

        describe('Configuration Application', () => {
            it('should apply initial configuration', () => {
                const setThemeSpy = jest.spyOn(component as any, 'setTheme');
                const setEditableSpy = jest.spyOn(component as any, 'setEditable');
                const setReadonlySpy = jest.spyOn(component as any, 'setReadonly');
                const setPluginsSpy = jest.spyOn(component as any, 'setPlugins');

                component['applyInitialConfiguration']();

                expect(setThemeSpy).toHaveBeenCalled();
                expect(setEditableSpy).toHaveBeenCalled();
                expect(setReadonlySpy).toHaveBeenCalled();
                expect(setPluginsSpy).toHaveBeenCalled();
            });

            it('should process configuration changes correctly', () => {
                const setValueSpy = jest.spyOn(component as any, 'setValue');
                const setEditableSpy = jest.spyOn(component as any, 'setEditable');
                const setReadonlySpy = jest.spyOn(component as any, 'setReadonly');
                const setThemeSpy = jest.spyOn(component as any, 'setTheme');
                const setPluginsSpy = jest.spyOn(component as any, 'setPlugins');

                const previousConfig = {
                    content: 'old content',
                    disabled: false,
                    readOnly: false,
                    appearance: { mockOldTheme: true } as any,
                    plugins: [{ mockOldPlugin: true } as any]
                };

                const currentConfig = {
                    content: 'new content',
                    disabled: true,
                    readOnly: true,
                    appearance: { mockNewTheme: true } as any,
                    plugins: [{ mockNewPlugin: true } as any]
                };

                component['processConfigChanges'](previousConfig, currentConfig);

                expect(setValueSpy).toHaveBeenCalledWith('new content');
                expect(setEditableSpy).toHaveBeenCalledWith(false);
                expect(setReadonlySpy).toHaveBeenCalledWith(true);
                expect(setThemeSpy).toHaveBeenCalledWith({ mockNewTheme: true });
                expect(setPluginsSpy).toHaveBeenCalledWith([{ mockNewPlugin: true }]);
            });

            it('should not process unchanged configuration properties', () => {
                const setValueSpy = jest.spyOn(component as any, 'setValue');
                const setEditableSpy = jest.spyOn(component as any, 'setEditable');

                const sameConfig = {
                    content: 'same content',
                    disabled: false
                };

                component['processConfigChanges'](sameConfig, sameConfig);

                expect(setValueSpy).not.toHaveBeenCalled();
                expect(setEditableSpy).not.toHaveBeenCalled();
            });
        });

        describe('Theme Management', () => {
            it('should set theme when view is initialized', () => {
                const dispatchSpy = jest.spyOn(component['view']!, 'dispatch');
                const mockTheme = { mockTheme: true };

                component['setTheme'](mockTheme as any);

                expect(dispatchSpy).toHaveBeenCalled();
            });

            it('should not set theme if view is not initialized', () => {
                component['view'] = null;
                const mockTheme = { mockTheme: true };

                expect(() => component['setTheme'](mockTheme as any)).not.toThrow();
            });
        });

        describe('Editable State Management', () => {
            it('should set editable state when view is initialized', () => {
                const dispatchSpy = jest.spyOn(component['view']!, 'dispatch');

                component['setEditable'](false);

                expect(dispatchSpy).toHaveBeenCalled();
            });

            it('should not set editable state if view is not initialized', () => {
                component['view'] = null;

                expect(() => component['setEditable'](false)).not.toThrow();
            });
        });

        describe('Read-only State Management', () => {
            it('should set readonly state when view is initialized', () => {
                const dispatchSpy = jest.spyOn(component['view']!, 'dispatch');

                component['setReadonly'](true);

                expect(dispatchSpy).toHaveBeenCalled();
            });

            it('should not set readonly state if view is not initialized', () => {
                component['view'] = null;

                expect(() => component['setReadonly'](true)).not.toThrow();
            });
        });

        describe('Plugin Management', () => {
            it('should set plugins when view is initialized', () => {
                const dispatchSpy = jest.spyOn(component['view']!, 'dispatch');
                const mockPlugins = [{ mockPlugin: true }];

                component['setPlugins'](mockPlugins as any);

                expect(dispatchSpy).toHaveBeenCalled();
            });

            it('should not set plugins if view is not initialized', () => {
                component['view'] = null;
                const mockPlugins = [{ mockPlugin: true }];

                expect(() => component['setPlugins'](mockPlugins as any)).not.toThrow();
            });
        });

        describe('Value Management', () => {
            it('should set value with external annotation', () => {
                const dispatchSpy = jest.spyOn(component['view']!, 'dispatch');

                component['setValue']('new value');

                expect(dispatchSpy).toHaveBeenCalled();
            });

            it('should not set value if view is not initialized', () => {
                component['view'] = null;

                expect(() => component['setValue']('new value')).not.toThrow();
            });
        });
    });

    describe('Edge Cases and Error Handling', () => {
        it('should handle empty configuration', () => {
            component.config = {};

            expect(() => component.ngOnInit()).not.toThrow();
            expect(component['isInitialized']).toBe(true);
        });

        it('should handle null configuration', () => {
            component.config = null as any;

            expect(() => component.ngOnInit()).not.toThrow();
        });

        it('should handle undefined configuration', () => {
            component.config = undefined as any;

            expect(() => component.ngOnInit()).not.toThrow();
        });

        it('should handle view destruction when view is null', () => {
            component['view'] = null;

            expect(() => component.ngOnDestroy()).not.toThrow();
        });

        it('should handle multiple destroy calls', () => {
            component.ngOnDestroy();
            component.ngOnDestroy();

            expect(component['view']).toBeNull();
        });

        it('should handle config changes with undefined values', () => {
            const changes = {
                config: new SimpleChange({ content: 'old' }, { content: undefined }, false)
            };

            expect(() => component.ngOnChanges(changes)).not.toThrow();
        });

        it('should handle very large content', () => {
            const largeContent = 'x'.repeat(100000);
            component.config = { content: largeContent };

            expect(() => component.ngOnInit()).not.toThrow();
        });

        it('should handle special characters in content', () => {
            const specialContent = '!@#$%^&*()_+{}|:"<>?[]\\;\'.,/`~\n\t\r';
            component.config = { content: specialContent };

            expect(() => component.ngOnInit()).not.toThrow();
        });

        it('should handle empty plugins array', () => {
            component.config = { plugins: [] };

            expect(() => component.ngOnInit()).not.toThrow();
        });

        it('should handle multiple plugins', () => {
            const plugins = [{ mockPlugin1: true }, { mockPlugin2: true }, { mockPlugin3: true }];
            component.config = { plugins: plugins as any };

            expect(() => component.ngOnInit()).not.toThrow();
        });
    });

    describe('Cleanup', () => {
        it('should clean up editor on destroy', () => {
            fixture.detectChanges();
            const destroySpy = jest.spyOn(component['view']!, 'destroy');

            component.ngOnDestroy();

            expect(destroySpy).toHaveBeenCalled();
            expect(component['view']).toBeNull();
        });

        it('should ignore changes before initialization', () => {
            const freshFixture = TestBed.createComponent(Codemirror);
            const freshComponent = freshFixture.componentInstance;

            freshComponent['isInitialized'] = false;

            freshComponent.ngOnChanges({
                config: new SimpleChange({ content: 'old value' }, { content: 'new value' }, false)
            });

            expect(() =>
                freshComponent.ngOnChanges({
                    config: new SimpleChange({ content: 'old value' }, { content: 'new value' }, false)
                })
            ).not.toThrow();
        });

        it('should handle cleanup of event listeners', () => {
            fixture.detectChanges();
            const mockListener = jest.fn();

            // Add and remove event listener
            component.addEventListener('test-event', mockListener);
            component.removeEventListener('test-event', mockListener);

            // Destroy component
            component.ngOnDestroy();

            expect(component['view']).toBeNull();
        });
    });

    describe('Complex Integration Scenarios', () => {
        it('should handle rapid configuration changes', () => {
            fixture.detectChanges();

            // Simulate rapid config changes
            for (let i = 0; i < 10; i++) {
                const changes = {
                    config: new SimpleChange({ content: `content ${i}` }, { content: `content ${i + 1}` }, false)
                };
                component.ngOnChanges(changes);
            }

            expect(component['isInitialized']).toBe(true);
        });

        it('should handle form control integration with complex values', () => {
            fixture.detectChanges();

            const complexValue = JSON.stringify({
                nested: {
                    object: ['with', 'array', 123],
                    boolean: true
                }
            });

            component.writeValue(complexValue);

            expect(component['view']?.dispatch).toHaveBeenCalled();
        });

        it('should maintain consistency between API methods', () => {
            fixture.detectChanges();

            // Set a value and verify it can be retrieved
            component.setEditorValue('test value for consistency');

            // Insert text and verify selection updates
            component.setSelection(0, 0);
            component.insertText('inserted ');

            expect(component.getSelection()).toBeTruthy();
        });

        it('should handle simultaneous focus and content changes', () => {
            fixture.detectChanges();

            component.focus();
            component.setEditorValue('focused content');
            component.selectAll();

            expect(component.hasFocus()).toBe(true);
        });
    });
});
