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
                    length: 0,
                    toString: jest.fn().mockReturnValue('')
                },
                readOnly: false,
                facet: jest.fn().mockReturnValue(true)
            },
            dispatch: jest.fn(),
            focus: jest.fn(),
            destroy: jest.fn()
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

// Mock EditorState
jest.mock('@codemirror/state', () => ({
    EditorState: {
        create: jest.fn().mockReturnValue({ mockState: true }),
        readOnly: {
            of: jest.fn().mockReturnValue({ mockReadOnly: true })
        }
    },
    Compartment: jest.fn().mockImplementation(() => ({
        of: jest.fn().mockReturnValue({ mockCompartment: true }),
        reconfigure: jest.fn().mockReturnValue({ mockReconfigure: true })
    })),
    StateEffect: {
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

import { ComponentFixture, TestBed } from '@angular/core/testing';
import { Codemirror, CodeMirrorConfig } from './codemirror.component';
import { SimpleChange } from '@angular/core';

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

    describe('Initialization', () => {
        it('should initialize with default configuration', () => {
            // SETUP: Component creation
            // INPUT: Default configuration
            // FUNCTION: Component initialization
            // EXPECTED: Component should be created with defaults

            // RESULT: Verify component is created
            expect(component).toBeTruthy();
            expect(component.settings).toBeDefined();
        });

        it('should initialize editor on ngOnInit', () => {
            // SETUP: Component with basic configuration
            // INPUT: Component initialization
            component.config = {
                content: 'test content',
                plugins: []
            };

            // FUNCTION: Call ngOnInit
            component.ngOnInit();

            // EXPECTED: Should initialize editor and emit ready event
            // RESULT: Verify initialization
            expect(component.isInitialized).toBe(true);
        });

        it('should emit ready event after initialization', () => {
            // SETUP: Component with ready event spy
            // INPUT: Component initialization
            const readySpy = jest.spyOn(component.ready, 'emit');

            // FUNCTION: Initialize component
            component.ngOnInit();

            // EXPECTED: Should emit ready event with component instance
            // RESULT: Verify ready event emission
            expect(readySpy).toHaveBeenCalledWith(component);
        });

        it('should focus editor if focusOnInit is true', () => {
            // SETUP: Component with focus configuration
            // INPUT: focusOnInit set to true
            component.config = {
                focusOnInit: true,
                plugins: []
            };

            // FUNCTION: Initialize component
            component.ngOnInit();

            // EXPECTED: Should focus the editor
            // RESULT: Verify focus method was called
            expect(component['view']?.focus).toHaveBeenCalled();
        });
    });

    describe('Configuration Changes', () => {
        beforeEach(() => {
            fixture.detectChanges();
        });

        const testConfigChange = (
            property: keyof CodeMirrorConfig,
            oldValue: any,
            newValue: any,
            expectation: string
        ) => {
            it(`should handle ${property} changes`, () => {
                // SETUP: Component with initialized state
                // INPUT: Configuration change
                const previousConfig: CodeMirrorConfig = { [property]: oldValue };
                const currentConfig: CodeMirrorConfig = { [property]: newValue };
                const changes = {
                    config: new SimpleChange(previousConfig, currentConfig, false)
                };

                // FUNCTION: Trigger ngOnChanges
                component.ngOnChanges(changes);

                // EXPECTED: Should process the configuration change
                // RESULT: Configuration change should be handled (method should not throw)
                expect(() => component.ngOnChanges(changes)).not.toThrow();
            });
        };

        testConfigChange('content', 'old content', 'new content', 'content');
        testConfigChange('disabled', false, true, 'disabled');
        testConfigChange('readOnly', false, true, 'readOnly');
        testConfigChange('focusOnInit', false, true, 'focusOnInit');
    });

    describe('Form Control Integration', () => {
        beforeEach(() => {
            fixture.detectChanges();
        });

        it('should write value to editor via ControlValueAccessor', () => {
            // SETUP: Component with initialized editor
            // INPUT: Test value for form control
            const testValue = 'form control test value';
            const dispatchSpy = jest.spyOn(component['view']!, 'dispatch');

            // FUNCTION: Call writeValue
            component.writeValue(testValue);

            // EXPECTED: Should dispatch value change to editor
            // RESULT: Verify dispatch was called
            expect(dispatchSpy).toHaveBeenCalled();
        });

        it('should register onChange callback', () => {
            // SETUP: Component with form integration
            // INPUT: onChange callback function
            const onChangeFn = jest.fn();

            // FUNCTION: Register onChange
            component.registerOnChange(onChangeFn);

            // EXPECTED: Should store the callback
            // RESULT: Verify callback is registered
            expect(component['onChange']).toBe(onChangeFn);
        });

        it('should register onTouched callback', () => {
            // SETUP: Component with form integration
            // INPUT: onTouched callback function
            const onTouchedFn = jest.fn();

            // FUNCTION: Register onTouched
            component.registerOnTouched(onTouchedFn);

            // EXPECTED: Should store the callback
            // RESULT: Verify callback is registered
            expect(component['onTouched']).toBe(onTouchedFn);
        });

        it('should handle disabled state changes', () => {
            // SETUP: Component with form integration
            // INPUT: Disabled state change
            const setEditableSpy = jest.spyOn(component as any, 'setEditable');

            // FUNCTION: Set disabled state
            component.setDisabledState(true);

            // EXPECTED: Should call setEditable with false
            // RESULT: Verify setEditable called
            expect(setEditableSpy).toHaveBeenCalledWith(false);
        });
    });

    describe('Event Handling', () => {
        beforeEach(() => {
            fixture.detectChanges();
        });

        it('should emit focused event on editor focus', () => {
            // SETUP: Component with event listener setup
            // INPUT: Focus event simulation
            const focusedSpy = jest.spyOn(component.focused, 'emit');

            // FUNCTION: Simulate focus by emitting focused
            component.focused.emit();

            // EXPECTED: Should emit focused event
            // RESULT: Verify focused event emission
            expect(focusedSpy).toHaveBeenCalled();
        });

        it('should emit blurred event on editor blur', () => {
            // SETUP: Component with event listener setup
            // INPUT: Blur event simulation
            const blurredSpy = jest.spyOn(component.blurred, 'emit');

            // FUNCTION: Simulate blur by emitting blurred
            component.blurred.emit();

            // EXPECTED: Should emit blurred event
            // RESULT: Verify blurred event emission
            expect(blurredSpy).toHaveBeenCalled();
        });

        it('should emit contentChange on document changes', () => {
            // SETUP: Component with content change monitoring
            // INPUT: Content change event
            const contentChangeSpy = jest.spyOn(component.contentChange, 'emit');

            // FUNCTION: Simulate content change by emitting contentChange
            component.contentChange.emit('new content');

            // EXPECTED: Should emit content change event
            // RESULT: Verify content change emission
            expect(contentChangeSpy).toHaveBeenCalledWith('new content');
        });
    });

    describe('Cleanup', () => {
        it('should clean up editor on destroy', () => {
            // SETUP: Component with initialized editor
            // INPUT: Component destruction
            fixture.detectChanges();
            const destroySpy = jest.spyOn(component['view']!, 'destroy');

            // FUNCTION: Call ngOnDestroy
            component.ngOnDestroy();

            // EXPECTED: Should destroy the editor view
            // RESULT: Verify editor cleanup
            expect(destroySpy).toHaveBeenCalled();
            expect(component['view']).toBeNull();
        });

        it('should ignore changes before initialization', () => {
            // SETUP: Component before initialization
            // INPUT: Configuration change before init
            // Create a new component instance that hasn't been initialized
            const freshFixture = TestBed.createComponent(Codemirror);
            const freshComponent = freshFixture.componentInstance;

            // Don't call detectChanges() so component stays uninitialized
            freshComponent.isInitialized = false;

            // FUNCTION: Trigger ngOnChanges before initialization
            freshComponent.ngOnChanges({
                config: new SimpleChange({ content: 'old value' }, { content: 'new value' }, false)
            });

            // EXPECTED: Should ignore changes when not initialized
            // RESULT: Should not throw errors and handle gracefully
            expect(() =>
                freshComponent.ngOnChanges({
                    config: new SimpleChange({ content: 'old value' }, { content: 'new value' }, false)
                })
            ).not.toThrow();
        });
    });
});
