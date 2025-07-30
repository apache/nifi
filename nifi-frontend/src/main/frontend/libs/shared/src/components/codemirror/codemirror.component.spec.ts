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
import { EditorView } from '@codemirror/view';
import { EditorState } from '@codemirror/state';
import { LanguageDescription } from '@codemirror/language';

// Mock implementation classes for CodeMirror dependencies
class MockMutationObserver {
    observe(): void {}
    disconnect(): void {}
    takeRecords(): any[] {
        return [];
    }
}

class MockResizeObserver {
    observe(): void {}
    disconnect(): void {}
}

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
    )
}));

// Mock EditorState for testing
jest.mock('@codemirror/state', () => ({
    EditorState: {
        create: jest.fn().mockReturnValue({ mockState: true }),
        readOnly: {
            of: jest.fn().mockReturnValue({ mockReadOnly: true })
        }
    },
    Annotation: {
        define: jest.fn().mockReturnValue({
            of: jest.fn().mockReturnValue({ mockAnnotation: true })
        })
    },
    Compartment: jest.fn().mockImplementation(() => ({
        of: jest.fn().mockReturnValue([]),
        reconfigure: jest.fn().mockReturnValue({ mockReconfigure: true })
    })),
    StateEffect: {
        reconfigure: {
            of: jest.fn().mockReturnValue({ mockStateEffect: true })
        }
    }
}));

// Mock default theme
jest.mock('./themes/defaultTheme', () => ({
    defaultTheme: { mockTheme: true }
}));

// Mock language data
jest.mock('@codemirror/language-data', () => ({
    languages: []
}));

/**
 * Test Suite for Codemirror Component using SIFERS approach:
 * S - Setup: Test environment configuration
 * I - Input: Test data and parameters
 * F - Function: Method being tested
 * E - Expected: Expected outcomes
 * R - Result: Actual test results
 * S - Summary: Cleanup and verification
 */
describe('Codemirror Component', () => {
    // SETUP: Test environment variables
    let component: Codemirror;
    let fixture: ComponentFixture<Codemirror>;

    // SETUP: Mock data and constants
    const mockDefaultTheme = { mockTheme: true };
    const mockLanguages: LanguageDescription[] = [];
    const testValue = 'test editor content';
    const customTheme = { customTheme: true };
    const mockLanguageForTesting = {
        name: 'TestLang',
        alias: ['test'],
        extensions: ['.test'],
        filename: /\.test$/,
        load: jest.fn().mockResolvedValue({ mockLanguageSupport: true })
    } as unknown as LanguageDescription;

    // SETUP: Global mocks and browser API setup
    beforeAll(() => {
        (window as any).MutationObserver = MockMutationObserver;
        (window as any).ResizeObserver = MockResizeObserver;

        Object.defineProperty(window, 'getSelection', {
            writable: true,
            value: jest.fn().mockImplementation(() => ({
                getRangeAt: jest.fn(),
                removeAllRanges: jest.fn(),
                addRange: jest.fn()
            }))
        });
    });

    // SETUP: Configure test environment before each test
    beforeEach(async () => {
        await TestBed.configureTestingModule({
            imports: [Codemirror]
        }).compileComponents();

        fixture = TestBed.createComponent(Codemirror);
        component = fixture.componentInstance;

        // Set up default config with languages
        component.settings = {
            ...component.settings,
            syntaxModes: mockLanguages,
            syntaxMode: ''
        };

        jest.clearAllMocks();
    });

    // SUMMARY: Cleanup after each test
    afterEach(() => {
        if (component['view']) {
            try {
                component['view'].destroy();
            } catch (error) {
                // Ignore cleanup errors in tests
            }
        }
        jest.clearAllMocks();
    });

    describe('Component Creation and Initialization', () => {
        it('should create component with default configuration successfully', () => {
            // SETUP: Component created in beforeEach
            // INPUT: No input needed for constructor test
            // FUNCTION: Component instantiation
            // EXPECTED: Component should be truthy with default config including new language properties
            // RESULT: Verify component instance and default properties
            expect(component).toBeTruthy();
            expect(component.settings).toEqual({
                appearance: mockDefaultTheme,
                focusOnInit: false,
                content: '',
                disabled: false,
                readOnly: false,
                plugins: [],
                syntaxModes: mockLanguages,
                syntaxMode: ''
            });
            expect(component).toBeInstanceOf(Codemirror);
            // SUMMARY: Component creation test complete
        });

        it('should initialize editor with proper configuration on ngOnInit', () => {
            // SETUP: Component with mock implementation
            // INPUT: Component initialization
            fixture.detectChanges();

            // EXPECTED: Should create editor view and emit ready event
            // RESULT: Verify editor initialization
            expect(EditorView).toHaveBeenCalled();
            expect(component.isInitialized).toBe(true);
            expect(component['view']).toBeDefined();
            // SUMMARY: Initialization test complete
        });

        it('should emit ready event after initialization', () => {
            // SETUP: Component with loaded event spy
            // INPUT: Component initialization
            const readySpy = jest.spyOn(component.ready, 'emit');

            // FUNCTION: Initialize component
            fixture.detectChanges();

            // EXPECTED: Should emit ready event with component instance
            // RESULT: Verify ready event emission
            expect(readySpy).toHaveBeenCalledWith(component);

            // SUMMARY: Ready event test complete
        });

        it('should handle focusOnInit configuration correctly', () => {
            // SETUP: Component with focusOnInit enabled
            // INPUT: Configuration with focusOnInit true
            component.settings = { ...component.settings, focusOnInit: true };

            // FUNCTION: Initialize component
            fixture.detectChanges();

            // EXPECTED: Should call focus on editor view
            // RESULT: Verify focus method called
            expect(component['view']?.focus).toHaveBeenCalled();
            // SUMMARY: AutoFocus test complete
        });
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
            // RESULT: Verify dispatch called with proper configuration
            expect(dispatchSpy).toHaveBeenCalled();
            // SUMMARY: Write value test complete
        });

        it('should register onChange and onTouched callbacks', () => {
            // SETUP: Component ready for callback registration
            // INPUT: Mock callback functions
            const onChangeMock = jest.fn();
            const onTouchedMock = jest.fn();

            // FUNCTION: Register callbacks
            component.registerOnChange(onChangeMock);
            component.registerOnTouched(onTouchedMock);

            // EXPECTED: Should store callback references
            // RESULT: Verify callbacks are stored
            expect(component.onChange).toBe(onChangeMock);
            expect(component.onTouched).toBe(onTouchedMock);
            // SUMMARY: Callback registration test complete
        });

        it('should handle disabled state changes', () => {
            // SETUP: Component with initialized editor
            // INPUT: Disabled state change
            const dispatchSpy = jest.spyOn(component['view']!, 'dispatch');

            // FUNCTION: Set disabled state
            component.setDisabledState(true);

            // EXPECTED: Should update settings and dispatch configuration
            // RESULT: Verify disabled state handling
            expect(component.settings.disabled).toBe(true);
            expect(dispatchSpy).toHaveBeenCalled();

            // SUMMARY: Disabled state test complete
        });
    });

    describe('Configuration Change Handling', () => {
        beforeEach(() => {
            fixture.detectChanges();
        });

        const testConfigChange = (
            propertyName: keyof CodeMirrorConfig,
            oldValue: any,
            newValue: any,
            description: string
        ) => {
            it(`should handle ${description} changes`, () => {
                // SETUP: Component with initial configuration
                // INPUT: Configuration change data
                const oldConfig = { ...component.settings, [propertyName]: oldValue };
                const newConfig = { ...component.settings, [propertyName]: newValue };
                const dispatchSpy = jest.spyOn(component['view']!, 'dispatch');

                component.settings = newConfig;

                // FUNCTION: Trigger ngOnChanges
                component.ngOnChanges({
                    settings: {
                        currentValue: newConfig,
                        previousValue: oldConfig,
                        firstChange: false,
                        isFirstChange: () => false
                    }
                });

                // EXPECTED: Should dispatch configuration change
                // RESULT: Verify dispatch occurred
                expect(dispatchSpy).toHaveBeenCalled();
                // SUMMARY: Configuration change test complete
            });
        };

        testConfigChange('content', 'old value', 'new value', 'content');
        testConfigChange('readOnly', false, true, 'readOnly');
        testConfigChange('disabled', false, true, 'disabled');
        testConfigChange('appearance', {}, { newTheme: true }, 'appearance');
        testConfigChange('plugins', [], [{}], 'plugins');
        testConfigChange('syntaxMode', 'javascript', 'typescript', 'syntaxMode');

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
                settings: {
                    currentValue: { content: 'new value' },
                    previousValue: { content: 'old value' },
                    firstChange: false,
                    isFirstChange: () => false
                }
            });

            // EXPECTED: Should not process changes when not initialized
            // RESULT: Verify component remains uninitialized
            expect(freshComponent.isInitialized).toBe(false);
            // SUMMARY: Pre-initialization change test complete
        });

        it('should ignore changes without settings property', () => {
            // SETUP: Component after initialization
            // INPUT: Changes object without settings property
            const dispatchSpy = jest.spyOn(component['view']!, 'dispatch');
            dispatchSpy.mockClear();

            // FUNCTION: Trigger ngOnChanges without settings
            component.ngOnChanges({
                otherProperty: {
                    currentValue: 'new value',
                    previousValue: 'old value',
                    firstChange: false,
                    isFirstChange: () => false
                }
            });

            // EXPECTED: Should not process changes
            // RESULT: Verify no dispatch occurred
            expect(dispatchSpy).not.toHaveBeenCalled();
            // SUMMARY: Non-settings change test complete
        });
    });

    describe('Language Configuration', () => {
        beforeEach(() => {
            fixture.detectChanges();
        });

        it('should handle language changes with valid language', async () => {
            // SETUP: Component with language support
            // INPUT: Configuration with valid language
            component.settings = {
                ...component.settings,
                syntaxModes: [mockLanguageForTesting],
                syntaxMode: 'TestLang'
            };
            const dispatchSpy = jest.spyOn(component['view']!, 'dispatch');

            // FUNCTION: Process language change
            await component['setLanguage']('TestLang');

            // EXPECTED: Should load and apply language
            // RESULT: Verify language loading
            expect(mockLanguageForTesting.load).toHaveBeenCalled();
            expect(dispatchSpy).toHaveBeenCalled();
            // SUMMARY: Language change test complete
        });

        it('should clear language when setting to plaintext', () => {
            // SETUP: Component with language support
            // INPUT: Plaintext language setting
            const dispatchSpy = jest.spyOn(component['view']!, 'dispatch');

            // FUNCTION: Set language to plaintext
            component['setLanguage']('plaintext');

            // EXPECTED: Should clear language configuration
            // RESULT: Verify language clearing
            expect(dispatchSpy).toHaveBeenCalled();
            // SUMMARY: Language clearing test complete
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

            // FUNCTION: Simulate focus by calling onTouched and emitting focused
            component.onTouched?.();
            component.focused.emit();

            // EXPECTED: Should emit focused event
            // RESULT: Verify focused event emission
            expect(focusedSpy).toHaveBeenCalled();
            // SUMMARY: Focus event test complete
        });

        it('should emit blurred event on editor blur', () => {
            // SETUP: Component with event listener setup
            // INPUT: Blur event simulation
            const blurredSpy = jest.spyOn(component.blurred, 'emit');

            // FUNCTION: Simulate blur by calling onTouched and emitting blurred
            component.onTouched?.();
            component.blurred.emit();

            // EXPECTED: Should emit blurred event
            // RESULT: Verify blurred event emission
            expect(blurredSpy).toHaveBeenCalled();
            // SUMMARY: Blur event test complete
        });
    });

    describe('Cleanup and Destruction', () => {
        it('should cleanup editor on ngOnDestroy', () => {
            // SETUP: Component with initialized editor
            // INPUT: Component destruction
            fixture.detectChanges();
            const destroySpy = jest.spyOn(component['view']!, 'destroy');

            // FUNCTION: Call ngOnDestroy
            component.ngOnDestroy();

            // EXPECTED: Should destroy editor and reset state
            // RESULT: Verify cleanup
            expect(destroySpy).toHaveBeenCalled();
            expect(component['view']).toBeNull();
            expect(component.isInitialized).toBe(false);
            // SUMMARY: Cleanup test complete
        });

        it('should handle destruction when editor is null', () => {
            // SETUP: Component without initialized editor
            // INPUT: Component destruction without editor
            component['view'] = null;

            // FUNCTION: Call ngOnDestroy
            // EXPECTED: Should not throw error
            // RESULT: Verify safe destruction
            expect(() => component.ngOnDestroy()).not.toThrow();
            // SUMMARY: Safe destruction test complete
        });
    });
});
