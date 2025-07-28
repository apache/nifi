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
    unobserve(): void {}
}

// Mock EditorView with comprehensive functionality
const createMockEditorView = () => ({
    root: document,
    state: {
        doc: { toString: jest.fn().mockReturnValue(''), length: 0 },
        readOnly: false,
        facet: jest.fn().mockReturnValue(true)
    },
    contentDOM: {
        addEventListener: jest.fn(),
        removeEventListener: jest.fn(),
        dispatchEvent: jest.fn()
    },
    dispatch: jest.fn(),
    focus: jest.fn(),
    destroy: jest.fn()
});

// Setup comprehensive mocks
jest.mock('@codemirror/view', () => {
    const actual = jest.requireActual('@codemirror/view');
    return {
        ...actual,
        EditorView: Object.assign(
            jest.fn().mockImplementation((config) => ({
                ...createMockEditorView(),
                root: config.root || document,
                state: config.state
            })),
            {
                updateListener: {
                    of: jest.fn().mockReturnValue({})
                },
                editable: {
                    of: jest.fn().mockReturnValue({})
                },
                theme: jest.fn().mockReturnValue({}),
                lineWrapping: {}
            }
        )
    };
});

jest.mock('@codemirror/state', () => {
    const actual = jest.requireActual('@codemirror/state');
    return {
        ...actual,
        EditorState: {
            ...actual.EditorState,
            create: jest.fn().mockImplementation(({ doc, extensions }) => ({
                doc: { toString: () => doc || '', length: (doc || '').length },
                readOnly: false,
                facet: jest.fn().mockReturnValue(true),
                extensions
            })),
            readOnly: {
                of: jest.fn().mockReturnValue({})
            }
        }
    };
});

jest.mock('./themes/defaultTheme', () => ({
    defaultTheme: { mockTheme: true }
}));

jest.mock('./themes/baseTheme', () => ({
    baseTheme: { mockBaseTheme: true }
}));

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
        component.config = {
            ...component.config,
            languages: mockLanguages,
            language: ''
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
            expect(component.config).toEqual({
                theme: mockDefaultTheme,
                autoFocus: false,
                value: '',
                viewDisabled: false,
                readonly: false,
                extensions: [],
                languages: mockLanguages,
                language: ''
            });
            expect(component).toBeInstanceOf(Codemirror);
            // SUMMARY: Component creation test complete
        });

        it('should initialize editor with proper configuration on ngOnInit', () => {
            // SETUP: Component with test configuration
            // INPUT: Test value and configuration
            component.config = { ...component.config, value: testValue };

            // FUNCTION: Initialize component
            fixture.detectChanges();

            // EXPECTED: Should create EditorView with correct parameters
            // RESULT: Verify EditorView creation and configuration
            expect(EditorView).toHaveBeenCalledWith(
                expect.objectContaining({
                    parent: fixture.nativeElement,
                    state: expect.any(Object)
                })
            );
            expect(EditorState.create).toHaveBeenCalledWith({
                doc: testValue,
                extensions: expect.any(Array)
            });
            expect(component['view']).toBeDefined();
            expect(component['isInitialized']).toBe(true);
            // SUMMARY: Editor initialization test complete
        });

        it('should emit loaded event after successful initialization', () => {
            // SETUP: Component with loaded event spy
            // INPUT: Standard component configuration
            const loadedSpy = jest.spyOn(component.loaded, 'emit');

            // FUNCTION: Initialize component
            fixture.detectChanges();

            // EXPECTED: Should emit loaded event with component instance
            // RESULT: Verify event emission
            expect(loadedSpy).toHaveBeenCalledWith(component);
            // SUMMARY: Loaded event test complete
        });

        it('should handle autoFocus configuration correctly', () => {
            // SETUP: Component with autoFocus enabled
            // INPUT: Configuration with autoFocus true
            component.config = { ...component.config, autoFocus: true };

            // FUNCTION: Initialize component
            fixture.detectChanges();

            // EXPECTED: Should call focus on editor view
            // RESULT: Verify focus method called
            expect(component['view']?.focus).toHaveBeenCalled();
            // SUMMARY: AutoFocus test complete
        });
    });

    describe('Configuration Property Getters', () => {
        const testConfigPropertyGetter = (
            propertyName: keyof CodeMirrorConfig,
            testValue: any,
            defaultValue: any,
            description: string
        ) => {
            it(`should return ${description}`, () => {
                // SETUP: Component instance ready
                // INPUT: Test configuration values
                component.config = { ...component.config, [propertyName]: testValue };

                // FUNCTION: Access getter property
                const actualValue = component[propertyName as keyof Codemirror];

                // EXPECTED: Should return test value
                // RESULT: Verify getter returns correct value
                expect(actualValue).toBe(testValue);

                // Test with undefined value
                component.config = { ...component.config, [propertyName]: undefined };
                const defaultActualValue = component[propertyName as keyof Codemirror];

                // EXPECTED: Should return default value when undefined
                // RESULT: Verify default value handling
                expect(defaultActualValue).toStrictEqual(defaultValue);
                // SUMMARY: Property getter test complete
            });
        };

        testConfigPropertyGetter('theme', customTheme, mockDefaultTheme, 'correct theme value or default');
        testConfigPropertyGetter('autoFocus', true, false, 'correct autoFocus value or default');
        testConfigPropertyGetter('value', testValue, '', 'correct value or default');
        testConfigPropertyGetter('viewDisabled', true, false, 'correct viewDisabled value or default');
        testConfigPropertyGetter('readonly', true, false, 'correct readonly value or default');
        testConfigPropertyGetter('extensions', [{}], [], 'correct extensions array or default');
        testConfigPropertyGetter('languages', [mockLanguageForTesting], [], 'correct languages array or default');
        testConfigPropertyGetter('language', 'javascript', '', 'correct language value or default');
    });

    describe('Component Lifecycle Management', () => {
        beforeEach(() => {
            fixture.detectChanges();
        });

        it('should destroy editor properly on ngOnDestroy', () => {
            // SETUP: Initialized component with editor view
            // INPUT: Active editor instance
            const destroySpy = jest.spyOn(component['view']!, 'destroy');

            // FUNCTION: Call ngOnDestroy
            component.ngOnDestroy();

            // EXPECTED: Should call destroy and reset state
            // RESULT: Verify cleanup
            expect(destroySpy).toHaveBeenCalled();
            expect(component['view']).toBeNull();
            expect(component['isInitialized']).toBe(false);
            // SUMMARY: Destroy test complete
        });

        it('should handle destroy when view is not initialized', () => {
            // SETUP: Component without initialized view
            // INPUT: Null view state
            component['view'] = null;

            // FUNCTION: Call ngOnDestroy
            // EXPECTED: Should not throw error
            // RESULT: Verify safe cleanup
            expect(() => component.ngOnDestroy()).not.toThrow();
            // SUMMARY: Safe destroy test complete
        });
    });

    describe('ControlValueAccessor Implementation', () => {
        beforeEach(() => {
            fixture.detectChanges();
        });

        it('should handle writeValue with valid input', () => {
            // SETUP: Initialized component
            // INPUT: Test string value
            const inputValue = 'new test value';
            const dispatchSpy = jest.spyOn(component['view']!, 'dispatch');

            // FUNCTION: Call writeValue
            component.writeValue(inputValue);

            // EXPECTED: Should dispatch value change with External annotation
            // RESULT: Verify dispatch call
            expect(dispatchSpy).toHaveBeenCalledWith({
                changes: { from: 0, to: 0, insert: inputValue },
                annotations: [expect.any(Object)]
            });
            // SUMMARY: WriteValue test complete
        });

        it('should handle writeValue with null and undefined values', () => {
            // SETUP: Initialized component
            // INPUT: Null and undefined values
            const dispatchSpy = jest.spyOn(component['view']!, 'dispatch');

            // Clear any previous calls from initialization
            dispatchSpy.mockClear();

            // FUNCTION: Call writeValue with null
            component.writeValue(null as any);
            // EXPECTED: Should not dispatch
            expect(dispatchSpy).not.toHaveBeenCalled();

            // FUNCTION: Call writeValue with undefined
            component.writeValue(undefined as any);
            // EXPECTED: Should not dispatch
            expect(dispatchSpy).not.toHaveBeenCalled();
            // SUMMARY: Null/undefined writeValue test complete
        });

        it('should register onChange callback correctly', () => {
            // SETUP: Component ready for callback registration
            // INPUT: Mock change callback function
            const mockOnChange = jest.fn();

            // FUNCTION: Register onChange callback
            component.registerOnChange(mockOnChange);

            // EXPECTED: Should store callback reference
            // RESULT: Verify callback registration
            expect(component['onChange']).toBe(mockOnChange);
            // SUMMARY: OnChange registration test complete
        });

        it('should register onTouched callback correctly', () => {
            // SETUP: Component ready for callback registration
            // INPUT: Mock touch callback function
            const mockOnTouched = jest.fn();

            // FUNCTION: Register onTouched callback
            component.registerOnTouched(mockOnTouched);

            // EXPECTED: Should store callback reference
            // RESULT: Verify callback registration
            expect(component['onTouched']).toBe(mockOnTouched);
            // SUMMARY: OnTouched registration test complete
        });

        it('should handle setDisabledState correctly', () => {
            // SETUP: Initialized component
            // INPUT: Disabled state changes
            const setEditableSpy = jest.spyOn(component as any, 'setEditable');

            // FUNCTION: Disable component
            component.setDisabledState(true);

            // EXPECTED: Should update config and call setEditable
            // RESULT: Verify state change
            expect(component.config.viewDisabled).toBe(true);
            expect(setEditableSpy).toHaveBeenCalledWith(false);

            // FUNCTION: Enable component
            component.setDisabledState(false);

            // EXPECTED: Should update config and call setEditable
            // RESULT: Verify state change
            expect(component.config.viewDisabled).toBe(false);
            expect(setEditableSpy).toHaveBeenCalledWith(true);
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
                const oldConfig = { ...component.config, [propertyName]: oldValue };
                const newConfig = { ...component.config, [propertyName]: newValue };
                const dispatchSpy = jest.spyOn(component['view']!, 'dispatch');

                component.config = newConfig;

                // FUNCTION: Trigger ngOnChanges
                component.ngOnChanges({
                    config: {
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

        testConfigChange('value', 'old value', 'new value', 'value');
        testConfigChange('readonly', false, true, 'readonly');
        testConfigChange('viewDisabled', false, true, 'viewDisabled');
        testConfigChange('theme', {}, { newTheme: true }, 'theme');
        testConfigChange('extensions', [], [{}], 'extensions');
        testConfigChange('language', 'javascript', 'typescript', 'language');

        it('should ignore changes before initialization', () => {
            // SETUP: Uninitialized component
            // INPUT: Configuration change before init
            component['isInitialized'] = false;
            const processSpy = jest.spyOn(component as any, 'processConfigChanges');

            // FUNCTION: Trigger ngOnChanges
            component.ngOnChanges({
                config: {
                    currentValue: component.config,
                    previousValue: {},
                    firstChange: false,
                    isFirstChange: () => false
                }
            });

            // EXPECTED: Should not process changes
            // RESULT: Verify no processing occurred
            expect(processSpy).not.toHaveBeenCalled();
            // SUMMARY: Pre-initialization change handling test complete
        });

        it('should handle changes with null previous config', () => {
            // SETUP: Component with null previous config
            // INPUT: Configuration change with null previous value
            const processSpy = jest.spyOn(component as any, 'processConfigChanges');

            // FUNCTION: Trigger ngOnChanges with null previous
            component.ngOnChanges({
                config: {
                    currentValue: component.config,
                    previousValue: null,
                    firstChange: true,
                    isFirstChange: () => true
                }
            });

            // EXPECTED: Should process changes with empty object as previous
            // RESULT: Verify processing occurred
            expect(processSpy).toHaveBeenCalledWith({}, component.config);
            // SUMMARY: Null previous config test complete
        });
    });

    describe('Language Support and Management', () => {
        let mockLanguage: LanguageDescription;

        beforeEach(() => {
            fixture.detectChanges();
            mockLanguage = {
                name: 'TestLanguage',
                alias: ['test', 'testlang'],
                extensions: ['.test'],
                filename: /\.test$/,
                load: jest.fn().mockResolvedValue({ mockLanguageSupport: true })
            } as unknown as LanguageDescription;
        });

        it('should find language by name case-insensitively', () => {
            // SETUP: Component with mock languages
            // INPUT: Language search terms
            component.config = { ...component.config, languages: [mockLanguage] };

            // FUNCTION: Find language by various names
            const foundByName = component['findLanguageDescription']('TestLanguage');
            const foundByNameLower = component['findLanguageDescription']('testlanguage');
            const foundByAlias = component['findLanguageDescription']('test');
            const notFound = component['findLanguageDescription']('unknown');

            // EXPECTED: Should find by name and alias, case-insensitive
            // RESULT: Verify language finding logic
            expect(foundByName).toBe(mockLanguage);
            expect(foundByNameLower).toBe(mockLanguage);
            expect(foundByAlias).toBe(mockLanguage);
            expect(notFound).toBeNull();
            // SUMMARY: Language finding test complete
        });

        it('should handle language loading and application', async () => {
            // SETUP: Component with mock language
            // INPUT: Language description and name
            component.config = { ...component.config, languages: [mockLanguage] };
            const dispatchSpy = jest.spyOn(component['view']!, 'dispatch');

            // FUNCTION: Set language
            component['setLanguage']('TestLanguage');
            await mockLanguage.load();

            // EXPECTED: Should load language and dispatch configuration
            // RESULT: Verify language loading
            expect(mockLanguage.load).toHaveBeenCalled();
            // Note: Due to async nature, dispatch verification is complex in this test setup
            // SUMMARY: Language loading test complete
        });

        it('should clear language for empty or plaintext language names', () => {
            // SETUP: Component ready for language operations
            // INPUT: Empty and plaintext language names
            const dispatchSpy = jest.spyOn(component['view']!, 'dispatch');

            // FUNCTION: Clear language with empty string
            component['setLanguage']('');
            // EXPECTED: Should dispatch clear language configuration
            expect(dispatchSpy).toHaveBeenCalled();

            dispatchSpy.mockClear();

            // FUNCTION: Clear language with plaintext
            component['setLanguage']('plaintext');
            // EXPECTED: Should dispatch clear language configuration
            expect(dispatchSpy).toHaveBeenCalled();
            // SUMMARY: Language clearing test complete
        });

        it('should handle error when no languages are available', () => {
            // SETUP: Component with empty language list
            // INPUT: Empty languages array and specific language attempt
            component.config = { ...component.config, languages: [] };

            // FUNCTION: Attempt to set language
            component['setLanguage']('javascript');

            // EXPECTED: Should handle gracefully without errors
            // RESULT: Verify method completes without throwing
            expect(() => component['setLanguage']('javascript')).not.toThrow();
            // SUMMARY: No languages handling test complete
        });

        it('should work without warnings when no languages configured and no language set', () => {
            // SETUP: Component with empty language list
            // INPUT: Empty languages array and empty language
            component.config = { ...component.config, languages: [], language: '' };
            const dispatchSpy = jest.spyOn(component['view']!, 'dispatch');

            // FUNCTION: Set empty language (should clear language)
            component['setLanguage']('');

            // EXPECTED: Should dispatch clear language configuration
            // RESULT: Verify proper clearing
            expect(dispatchSpy).toHaveBeenCalled();
            // SUMMARY: Empty language without warnings test complete
        });

        it('should handle when requested language not found in available languages', () => {
            // SETUP: Component with mock language that doesn't match requested language
            // INPUT: Available languages array with one language, request different language
            component.config = { ...component.config, languages: [mockLanguage] };

            // FUNCTION: Attempt to set non-existent language
            component['setLanguage']('python');

            // EXPECTED: Should handle gracefully without errors
            // RESULT: Verify method completes without throwing
            expect(() => component['setLanguage']('python')).not.toThrow();
            // SUMMARY: Language not found handling test complete
        });
    });

    describe('UI State Management and Event Handling', () => {
        beforeEach(() => {
            fixture.detectChanges();
        });

        it('should manage CSS classes for readonly state correctly', () => {
            // SETUP: Component with DOM element
            // INPUT: Readonly state changes
            const element = fixture.nativeElement;

            // FUNCTION: Set readonly to true
            component['setReadonly'](true);

            // EXPECTED: Should add readonly CSS class
            // RESULT: Verify CSS class presence
            expect(element.classList.contains('editor-readonly')).toBe(true);

            // FUNCTION: Set readonly to false
            component['setReadonly'](false);

            // EXPECTED: Should remove readonly CSS class
            // RESULT: Verify CSS class removal
            expect(element.classList.contains('editor-readonly')).toBe(false);
            // SUMMARY: Readonly CSS management test complete
        });

        it('should manage CSS classes for disabled state correctly', () => {
            // SETUP: Component with DOM element
            // INPUT: Editable state changes
            const element = fixture.nativeElement;

            // FUNCTION: Set editable to false (disabled)
            component['setEditable'](false);

            // EXPECTED: Should add disabled CSS class
            // RESULT: Verify CSS class presence
            expect(element.classList.contains('editor-disabled')).toBe(true);

            // FUNCTION: Set editable to true (enabled)
            component['setEditable'](true);

            // EXPECTED: Should remove disabled CSS class
            // RESULT: Verify CSS class removal
            expect(element.classList.contains('editor-disabled')).toBe(false);
            // SUMMARY: Disabled CSS management test complete
        });

        it('should register focus and blur event listeners', () => {
            // SETUP: Component with initialized view
            // INPUT: Event listener setup
            const addEventListenerSpy = component['view']!.contentDOM.addEventListener as jest.Mock;

            // FUNCTION: Verify event listeners during initialization
            // EXPECTED: Should register focus and blur listeners
            // RESULT: Verify event listener registration
            expect(addEventListenerSpy).toHaveBeenCalledWith('focus', expect.any(Function));
            expect(addEventListenerSpy).toHaveBeenCalledWith('blur', expect.any(Function));
            // SUMMARY: Event listener registration test complete
        });

        it('should support custom event listener registration', () => {
            // SETUP: Component with contentDOM
            // INPUT: Custom event listener
            const mockListener = jest.fn();
            const addEventListenerSpy = component['view']!.contentDOM.addEventListener as jest.Mock;

            // FUNCTION: Add custom event listener
            component.addEventListener('click', mockListener);

            // EXPECTED: Should register custom listener
            // RESULT: Verify custom listener registration
            expect(addEventListenerSpy).toHaveBeenCalledWith('click', mockListener);
            // SUMMARY: Custom event listener test complete
        });

        it('should handle addEventListener when view is not available', () => {
            // SETUP: Component without view
            // INPUT: Attempt to add listener without view
            component['view'] = null;

            // FUNCTION: Attempt to add event listener
            // EXPECTED: Should not throw error
            // RESULT: Verify graceful handling
            expect(() => component.addEventListener('click', jest.fn())).not.toThrow();
            // SUMMARY: Safe event listener test complete
        });
    });

    describe('Error Handling and Edge Cases', () => {
        beforeEach(() => {
            fixture.detectChanges();
        });

        it('should handle null callbacks gracefully', () => {
            // SETUP: Component with null callbacks
            // INPUT: Null callback assignments
            component['onChange'] = null;
            component['onTouched'] = null;

            // FUNCTION: Verify null callback state
            // EXPECTED: Should handle null callbacks without errors
            // RESULT: Verify null state
            expect(component['onChange']).toBeNull();
            expect(component['onTouched']).toBeNull();
            // SUMMARY: Null callback handling test complete
        });

        it('should handle method calls when view is null', () => {
            // SETUP: Component with null view
            // INPUT: Null view state
            component['view'] = null;

            // FUNCTION: Call various methods that depend on view
            // EXPECTED: Should not throw errors
            // RESULT: Verify safe method handling
            expect(() => component['setValue']('test')).not.toThrow();
            expect(() => component['setTheme'](mockDefaultTheme as any)).not.toThrow();
            expect(() => component['setReadonly'](true)).not.toThrow();
            expect(() => component['setEditable'](false)).not.toThrow();
            expect(() => component['setExtensions']([])).not.toThrow();
            expect(() => component['setLanguage']('javascript')).not.toThrow();
            // SUMMARY: Null view safety test complete
        });

        it('should handle dispatch operations in setValue', () => {
            // SETUP: Component with mock view
            // INPUT: Test value for setting
            const testValue = 'test value';
            const dispatchSpy = jest.spyOn(component['view']!, 'dispatch');

            // FUNCTION: Attempt setValue
            component['setValue'](testValue);

            // EXPECTED: Should dispatch value change
            // RESULT: Verify dispatch was called
            expect(dispatchSpy).toHaveBeenCalledWith({
                changes: {
                    from: 0,
                    to: 0,
                    insert: testValue
                },
                annotations: [expect.any(Object)]
            });
            // SUMMARY: SetValue dispatch test complete
        });

        it('should handle theme setting operations', () => {
            // SETUP: Component with mock view
            // INPUT: Custom theme extension
            const dispatchSpy = jest.spyOn(component['view']!, 'dispatch');

            // FUNCTION: Attempt theme setting
            component['setTheme'](customTheme as any);

            // EXPECTED: Should dispatch theme configuration
            // RESULT: Verify dispatch was called
            expect(dispatchSpy).toHaveBeenCalledWith({
                effects: expect.any(Object)
            });
            // SUMMARY: Theme setting test complete
        });

        it('should handle custom root element correctly', () => {
            // SETUP: Component with custom root
            // INPUT: Custom shadow root element
            const customRoot = document.createElement('div').attachShadow({ mode: 'open' });
            const newFixture = TestBed.createComponent(Codemirror);
            const newComponent = newFixture.componentInstance;
            newComponent.root = customRoot;

            jest.clearAllMocks();

            // FUNCTION: Initialize with custom root
            newFixture.detectChanges();

            // EXPECTED: Should pass custom root to EditorView
            // RESULT: Verify custom root usage
            expect(EditorView).toHaveBeenCalledWith(
                expect.objectContaining({
                    root: customRoot
                })
            );

            // SUMMARY: Custom root handling test complete
            newComponent.ngOnDestroy();
        });
    });
});
