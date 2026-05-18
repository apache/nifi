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

import { TestBed } from '@angular/core/testing';
import { ElementRef, signal } from '@angular/core';
import { By } from '@angular/platform-browser';
import { MatIconTestingModule } from '@angular/material/icon/testing';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { MatSelect } from '@angular/material/select';

import { SearchableSelect } from './searchable-select.component';
import { SearchableSelectOption } from '../../types';

describe('SearchableSelect', () => {
    interface SetupOptions {
        multiple?: boolean;
        enableVirtualScrolling?: boolean;
        options?: SearchableSelectOption<string>[];
        searchString?: string;
        placeholder?: string;
        allowClear?: boolean;
        loadError?: boolean;
        loadErrorMessage?: string;
    }

    class MockResizeObserver {
        disconnect = vi.fn();
        observe = vi.fn();
        unobserve = vi.fn();
        constructor(_callback: ResizeObserverCallback) {
            // noop
        }
    }

    const originalResizeObserver = (globalThis as { ResizeObserver?: typeof ResizeObserver }).ResizeObserver;

    beforeAll(() => {
        (globalThis as { ResizeObserver?: typeof ResizeObserver }).ResizeObserver =
            MockResizeObserver as unknown as typeof ResizeObserver;
    });

    afterAll(() => {
        if (originalResizeObserver) {
            (globalThis as { ResizeObserver?: typeof ResizeObserver }).ResizeObserver = originalResizeObserver;
        } else {
            delete (globalThis as { ResizeObserver?: typeof ResizeObserver }).ResizeObserver;
        }
    });

    async function setup({
        multiple = false,
        enableVirtualScrolling = false,
        options = [],
        searchString = '',
        placeholder = 'Select...',
        allowClear = false,
        loadError = false,
        loadErrorMessage = ''
    }: SetupOptions = {}) {
        await TestBed.configureTestingModule({
            imports: [SearchableSelect, MatIconTestingModule, NoopAnimationsModule]
        }).compileComponents();

        const fixture = TestBed.createComponent(SearchableSelect<string>);
        const component = fixture.componentInstance;

        fixture.componentRef.setInput('multiple', multiple);
        fixture.componentRef.setInput('enableVirtualScrolling', enableVirtualScrolling);
        fixture.componentRef.setInput('options', options);
        fixture.componentRef.setInput('placeholder', placeholder);
        fixture.componentRef.setInput('allowClear', allowClear);
        fixture.componentRef.setInput('loadError', loadError);
        fixture.componentRef.setInput('loadErrorMessage', loadErrorMessage);

        component.registerOnChange(() => {
            /* noop default */
        });
        component.registerOnTouched(() => {
            /* noop */
        });

        component.searchString = searchString;

        fixture.detectChanges();
        fixture.detectChanges();

        return { fixture, component };
    }

    it('should create', async () => {
        const { component } = await setup();
        expect(component).toBeTruthy();
    });

    describe('Search Functionality', () => {
        const mockOptions: SearchableSelectOption<string>[] = [
            { value: 'one', label: 'one test' },
            { value: 'two', label: 'two test' },
            { value: 'three', label: 'three test' }
        ];

        it('should filter options by search string', async () => {
            const { component, fixture } = await setup({ options: mockOptions, searchString: 'o' });

            let searchMatches = component.filteredOptions().filter((f) => !f.hidden);
            expect(searchMatches).toHaveLength(2);

            component.searchString = 'test';
            fixture.detectChanges();
            searchMatches = component.filteredOptions().filter((f) => !f.hidden);
            expect(searchMatches).toHaveLength(3);

            component.searchString = 'one';
            fixture.detectChanges();
            searchMatches = component.filteredOptions().filter((f) => !f.hidden);
            expect(searchMatches).toHaveLength(1);
        });

        it('should be case insensitive', async () => {
            const { component } = await setup({ options: mockOptions, searchString: 'ONE' });
            const visible = component.filteredOptions().filter((o) => !o.hidden);
            expect(visible.length).toBe(1);
            expect(visible[0].value).toBe('one');
        });

        it('should show all options when search is cleared', async () => {
            const { component } = await setup({ options: mockOptions, searchString: 'one' });
            let visible = component.filteredOptions().filter((o) => !o.hidden);
            expect(visible.length).toBe(1);

            component.searchString = '';
            visible = component.filteredOptions().filter((o) => !o.hidden);
            expect(visible.length).toBe(mockOptions.length);
        });

        it('should handle special characters in search', async () => {
            const specialOptions = [
                { value: 'one', label: 'one_test ?' },
                { value: 'two', label: 'two test **' },
                { value: 'three', label: 'three test **' }
            ];

            const { component, fixture } = await setup({ options: specialOptions });

            component.searchString = 'one_test';
            fixture.detectChanges();
            let searchMatches = component.filteredOptions().filter((f) => !f.hidden);
            expect(searchMatches).toHaveLength(1);

            component.searchString = '**';
            fixture.detectChanges();
            searchMatches = component.filteredOptions().filter((f) => !f.hidden);
            expect(searchMatches).toHaveLength(2);

            component.searchString = '///';
            fixture.detectChanges();
            searchMatches = component.filteredOptions().filter((f) => !f.hidden);
            expect(searchMatches).toHaveLength(0);
        });

        it('should focus search input when panel opens', async () => {
            const { component, fixture } = await setup({ options: mockOptions, searchString: 'o' });

            component.selectionPanelToggled(true);
            component.select().open();
            fixture.detectChanges();
            await new Promise((r) => setTimeout(r, 0));
            fixture.detectChanges();

            const searchInput = component.searchInput();
            const overlayContainer = document.querySelector('.cdk-overlay-container');
            const clearSearchBtn = overlayContainer?.querySelector(
                'button[data-qa="searchable-select-clear-search-btn"]'
            ) as HTMLButtonElement | null;

            expect(searchInput.nativeElement.value).toBe('o');
            expect(searchInput.nativeElement.getAttribute('aria-label')).toBe('Search');
            expect(clearSearchBtn?.getAttribute('aria-label')).toBe('Clear search');
            expect(clearSearchBtn).toBeTruthy();
        });

        it('should clear search input when clear button is clicked', async () => {
            const { component, fixture } = await setup({ options: mockOptions, searchString: 'test' });

            component.selectionPanelToggled(true);
            component.select().open();
            fixture.detectChanges();
            await new Promise((r) => setTimeout(r, 0));
            fixture.detectChanges();

            const searchInput = component.searchInput();
            const overlayContainer = document.querySelector('.cdk-overlay-container');
            const clearSearchBtn = overlayContainer?.querySelector(
                'button[data-qa="searchable-select-clear-search-btn"]'
            ) as HTMLButtonElement | null;

            vi.spyOn(searchInput.nativeElement, 'focus');
            clearSearchBtn?.click();

            fixture.detectChanges();
            await new Promise((r) => setTimeout(r, 0));

            expect(searchInput.nativeElement.value).toBe('');
            expect(searchInput.nativeElement.focus).toHaveBeenCalled();
        });

        it('should update aria labels for accessibility', async () => {
            const { component, fixture } = await setup({
                options: mockOptions,
                searchString: 'test'
            });

            fixture.componentRef.setInput('a11ySearchLabel', 'Search Input Label');
            fixture.componentRef.setInput('a11yClearSearchLabel', 'Clear Search Button Label');
            component.selectionPanelToggled(true);
            component.select().open();

            fixture.detectChanges();
            await new Promise((r) => setTimeout(r, 0));
            fixture.detectChanges();

            const searchInput = component.searchInput();
            const overlayContainer = document.querySelector('.cdk-overlay-container');
            const clearSearchBtn = overlayContainer?.querySelector(
                'button[data-qa="searchable-select-clear-search-btn"]'
            ) as HTMLButtonElement | null;

            expect(searchInput.nativeElement.getAttribute('aria-label')).toBe('Search Input Label');
            if (clearSearchBtn) {
                expect(clearSearchBtn.getAttribute('aria-label')).toBe('Clear Search Button Label');
            }
        });
    });

    describe('Keyboard Navigation', () => {
        it('should handle tab navigation to clear button', async () => {
            const { component } = await setup();

            const tabEvent = {
                key: 'Tab',
                code: 'Tab',
                preventDefault: vi.fn(),
                stopPropagation: vi.fn()
            } as unknown as KeyboardEvent;

            component.clearSearchBtn = signal({
                focus: vi.fn()
            } as unknown as never) as never;

            component.onSearchInputKeydown(tabEvent);

            expect(tabEvent.preventDefault).toHaveBeenCalled();
            expect(tabEvent.stopPropagation).toHaveBeenCalled();
            expect(component.clearSearchBtn()?.focus).toHaveBeenCalled();
        });

        it('should handle space key events', async () => {
            const { component } = await setup();

            const spaceEvent = {
                key: 'A',
                code: 'Space',
                preventDefault: vi.fn(),
                stopPropagation: vi.fn()
            } as unknown as KeyboardEvent;

            component.onSearchInputKeydown(spaceEvent);
            expect(spaceEvent.stopPropagation).toHaveBeenCalled();
        });

        it('should handle page navigation keys', async () => {
            const { component } = await setup();

            const mockEvent = {
                key: 'PageDown',
                preventDefault: vi.fn(),
                stopPropagation: vi.fn(),
                stopImmediatePropagation: vi.fn()
            } as unknown as KeyboardEvent;

            component.onSearchInputKeydown(mockEvent);

            expect(mockEvent.preventDefault).toHaveBeenCalled();
            expect(mockEvent.stopPropagation).toHaveBeenCalled();
        });

        it('should initialize navigation state correctly', async () => {
            const { component } = await setup();

            expect(component['_activeOptionIndex']).toBe(-1);
            expect(component['_isNavigating']).toBe(false);
            expect(component['_allowNextValueChange']).toBe(false);
        });

        it('should reset navigation state on filter', async () => {
            const { component } = await setup();

            component['_activeOptionIndex'] = 2;
            component['_isNavigating'] = true;

            component['filter']('test');

            expect(component['_activeOptionIndex']).toBe(-1);
            expect(component['_isNavigating']).toBe(false);
        });
    });

    describe('Focus Management', () => {
        it('should handle focus and clear for keydown events', async () => {
            const { component } = await setup();

            component.searchString = 'test';
            component.searchInput = signal({
                nativeElement: { focus: vi.fn() }
            } as unknown as ElementRef<HTMLInputElement>) as never;

            const enterEvent = {
                type: 'keydown',
                code: 'Enter',
                stopPropagation: vi.fn(),
                preventDefault: vi.fn()
            } as unknown as KeyboardEvent;

            component.focusSearchAndOrClear(enterEvent);

            expect(enterEvent.stopPropagation).toHaveBeenCalled();
            expect(enterEvent.preventDefault).toHaveBeenCalled();
        });

        it('should handle focus and clear for click events', async () => {
            const { component } = await setup();

            component.searchString = 'test';
            component.searchInput = signal({
                nativeElement: { focus: vi.fn() }
            } as unknown as ElementRef<HTMLInputElement>) as never;

            const clickEvent = {
                type: 'click',
                stopPropagation: vi.fn(),
                preventDefault: vi.fn()
            } as unknown as MouseEvent;

            component.focusSearchAndOrClear(clickEvent);

            expect(clickEvent.stopPropagation).toHaveBeenCalled();
            expect(clickEvent.preventDefault).toHaveBeenCalled();
            expect(component.searchInput().nativeElement.focus).toHaveBeenCalled();
            expect(component.searchString).toBeNull();
        });
    });

    describe('Option Display', () => {
        it('should display option descriptions when provided', async () => {
            const optionsWithDescriptions = [
                { value: 'one', label: 'Option One', description: 'First option description' },
                { value: 'two', label: 'Option Two' },
                { value: 'three', label: 'Option Three', description: 'Third option description' }
            ];

            const { component, fixture } = await setup({ options: optionsWithDescriptions });

            component.filteredOptions.set(
                component.options().map((opt: SearchableSelectOption<string>) => ({ ...opt, hidden: false }))
            );
            fixture.detectChanges();

            const matSelect = fixture.debugElement.query(By.directive(MatSelect));
            expect(matSelect).toBeTruthy();

            matSelect.componentInstance.open();
            fixture.detectChanges();
            await fixture.whenStable();

            const options = document.querySelectorAll('multi-select-option');
            expect(options.length).toBe(3);

            const descriptions = document.querySelectorAll('.tertiary-color');
            expect(descriptions.length).toBeGreaterThanOrEqual(2);

            const descriptionTexts = Array.from(descriptions).map((el) => el.textContent?.trim());
            expect(descriptionTexts).toContain('First option description');
            expect(descriptionTexts).toContain('Third option description');

            matSelect.componentInstance.close();
            fixture.detectChanges();
        });
    });

    describe('Display Value', () => {
        const mockOptions = [
            { value: 'one', label: 'First Option', description: 'Description 1' },
            { value: 'two', label: 'Second Option', description: 'Description 2' },
            { value: 'three', label: 'Third Option', description: 'Description 3' }
        ];

        it('should return empty string when no value is selected', async () => {
            const { component } = await setup({ options: mockOptions });

            component.select = signal({ value: null } as unknown as MatSelect) as never;
            expect(component.displayValue).toBe('');
        });

        it('should display single selection label', async () => {
            const { component } = await setup({ options: mockOptions });

            component.select = signal({ value: 'two' } as unknown as MatSelect) as never;
            component['_virtualSelectedValues'] = ['two'];
            expect(component.displayValue).toBe('Second Option');
        });

        it('should display comma-separated labels for multiple selections', async () => {
            const { component } = await setup({ options: mockOptions, multiple: true });

            component.select = signal({ value: ['one', 'three'] } as unknown as MatSelect) as never;
            component['_virtualSelectedValues'] = ['one', 'three'];
            expect(component.displayValue).toBe('First Option, Third Option');
        });

        it('should handle invalid values gracefully', async () => {
            const { component } = await setup({ options: mockOptions, multiple: true });

            component.select = signal({ value: ['one', 'invalid', 'three'] } as unknown as MatSelect) as never;
            component['_virtualSelectedValues'] = ['one', 'three'];
            expect(component.displayValue).toBe('First Option, Third Option');
        });

        it('should return empty string for invalid single selection', async () => {
            const { component } = await setup({ options: mockOptions });

            component.select = signal({ value: 'invalid' } as unknown as MatSelect) as never;
            expect(component.displayValue).toBe('');
        });

        it('should return empty string when no value is selected (getDisplayValue)', async () => {
            const { component } = await setup({ options: mockOptions });

            component.select = signal({ value: null } as unknown as MatSelect) as never;
            expect(component.getDisplayValue()).toBe('');
        });

        it('should display only the label for single selection (getDisplayValue)', async () => {
            const { component } = await setup({ options: mockOptions });

            component.select = signal({ value: 'two' } as unknown as MatSelect) as never;
            component['_virtualSelectedValues'] = ['two'];
            expect(component.getDisplayValue()).toBe('Second Option');
        });

        it('should display comma-separated labels for multiple selections (getDisplayValue)', async () => {
            const { component } = await setup({ options: mockOptions, multiple: true });

            component.select = signal({ value: ['one', 'three'] } as unknown as MatSelect) as never;
            component['_virtualSelectedValues'] = ['one', 'three'];
            expect(component.getDisplayValue()).toBe('First Option, Third Option');
        });

        it('should handle invalid values gracefully (getDisplayValue)', async () => {
            const { component } = await setup({ options: mockOptions, multiple: true });

            component.select = signal({ value: ['one', 'invalid', 'three'] } as unknown as MatSelect) as never;
            component['_virtualSelectedValues'] = ['one', 'three'];
            expect(component.getDisplayValue()).toBe('First Option, Third Option');
        });

        it('should return empty string for invalid single selection (getDisplayValue)', async () => {
            const { component } = await setup({ options: mockOptions });

            component.select = signal({ value: 'invalid' } as unknown as MatSelect) as never;
            expect(component.getDisplayValue()).toBe('');
        });
    });

    describe('ControlValueAccessor', () => {
        const mockOptions: SearchableSelectOption<string>[] = [
            { value: 'one', label: 'one test' },
            { value: 'two', label: 'two test' },
            { value: 'three', label: 'three test' }
        ];

        it('should accept a single value via writeValue and expose it in matSelectValue', async () => {
            const { component, fixture } = await setup({ options: mockOptions });
            component.writeValue('two');
            fixture.detectChanges();
            expect(component.matSelectValue).toBe('two');
        });

        it('should accept an array of values via writeValue in multiple mode', async () => {
            const { component, fixture } = await setup({ multiple: true, options: mockOptions });
            component.writeValue(['one', 'three']);
            fixture.detectChanges();
            const value = component.matSelectValue as string[];
            expect(Array.isArray(value)).toBe(true);
            expect(value).toContain('one');
            expect(value).toContain('three');
        });

        it('should emit changes through the registered onChange callback', async () => {
            const { component, fixture } = await setup({ multiple: true, options: mockOptions });
            const onChange = vi.fn();
            component.registerOnChange(onChange);

            component.onValueChanged(['one', 'two'] as never);
            fixture.detectChanges();
            expect(onChange).toHaveBeenCalledWith(['one', 'two']);
        });

        it('should update disabled state from setDisabledState', async () => {
            const { component, fixture } = await setup();
            component.setDisabledState(true);
            fixture.detectChanges();
            expect(component.disabled).toBe(true);

            component.setDisabledState(false);
            fixture.detectChanges();
            expect(component.disabled).toBe(false);
        });
    });

    describe('Value Change Management', () => {
        interface DuplicateDetectionSetupOptions extends SetupOptions {
            initialSelectedValues?: string[];
        }

        async function setupDuplicateDetection({
            initialSelectedValues = ['option1', 'option2'],
            ...options
        }: DuplicateDetectionSetupOptions = {}) {
            const defaultOptions = [
                { value: 'option1', label: 'Option 1' },
                { value: 'option2', label: 'Option 2' },
                { value: 'option3', label: 'Option 3' }
            ];

            const { component, fixture } = await setup({
                multiple: true,
                enableVirtualScrolling: true,
                options: defaultOptions,
                ...options
            });

            await fixture.whenStable();
            component.writeValue(initialSelectedValues);

            const mockOnChange = vi.fn();
            component.onChange = mockOnChange;

            return { component, fixture, mockOnChange };
        }

        it('should handle duplicate detection for unselection', async () => {
            const { component, mockOnChange } = await setupDuplicateDetection();

            const eventWithDuplicate = ['option1', 'option2', 'option3', 'option3'];
            component.onValueChanged(eventWithDuplicate);

            expect(mockOnChange).not.toHaveBeenCalled();
        });

        it('should remove duplicated values and emit change when selection changes', async () => {
            const { component, mockOnChange } = await setupDuplicateDetection();

            const eventWithDuplicate = ['option1', 'option1', 'option3'];
            component.onValueChanged(eventWithDuplicate);

            expect(mockOnChange).toHaveBeenCalledWith(['option3']);
        });

        it('should handle selection without duplicates', async () => {
            const { component, mockOnChange } = await setupDuplicateDetection();

            const eventWithoutDuplicates = ['option1', 'option2', 'option3'];
            component.onValueChanged(eventWithoutDuplicates);

            expect(mockOnChange).toHaveBeenCalledWith(['option1', 'option2', 'option3']);
        });

        it('should handle multiple duplicates correctly', async () => {
            const { component, mockOnChange } = await setupDuplicateDetection();

            const eventWithMultipleDuplicates = ['option1', 'option1', 'option2', 'option3', 'option3', 'option4'];
            component.onValueChanged(eventWithMultipleDuplicates);

            expect(mockOnChange).toHaveBeenCalledWith(['option2', 'option4']);
        });

        it('should handle empty selection', async () => {
            const { component, mockOnChange } = await setupDuplicateDetection();

            const emptyEvent: string[] = [];
            component.onValueChanged(emptyEvent);

            expect(mockOnChange).toHaveBeenCalledWith([]);
        });

        it('should handle complete unselection through duplication', async () => {
            const { component, mockOnChange } = await setupDuplicateDetection({
                initialSelectedValues: ['option1']
            });

            const duplicateEvent = ['option1', 'option1'];
            component.onValueChanged(duplicateEvent);

            expect(mockOnChange).toHaveBeenCalledWith([]);
        });

        it('should manage permission-based value changes for single-select', async () => {
            const { component } = await setup({ multiple: false });

            component['_allowNextValueChange'] = true;
            expect(component['_allowNextValueChange']).toBe(true);

            component['_allowNextValueChange'] = false;
            expect(component['_allowNextValueChange']).toBe(false);
        });
    });

    describe('Virtual Scrolling - Ghost Deduplication and Emission Guard', () => {
        it('should dedupe by label when different values share the same label (ghost copies)', async () => {
            const options = [
                { value: 'roleA_1', label: 'ROLE_A' },
                { value: 'roleA_2', label: 'ROLE_A' },
                { value: 'roleB', label: 'ROLE_B' }
            ];

            const { component } = await setup({ multiple: true, enableVirtualScrolling: true, options });

            const mockOnChange = vi.fn();
            component.onChange = mockOnChange;

            component.onValueChanged(['roleA_1', 'roleA_2', 'roleB']);

            expect(mockOnChange).toHaveBeenCalledTimes(1);
            expect(mockOnChange).toHaveBeenCalledWith(['roleB']);
        });

        it('should not emit multiple times for a single onValueChanged call with duplicates', async () => {
            const options = [
                { value: 'option1', label: 'Option 1' },
                { value: 'option2', label: 'Option 2' },
                { value: 'option3', label: 'Option 3' }
            ];

            const { component } = await setup({ multiple: true, enableVirtualScrolling: true, options });

            const mockOnChange = vi.fn();
            component.onChange = mockOnChange;

            component.onValueChanged(['option1', 'option1', 'option3']);
            expect(mockOnChange).toHaveBeenCalledTimes(1);
            expect(mockOnChange).toHaveBeenCalledWith(['option3']);
        });

        it('should suppress duplicate emissions when the selection set does not change (order-insensitive)', async () => {
            const options = [
                { value: 'option1', label: 'Option 1' },
                { value: 'option2', label: 'Option 2' },
                { value: 'option3', label: 'Option 3' }
            ];

            const { component } = await setup({ multiple: true, enableVirtualScrolling: true, options });

            const mockOnChange = vi.fn();
            component.onChange = mockOnChange;

            component.onValueChanged(['option1', 'option2']);
            component.onValueChanged(['option2', 'option1']);

            expect(mockOnChange).toHaveBeenCalledTimes(1);
            expect(mockOnChange).toHaveBeenCalledWith(['option1', 'option2']);
        });

        it('should not register mat-select onChange in virtual mode to avoid duplicate emission paths', async () => {
            const { component } = await setup({ enableVirtualScrolling: true });

            const registerSpy = vi.fn();
            component.select = signal({ registerOnChange: registerSpy } as unknown as MatSelect) as never;

            const externalOnChange = vi.fn();
            component.registerOnChange(externalOnChange);

            expect(registerSpy).not.toHaveBeenCalled();
        });

        it('should not register mat-select onChange in non-virtual mode to avoid duplicate emission paths', async () => {
            const { component } = await setup({ enableVirtualScrolling: false });

            const registerSpy = vi.fn();
            component.select = signal({ registerOnChange: registerSpy } as unknown as MatSelect) as never;

            const externalOnChange = vi.fn();
            component.registerOnChange(externalOnChange);

            expect(registerSpy).not.toHaveBeenCalled();
        });
    });

    describe('Virtual Scrolling', () => {
        it('should track virtual selected values', async () => {
            const { component } = await setup({ enableVirtualScrolling: true });

            component['_virtualSelectedValues'] = ['opt1', 'opt2'];
            const selectedValues = component['getCurrentSelectedValues']();
            expect(selectedValues).toEqual(['opt1', 'opt2']);
        });

        it('should provide ghost options for virtual mode', async () => {
            const { component } = await setup({ enableVirtualScrolling: true });

            component['_virtualSelectedValues'] = ['opt1'];
            const ghostOptions = component.getGhostOptions();
            expect(ghostOptions.length).toBeGreaterThanOrEqual(0);
        });

        it('should calculate virtual scroll height', async () => {
            const { component, fixture } = await setup({ enableVirtualScrolling: true });

            fixture.componentRef.setInput('virtualScrollItemSize', 50);
            const height = component.getVirtualScrollHeight();

            expect(typeof height).toBe('string');
            expect(height.endsWith('px')).toBe(true);
        });

        it('should update display value signal', async () => {
            const { component } = await setup();

            component['updateDisplayValueSignal']();
            expect(typeof component['displayValueSignal']()).toBe('string');
        });

        it('should handle falsy values correctly (0, false, empty string)', async () => {
            const { component, fixture } = await setup();

            const numberComponent = component as unknown as SearchableSelect<number>;
            fixture.componentRef.setInput('options', [
                { value: 0, label: 'Zero Option' },
                { value: 1, label: 'One Option' }
            ]);
            fixture.detectChanges();
            numberComponent.select = signal({ value: 0 } as unknown as MatSelect) as never;
            (numberComponent as unknown as { _virtualSelectedValues: number[] })._virtualSelectedValues = [0];
            expect(numberComponent.getDisplayValue()).toBe('Zero Option');

            const booleanComponent = component as unknown as SearchableSelect<boolean>;
            fixture.componentRef.setInput('options', [
                { value: false, label: 'False Option' },
                { value: true, label: 'True Option' }
            ]);
            fixture.detectChanges();
            booleanComponent.select = signal({ value: false } as unknown as MatSelect) as never;
            (booleanComponent as unknown as { _virtualSelectedValues: boolean[] })._virtualSelectedValues = [false];
            expect(booleanComponent.getDisplayValue()).toBe('False Option');

            const stringComponent = component as SearchableSelect<string>;
            fixture.componentRef.setInput('options', [
                { value: '', label: 'Empty String Option' },
                { value: 'test', label: 'Test Option' }
            ]);
            fixture.detectChanges();
            stringComponent.select = signal({ value: '' } as unknown as MatSelect) as never;
            stringComponent['_virtualSelectedValues'] = [''];
            expect(stringComponent.getDisplayValue()).toBe('Empty String Option');
        });
    });

    describe('Emission Tracking Reset', () => {
        it('should reset emission tracking to allow same value to be emitted again', async () => {
            const options = [
                { value: 'option1', label: 'Option 1' },
                { value: 'option2', label: 'Option 2' }
            ];

            const { component } = await setup({ multiple: true, options });

            const mockOnChange = vi.fn();
            component.onChange = mockOnChange;

            component.onValueChanged(['option1']);
            expect(mockOnChange).toHaveBeenCalledTimes(1);
            expect(mockOnChange).toHaveBeenCalledWith(['option1']);

            component.onValueChanged(['option1']);
            expect(mockOnChange).toHaveBeenCalledTimes(1);

            component.resetEmissionTracking();

            component.onValueChanged(['option1']);
            expect(mockOnChange).toHaveBeenCalledTimes(2);
            expect(mockOnChange).toHaveBeenLastCalledWith(['option1']);
        });
    });

    describe('CSS Classes Support', () => {
        let component: SearchableSelect<string>;
        let fixture: Awaited<ReturnType<typeof setup>>['fixture'];

        beforeEach(async () => {
            const setupResult = await setup();
            component = setupResult.component;
            fixture = setupResult.fixture;

            fixture.componentRef.setInput('options', [
                { value: 'normal', label: 'Normal Option' },
                { value: 'styled', label: 'Styled Option', labelCssClasses: ['custom-class', 'another-class'] },
                { value: 'nil', label: 'Nil Option', labelCssClasses: ['neutral-color', 'unset'] }
            ]);
            component.filteredOptions.set(component.options().map((opt) => ({ ...opt, hidden: false })));
            fixture.detectChanges();
        });

        it('should apply CSS classes to options when provided', async () => {
            const matSelect = fixture.debugElement.query(By.directive(MatSelect));
            matSelect.componentInstance.open();
            fixture.detectChanges();
            await fixture.whenStable();

            const styledElements = document.querySelectorAll('.custom-class');
            expect(styledElements.length).toBeGreaterThan(0);

            const nilElements = document.querySelectorAll('.neutral-color.unset');
            expect(nilElements.length).toBeGreaterThan(0);

            matSelect.componentInstance.close();
            fixture.detectChanges();
        });

        it('should not apply CSS classes when not provided', async () => {
            const matSelect = fixture.debugElement.query(By.directive(MatSelect));
            matSelect.componentInstance.open();
            fixture.detectChanges();
            await fixture.whenStable();

            const optionElements = document.querySelectorAll(
                'multi-select-option [data-qa="searchable-select-option-label"]'
            );
            const normalOption = Array.from(optionElements).find((el) => el.textContent?.trim() === 'Normal Option');

            expect(normalOption).toBeTruthy();
            expect(normalOption?.classList.contains('custom-class')).toBe(false);
            expect(normalOption?.classList.contains('neutral-color')).toBe(false);

            matSelect.componentInstance.close();
            fixture.detectChanges();
        });
    });

    describe('Async Search Mode', () => {
        describe('selectedOptionCache', () => {
            it('should populate cache when options are set', async () => {
                const { component, fixture } = await setup();
                const options = [
                    { value: 'opt1', label: 'Option 1' },
                    { value: 'opt2', label: 'Option 2', svgIcon: 'test-icon' }
                ];

                fixture.componentRef.setInput('options', options);
                fixture.detectChanges();

                const cache = component['selectedOptionCache'];
                expect(cache.get('opt1')).toMatchObject({
                    value: 'opt1',
                    label: 'Option 1'
                });
                expect(cache.get('opt2')).toMatchObject({
                    value: 'opt2',
                    label: 'Option 2',
                    svgIcon: 'test-icon'
                });
            });

            it('should preserve cache across option updates', async () => {
                const { component, fixture } = await setup();

                fixture.componentRef.setInput('options', [
                    { value: 'opt1', label: 'Option 1' },
                    { value: 'opt2', label: 'Option 2' }
                ]);
                fixture.detectChanges();

                fixture.componentRef.setInput('options', [
                    { value: 'opt2', label: 'Option 2' },
                    { value: 'opt3', label: 'Option 3' }
                ]);
                fixture.detectChanges();

                const cache = component['selectedOptionCache'];
                expect(cache.get('opt1')).toBeDefined();
                expect(cache.get('opt2')).toBeDefined();
                expect(cache.get('opt3')).toBeDefined();
            });

            it('should handle options with all metadata fields', async () => {
                const { component, fixture } = await setup();
                const options = [
                    {
                        value: 'complex',
                        label: 'Complex Option',
                        description: 'A complex option with all fields',
                        svgIcon: 'complex-icon',
                        hidden: false
                    }
                ];

                fixture.componentRef.setInput('options', options);
                fixture.detectChanges();

                const cache = component['selectedOptionCache'];
                expect(cache.get('complex')).toMatchObject({
                    value: 'complex',
                    label: 'Complex Option',
                    description: 'A complex option with all fields',
                    svgIcon: 'complex-icon'
                });
            });
        });

        describe('Display Value with Cache Fallback', () => {
            it('should use cache for display value when asyncSearchEnabled and option not in current batch', async () => {
                const { component, fixture } = await setup({ multiple: false });

                fixture.componentRef.setInput('asyncSearchEnabled', true);
                fixture.detectChanges();

                fixture.componentRef.setInput('options', [
                    { value: 'opt1', label: 'Option 1' },
                    { value: 'opt2', label: 'Option 2' }
                ]);
                fixture.detectChanges();

                component.select = signal({ value: 'opt1' } as unknown as MatSelect) as never;

                fixture.componentRef.setInput('options', [
                    { value: 'opt3', label: 'Option 3' },
                    { value: 'opt4', label: 'Option 4' }
                ]);

                expect(component.getDisplayValue()).toBe('Option 1');
            });

            it('should use cache for multiple selected values not in current options', async () => {
                const { component, fixture } = await setup({ multiple: true });

                fixture.componentRef.setInput('asyncSearchEnabled', true);
                fixture.detectChanges();

                fixture.componentRef.setInput('options', [
                    { value: 'opt1', label: 'Option 1' },
                    { value: 'opt2', label: 'Option 2' },
                    { value: 'opt3', label: 'Option 3' }
                ]);
                fixture.detectChanges();

                component.select = signal({ value: ['opt1', 'opt2'] } as unknown as MatSelect) as never;

                fixture.componentRef.setInput('options', [
                    { value: 'opt2', label: 'Option 2' },
                    { value: 'opt4', label: 'Option 4' }
                ]);
                fixture.detectChanges();

                const displayValue = component.getDisplayValue();
                expect(displayValue).toBe('Option 1, Option 2');
            });

            it('should not use cache when asyncSearchEnabled is false', async () => {
                const { component, fixture } = await setup({ multiple: false });

                fixture.componentRef.setInput('asyncSearchEnabled', false);
                fixture.detectChanges();

                fixture.componentRef.setInput('options', [
                    { value: 'opt1', label: 'Option 1' },
                    { value: 'opt2', label: 'Option 2' }
                ]);
                fixture.detectChanges();

                component.select = signal({ value: 'opt1' } as unknown as MatSelect) as never;

                fixture.componentRef.setInput('options', [{ value: 'opt3', label: 'Option 3' }]);
                fixture.detectChanges();

                expect(component.getDisplayValue()).toBe('');
            });

            it('should fall back to string value when cache miss occurs', async () => {
                const { component, fixture } = await setup({ multiple: false });

                fixture.componentRef.setInput('asyncSearchEnabled', true);
                fixture.detectChanges();

                fixture.componentRef.setInput('options', []);

                component.select = signal({ value: 'unknown' } as unknown as MatSelect) as never;
                fixture.detectChanges();

                expect(component.getDisplayValue()).toBe('unknown');
            });
        });

        describe('Ghost Options for mat-select Consistency', () => {
            it('should create ghost options for selected values not in current options when asyncSearchEnabled', async () => {
                const { component, fixture } = await setup({ multiple: true, enableVirtualScrolling: true });

                fixture.componentRef.setInput('asyncSearchEnabled', true);
                fixture.detectChanges();

                fixture.componentRef.setInput('options', [
                    { value: 'opt1', label: 'Option 1' },
                    { value: 'opt2', label: 'Option 2' },
                    { value: 'opt3', label: 'Option 3' }
                ]);
                fixture.detectChanges();

                component['_virtualSelectedValues'] = ['opt1', 'opt2', 'opt3'];

                fixture.componentRef.setInput('options', [
                    { value: 'opt1', label: 'Option 1' },
                    { value: 'opt4', label: 'Option 4' }
                ]);
                fixture.detectChanges();

                const ghostOptions = component.getGhostOptions();

                const ghostValues = ghostOptions.map((ghost) => ghost.value);
                expect(ghostValues).toContain('opt2');
                expect(ghostValues).toContain('opt3');

                ghostOptions.forEach((ghost) => {
                    if (ghost.value === 'opt2' || ghost.value === 'opt3') {
                        expect(ghost.hidden).toBe(true);
                    }
                });
            });

            it('should not create ghost options when asyncSearchEnabled is false', async () => {
                const { component, fixture } = await setup({ multiple: true });

                fixture.componentRef.setInput('asyncSearchEnabled', false);
                fixture.detectChanges();

                fixture.componentRef.setInput('options', [{ value: 'opt1', label: 'Option 1' }]);
                component['_virtualSelectedValues'] = ['opt1', 'opt2'];

                const ghostOptions = component.getGhostOptions();
                const ghostValues = ghostOptions.map((ghost) => ghost.value);
                expect(ghostValues).not.toContain('opt2');
            });

            it('should use cache labels for ghost options', async () => {
                const { component, fixture } = await setup({ multiple: true, enableVirtualScrolling: true });

                fixture.componentRef.setInput('asyncSearchEnabled', true);
                fixture.detectChanges();

                fixture.componentRef.setInput('options', [{ value: 'cached', label: 'Cached Option' }]);
                fixture.detectChanges();

                component['_virtualSelectedValues'] = ['cached'];

                fixture.componentRef.setInput('options', [{ value: 'other', label: 'Other Option' }]);
                fixture.detectChanges();

                const ghostOptions = component.getGhostOptions();
                const cachedGhost = ghostOptions.find((ghost) => ghost.value === 'cached');

                expect(cachedGhost).toBeDefined();
                expect(cachedGhost!.label).toBe('Cached Option');
                expect(cachedGhost!.hidden).toBe(true);
            });
        });

        describe('Panel Close Behavior', () => {
            it('should reset search and emit searchChange when panel closes in async mode', async () => {
                const { component, fixture } = await setup();

                fixture.componentRef.setInput('asyncSearchEnabled', true);
                fixture.detectChanges();
                const searchChangeSpy = vi.spyOn(component.searchChange, 'emit');

                component['_searchString'] = 'test search';

                component.selectionPanelToggled(false);

                expect(component.searchString).toBeNull();
                expect(searchChangeSpy).toHaveBeenCalledWith('');
            });

            it('should reset search in non-async mode when panel closes', async () => {
                const { component, fixture } = await setup();

                fixture.componentRef.setInput('asyncSearchEnabled', false);
                fixture.detectChanges();

                component.searchString = 'test search';
                expect(component.searchString).toBe('test search');

                component.selectionPanelToggled(false);

                expect(component.searchString).toBeNull();
            });

            it('should not affect behavior when panel opens', async () => {
                const { component, fixture } = await setup();

                fixture.componentRef.setInput('asyncSearchEnabled', true);
                fixture.detectChanges();
                const searchChangeSpy = vi.spyOn(component.searchChange, 'emit');

                component.searchString = 'initial search';

                component.selectionPanelToggled(true);

                expect(searchChangeSpy).not.toHaveBeenCalledWith('');
                expect(component.searchString).toBe('initial search');
            });
        });

        describe('Filtering Behavior', () => {
            it('should skip internal filtering when asyncSearchEnabled is true', async () => {
                const { component, fixture } = await setup({ enableVirtualScrolling: true });

                fixture.componentRef.setInput('asyncSearchEnabled', true);
                fixture.detectChanges();
                fixture.componentRef.setInput('options', [
                    { value: 'apple', label: 'Apple' },
                    { value: 'banana', label: 'Banana' },
                    { value: 'cherry', label: 'Cherry' }
                ]);
                fixture.detectChanges();

                component.searchString = 'app';

                const visibleOptions = component.getVisibleOptions();
                expect(visibleOptions).toHaveLength(3);
                expect(visibleOptions.map((o) => o.value)).toEqual(['apple', 'banana', 'cherry']);
            });

            it('should preserve search string in async mode for clear button visibility', async () => {
                const { component, fixture } = await setup();

                fixture.componentRef.setInput('asyncSearchEnabled', true);
                fixture.detectChanges();

                component.searchString = 'test search';

                expect(component.searchString).toBe('test search');
            });

            it('should filter options in non-async mode', async () => {
                const { component, fixture } = await setup();

                fixture.componentRef.setInput('asyncSearchEnabled', false);
                fixture.detectChanges();
                fixture.componentRef.setInput('options', [
                    { value: 'apple', label: 'Apple' },
                    { value: 'banana', label: 'Banana' },
                    { value: 'cherry', label: 'Cherry' }
                ]);
                fixture.detectChanges();

                component.searchString = 'app';

                const visibleOptions = component.getVisibleOptions();
                expect(visibleOptions).toHaveLength(1);
                expect(visibleOptions[0].value).toBe('apple');
            });

            it('should emit searchChange when search length meets minimum in async mode', async () => {
                const { component, fixture } = await setup();

                fixture.componentRef.setInput('asyncSearchEnabled', true);
                fixture.detectChanges();
                fixture.componentRef.setInput('minSearchLength', 2);

                const mockSelect = {
                    panelOpen: true
                } as unknown as MatSelect;
                component.select = signal(mockSelect) as never;

                const searchChangeSpy = vi.spyOn(component.searchChange, 'emit');

                component.searchString = 'a';
                await new Promise((resolve) => setTimeout(resolve, 350));
                expect(searchChangeSpy).not.toHaveBeenCalledWith('a');

                component.searchString = 'ab';
                await new Promise((resolve) => setTimeout(resolve, 350));
                expect(searchChangeSpy).toHaveBeenCalledWith('ab');
            });
        });

        describe('Virtual Scrolling with Async Search', () => {
            it('should include "more options" footer when asyncSearchOptionsHaveMore is true', async () => {
                const { component, fixture } = await setup({ enableVirtualScrolling: true });

                fixture.componentRef.setInput('asyncSearchEnabled', true);
                fixture.detectChanges();
                fixture.componentRef.setInput('asyncSearchOptionsHaveMore', true);
                fixture.componentRef.setInput('options', [
                    { value: 'opt1', label: 'Option 1' },
                    { value: 'opt2', label: 'Option 2' }
                ]);
                fixture.detectChanges();

                const virtualItems = component.getVirtualVisibleItems();

                expect(virtualItems).toHaveLength(3);
                expect(virtualItems[2]).toEqual({ __kind: 'more' });
            });

            it('should not include footer when asyncSearchOptionsHaveMore is false', async () => {
                const { component, fixture } = await setup({ enableVirtualScrolling: true });

                fixture.componentRef.setInput('asyncSearchEnabled', true);
                fixture.detectChanges();
                fixture.componentRef.setInput('asyncSearchOptionsHaveMore', false);
                fixture.componentRef.setInput('options', [
                    { value: 'opt1', label: 'Option 1' },
                    { value: 'opt2', label: 'Option 2' }
                ]);
                fixture.detectChanges();

                const virtualItems = component.getVirtualVisibleItems();

                expect(virtualItems).toHaveLength(2);
                expect(virtualItems.every((item) => 'value' in item)).toBe(true);
            });

            it('should not include footer when options are empty even if hasMore is true', async () => {
                const { component, fixture } = await setup({ enableVirtualScrolling: true });

                fixture.componentRef.setInput('asyncSearchEnabled', true);
                fixture.detectChanges();
                fixture.componentRef.setInput('asyncSearchOptionsHaveMore', true);
                fixture.componentRef.setInput('options', []);

                const virtualItems = component.getVirtualVisibleItems();

                expect(virtualItems).toHaveLength(0);
            });
        });

        describe('updateDisplayValueSignal with Async Cache', () => {
            it('should update display value signal using cache in async mode', async () => {
                const { component, fixture } = await setup({ multiple: false });

                fixture.componentRef.setInput('asyncSearchEnabled', true);
                fixture.detectChanges();

                fixture.componentRef.setInput('options', [{ value: 'test', label: 'Test Option' }]);
                fixture.detectChanges();
                component.select = signal({ value: 'test' } as unknown as MatSelect) as never;

                fixture.componentRef.setInput('options', [{ value: 'other', label: 'Other Option' }]);
                fixture.detectChanges();

                component['updateDisplayValueSignal']();

                expect(component['displayValueSignal']()).toBe('Test Option');
            });
        });

        describe('Option Selection Panel Behavior', () => {
            describe('onValueChanged', () => {
                it('should close panel after selection in single-select mode', async () => {
                    const { component } = await setup({
                        multiple: false,
                        options: [
                            { value: 'opt1', label: 'Option 1' },
                            { value: 'opt2', label: 'Option 2' }
                        ]
                    });

                    const mockClose = vi.fn();
                    const mockWriteValue = vi.fn();
                    const mockOnChange = vi.fn();

                    component.select = signal({
                        close: mockClose,
                        panelOpen: true,
                        writeValue: mockWriteValue
                    } as unknown as MatSelect) as never;

                    component.onChange = mockOnChange;

                    component.onValueChanged('opt1');

                    expect(mockClose).toHaveBeenCalled();
                });

                it('should keep panel open after selection in multi-select mode', async () => {
                    const { component } = await setup({ multiple: true, enableVirtualScrolling: false });

                    const mockClose = vi.fn();
                    const mockOnChange = vi.fn();

                    component.select = signal({
                        close: mockClose,
                        panelOpen: true,
                        writeValue: vi.fn()
                    } as unknown as MatSelect) as never;

                    component.onChange = mockOnChange;

                    component['_virtualSelectedValues'] = ['opt1'];

                    component.onValueChanged(['opt1', 'opt2']);

                    expect(mockClose).not.toHaveBeenCalled();
                });

                it('should update display value signal after selection when value changes', async () => {
                    const { component } = await setup({
                        multiple: false,
                        options: [
                            { value: 'opt1', label: 'Option 1' },
                            { value: 'opt2', label: 'Option 2' }
                        ]
                    });

                    const mockClose = vi.fn();
                    const mockWriteValue = vi.fn();
                    const mockOnChange = vi.fn();

                    component.select = signal({
                        close: mockClose,
                        panelOpen: true,
                        writeValue: mockWriteValue
                    } as unknown as MatSelect) as never;

                    component.onChange = mockOnChange;

                    const updateDisplayValueSignalSpy = vi.spyOn(
                        component as unknown as { updateDisplayValueSignal: () => void },
                        'updateDisplayValueSignal'
                    );

                    component['_virtualSelectedValues'] = [];
                    component['_allowNextValueChange'] = true;
                    component.onValueChanged('opt1');

                    expect(updateDisplayValueSignalSpy).toHaveBeenCalled();
                });

                it('should handle virtual scrolling mode properly in multi-select', async () => {
                    const { component } = await setup({ multiple: true, enableVirtualScrolling: true });

                    const mockClose = vi.fn();
                    const mockOnChange = vi.fn();

                    component.select = signal({
                        close: mockClose,
                        panelOpen: true,
                        writeValue: vi.fn()
                    } as unknown as MatSelect) as never;

                    component.onChange = mockOnChange;

                    component['_virtualSelectedValues'] = ['opt1'];

                    component.onValueChanged(['opt1', 'opt2']);

                    expect(mockClose).not.toHaveBeenCalled();
                });
            });
        });

        describe('Clear Selection Functionality', () => {
            const mockOptions: SearchableSelectOption<string>[] = [
                { value: 'opt1', label: 'Option 1' },
                { value: 'opt2', label: 'Option 2' },
                { value: 'opt3', label: 'Option 3' }
            ];

            describe('shouldShowClearButton', () => {
                it('should return true when allowClear is true and there is a selected value', async () => {
                    const { component, fixture } = await setup({
                        enableVirtualScrolling: true,
                        options: mockOptions
                    });

                    fixture.componentRef.setInput('allowClear', true);
                    component['_virtualSelectedValues'] = ['opt1'];
                    fixture.detectChanges();

                    expect(component.shouldShowClearButton()).toBe(true);
                });

                it('should return false when allowClear is false', async () => {
                    const { component, fixture } = await setup({
                        enableVirtualScrolling: true,
                        options: mockOptions
                    });

                    fixture.componentRef.setInput('allowClear', false);
                    component['_virtualSelectedValues'] = ['opt1'];
                    fixture.detectChanges();

                    expect(component.shouldShowClearButton()).toBe(false);
                });

                it('should return false when there is no selected value', async () => {
                    const { component, fixture } = await setup({
                        enableVirtualScrolling: true,
                        options: mockOptions
                    });

                    fixture.componentRef.setInput('allowClear', true);
                    component['_virtualSelectedValues'] = [];
                    fixture.detectChanges();

                    expect(component.shouldShowClearButton()).toBe(false);
                });

                it('should return true for multi-select with selected values', async () => {
                    const { component, fixture } = await setup({
                        multiple: true,
                        enableVirtualScrolling: true,
                        options: mockOptions
                    });

                    fixture.componentRef.setInput('allowClear', true);
                    component['_virtualSelectedValues'] = ['opt1', 'opt2'];
                    fixture.detectChanges();

                    expect(component.shouldShowClearButton()).toBe(true);
                });

                it('should work with non-virtual scrolling mode', async () => {
                    const { component, fixture } = await setup({
                        enableVirtualScrolling: false,
                        options: mockOptions
                    });

                    fixture.componentRef.setInput('allowClear', true);

                    component.select = signal({
                        value: 'opt1'
                    } as unknown as MatSelect) as never;

                    fixture.detectChanges();

                    expect(component.shouldShowClearButton()).toBe(true);
                });
            });

            describe('clearSelection', () => {
                it('should clear selection in single-select mode with virtual scrolling', async () => {
                    const { component } = await setup({
                        enableVirtualScrolling: true,
                        options: mockOptions
                    });

                    const mockOnChange = vi.fn();
                    component.onChange = mockOnChange;
                    component['_virtualSelectedValues'] = ['opt1'];
                    component['_lastEmittedValue'] = 'opt1';

                    const mockEvent = new Event('click');
                    vi.spyOn(mockEvent, 'stopPropagation');
                    vi.spyOn(mockEvent, 'preventDefault');

                    component.clearSelection(mockEvent);

                    expect(mockEvent.stopPropagation).toHaveBeenCalled();
                    expect(mockEvent.preventDefault).toHaveBeenCalled();
                    expect(component['_virtualSelectedValues']).toEqual([]);
                    expect(mockOnChange).toHaveBeenCalledWith(null);
                });

                it('should clear selection in multi-select mode with virtual scrolling', async () => {
                    const { component } = await setup({
                        multiple: true,
                        enableVirtualScrolling: true,
                        options: mockOptions
                    });

                    const mockOnChange = vi.fn();
                    component.onChange = mockOnChange;
                    component['_virtualSelectedValues'] = ['opt1', 'opt2'];
                    component['_lastEmittedValue'] = ['opt1', 'opt2'];

                    const mockEvent = new Event('click');
                    component.clearSelection(mockEvent);

                    expect(component['_virtualSelectedValues']).toEqual([]);
                    expect(mockOnChange).toHaveBeenCalledWith([]);
                });

                it('should clear selection in single-select mode without virtual scrolling', async () => {
                    const { component } = await setup({
                        enableVirtualScrolling: false,
                        options: mockOptions
                    });

                    const mockOnChange = vi.fn();
                    const mockWriteValue = vi.fn();

                    component.onChange = mockOnChange;
                    component.select = signal({
                        writeValue: mockWriteValue,
                        value: 'opt1'
                    } as unknown as MatSelect) as never;

                    component['_lastIntentionalValue'] = 'opt1';
                    component['_lastEmittedValue'] = 'opt1';

                    const mockEvent = new Event('click');
                    component.clearSelection(mockEvent);

                    expect(mockWriteValue).toHaveBeenCalledWith(null);
                    expect(component['_lastIntentionalValue']).toBeNull();
                    expect(mockOnChange).toHaveBeenCalledWith(null);
                });

                it('should clear selection in multi-select mode without virtual scrolling', async () => {
                    const { component } = await setup({
                        multiple: true,
                        enableVirtualScrolling: false,
                        options: mockOptions
                    });

                    const mockOnChange = vi.fn();
                    const mockWriteValue = vi.fn();

                    component.onChange = mockOnChange;
                    component.select = signal({
                        writeValue: mockWriteValue,
                        value: ['opt1', 'opt2']
                    } as unknown as MatSelect) as never;

                    component['_lastEmittedValue'] = ['opt1', 'opt2'];

                    const mockEvent = new Event('click');
                    component.clearSelection(mockEvent);

                    expect(mockWriteValue).toHaveBeenCalledWith([]);
                    expect(mockOnChange).toHaveBeenCalledWith([]);
                });

                it('should update display value signal after clearing', async () => {
                    const { component } = await setup({
                        enableVirtualScrolling: false,
                        options: mockOptions
                    });

                    const mockOnChange = vi.fn();
                    const mockWriteValue = vi.fn();

                    component.onChange = mockOnChange;
                    component.select = signal({
                        writeValue: mockWriteValue,
                        value: 'opt1'
                    } as unknown as MatSelect) as never;

                    const updateDisplayValueSignalSpy = vi.spyOn(
                        component as unknown as { updateDisplayValueSignal: () => void },
                        'updateDisplayValueSignal'
                    );

                    const mockEvent = new Event('click');
                    component.clearSelection(mockEvent);

                    expect(updateDisplayValueSignalSpy).toHaveBeenCalled();
                });

                it('should stop propagation and prevent default', async () => {
                    const { component } = await setup({
                        enableVirtualScrolling: true,
                        options: mockOptions
                    });

                    const mockOnChange = vi.fn();
                    component.registerOnChange(mockOnChange);

                    const mockEvent = new Event('click');
                    const stopPropagationSpy = vi.spyOn(mockEvent, 'stopPropagation');
                    const preventDefaultSpy = vi.spyOn(mockEvent, 'preventDefault');

                    component.clearSelection(mockEvent);

                    expect(stopPropagationSpy).toHaveBeenCalled();
                    expect(preventDefaultSpy).toHaveBeenCalled();
                });
            });

            describe('onClearSelectionKeydown', () => {
                it('should trigger clearSelection when Enter key is pressed', async () => {
                    const { component } = await setup({ options: mockOptions });

                    const mockOnChange = vi.fn();
                    component.registerOnChange(mockOnChange);

                    const clearSelectionSpy = vi.spyOn(component, 'clearSelection');
                    const mockEvent = new KeyboardEvent('keydown', { code: 'Enter' });
                    vi.spyOn(mockEvent, 'stopPropagation');
                    vi.spyOn(mockEvent, 'preventDefault');

                    component.onClearSelectionKeydown(mockEvent);

                    expect(mockEvent.stopPropagation).toHaveBeenCalled();
                    expect(mockEvent.preventDefault).toHaveBeenCalled();
                    expect(clearSelectionSpy).toHaveBeenCalledWith(mockEvent);
                });

                it('should trigger clearSelection when Space key is pressed', async () => {
                    const { component } = await setup({ options: mockOptions });

                    const mockOnChange = vi.fn();
                    component.registerOnChange(mockOnChange);

                    const clearSelectionSpy = vi.spyOn(component, 'clearSelection');
                    const mockEvent = new KeyboardEvent('keydown', { code: 'Space' });
                    vi.spyOn(mockEvent, 'stopPropagation');
                    vi.spyOn(mockEvent, 'preventDefault');

                    component.onClearSelectionKeydown(mockEvent);

                    expect(mockEvent.stopPropagation).toHaveBeenCalled();
                    expect(mockEvent.preventDefault).toHaveBeenCalled();
                    expect(clearSelectionSpy).toHaveBeenCalledWith(mockEvent);
                });

                it('should not trigger clearSelection for other keys', async () => {
                    const { component } = await setup({ options: mockOptions });

                    const clearSelectionSpy = vi.spyOn(component, 'clearSelection');
                    const mockEvent = new KeyboardEvent('keydown', { code: 'KeyA' });
                    vi.spyOn(mockEvent, 'stopPropagation');
                    vi.spyOn(mockEvent, 'preventDefault');

                    component.onClearSelectionKeydown(mockEvent);

                    expect(mockEvent.stopPropagation).not.toHaveBeenCalled();
                    expect(mockEvent.preventDefault).not.toHaveBeenCalled();
                    expect(clearSelectionSpy).not.toHaveBeenCalled();
                });

                it('should not trigger clearSelection for Escape key', async () => {
                    const { component } = await setup({ options: mockOptions });

                    const clearSelectionSpy = vi.spyOn(component, 'clearSelection');
                    const mockEvent = new KeyboardEvent('keydown', { code: 'Escape' });

                    component.onClearSelectionKeydown(mockEvent);

                    expect(clearSelectionSpy).not.toHaveBeenCalled();
                });

                it('should not trigger clearSelection for Tab key', async () => {
                    const { component } = await setup({ options: mockOptions });

                    const clearSelectionSpy = vi.spyOn(component, 'clearSelection');
                    const mockEvent = new KeyboardEvent('keydown', { code: 'Tab' });

                    component.onClearSelectionKeydown(mockEvent);

                    expect(clearSelectionSpy).not.toHaveBeenCalled();
                });
            });
        });
    });

    describe('Load Error State', () => {
        it('should reflect loadError input', async () => {
            const { component } = await setup({ options: [], loadError: true, loadErrorMessage: 'Nope' });
            expect(component.loadError()).toBe(true);
            expect(component.loadErrorMessage()).toBe('Nope');
        });
    });

    describe('Grouping Functionality', () => {
        const groupedOptions = [
            { value: 'aws-1', label: 'Secret-1', group: 'AWS Secrets Manager' },
            { value: 'aws-2', label: 'Secret-2', group: 'AWS Secrets Manager' },
            { value: 'aws-3', label: 'Secret-3', group: 'AWS Secrets Manager' },
            { value: 'az-1', label: 'Secret-4', group: 'Azure Key Vault' },
            { value: 'az-2', label: 'Secret-5', group: 'Azure Key Vault' }
        ];

        async function setupGrouped(options: SetupOptions = {}) {
            const { fixture, component } = await setup({
                options: groupedOptions,
                ...options
            });

            fixture.detectChanges();

            return { fixture, component };
        }

        describe('groupedOptions computed signal', () => {
            it('should return null when no options have group property', async () => {
                const ungroupedOptions = [
                    { value: 'opt-1', label: 'Option 1' },
                    { value: 'opt-2', label: 'Option 2' }
                ];
                const { component } = await setup({ options: ungroupedOptions });

                expect(component['groupedOptions']()).toBeNull();
            });

            it('should organize options into groups when options have group property', async () => {
                const { component } = await setupGrouped();

                const groups = component['groupedOptions']();
                expect(groups).not.toBeNull();
                expect(groups!.length).toBe(2);

                const awsGroup = groups!.find((g) => g.groupId === 'AWS Secrets Manager');
                expect(awsGroup).toBeDefined();
                expect(awsGroup!.groupLabel).toBe('AWS Secrets Manager');
                expect(awsGroup!.options.length).toBe(3);

                const azureGroup = groups!.find((g) => g.groupId === 'Azure Key Vault');
                expect(azureGroup).toBeDefined();
                expect(azureGroup!.groupLabel).toBe('Azure Key Vault');
                expect(azureGroup!.options.length).toBe(2);
            });

            it('should order groups alphabetically', async () => {
                const { component } = await setupGrouped();

                const groups = component['groupedOptions']();
                expect(groups).not.toBeNull();
                expect(groups![0].groupId).toBe('AWS Secrets Manager');
                expect(groups![1].groupId).toBe('Azure Key Vault');
            });

            it('should place ungrouped options first with no header', async () => {
                const mixedOptions = [{ value: 'ungrouped-1', label: 'Ungrouped Option' }, ...groupedOptions];

                const { component } = await setup({ options: mixedOptions });

                const groups = component['groupedOptions']();
                expect(groups).not.toBeNull();
                expect(groups!.length).toBe(3);

                expect(groups![0].groupId).toBe('__ungrouped__');
                expect(groups![0].groupLabel).toBe('');
                expect(groups![0].options.length).toBe(1);
            });
        });

        describe('Filtering with groups', () => {
            it('should hide groups with no matching options when filtering', async () => {
                const { component, fixture } = await setupGrouped();

                component.searchString = 'Secret-4';
                fixture.detectChanges();

                const groups = component['groupedOptions']();
                expect(groups).not.toBeNull();
                const visibleGroups = groups!.filter((g) => g.options.length > 0);
                expect(visibleGroups.length).toBe(1);
                expect(visibleGroups[0].groupId).toBe('Azure Key Vault');
            });

            it('should show all groups when filter is cleared', async () => {
                const { component, fixture } = await setupGrouped();

                component.searchString = 'Secret-4';
                fixture.detectChanges();

                component.searchString = null;
                fixture.detectChanges();

                const groups = component['groupedOptions']();
                expect(groups!.filter((g) => g.options.length > 0).length).toBe(2);
            });
        });

        describe('getVisibleOptions with grouping', () => {
            it('should return flat list of all visible options for keyboard navigation', async () => {
                const { component } = await setupGrouped();

                const visibleOptions = component.getVisibleOptions();
                expect(visibleOptions.length).toBe(5);
                expect(visibleOptions.map((o) => o.value)).toEqual(['aws-1', 'aws-2', 'aws-3', 'az-1', 'az-2']);
            });
        });

        describe('Virtual scrolling with groups', () => {
            it('should include group headers in virtual items', async () => {
                const { component } = await setupGrouped({ enableVirtualScrolling: true });

                const virtualItems = component.getVirtualVisibleItems();

                expect(virtualItems.length).toBe(7);

                expect(component.isGroupHeaderItem(virtualItems[0])).toBe(true);
                expect(component.getGroupHeaderLabel(virtualItems[0])).toBe('AWS Secrets Manager');

                expect(component.isOptionItem(virtualItems[1])).toBe(true);
                expect(component.isOptionItem(virtualItems[2])).toBe(true);
                expect(component.isOptionItem(virtualItems[3])).toBe(true);

                expect(component.isGroupHeaderItem(virtualItems[4])).toBe(true);
                expect(component.getGroupHeaderLabel(virtualItems[4])).toBe('Azure Key Vault');
            });

            it('should calculate viewport height including group headers', async () => {
                const { component, fixture } = await setupGrouped({ enableVirtualScrolling: true });

                fixture.componentRef.setInput('virtualScrollItemSize', 32);
                fixture.detectChanges();

                const height = component.getVirtualScrollHeight();
                expect(height).toBe('217px');
            });

            it('should track virtual items including group headers', async () => {
                const { component } = await setupGrouped({ enableVirtualScrolling: true });

                const virtualItems = component.getVirtualVisibleItems();
                const header = virtualItems[0];
                const option = virtualItems[1];

                expect(component.trackByVirtualItem(0, header)).toBe('__group-header-AWS Secrets Manager');

                expect(component.trackByVirtualItem(1, option)).toBe('aws-1');
            });
        });

        describe('getGroupHeaderId', () => {
            it('should generate unique IDs for group headers', async () => {
                const { component } = await setupGrouped();

                const awsId = component.getGroupHeaderId('AWS Secrets Manager');
                const azureId = component.getGroupHeaderId('Azure Key Vault');

                expect(awsId).toContain('group-header-AWS Secrets Manager');
                expect(azureId).toContain('group-header-Azure Key Vault');
                expect(awsId).not.toBe(azureId);
            });
        });
    });

    describe('setDisabledState', () => {
        it('should set disabled property and forward to MatSelect after view init', async () => {
            const { fixture, component } = await setup({
                options: [{ value: 'one', label: 'One' }]
            });

            const matSelect = fixture.debugElement.query(By.directive(MatSelect)).componentInstance as MatSelect;
            const matSelectSpy = vi.spyOn(matSelect, 'setDisabledState');

            component.setDisabledState(true);

            expect(component.disabled).toBe(true);
            expect(matSelectSpy).toHaveBeenCalledWith(true);
        });

        it('should re-enable after being disabled', async () => {
            const { fixture, component } = await setup({
                options: [{ value: 'one', label: 'One' }]
            });

            const matSelect = fixture.debugElement.query(By.directive(MatSelect)).componentInstance as MatSelect;
            const matSelectSpy = vi.spyOn(matSelect, 'setDisabledState');

            component.setDisabledState(true);
            component.setDisabledState(false);

            expect(component.disabled).toBe(false);
            expect(matSelectSpy).toHaveBeenLastCalledWith(false);
        });

        it('should defer disabled state when called before view init', async () => {
            await TestBed.configureTestingModule({
                imports: [SearchableSelect, MatIconTestingModule, NoopAnimationsModule]
            }).compileComponents();

            const fixture = TestBed.createComponent(SearchableSelect<string>);
            const component = fixture.componentInstance;
            fixture.componentRef.setInput('options', [{ value: 'one', label: 'One' }]);

            component.setDisabledState(true);

            expect(component.disabled).toBe(true);
            expect(component['_pendingDisabledState']).toBe(true);

            fixture.detectChanges();

            const matSelect = fixture.debugElement.query(By.directive(MatSelect)).componentInstance as MatSelect;
            expect(matSelect.disabled).toBe(true);
            expect(component['_pendingDisabledState']).toBeNull();
        });

        it('should not apply pending state in ngAfterViewInit when no deferred call was made', async () => {
            const { fixture, component } = await setup({
                options: [{ value: 'one', label: 'One' }]
            });

            const matSelect = fixture.debugElement.query(By.directive(MatSelect)).componentInstance as MatSelect;
            const matSelectSpy = vi.spyOn(matSelect, 'setDisabledState');

            component.ngAfterViewInit();

            expect(matSelectSpy).not.toHaveBeenCalled();
        });
    });
});
