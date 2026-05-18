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
    AfterViewInit,
    ChangeDetectorRef,
    Component,
    computed,
    effect,
    ElementRef,
    EventEmitter,
    forwardRef,
    inject,
    input,
    NgZone,
    OnDestroy,
    OnInit,
    Output,
    signal,
    Signal,
    viewChild,
    ViewEncapsulation,
    WritableSignal
} from '@angular/core';
import { CommonModule } from '@angular/common';
import {
    MatError,
    MatFormField,
    MatHint,
    MatPrefix,
    MatSelect,
    MatSelectTrigger,
    MatSuffix
} from '@angular/material/select';
import { MatInput } from '@angular/material/input';
import { MatIcon } from '@angular/material/icon';
import { MatIconButton } from '@angular/material/button';
import { MatProgressSpinner } from '@angular/material/progress-spinner';
import { ControlValueAccessor, FormsModule, NG_VALUE_ACCESSOR } from '@angular/forms';
import { CdkFixedSizeVirtualScroll, CdkVirtualForOf, CdkVirtualScrollViewport } from '@angular/cdk/scrolling';
import { FlexibleConnectedPositionStrategy, OverlayRef } from '@angular/cdk/overlay';
import { Subject } from 'rxjs';
import { debounceTime, distinctUntilChanged, takeUntil } from 'rxjs/operators';

import { FilteredSearchableSelectOption, SearchableSelectGroup, SearchableSelectOption } from '../../types';
import { EllipsisTooltipDirective } from '../../directives/ellipsis-tooltip/ellipsis-tooltip.directive';
import { MultiSelectOption } from '../multi-select-option/multi-select-option.component';

type MatSelectOverlayInternals = MatSelect & {
    _overlayDir?: {
        overlayRef?: OverlayRef;
    };
    _elementRef?: ElementRef<HTMLElement>;
};

// Internal helper types for virtual renderer items
type FooterItem = { __kind: 'more' | 'error' };
type GroupHeaderItem = {
    __kind: 'group-header';
    groupId: string;
    groupLabel: string;
    isFirstGroup: boolean;
    isLastGroup: boolean;
};
type VirtualItem<T> = FilteredSearchableSelectOption<T> | FooterItem | GroupHeaderItem;

@Component({
    selector: 'searchable-select',
    standalone: true,
    imports: [
        CommonModule,
        MatSelect,
        MatSelectTrigger,
        MatInput,
        MatIcon,
        FormsModule,
        MatFormField,
        MatPrefix,
        MatError,
        MatHint,
        MultiSelectOption,
        CdkVirtualScrollViewport,
        CdkVirtualForOf,
        CdkFixedSizeVirtualScroll,
        MatIconButton,
        MatSuffix,
        MatProgressSpinner,
        EllipsisTooltipDirective
    ],
    templateUrl: './searchable-select.component.html',
    styleUrl: './searchable-select.component.scss',
    providers: [
        {
            provide: NG_VALUE_ACCESSOR,
            useExisting: forwardRef(() => SearchableSelect),
            multi: true
        }
    ],
    encapsulation: ViewEncapsulation.None
})
export class SearchableSelect<T = never> implements ControlValueAccessor, OnInit, OnDestroy, AfterViewInit {
    private cdr = inject(ChangeDetectorRef);
    private zone = inject(NgZone);
    private _options: FilteredSearchableSelectOption<T>[] = [];

    // Cache the display metadata for selected values so the trigger preserves labels
    // even when async options batches omit those values. Display-only; does not affect list rendering.
    private selectedOptionCache = new Map<T, SearchableSelectOption<T>>();

    protected filteredLength: number | null = null;

    // Signal input for options
    options = input<SearchableSelectOption<T>[]>([]);

    // Effect to handle options changes (replaces the setter logic)
    private optionsEffect = effect(() => {
        const options = this.options();
        this.searchInProgress = false;
        const currentSearch = this._searchString; // preserve user-entered search text
        options.forEach((o) => {
            this.selectedOptionCache.set(o.value, {
                value: o.value,
                label: o.label,
                svgIcon: o.svgIcon,
                description: o.description,
                disabled: o.disabled,
                labelCssClasses: o.labelCssClasses
            });
        });
        this._options = options.map((o) => {
            return {
                ...o,
                hidden: false
            } as FilteredSearchableSelectOption<T>;
        });
        if (currentSearch === null) {
            this.resetFilter();
        } else {
            this.filter(currentSearch);
        }
        if (this.asyncSearchEnabled()) {
            this.filteredLength = this._options.length;
        }
        this.updateDisplayValueSignal();
        this.cdr.markForCheck();
    });

    multiple = input(false);
    searchPlaceholder = input('');
    placeholder = input('');
    allowClear = input(false);
    a11ySearchLabel = input('Search');
    a11yClearSearchLabel = input('Clear search');

    enableVirtualScrolling = input(false);
    virtualScrollItemSize = input(32); // 32 for no description, 54 for description. Both assume no line wrapping.
    virtualScrollMinBufferPx = input(200);
    virtualScrollMaxBufferPx = input(400);
    private virtualScrollMaxHeight = 217;

    searchDebounceMs = input(300);
    asyncSearchEnabled = input(false);
    asyncSearchOptionsHaveMore = input(false);
    minSearchLength = input(1);
    loadError = input(false);
    loadErrorMessage = input('');

    hint = input<string>('');
    showHint = input(true);
    validationError = input<string>('');
    verificationError = input<string>('');

    @Output() searchChange = new EventEmitter<string>();

    /** Computed signal that detects if any option has a group property. */
    protected hasGroupedOptions: Signal<boolean> = computed(() => {
        return this.filteredOptions().some((opt) => !opt.hidden && opt.group);
    });

    private _virtualSelectedValues: T[] = [];

    protected displayValueSignal = signal('');

    private _activeOptionIndex = -1;
    private _isNavigating = false;

    // Permission-based value change system for single-select mode
    // Prevents Material Design from auto-selecting on arrow keys.
    private _lastIntentionalValue: T | null = null;
    private _allowNextValueChange = false;

    filteredOptions: WritableSignal<FilteredSearchableSelectOption<T>[]> = signal([]);
    searchInProgress = false;

    protected activeTemplateIndex = signal<number>(-1);

    private filteredOptionsEffect = effect(() => {
        void this.filteredOptions();
        this.updateActiveTemplateIndex();
    });

    /**
     * Computed signal that organizes filtered options into groups when any option has a group property.
     * Returns null when no options have groups, allowing template to use ungrouped rendering.
     */
    protected groupedOptions: Signal<SearchableSelectGroup<T>[] | null> = computed(() => {
        if (!this.hasGroupedOptions()) {
            return null;
        }

        const options = this.filteredOptions();
        const groupMap = new Map<string, FilteredSearchableSelectOption<T>[]>();
        const ungrouped: FilteredSearchableSelectOption<T>[] = [];

        options.forEach((opt) => {
            if (opt.hidden) return;
            if (opt.group) {
                if (!groupMap.has(opt.group)) {
                    groupMap.set(opt.group, []);
                }
                groupMap.get(opt.group)!.push(opt);
            } else {
                ungrouped.push(opt);
            }
        });

        const groups: SearchableSelectGroup<T>[] = [];

        if (ungrouped.length > 0) {
            groups.push({
                groupId: '__ungrouped__',
                groupLabel: '',
                options: ungrouped
            });
        }

        const sortedGroups = Array.from(groupMap.entries())
            .filter(([, opts]) => opts.length > 0)
            .sort((a, b) => a[0].localeCompare(b[0]));

        sortedGroups.forEach(([groupId, opts]) => {
            groups.push({
                groupId,
                groupLabel: groupId,
                options: opts
            });
        });

        return groups;
    });

    getGroupHeaderId(groupId: string): string {
        return this.getUniqueId(`group-header-${groupId}`);
    }

    select = viewChild.required<MatSelect>(MatSelect);
    searchInput = viewChild.required<ElementRef<HTMLInputElement>>('searchInput');
    clearSearchBtn = viewChild<MatIconButton>('clearSearchBtn');
    virtualScrollViewport = viewChild<CdkVirtualScrollViewport>('virtualScrollViewport');

    onChange!: (value: T[] | T | null) => void;
    onTouched!: () => void;
    // Reserved state field for future touched-tracking work; today it is only referenced
    // via the ControlValueAccessor registered onTouched callback.
    touched = false;
    disabled = false;
    private _viewInitialized = false;
    private _pendingDisabledState: boolean | null = null;

    private _lastEmittedValue: T[] | T | null = null;
    private _isSyncingMatValue = false;

    private _searchString: string | null = null;
    set searchString(text: string | null) {
        this._searchString = text;
        this.filter(this._searchString);

        if (this.select && this.select() && this.select().panelOpen) {
            const term = (text ?? '').trim();
            if (term.length === 0 || term.length >= this.minSearchLength()) {
                this._searchTerms$.next(term);
            }
        }
    }

    get searchString(): string | null {
        return this._searchString;
    }

    get displayValue(): string {
        this.updateDisplayValueSignal();
        return this.displayValueSignal();
    }

    getDisplayValue(): string {
        const selectedValues = this.getCurrentSelectedValues();
        if (!selectedValues || selectedValues.length === 0) return '';

        if (this.multiple()) {
            const labels = selectedValues
                .map((value) => {
                    const option = this._options.find((opt) => opt.value === value);
                    if (option) return option.label;
                    if (this.asyncSearchEnabled()) {
                        const cached = this.selectedOptionCache.get(value as T);
                        return cached ? cached.label : String(value);
                    }
                    return '';
                })
                .filter((label) => label);
            return labels.join(', ');
        } else {
            const selectedValue = selectedValues[0];
            if (selectedValue !== null && selectedValue !== undefined) {
                const option = this._options.find((opt) => opt.value === selectedValue);
                if (option) return option.label;
                if (this.asyncSearchEnabled()) {
                    const cached = this.selectedOptionCache.get(selectedValue as T);
                    return cached ? cached.label : String(selectedValue);
                }
                return '';
            }
            return '';
        }
    }

    get matSelectValue(): T[] | T | null {
        if (this.enableVirtualScrolling()) {
            return this.multiple() ? this._virtualSelectedValues : (this._virtualSelectedValues[0] ?? null);
        } else {
            const selectValue = this.select()?.value;
            return selectValue !== undefined ? selectValue : null;
        }
    }

    selectionPanelToggled(open: boolean) {
        if (open) {
            setTimeout(() => {
                const viewport = this.virtualScrollViewport();
                if (this.enableVirtualScrolling() && viewport) {
                    viewport.scrollToIndex(0);
                    viewport.checkViewportSize();
                    this.syncMatSelectValue();
                    this.cdr.detectChanges();
                }
                this.searchInput().nativeElement.focus();
                this.startOverlayAutoReposition();
            }, 0);
            this._activeOptionIndex = -1;
            this._isNavigating = false;
        }
        if (!open) {
            this.stopOverlayAutoReposition();
            if (this.asyncSearchEnabled()) {
                this.searchString = null;
                this.searchChange.emit('');
            } else {
                this.resetFilter();
            }
            this.select().focus();
            this._activeOptionIndex = -1;
            this._isNavigating = false;
            this.updateActiveDescendant();
            this.updateActiveTemplateIndex();
        }
    }

    private resetFilter() {
        if (this.asyncSearchEnabled()) {
            this.searchChange.emit('');
        }

        this._options = this.options().map((o) => ({ ...o, hidden: false }));
        this.filteredLength = this._options.length;
        this.filteredOptions.set(this._options);
        this._searchString = null;

        this._activeOptionIndex = -1;
        this._isNavigating = false;
        this.updateActiveDescendant();
        this.updateActiveTemplateIndex();
    }

    private filter(text: string | null) {
        if (this.asyncSearchEnabled()) {
            const normalized = this._options.map((option) => {
                option.hidden = false;
                return option;
            });
            this.filteredOptions.set(normalized);
            this.filteredLength = normalized.length;
        } else if (text === null) {
            this.resetFilter();
        } else {
            let filteredCount = 0;
            const filtered = this._options.map((option) => {
                option.hidden = !option.label.toLowerCase().includes(text.toLowerCase());
                if (option.hidden) {
                    filteredCount++;
                }
                return option;
            });
            this.filteredLength = filtered.length - filteredCount;
            this.filteredOptions.set(filtered);
        }

        this._activeOptionIndex = -1;
        this._isNavigating = false;
        this.updateActiveDescendant();
        this.updateActiveTemplateIndex();
    }

    onValueChanged($event: T[] | T) {
        // Material Design's mat-select changes the bound value when arrow keys navigate
        // in single-select mode (but not in multi-select). We normalise both modes so that
        // arrow keys only move highlight and Enter/click/touch performs the actual selection.
        if (!this.multiple() && !this.select().panelOpen && this.enableVirtualScrolling()) {
            this.syncMatSelectValue();
            this.cdr.detectChanges();
            return;
        }

        if (this.enableVirtualScrolling()) {
            let eventArray: T[];
            if (Array.isArray($event)) {
                eventArray = $event || [];
            } else if ($event != null) {
                eventArray = [$event];
            } else {
                eventArray = [];
            }

            const incomingSelection = this.canonicalizeSelection(eventArray);

            const currentSet = new Set(this._virtualSelectedValues);
            const incomingSet = new Set(incomingSelection);

            const hasChanged =
                currentSet.size !== incomingSet.size ||
                !Array.from(currentSet).every((val) => incomingSet.has(val)) ||
                !Array.from(incomingSet).every((val) => currentSet.has(val));

            if (hasChanged) {
                this.updateVirtualSelectedValues(incomingSelection);

                const formValue = this.multiple()
                    ? this._virtualSelectedValues
                    : this._virtualSelectedValues[0] || null;

                this.emitIfChanged(formValue);
                this.syncMatSelectValue();
                this.cdr.detectChanges();
            }
        } else {
            if (!this.multiple()) {
                if (this._allowNextValueChange || this.select().panelOpen) {
                    this._allowNextValueChange = false;
                    this._lastIntentionalValue = $event as T;
                    this.emitIfChanged($event);
                    this.updateDisplayValueSignal();
                } else {
                    // Arrow-key auto-selection detected; revert to the last intentional value.
                    setTimeout(() => {
                        this.select().writeValue(this._lastIntentionalValue);
                        this.updateDisplayValueSignal();
                    }, 0);
                    return;
                }
            } else {
                this.emitIfChanged($event);
                this.updateDisplayValueSignal();
            }
        }
        if (!this.multiple()) {
            this.select().close();
        }
    }

    onSearchInputKeydown(event: KeyboardEvent) {
        if (event.key === 'Tab') {
            event.preventDefault();
            event.stopPropagation();
            this.clearSearchBtn()?.focus();
        }

        if (event.code === 'Space') {
            event.stopPropagation();
        }

        if (event.key === 'ArrowDown' || event.key === 'ArrowUp') {
            event.preventDefault();
            event.stopPropagation();
            event.stopImmediatePropagation();
            this.handleArrowNavigation(event.key);
        }

        if (event.key === 'PageDown' || event.key === 'PageUp') {
            event.preventDefault();
            event.stopPropagation();
            event.stopImmediatePropagation();
            this.handlePageNavigation(event.key);
        }

        if (event.key === 'Enter') {
            event.preventDefault();
            event.stopPropagation();
            event.stopImmediatePropagation();
            this.handleEnterKey();
        }

        if (event.key === 'Escape') {
            event.preventDefault();
            event.stopPropagation();
            this.select().close();
            this._activeOptionIndex = -1;
        }
    }

    onMatSelectKeydown(event: KeyboardEvent): boolean | void {
        if (event.key === 'ArrowDown' && !this.select().panelOpen) {
            event.preventDefault();
            event.stopPropagation();

            this.select().open();

            return false;
        }

        return;
    }

    focusSearchAndOrClear(event: KeyboardEvent | MouseEvent) {
        if (event.type === 'keydown') {
            const key = (event as KeyboardEvent).code;
            if (key === 'Enter' || key === 'Space') {
                this.resetFilter();
            }
            if (['Enter', 'Space', 'Tab'].includes(key)) {
                event.stopPropagation();
                event.preventDefault();
                this.searchInput().nativeElement.focus();
            }
        }

        if (event.type === 'click') {
            event.stopPropagation();
            event.preventDefault();
            this.resetFilter();
            this.searchInput().nativeElement.focus();
        }
    }

    clearSelection(event: Event) {
        event.stopPropagation();
        event.preventDefault();

        if (this.enableVirtualScrolling()) {
            this.updateVirtualSelectedValues([]);
            const formValue = this.multiple() ? [] : null;
            this.onChange(formValue);
            this.syncMatSelectValue();
        } else {
            const formValue = this.multiple() ? [] : null;
            this.select().writeValue(formValue);
            this._lastIntentionalValue = null;
            this.onChange(formValue);
            this.updateDisplayValueSignal();
        }

        this.cdr.detectChanges();
    }

    onClearSelectionKeydown(event: KeyboardEvent) {
        if (event.code === 'Enter' || event.code === 'Space') {
            event.stopPropagation();
            event.preventDefault();
            this.clearSelection(event);
        }
    }

    shouldShowClearButton(): boolean {
        if (!this.allowClear()) return false;
        const selectedValues = this.getCurrentSelectedValues();
        return selectedValues.length > 0;
    }

    private getCurrentSelectedValues(): T[] {
        if (this.enableVirtualScrolling()) {
            return this._virtualSelectedValues;
        } else {
            const matSelectValue = this.select()?.value;
            if (Array.isArray(matSelectValue)) {
                return matSelectValue;
            } else if (matSelectValue !== null && matSelectValue !== undefined) {
                return [matSelectValue];
            } else {
                return [];
            }
        }
    }

    /**
     * Get all selected options that need ghost rendering (unified for both modes).
     *
     * For virtual scrolling (both single and multi-select), mat-select needs hidden
     * <multi-select-option> elements to track selected values that may be scrolled
     * off-screen.
     */
    getGhostOptions(): FilteredSearchableSelectOption<T>[] {
        const selectedValues = this.getCurrentSelectedValues();

        if (selectedValues.length === 0) {
            return [];
        }

        const ghosts: FilteredSearchableSelectOption<T>[] = [];

        if (this.enableVirtualScrolling()) {
            ghosts.push(...this._options.filter((option) => selectedValues.includes(option.value)));
        } else if (this.multiple()) {
            const visibleFilteredOptions = this.filteredOptions().filter((option) => !option.hidden);
            const visibleValues = new Set(visibleFilteredOptions.map((option) => option.value));
            ghosts.push(
                ...this._options.filter(
                    (option) => selectedValues.includes(option.value) && !visibleValues.has(option.value)
                )
            );
        }

        if (this.asyncSearchEnabled()) {
            const optionValues = new Set(this._options.map((o) => o.value));
            selectedValues.forEach((val) => {
                if (!optionValues.has(val)) {
                    const cached = this.selectedOptionCache.get(val as T);
                    const label = cached?.label ?? String(val);
                    ghosts.push({
                        value: val as T,
                        label,
                        svgIcon: cached?.svgIcon,
                        description: cached?.description,
                        disabled: cached?.disabled ?? false,
                        labelCssClasses: cached?.labelCssClasses,
                        hidden: true
                    } as FilteredSearchableSelectOption<T>);
                }
            });
        }

        return ghosts;
    }

    private handleArrowNavigation(key: string) {
        const visibleOptions = this.getVisibleOptions();
        if (visibleOptions.length === 0) return;

        if (key === 'ArrowDown') {
            if (this._activeOptionIndex < 0) {
                this._activeOptionIndex = 0;
            } else {
                this._activeOptionIndex = Math.min(this._activeOptionIndex + 1, visibleOptions.length - 1);
            }
        } else if (key === 'ArrowUp') {
            if (this._activeOptionIndex < 0) {
                this._activeOptionIndex = visibleOptions.length - 1;
            } else {
                this._activeOptionIndex = Math.max(this._activeOptionIndex - 1, 0);
            }
        }

        this._isNavigating = true;
        this.scrollToActiveOption();
        this.updateActiveDescendant();
        this.updateActiveTemplateIndex();
    }

    private handlePageNavigation(key: string) {
        const visibleOptions = this.getVisibleOptions();
        if (visibleOptions.length === 0) return;

        let pageSize: number;
        if (this.enableVirtualScrolling()) {
            const viewport = this.virtualScrollViewport();
            if (viewport) {
                const viewportSize = viewport.getViewportSize();
                const itemSize = this.virtualScrollItemSize();
                pageSize = Math.floor(viewportSize / itemSize);
            } else {
                pageSize = 10;
            }
        } else {
            pageSize = 10;
        }

        pageSize = Math.max(1, pageSize);

        if (key === 'PageDown') {
            if (this._activeOptionIndex < 0) {
                this._activeOptionIndex = 0;
            } else {
                this._activeOptionIndex = Math.min(this._activeOptionIndex + pageSize, visibleOptions.length - 1);
            }
        } else if (key === 'PageUp') {
            if (this._activeOptionIndex < 0) {
                this._activeOptionIndex = visibleOptions.length - 1;
            } else {
                this._activeOptionIndex = Math.max(this._activeOptionIndex - pageSize, 0);
            }
        }

        this._isNavigating = true;
        this.scrollToActiveOption();
        this.updateActiveDescendant();
        this.updateActiveTemplateIndex();
    }

    private handleEnterKey() {
        if (this._activeOptionIndex >= 0) {
            const visibleOptions = this.getVisibleOptions();
            const activeOption = visibleOptions[this._activeOptionIndex];
            if (activeOption && !activeOption.disabled) {
                this._allowNextValueChange = true;
                this._isNavigating = false;
                this.toggleOption(activeOption.value);
            }
        }
    }

    private scrollToActiveOption() {
        if (this._activeOptionIndex >= 0) {
            if (this.enableVirtualScrolling()) {
                const viewport = this.virtualScrollViewport();
                if (viewport) {
                    const viewportSize = viewport.getViewportSize();
                    const itemSize = this.virtualScrollItemSize();
                    const scrollOffset = viewport.measureScrollOffset();

                    const virtualIndex = this.getVirtualIndexForOptionIndex(this._activeOptionIndex);

                    const firstFullyVisibleIndex = Math.floor((scrollOffset + itemSize * 0.5) / itemSize);
                    const lastFullyVisibleIndex = Math.floor((scrollOffset + viewportSize - itemSize * 0.5) / itemSize);

                    if (virtualIndex < firstFullyVisibleIndex) {
                        viewport.scrollToIndex(virtualIndex, 'smooth');
                    } else if (virtualIndex > lastFullyVisibleIndex) {
                        const targetOffset = (virtualIndex + 1) * itemSize - viewportSize;
                        viewport.scrollToOffset(Math.max(0, targetOffset), 'smooth');
                    }
                }
            } else {
                const activeOptionElement = document.getElementById(this.getOptionId(this._activeOptionIndex));
                if (activeOptionElement) {
                    activeOptionElement.scrollIntoView({
                        behavior: 'smooth',
                        block: 'nearest',
                        inline: 'nearest'
                    });
                }
            }
        }
    }

    private updateActiveDescendant() {
        const searchInput = this.searchInput()?.nativeElement;
        if (!searchInput) return;

        if (this._activeOptionIndex >= 0 && this._isNavigating) {
            const optionId = this.activeOptionId;
            if (searchInput.setAttribute) {
                searchInput.setAttribute('aria-activedescendant', optionId);
            }
        } else {
            if (searchInput.removeAttribute) {
                searchInput.removeAttribute('aria-activedescendant');
            }
        }
    }

    private toggleOption(value: T) {
        if (this.enableVirtualScrolling()) {
            if (this.multiple()) {
                const currentValues = [...this._virtualSelectedValues];
                const index = currentValues.indexOf(value);

                if (index > -1) {
                    currentValues.splice(index, 1);
                } else {
                    currentValues.push(value);
                }

                this.updateVirtualSelectedValues(currentValues);
                const formValue = this._virtualSelectedValues;
                this.emitIfChanged(formValue);
                this.syncMatSelectValue();
            } else {
                const newValue = this._virtualSelectedValues.includes(value) ? null : value;
                this.updateVirtualSelectedValues(newValue ? [newValue] : []);
                this.emitIfChanged(newValue);
                this.syncMatSelectValue();
                this.select().close();
            }

            this.cdr.detectChanges();
        } else {
            const visibleOptions = this.getVisibleOptions();
            const activeOptionWithIndex = visibleOptions[
                this._activeOptionIndex
            ] as FilteredSearchableSelectOption<T> & { templateIndex?: number };

            const templateIndex = activeOptionWithIndex?.templateIndex ?? this._activeOptionIndex;
            const activeOptionId = this.getOptionId(templateIndex);
            let optionElement = document.getElementById(activeOptionId);

            if (!optionElement) {
                const allOptions = document.querySelectorAll('multi-select-option');
                allOptions.forEach((element) => {
                    const htmlElement = element as HTMLElement & { value: T };
                    if (htmlElement.value === value) {
                        optionElement = element as HTMLElement;
                    }
                });
            }

            if (optionElement) {
                const clickEvent = new MouseEvent('click', {
                    view: window,
                    bubbles: true,
                    cancelable: true
                });
                optionElement.dispatchEvent(clickEvent);
            }
        }
    }

    isOptionActive(index: number): boolean {
        if (!this._isNavigating) return false;

        if (this.enableVirtualScrolling()) {
            return this._activeOptionIndex === index;
        } else {
            const visibleOptions = this.getVisibleOptions();
            const activeOption = visibleOptions[this._activeOptionIndex] as FilteredSearchableSelectOption<T> & {
                templateIndex?: number;
            };
            const templateIndex = activeOption?.templateIndex;
            return templateIndex === index;
        }
    }

    isOptionActiveByValue(value: T): boolean {
        if (!this._isNavigating || this._activeOptionIndex < 0) {
            return false;
        }

        const visibleOptions = this.getVisibleOptions();
        const activeOption = visibleOptions[this._activeOptionIndex];
        return activeOption?.value === value;
    }

    /**
     * Get visible options for keyboard navigation - should match what's actually rendered.
     */
    getVisibleOptions(): FilteredSearchableSelectOption<T>[] {
        if (this.enableVirtualScrolling()) {
            const searchTerm = this._searchString?.toLowerCase();

            if (!searchTerm || this.asyncSearchEnabled()) {
                return this._options;
            } else {
                return this._options.filter((option) => option.label.toLowerCase().includes(searchTerm));
            }
        } else if (this.hasGroupedOptions()) {
            const groups = this.groupedOptions();
            if (groups) {
                return groups.flatMap((g) => g.options);
            }
            return [];
        } else {
            const allFilteredOptions = this.filteredOptions();
            const visibleWithIndices: (FilteredSearchableSelectOption<T> & { templateIndex: number })[] = [];

            allFilteredOptions.forEach((option, index) => {
                if (!option.hidden) {
                    visibleWithIndices.push({ ...option, templateIndex: index });
                }
            });

            return visibleWithIndices;
        }
    }

    getVirtualVisibleItems(): VirtualItem<T>[] {
        const options = this.getVisibleOptions();

        if (this.loadError() && options.length === 0) {
            return [{ __kind: 'error' } as FooterItem];
        }

        let items: VirtualItem<T>[];

        if (this.hasGroupedOptions()) {
            const groups = this.groupedOptions();
            if (groups && groups.length > 0) {
                items = [];
                const firstGroupHeaderIndex = groups.findIndex((g) => g.groupLabel);
                groups.forEach((group, groupIndex) => {
                    const isFirstGroup = groupIndex === firstGroupHeaderIndex;
                    const isLastGroup = groupIndex === groups.length - 1;
                    if (group.groupLabel) {
                        items.push({
                            __kind: 'group-header',
                            groupId: group.groupId,
                            groupLabel: group.groupLabel,
                            isFirstGroup,
                            isLastGroup
                        } as GroupHeaderItem);
                    }
                    group.options.forEach((opt) => {
                        items.push(opt);
                    });
                });
            } else {
                items = options as VirtualItem<T>[];
            }
        } else {
            items = options as VirtualItem<T>[];
        }

        if (this.asyncSearchEnabled() && items.length > 0 && this.asyncSearchOptionsHaveMore()) {
            return [...items, { __kind: 'more' } as FooterItem];
        }

        return items;
    }

    isGroupHeaderItem(item: VirtualItem<T>): item is GroupHeaderItem {
        return (item as GroupHeaderItem).__kind === 'group-header';
    }

    getGroupHeaderLabel(item: VirtualItem<T>): string {
        return this.isGroupHeaderItem(item) ? (item as GroupHeaderItem).groupLabel : '';
    }

    isFirstGroupHeader(item: VirtualItem<T>): boolean {
        return this.isGroupHeaderItem(item) ? (item as GroupHeaderItem).isFirstGroup : false;
    }

    isLastGroupHeader(item: VirtualItem<T>): boolean {
        return this.isGroupHeaderItem(item) ? (item as GroupHeaderItem).isLastGroup : false;
    }

    private getVirtualIndexForOptionIndex(optionIndex: number): number {
        if (!this.hasGroupedOptions()) {
            return optionIndex;
        }

        const virtualItems = this.getVirtualVisibleItems();
        let optionCount = 0;

        for (let i = 0; i < virtualItems.length; i++) {
            const item = virtualItems[i];
            if (this.isOptionItem(item)) {
                if (optionCount === optionIndex) {
                    return i;
                }
                optionCount++;
            }
        }

        return virtualItems.length - 1;
    }

    trackByValue = (index: number, option: FilteredSearchableSelectOption<T>): T => {
        return option.value;
    };

    trackByVirtualItem = (index: number, item: VirtualItem<T>): T | string => {
        if (this.isFooterItem(item)) {
            return `__footer-${(item as FooterItem).__kind}`;
        }
        if (this.isGroupHeaderItem(item)) {
            return `__group-header-${(item as GroupHeaderItem).groupId}`;
        }
        return (item as FilteredSearchableSelectOption<T>).value;
    };

    isFooterItem(item: VirtualItem<T>): item is FooterItem {
        const kind = (item as FooterItem).__kind;
        return kind === 'more' || kind === 'error';
    }

    isOptionItem(item: VirtualItem<T>): boolean {
        const kind = (item as FooterItem | GroupHeaderItem).__kind;
        return kind !== 'more' && kind !== 'error' && kind !== 'group-header';
    }
    isMoreItem(item: VirtualItem<T>): item is FooterItem {
        return (item as unknown as FooterItem).__kind === 'more';
    }

    asOptionItem(item: VirtualItem<T>): FilteredSearchableSelectOption<T> | null {
        if (this.isFooterItem(item) || this.isGroupHeaderItem(item)) {
            return null;
        }
        return item as FilteredSearchableSelectOption<T>;
    }

    getItemValue(item: VirtualItem<T>): T {
        const opt = this.asOptionItem(item);
        return opt ? opt.value : (undefined as unknown as T);
    }

    getItemDisabled(item: VirtualItem<T>): boolean {
        const opt = this.asOptionItem(item);
        return !!opt?.disabled;
    }

    getItemSvgIcon(item: VirtualItem<T>): string {
        const opt = this.asOptionItem(item);
        return opt?.svgIcon ?? '';
    }

    getItemLabelCss(item: VirtualItem<T>): string[] | undefined {
        const opt = this.asOptionItem(item);
        return opt?.labelCssClasses;
    }

    getItemLabel(item: VirtualItem<T>): string {
        const opt = this.asOptionItem(item);
        return opt?.label ?? '';
    }

    getItemDescription(item: VirtualItem<T>): string | undefined {
        const opt = this.asOptionItem(item);
        return opt?.description;
    }

    isVirtualOptionSelected(value: T): boolean {
        return this._virtualSelectedValues.includes(value);
    }

    getVirtualScrollHeight(): string {
        const virtualItems = this.getVirtualVisibleItems();
        const itemCount = virtualItems.length;

        if (itemCount === 0) {
            if (this.loadError()) {
                return `${this.virtualScrollItemSize()}px`;
            }
            return '0px';
        }

        const idealHeight = itemCount * this.virtualScrollItemSize();

        const actualHeight = Math.min(idealHeight, this.virtualScrollMaxHeight);

        return `${actualHeight}px`;
    }

    private getUniqueId(suffix: string): string {
        return `${this.componentId}-${suffix}`;
    }

    get optionsListId(): string {
        return this.getUniqueId(this.enableVirtualScrolling() ? 'virtual-options-list' : 'options-list');
    }

    getOptionId(index: number | string): string {
        return this.getUniqueId(`option-${index}`);
    }

    get activeOptionId(): string {
        return this.getUniqueId('option-active');
    }

    private updateVirtualSelectedValues(values: T[]): void {
        this._virtualSelectedValues = values;
        this.updateDisplayValueSignal();
        this.cdr.markForCheck();
    }

    private updateActiveTemplateIndex(): void {
        if (!this._isNavigating) {
            this.activeTemplateIndex.set(-1);
            return;
        }

        if (this.enableVirtualScrolling()) {
            this.activeTemplateIndex.set(this._activeOptionIndex);
            return;
        }

        const allFiltered = this.filteredOptions();
        if (this._activeOptionIndex < 0) {
            this.activeTemplateIndex.set(-1);
            return;
        }

        let visibleIndex = -1;
        for (let idx = 0; idx < allFiltered.length; idx++) {
            const option = allFiltered[idx];
            if (!option.hidden) {
                visibleIndex++;
                if (visibleIndex === this._activeOptionIndex) {
                    this.activeTemplateIndex.set(idx);
                    return;
                }
            }
        }
        this.activeTemplateIndex.set(-1);
    }

    private updateDisplayValueSignal(): void {
        const selectedValues = this.getCurrentSelectedValues();

        let displayText = '';
        if (this.multiple()) {
            const labels = selectedValues
                .map((value) => {
                    const option = this._options.find((opt) => opt.value === value);
                    if (option) return option.label;
                    if (this.asyncSearchEnabled()) {
                        const cached = this.selectedOptionCache.get(value as T);
                        return cached ? cached.label : String(value);
                    }
                    return '';
                })
                .filter((label) => label);
            displayText = labels.join(', ');
        } else {
            const selectedValue = selectedValues[0];
            if (selectedValue !== null && selectedValue !== undefined) {
                const option = this._options.find((opt) => opt.value === selectedValue);
                if (option) {
                    displayText = option.label;
                } else if (this.asyncSearchEnabled()) {
                    const cached = this.selectedOptionCache.get(selectedValue as T);
                    displayText = cached ? cached.label : String(selectedValue);
                } else {
                    displayText = '';
                }
            }
        }

        this.displayValueSignal.set(displayText);
    }

    private syncMatSelectValue(): void {
        if (!this.enableVirtualScrolling()) return;
        if (this._isSyncingMatValue) return;
        this._isSyncingMatValue = true;
        const valueToSync = this.multiple() ? this._virtualSelectedValues : this._virtualSelectedValues[0] || null;
        this.select().writeValue(valueToSync);
        this._isSyncingMatValue = false;
    }

    // ========================================================================================
    // ControlValueAccessor Implementation
    // ========================================================================================

    writeValue(value: T[] | T | null): void {
        if (this.enableVirtualScrolling()) {
            let newValues: T[];
            if (this.multiple()) {
                newValues = Array.isArray(value) ? value : value ? [value] : [];
            } else {
                newValues = value !== null ? [value as T] : [];
            }

            this.updateVirtualSelectedValues(newValues);

            // Defer mat-select sync so dynamically added controls finish wiring up before we
            // write into the child mat-select and to avoid feedback loops during form init.
            setTimeout(() => {
                this.syncMatSelectValue();
                this.cdr.detectChanges();
            }, 0);
        } else {
            this.select().writeValue(value);

            if (!this.multiple()) {
                this._lastIntentionalValue = Array.isArray(value) ? value[0] || null : value;
            }

            this.updateDisplayValueSignal();
        }

        if (this.multiple()) {
            this.scheduleOverlayReposition();
        }
    }

    // ========================================================================================
    // Deduped Emission Logic
    // ========================================================================================

    private emitIfChanged(next: T[] | T | null): void {
        const toEmit = next;
        if (!this.areValuesEqual(toEmit, this._lastEmittedValue)) {
            this._lastEmittedValue = Array.isArray(toEmit) ? [...(toEmit as T[])] : (toEmit as T | null);
            this.onChange(toEmit);
        }
    }

    private areValuesEqual(a: T[] | T | null, b: T[] | T | null): boolean {
        if (a === b) {
            return true;
        }

        if (a == null || b == null) {
            return false;
        }

        const aArr = Array.isArray(a) ? a : [a];
        const bArr = Array.isArray(b) ? b : [b];

        if (aArr.length !== bArr.length) {
            return false;
        }

        const aSet = new Set(aArr);
        for (const item of bArr) {
            if (!aSet.has(item)) {
                return false;
            }
        }

        return true;
    }

    private canonicalizeSelection(values: T[]): T[] {
        const labelCounts = new Map<string, number>();
        const valueToLabel = new Map<T, string>();
        for (const value of values) {
            const option = this._options.find((opt) => opt.value === value);
            const label = option?.label ?? String(value);
            valueToLabel.set(value, label);
            labelCounts.set(label, (labelCounts.get(label) || 0) + 1);
        }
        return values.filter((value) => (labelCounts.get(valueToLabel.get(value) as string) || 0) === 1);
    }

    registerOnChange(onChange: (value: T[] | T | null) => void): void {
        this.onChange = onChange;
        // Value emissions are routed through onValueChanged/emitIfChanged (via the template's
        // (valueChange) binding) or direct this.onChange() calls (e.g. clearSelection()).
        // Registering onChange on the internal mat-select would create a second emission path.
    }

    registerOnTouched(onTouch: () => NonNullable<unknown>): void {
        this.onTouched = onTouch;
        this.select().registerOnTouched(onTouch);
    }

    setDisabledState(isDisabled: boolean): void {
        this.disabled = isDisabled;
        if (this._viewInitialized) {
            this.select().setDisabledState(isDisabled);
        } else {
            this._pendingDisabledState = isDisabled;
        }
    }

    private readonly componentId = `searchable-select-${Math.random().toString(36).substring(2, 11)}`;

    private readonly _searchTerms$ = new Subject<string>();

    /**
     * Reset the emission tracking state to allow the same value to be emitted again.
     */
    resetEmissionTracking(): void {
        this._lastEmittedValue = null;
    }

    ngOnInit(): void {
        this._searchTerms$
            .pipe(distinctUntilChanged(), debounceTime(this.searchDebounceMs()), takeUntil(this._destroyed$))
            .subscribe((term: string) => {
                if (this.asyncSearchEnabled()) {
                    this.searchInProgress = true;
                    this.searchChange.emit(term);
                }
            });
    }

    ngAfterViewInit(): void {
        this._viewInitialized = true;
        if (this._pendingDisabledState !== null) {
            this.select().setDisabledState(this._pendingDisabledState);
            this._pendingDisabledState = null;
        }
    }

    private readonly _destroyed$ = new Subject<void>();
    ngOnDestroy(): void {
        this.stopOverlayAutoReposition();
        this._destroyed$.next();
        this._destroyed$.complete();
    }

    private scheduleOverlayReposition(): void {
        if (!this.multiple()) {
            return;
        }

        const overlayRef = this.getOverlayRef();
        if (!overlayRef) {
            return;
        }

        if (this.overlayRafHandle !== null) {
            this.cancelAnimationFrame(this.overlayRafHandle);
        }

        this.zone.runOutsideAngular(() => {
            this.overlayRafHandle = this.requestAnimationFrame(() => {
                this.applyOverlayReposition(overlayRef);
                this.overlayRafHandle = null;
            });
        });
    }

    private startOverlayAutoReposition(): void {
        if (!this.multiple()) {
            return;
        }

        const selectInstance = this.tryGetMatSelectInternals();
        if (!selectInstance?.panelOpen) {
            return;
        }

        const triggerElement = selectInstance._elementRef?.nativeElement;
        if (!triggerElement) {
            return;
        }

        this.stopOverlayAutoReposition();

        if (typeof window !== 'undefined' && 'ResizeObserver' in window) {
            this.triggerResizeObserver = new ResizeObserver(() => {
                this.lastTriggerRect = null;
                this.scheduleOverlayReposition();
            });
            this.triggerResizeObserver.observe(triggerElement);
        }

        if (typeof window === 'undefined') {
            this.scheduleOverlayReposition();
            return;
        }

        this.zone.runOutsideAngular(() => {
            const monitorGeometry = () => {
                const activeSelect = this.tryGetMatSelectInternals();
                if (!activeSelect?.panelOpen) {
                    this.triggerGeometryRafHandle = null;
                    return;
                }

                const currentRect = triggerElement.getBoundingClientRect();
                if (!this.lastTriggerRect || !this.areRectsEqual(currentRect, this.lastTriggerRect)) {
                    this.lastTriggerRect = currentRect;
                    this.scheduleOverlayReposition();
                }

                this.triggerGeometryRafHandle = this.requestAnimationFrame(monitorGeometry);
            };

            this.lastTriggerRect = triggerElement.getBoundingClientRect();
            this.triggerGeometryRafHandle = this.requestAnimationFrame(monitorGeometry);
        });

        this.scheduleOverlayReposition();
    }

    private stopOverlayAutoReposition(): void {
        if (this.triggerResizeObserver) {
            this.triggerResizeObserver.disconnect();
            this.triggerResizeObserver = null;
        }

        if (this.triggerGeometryRafHandle !== null) {
            this.cancelAnimationFrame(this.triggerGeometryRafHandle);
            this.triggerGeometryRafHandle = null;
        }

        if (this.overlayRafHandle !== null) {
            this.cancelAnimationFrame(this.overlayRafHandle);
            this.overlayRafHandle = null;
        }

        this.lastTriggerRect = null;
    }

    private applyOverlayReposition(overlayRef: OverlayRef): void {
        const strategy = overlayRef.getConfig().positionStrategy as FlexibleConnectedPositionStrategy;
        strategy.reapplyLastPosition();
    }

    private areRectsEqual(a: DOMRect | DOMRectReadOnly, b: DOMRect | DOMRectReadOnly): boolean {
        return a.top === b.top && a.left === b.left && a.width === b.width && a.height === b.height;
    }

    private requestAnimationFrame(callback: FrameRequestCallback): number {
        return window.requestAnimationFrame(callback);
    }

    private cancelAnimationFrame(handle: number): void {
        window.cancelAnimationFrame(handle);
        return;
    }

    private overlayRafHandle: number | null = null;
    private triggerResizeObserver: ResizeObserver | null = null;
    private triggerGeometryRafHandle: number | null = null;
    private lastTriggerRect: DOMRect | DOMRectReadOnly | null = null;

    private tryGetMatSelectInternals(): MatSelectOverlayInternals | null {
        try {
            const instance = this.select();
            return instance as MatSelectOverlayInternals;
        } catch {
            return null;
        }
    }

    private getOverlayRef(): OverlayRef | undefined {
        return this.tryGetMatSelectInternals()?._overlayDir?.overlayRef;
    }
}
