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

import { TestBed, fakeAsync, tick, discardPeriodicTasks } from '@angular/core/testing';
import { Component, Directive, Input } from '@angular/core';
import { Observable, of, throwError } from 'rxjs';
import { HttpErrorResponse } from '@angular/common/http';
import { CdkConnectedOverlay } from '@angular/cdk/overlay';
import { CanvasHeaderSearchComponent } from './canvas-header-search.component';
import { ComponentType } from '@nifi/shared';
import { ComponentSearchResult, SearchResultsEntity } from '../../../state/shared';

@Directive({ selector: '[cdkConnectedOverlay]', standalone: true })
class MockCdkConnectedOverlay {
    @Input() cdkConnectedOverlayOpen: any;
    @Input() cdkConnectedOverlayOrigin: any;
    @Input() cdkConnectedOverlayPositions: any;
    @Input() cdkConnectedOverlayHasBackdrop: any;
    @Input() cdkConnectedOverlayBackdropClass: any;
    @Input() cdkConnectedOverlayDisableClose: any;
}

function createMockResult(overrides: Partial<ComponentSearchResult> = {}): ComponentSearchResult {
    return {
        id: 'result-1',
        groupId: 'pg-1',
        parentGroup: { id: 'pg-1', name: 'Root' },
        versionedGroup: { id: '', name: '' },
        name: 'Test Component',
        matches: ['Name: Test Component'],
        ...overrides
    };
}

function createEmptySearchResults(): SearchResultsEntity {
    return {
        searchResultsDTO: {
            processorResults: [],
            connectionResults: [],
            processGroupResults: [],
            inputPortResults: [],
            outputPortResults: [],
            remoteProcessGroupResults: [],
            funnelResults: [],
            labelResults: [],
            controllerServiceNodeResults: [],
            parameterContextResults: [],
            parameterProviderNodeResults: [],
            parameterResults: []
        }
    };
}

@Component({
    standalone: true,
    imports: [CanvasHeaderSearchComponent],
    template: `
        <canvas-header-search
            [searchFn]="searchFn"
            [selectedComponentId]="selectedComponentId"
            (goToComponent)="onGoToComponent($event)">
        </canvas-header-search>
    `
})
class TestHostComponent {
    searchFn: (query: string) => Observable<SearchResultsEntity> = () => of(createEmptySearchResults());
    selectedComponentId: string | null = null;
    onGoToComponent = vi.fn();
}

interface SetupOptions {
    selectedComponentId?: string | null;
    searchResponse?: SearchResultsEntity;
    searchError?: HttpErrorResponse;
}

async function setup(options: SetupOptions = {}) {
    const mockSearchFn = vi.fn().mockReturnValue(of(options.searchResponse ?? createEmptySearchResults()));

    if (options.searchError) {
        mockSearchFn.mockReturnValue(throwError(() => options.searchError));
    }

    await TestBed.configureTestingModule({
        imports: [TestHostComponent]
    })
        .overrideComponent(CanvasHeaderSearchComponent, {
            remove: { imports: [CdkConnectedOverlay] },
            add: { imports: [MockCdkConnectedOverlay] }
        })
        .compileComponents();

    const hostFixture = TestBed.createComponent(TestHostComponent);
    const host = hostFixture.componentInstance;

    host.searchFn = mockSearchFn;

    if (options.selectedComponentId !== undefined) {
        host.selectedComponentId = options.selectedComponentId;
    }

    hostFixture.detectChanges();

    const searchEl = hostFixture.debugElement.children[0];
    const searchComponent = searchEl.componentInstance as CanvasHeaderSearchComponent;

    return { hostFixture, host, searchComponent, mockSearchFn };
}

describe('CanvasHeaderSearchComponent', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    describe('Component initialization', () => {
        it('should create', async () => {
            const { searchComponent } = await setup();
            expect(searchComponent).toBeTruthy();
        });

        it('should render the search container', async () => {
            const { hostFixture } = await setup();
            const container = hostFixture.nativeElement.querySelector('.search-container');
            expect(container).toBeTruthy();
        });

        it('should render the search toggle button', async () => {
            const { hostFixture } = await setup();
            const button = hostFixture.nativeElement.querySelector('.search-container button');
            expect(button).toBeTruthy();
        });

        it('should have search input hidden by default', async () => {
            const { searchComponent } = await setup();
            expect(searchComponent.searchInputVisible()).toBe(false);
        });
    });

    describe('Search visibility toggle', () => {
        it('should show search input when toggle button is clicked', async () => {
            const { hostFixture, searchComponent } = await setup();
            const button = hostFixture.nativeElement.querySelector('.search-container button');
            button.click();
            expect(searchComponent.searchInputVisible()).toBe(true);
        });

        it('should hide search input when toggle button is clicked twice', async () => {
            const { hostFixture, searchComponent } = await setup();
            const button = hostFixture.nativeElement.querySelector('.search-container button');
            button.click();
            button.click();
            expect(searchComponent.searchInputVisible()).toBe(false);
        });
    });

    describe('Search execution', () => {
        it('should call searchFn after debounce when input has value', fakeAsync(async () => {
            const { searchComponent, mockSearchFn } = await setup();

            searchComponent.searchControl.setValue('test query');
            tick(500);

            expect(mockSearchFn).toHaveBeenCalledWith('test query');
        }));

        it('should not call searchFn for empty input', fakeAsync(async () => {
            const { searchComponent, mockSearchFn } = await setup();

            searchComponent.searchControl.setValue('   ');
            tick(500);

            expect(mockSearchFn).not.toHaveBeenCalled();
        }));

        it('should set searching to false after request completes', fakeAsync(async () => {
            const { searchComponent } = await setup();

            searchComponent.searchControl.setValue('test');
            tick(500);

            expect(searchComponent.searching()).toBe(false);
        }));

        it('should populate results on successful search', fakeAsync(async () => {
            const processor = createMockResult({ id: 'p1', name: 'MyProcessor' });
            const searchResponse = createEmptySearchResults();
            searchResponse.searchResultsDTO.processorResults = [processor];

            const { searchComponent } = await setup({ searchResponse });

            searchComponent.searchControl.setValue('My');
            tick(500);

            expect(searchComponent.results().processorResults).toEqual([processor]);
            expect(searchComponent.searchingResultsVisible()).toBe(true);
        }));

        it('should populate all result categories', fakeAsync(async () => {
            const searchResponse = createEmptySearchResults();
            searchResponse.searchResultsDTO.processorResults = [createMockResult({ id: 'p1' })];
            searchResponse.searchResultsDTO.connectionResults = [createMockResult({ id: 'c1' })];
            searchResponse.searchResultsDTO.processGroupResults = [createMockResult({ id: 'pg1' })];
            searchResponse.searchResultsDTO.controllerServiceNodeResults = [createMockResult({ id: 'cs1' })];

            const { searchComponent } = await setup({ searchResponse });

            searchComponent.searchControl.setValue('test');
            tick(500);

            expect(searchComponent.results().processorResults.length).toBe(1);
            expect(searchComponent.results().connectionResults.length).toBe(1);
            expect(searchComponent.results().processGroupResults.length).toBe(1);
            expect(searchComponent.results().controllerServiceNodeResults.length).toBe(1);
        }));

        it('should hide results and stop searching on error', fakeAsync(async () => {
            const { searchComponent } = await setup({
                searchError: new HttpErrorResponse({ status: 500, statusText: 'Server Error' })
            });

            searchComponent.searchControl.setValue('fail');
            tick(500);

            expect(searchComponent.searchingResultsVisible()).toBe(false);
            expect(searchComponent.searching()).toBe(false);
        }));
    });

    describe('hasResults', () => {
        it('should return false when all results are empty', async () => {
            const { searchComponent } = await setup();
            expect(searchComponent.hasResults()).toBe(false);
        });

        it('should return true when processorResults has entries', fakeAsync(async () => {
            const processor = createMockResult();
            const searchResponse = createEmptySearchResults();
            searchResponse.searchResultsDTO.processorResults = [processor];

            const { searchComponent } = await setup({ searchResponse });

            searchComponent.searchControl.setValue('test');
            tick(500);

            expect(searchComponent.hasResults()).toBe(true);
        }));

        it('should return true when controllerServiceNodeResults has entries', fakeAsync(async () => {
            const searchResponse = createEmptySearchResults();
            searchResponse.searchResultsDTO.controllerServiceNodeResults = [createMockResult({ id: 'cs1' })];

            const { searchComponent } = await setup({ searchResponse });

            searchComponent.searchControl.setValue('test');
            tick(500);

            expect(searchComponent.hasResults()).toBe(true);
        }));
    });

    describe('Result click', () => {
        it('should emit goToComponent with result and type when a result is clicked', fakeAsync(async () => {
            const processor = createMockResult({ id: 'p1', groupId: 'pg-1' });
            const searchResponse = createEmptySearchResults();
            searchResponse.searchResultsDTO.processorResults = [processor];

            const { host, searchComponent } = await setup({ searchResponse });

            searchComponent.searchControl.setValue('test');
            tick(500);

            searchComponent.resultClicked(processor, ComponentType.Processor);

            expect(host.onGoToComponent).toHaveBeenCalledWith({
                result: processor,
                type: ComponentType.Processor
            });
        }));
    });

    describe('Backdrop click', () => {
        it('should clear results on backdrop click', fakeAsync(async () => {
            const processor = createMockResult();
            const searchResponse = createEmptySearchResults();
            searchResponse.searchResultsDTO.processorResults = [processor];

            const { searchComponent } = await setup({ searchResponse });

            searchComponent.searchControl.setValue('test');
            tick(500);
            expect(searchComponent.searchingResultsVisible()).toBe(true);

            const mockEvent = { stopPropagation: vi.fn(), preventDefault: vi.fn() } as unknown as MouseEvent;
            searchComponent.backdropClicked(mockEvent);

            expect(searchComponent.searchingResultsVisible()).toBe(false);
            expect(searchComponent.results().processorResults).toEqual([]);
            expect(searchComponent.searchControl.value).toBe('');
            discardPeriodicTasks();
        }));
    });

    describe('Keyboard handling', () => {
        it('should hide results on Escape key', fakeAsync(async () => {
            const processor = createMockResult();
            const searchResponse = createEmptySearchResults();
            searchResponse.searchResultsDTO.processorResults = [processor];

            const { searchComponent } = await setup({ searchResponse });

            searchComponent.searchControl.setValue('test');
            tick(500);

            searchComponent.onKeydown({ key: 'Escape' } as KeyboardEvent);

            expect(searchComponent.searchingResultsVisible()).toBe(false);
            discardPeriodicTasks();
        }));

        it('should not hide results on non-Escape key', fakeAsync(async () => {
            const processor = createMockResult();
            const searchResponse = createEmptySearchResults();
            searchResponse.searchResultsDTO.processorResults = [processor];

            const { searchComponent } = await setup({ searchResponse });

            searchComponent.searchControl.setValue('test');
            tick(500);

            searchComponent.onKeydown({ key: 'Enter' } as KeyboardEvent);

            expect(searchComponent.searchingResultsVisible()).toBe(true);
        }));
    });

    describe('getSearchMatchTipInput', () => {
        it('should return matches from the result', async () => {
            const { searchComponent } = await setup();
            const result = createMockResult({ matches: ['Name: Foo', 'Type: Bar'] });

            const tipInput = searchComponent.getSearchMatchTipInput(result);

            expect(tipInput).toEqual({ matches: ['Name: Foo', 'Type: Bar'] });
        });
    });

    describe('Accessibility', () => {
        it('should have aria-label on the search button', async () => {
            const { hostFixture } = await setup();
            const button = hostFixture.nativeElement.querySelector('.search-container button');
            expect(button.getAttribute('aria-label')).toBe('Search canvas');
        });

        it('should have aria-expanded="false" on the search button by default', async () => {
            const { hostFixture } = await setup();
            const button = hostFixture.nativeElement.querySelector('.search-container button');
            expect(button.getAttribute('aria-expanded')).toBe('false');
        });

        it('should have aria-expanded="true" on the search button after clicking', async () => {
            const { hostFixture } = await setup();
            const button = hostFixture.nativeElement.querySelector('.search-container button');
            button.click();
            hostFixture.detectChanges();
            expect(button.getAttribute('aria-expanded')).toBe('true');
        });

        it('should have aria-label on the search input', async () => {
            const { hostFixture } = await setup();
            const input = hostFixture.nativeElement.querySelector('.search-input');
            expect(input.getAttribute('aria-label')).toBe('Search canvas components');
        });
    });
});
