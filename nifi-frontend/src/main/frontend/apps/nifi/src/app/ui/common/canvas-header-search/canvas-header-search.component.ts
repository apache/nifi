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
    afterNextRender,
    Component,
    DestroyRef,
    inject,
    Injector,
    input,
    output,
    signal,
    viewChild
} from '@angular/core';
import { FormControl, ReactiveFormsModule } from '@angular/forms';
import { catchError, debounceTime, EMPTY, filter, Observable, switchMap, tap } from 'rxjs';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import {
    CdkConnectedOverlay,
    CdkOverlayOrigin,
    ConnectionPositionPair,
    OriginConnectionPosition,
    OverlayConnectionPosition
} from '@angular/cdk/overlay';
import { NgTemplateOutlet } from '@angular/common';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { ComponentSearchResult, SearchMatchTipInput, SearchResults, SearchResultsEntity } from '../../../state/shared';
import { ComponentType, NifiTooltipDirective } from '@nifi/shared';
import { SearchMatchTip } from '../tooltips/search-match-tip/search-match-tip.component';

const EMPTY_SEARCH_RESULTS: SearchResults = {
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
};

@Component({
    selector: 'canvas-header-search',
    standalone: true,
    imports: [
        ReactiveFormsModule,
        CdkOverlayOrigin,
        CdkConnectedOverlay,
        NgTemplateOutlet,
        MatFormFieldModule,
        MatInputModule,
        NifiTooltipDirective
    ],
    templateUrl: './canvas-header-search.component.html',
    styleUrls: ['./canvas-header-search.component.scss']
})
export class CanvasHeaderSearchComponent {
    private destroyRef = inject(DestroyRef);
    private injector = inject(Injector);

    protected readonly ComponentType = ComponentType;
    protected readonly SearchMatchTip = SearchMatchTip;

    searchFn = input.required<(query: string) => Observable<SearchResultsEntity>>();
    selectedComponentId = input<string | null>(null);

    goToComponent = output<{ result: ComponentSearchResult; type: ComponentType }>();

    searchInput = viewChild.required('searchInput', { read: CdkOverlayOrigin });

    private originPos: OriginConnectionPosition = { originX: 'end', originY: 'bottom' };
    private overlayPos: OverlayConnectionPosition = { overlayX: 'end', overlayY: 'top' };
    private position = new ConnectionPositionPair(this.originPos, this.overlayPos, 0, 2);
    positions: ConnectionPositionPair[] = [this.position];

    searchControl = new FormControl('');
    searchInputVisible = signal(false);
    searching = signal(false);
    searchingResultsVisible = signal(false);
    results = signal<SearchResults>(EMPTY_SEARCH_RESULTS);

    constructor() {
        this.searchControl.valueChanges
            .pipe(
                takeUntilDestroyed(this.destroyRef),
                filter((query): query is string => query !== null && query.trim().length > 0),
                debounceTime(500),
                tap(() => this.searching.set(true)),
                switchMap((query) =>
                    this.searchFn()(query).pipe(
                        catchError((_err: unknown) => {
                            this.searchingResultsVisible.set(false);
                            this.searching.set(false);
                            return EMPTY;
                        })
                    )
                )
            )
            .subscribe((response) => {
                const dto = response.searchResultsDTO;
                this.results.set({
                    processorResults: dto.processorResults,
                    connectionResults: dto.connectionResults,
                    processGroupResults: dto.processGroupResults,
                    inputPortResults: dto.inputPortResults,
                    outputPortResults: dto.outputPortResults,
                    remoteProcessGroupResults: dto.remoteProcessGroupResults,
                    funnelResults: dto.funnelResults,
                    labelResults: dto.labelResults,
                    controllerServiceNodeResults: dto.controllerServiceNodeResults ?? [],
                    parameterContextResults: dto.parameterContextResults ?? [],
                    parameterProviderNodeResults: dto.parameterProviderNodeResults ?? [],
                    parameterResults: dto.parameterResults ?? []
                });
                this.searchingResultsVisible.set(true);
                this.searching.set(false);
            });
    }

    toggleSearchVisibility(): void {
        this.searchInputVisible.update((v) => !v);

        if (this.searchInputVisible()) {
            afterNextRender(
                () => {
                    this.searchInput().elementRef.nativeElement.focus();
                },
                { injector: this.injector }
            );
        }
    }

    hasResults(): boolean {
        return Object.values(this.results()).some((arr) => arr.length > 0);
    }

    backdropClicked(event: MouseEvent): void {
        event.stopPropagation();
        event.preventDefault();
        this.clearResults();
    }

    private clearResults(): void {
        this.searchingResultsVisible.set(false);
        this.searchControl.setValue('');
        this.results.set(EMPTY_SEARCH_RESULTS);
    }

    getSearchMatchTipInput(result: ComponentSearchResult): SearchMatchTipInput {
        return { matches: result.matches };
    }

    resultClicked(result: ComponentSearchResult, componentType: ComponentType): void {
        this.goToComponent.emit({
            result,
            type: componentType
        });
    }

    onKeydown(event: KeyboardEvent): void {
        if (event.key === 'Escape') {
            this.searchingResultsVisible.set(false);
        }
    }
}
