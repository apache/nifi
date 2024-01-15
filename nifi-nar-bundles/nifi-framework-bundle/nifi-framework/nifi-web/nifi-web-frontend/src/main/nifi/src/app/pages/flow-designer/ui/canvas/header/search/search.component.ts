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

import { Component, ElementRef, Input, OnInit, ViewChild } from '@angular/core';
import { FormBuilder, FormGroup, ReactiveFormsModule } from '@angular/forms';
import { initialState } from '../../../../state/flow/flow.reducer';
import { debounceTime, filter, switchMap, tap } from 'rxjs';
import { ComponentSearchResult, SearchService } from '../../../../service/search.service';
import {
    CdkConnectedOverlay,
    CdkOverlayOrigin,
    ConnectionPositionPair,
    OriginConnectionPosition,
    OverlayConnectionPosition
} from '@angular/cdk/overlay';
import { Store } from '@ngrx/store';
import { CanvasState } from '../../../../state';
import { ComponentType } from '../../../../../../state/shared';
import { NgForOf, NgIf, NgTemplateOutlet } from '@angular/common';
import { RouterLink } from '@angular/router';

@Component({
    selector: 'search',
    standalone: true,
    templateUrl: './search.component.html',
    imports: [ReactiveFormsModule, CdkOverlayOrigin, CdkConnectedOverlay, NgIf, NgTemplateOutlet, RouterLink, NgForOf],
    styleUrls: ['./search.component.scss']
})
export class Search implements OnInit {
    protected readonly ComponentType = ComponentType;

    @Input() currentProcessGroupId: string = initialState.id;
    @ViewChild('searchInput') searchInput!: CdkOverlayOrigin;

    private originPos: OriginConnectionPosition = {
        originX: 'end',
        originY: 'bottom'
    };
    private overlayPos: OverlayConnectionPosition = {
        overlayX: 'end',
        overlayY: 'top'
    };
    private position: ConnectionPositionPair = new ConnectionPositionPair(this.originPos, this.overlayPos, 0, 2);
    public positions: ConnectionPositionPair[] = [this.position];

    searchForm: FormGroup;
    searchInputVisible: boolean = false;

    searching: boolean = false;
    searchingResultsVisible: boolean = false;

    processorResults: ComponentSearchResult[] = [];
    connectionResults: ComponentSearchResult[] = [];
    processGroupResults: ComponentSearchResult[] = [];
    inputPortResults: ComponentSearchResult[] = [];
    outputPortResults: ComponentSearchResult[] = [];
    remoteProcessGroupResults: ComponentSearchResult[] = [];
    funnelResults: ComponentSearchResult[] = [];
    labelResults: ComponentSearchResult[] = [];
    controllerServiceNodeResults: ComponentSearchResult[] = [];
    parameterContextResults: ComponentSearchResult[] = [];
    parameterProviderNodeResults: ComponentSearchResult[] = [];
    parameterResults: ComponentSearchResult[] = [];

    constructor(
        private formBuilder: FormBuilder,
        private searchService: SearchService
    ) {
        this.searchForm = this.formBuilder.group({ searchBar: '' });
    }

    ngOnInit(): void {
        this.searchForm
            .get('searchBar')
            ?.valueChanges.pipe(
                filter((data) => data?.trim().length > 0),
                debounceTime(500),
                tap(() => (this.searching = true)),
                switchMap((query: string) => this.searchService.search(query, this.currentProcessGroupId))
            )
            .subscribe((response) => {
                const results = response.searchResultsDTO;

                this.processorResults = results.processorResults;
                this.connectionResults = results.connectionResults;
                this.processGroupResults = results.processGroupResults;
                this.inputPortResults = results.inputPortResults;
                this.outputPortResults = results.outputPortResults;
                this.remoteProcessGroupResults = results.remoteProcessGroupResults;
                this.funnelResults = results.funnelResults;
                this.labelResults = results.labelResults;
                this.controllerServiceNodeResults = results.controllerServiceNodeResults;
                this.parameterContextResults = results.parameterContextResults;
                this.parameterProviderNodeResults = results.parameterProviderNodeResults;
                this.parameterResults = results.parameterResults;

                this.searchingResultsVisible = true;
                this.searching = false;
            });
    }

    toggleSearchVisibility() {
        this.searchInputVisible = !this.searchInputVisible;

        if (this.searchInputVisible) {
            const inputRef: ElementRef = this.searchInput?.elementRef;
            if (inputRef) {
                inputRef.nativeElement.focus();
            }
        }
    }

    hasResults(): boolean {
        return (
            this.processorResults.length > 0 ||
            this.connectionResults.length > 0 ||
            this.processGroupResults.length > 0 ||
            this.inputPortResults.length > 0 ||
            this.outputPortResults.length > 0 ||
            this.remoteProcessGroupResults.length > 0 ||
            this.funnelResults.length > 0 ||
            this.labelResults.length > 0 ||
            this.controllerServiceNodeResults.length > 0 ||
            this.parameterContextResults.length > 0 ||
            this.parameterProviderNodeResults.length > 0 ||
            this.parameterResults.length > 0
        );
    }

    backdropClicked() {
        this.searchingResultsVisible = false;
        this.searchForm.get('searchBar')?.setValue('');
        this.searchInputVisible = false;

        this.processorResults = [];
        this.connectionResults = [];
        this.processGroupResults = [];
        this.inputPortResults = [];
        this.outputPortResults = [];
        this.remoteProcessGroupResults = [];
        this.funnelResults = [];
        this.labelResults = [];
        this.controllerServiceNodeResults = [];
        this.parameterContextResults = [];
        this.parameterProviderNodeResults = [];
        this.parameterResults = [];
    }
}
