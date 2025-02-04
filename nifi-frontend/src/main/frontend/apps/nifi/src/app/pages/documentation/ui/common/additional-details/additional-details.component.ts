/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { AfterViewChecked, Component, Input, OnDestroy, viewChild } from '@angular/core';
import { ConfigurableExtensionDefinition } from '../../../state';
import { MatExpansionModule, MatExpansionPanel } from '@angular/material/expansion';
import { NiFiState } from '../../../../../state';
import { Store } from '@ngrx/store';
import {
    loadAdditionalDetails,
    resetAdditionalDetailsState
} from '../../../state/additional-details/additional-details.actions';
import { selectAdditionalDetailsState } from '../../../state/additional-details/additional-details.selectors';
import { NgxSkeletonLoaderModule } from 'ngx-skeleton-loader';
import { AdditionalDetailsState } from '../../../state/additional-details';
import { MarkdownComponent } from 'ngx-markdown';

@Component({
    selector: 'additional-details',
    imports: [MatExpansionModule, NgxSkeletonLoaderModule, MarkdownComponent],
    templateUrl: './additional-details.component.html',
    styleUrl: './additional-details.component.scss'
})
export class AdditionalDetailsComponent implements AfterViewChecked, OnDestroy {
    @Input() set configurableExtensionDefinition(configurableExtensionDefinition: ConfigurableExtensionDefinition) {
        if (
            this.group !== configurableExtensionDefinition.group ||
            this.artifact !== configurableExtensionDefinition.artifact ||
            this.version !== configurableExtensionDefinition.version ||
            this.type !== configurableExtensionDefinition.type
        ) {
            this.group = configurableExtensionDefinition.group;
            this.artifact = configurableExtensionDefinition.artifact;
            this.version = configurableExtensionDefinition.version;
            this.type = configurableExtensionDefinition.type;

            this.expanded = false;

            if (this.viewChecked) {
                this.additionalDetailsPanel().close();
            }
        }
    }

    additionalDetailsState = this.store.selectSignal(selectAdditionalDetailsState);
    additionalDetailsPanel = viewChild.required(MatExpansionPanel);

    group: string | null = null;
    artifact: string | null = null;
    version: string | null = null;
    type: string | null = null;

    private expanded = false;
    private viewChecked = false;

    constructor(private store: Store<NiFiState>) {}

    ngAfterViewChecked(): void {
        this.viewChecked = true;
    }

    opened(group: string, artifact: string, version: string, type: string): void {
        if (!this.expanded) {
            this.store.dispatch(
                loadAdditionalDetails({
                    coordinates: {
                        group,
                        artifact,
                        version,
                        type
                    }
                })
            );

            this.expanded = true;
        }
    }

    isInitialLoading(state: AdditionalDetailsState): boolean {
        return state.additionalDetails === null && state.error === null;
    }

    ngOnDestroy(): void {
        this.store.dispatch(resetAdditionalDetailsState());
    }
}
