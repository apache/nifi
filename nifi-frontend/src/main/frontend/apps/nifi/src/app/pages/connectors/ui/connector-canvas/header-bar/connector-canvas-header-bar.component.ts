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

import { NgClass } from '@angular/common';
import { Component, inject, input, output } from '@angular/core';
import { EMPTY, Observable } from 'rxjs';
import { ComponentSearchResult, SearchResultsEntity } from '../../../../../state/shared';
import { ComponentType } from '@nifi/shared';
import { ConnectorService } from '../../../service/connector.service';
import { CanvasHeaderSearchComponent } from '../../../../../ui/common/canvas-header-search/canvas-header-search.component';

@Component({
    selector: 'connector-canvas-header-bar',
    standalone: true,
    imports: [NgClass, CanvasHeaderSearchComponent],
    templateUrl: './connector-canvas-header-bar.component.html',
    styleUrls: ['./connector-canvas-header-bar.component.scss']
})
export class ConnectorCanvasHeaderBarComponent {
    private connectorService = inject(ConnectorService);

    connectorId = input.required<string>();
    selectedComponentId = input<string | null>(null);
    graphControlsOpen = input<boolean>(true);

    backToConnectors = output<void>();
    goToComponent = output<{ id: string; type: ComponentType; groupId: string }>();
    toggleGraphControls = output<void>();

    protected searchFn = (query: string): Observable<SearchResultsEntity> => {
        const connectorId = this.connectorId();
        if (!connectorId) {
            return EMPTY;
        }
        return this.connectorService.searchConnector(connectorId, query);
    };

    onGoToComponent(event: { result: ComponentSearchResult; type: ComponentType }): void {
        this.goToComponent.emit({
            id: event.result.id,
            type: event.type,
            groupId: event.result.parentGroup?.id ?? event.result.groupId
        });
    }
}
