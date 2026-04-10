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

import { Component, OnDestroy, OnInit, DestroyRef, inject, HostListener } from '@angular/core';
import { CommonModule } from '@angular/common';
import { Router } from '@angular/router';
import { MatButton } from '@angular/material/button';
import { MatDialog } from '@angular/material/dialog';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { Store } from '@ngrx/store';
import { selectUrl } from '@nifi/shared';
import { DocumentedType, RegistryClientEntity } from '../../../../state/shared';
import { combineLatest, distinctUntilChanged, filter, map, Observable, of } from 'rxjs';
import { NiFiState } from '../../../../state';
import { CanvasConfiguration } from '../../../../state/canvas-ui';
import { setConfiguration } from '../../../../state/canvas-ui/canvas-ui.actions';
import { CanvasComponent } from '../../../../ui/common/canvas/canvas.component';
import { Navigation } from '../../../../ui/common/navigation/navigation.component';
import * as ConnectorCanvasActions from '../../state/connector-canvas/connector-canvas.actions';
import * as ConnectorCanvasSelectors from '../../state/connector-canvas/connector-canvas.selectors';
import * as ConnectorCanvasEntityActions from '../../state/connector-canvas-entity/connector-canvas-entity.actions';

@Component({
    selector: 'connector-canvas',
    standalone: true,
    imports: [CommonModule, CanvasComponent, MatButton, Navigation],
    templateUrl: './connector-canvas.component.html'
})
export class ConnectorCanvasComponent implements OnInit, OnDestroy {
    private store = inject(Store<NiFiState>);
    private destroyRef = inject(DestroyRef);
    private router = inject(Router);
    private dialog = inject(MatDialog);

    currentConnectorId = '';
    currentProcessGroupId: string | null = null;
    selectedComponentIds: string[] = [];
    canNavigateToParent = false;

    // Subscribe to connector canvas state (flow data)
    labels$: Observable<unknown[]> = this.store.select(ConnectorCanvasSelectors.selectLabels);
    processors$: Observable<unknown[]> = this.store.select(ConnectorCanvasSelectors.selectProcessors);
    funnels$: Observable<unknown[]> = this.store.select(ConnectorCanvasSelectors.selectFunnels);
    allPorts$: Observable<unknown[]> = this.store.select(ConnectorCanvasSelectors.selectAllPorts);
    remoteProcessGroups$: Observable<unknown[]> = this.store.select(ConnectorCanvasSelectors.selectRemoteProcessGroups);
    processGroups$: Observable<unknown[]> = this.store.select(ConnectorCanvasSelectors.selectProcessGroups);
    connections$: Observable<unknown[]> = this.store.select(ConnectorCanvasSelectors.selectConnections);
    registryClients$: Observable<RegistryClientEntity[]> = this.store.select(
        ConnectorCanvasSelectors.selectRegistryClients
    );
    previewExtensions$: Observable<DocumentedType[]> = of([]);

    // Data ready signal for canvas - true when flow data is successfully loaded
    dataReady$: Observable<boolean> = this.store
        .select(ConnectorCanvasSelectors.selectLoadingStatus)
        .pipe(map((status) => status === 'success'));

    // Error state - true when flow load failed
    hasError$: Observable<boolean> = this.store
        .select(ConnectorCanvasSelectors.selectLoadingStatus)
        .pipe(map((status) => status === 'error'));

    skipTransform$ = this.store.select(ConnectorCanvasSelectors.selectSkipTransform);

    ngOnInit(): void {
        // Configure global canvas UI state - read-only view (no editing)
        const config: CanvasConfiguration = {
            features: {
                canEdit: false,
                canSelect: true
            }
        };
        this.store.dispatch(setConfiguration({ configuration: config }));

        // Subscribe to connector ID and process group ID from route and load flow data
        combineLatest([
            this.store.select(ConnectorCanvasSelectors.selectConnectorIdFromRoute),
            this.store.select(ConnectorCanvasSelectors.selectProcessGroupIdFromRoute),
            this.store.select(selectUrl)
        ])
            .pipe(
                // Skip if params are missing or if this is a controller-services route
                filter(
                    ([connectorId, processGroupId, url]) =>
                        connectorId != null && processGroupId != null && !url.includes('controller-services')
                ),
                distinctUntilChanged(
                    ([prevConnectorId, prevProcessGroupId], [currConnectorId, currProcessGroupId]) =>
                        prevConnectorId === currConnectorId && prevProcessGroupId === currProcessGroupId
                ),
                takeUntilDestroyed(this.destroyRef)
            )
            .subscribe(([connectorId, processGroupId]) => {
                this.currentConnectorId = connectorId!;
                this.currentProcessGroupId = processGroupId;
                this.store.dispatch(
                    ConnectorCanvasActions.loadConnectorFlow({
                        connectorId: connectorId!,
                        processGroupId: processGroupId!
                    })
                );
            });

        // Subscribe to parent process group ID for navigation
        this.store
            .select(ConnectorCanvasSelectors.selectParentProcessGroupId)
            .pipe(takeUntilDestroyed(this.destroyRef))
            .subscribe((parentProcessGroupId) => {
                this.canNavigateToParent = parentProcessGroupId != null;
            });
    }

    ngOnDestroy(): void {
        this.store.dispatch(ConnectorCanvasActions.resetConnectorCanvasState());
        this.store.dispatch(ConnectorCanvasEntityActions.resetConnectorCanvasEntityState());
    }

    onTransformChange(_event: { translate: { x: number; y: number }; scale: number }): void {
        // placeholder for future viewport persistence
    }

    onProcessGroupDoubleClick(event: { processGroupId: string }): void {
        this.store.dispatch(ConnectorCanvasActions.enterProcessGroup({ request: { id: event.processGroupId } }));
    }

    leaveGroupAction(): void {
        if (this.canNavigateToParent) {
            this.store.dispatch(ConnectorCanvasActions.leaveProcessGroup());
        }
    }

    @HostListener('window:keydown.escape', ['$event'])
    handleEscapeShortcut(event: Event): void {
        if (this.shouldProcessKeyboardEvent(event)) {
            this.leaveGroupAction();
        }
    }

    @HostListener('window:keydown.control.r', ['$event'])
    @HostListener('window:keydown.meta.r', ['$event'])
    handleRefreshShortcut(event: Event): void {
        if (this.shouldProcessKeyboardEvent(event)) {
            event.preventDefault();
            this.store.dispatch(
                ConnectorCanvasActions.loadConnectorFlow({
                    connectorId: this.currentConnectorId,
                    processGroupId: this.currentProcessGroupId!
                })
            );
        }
    }

    // =========================================================================
    // Keyboard Shortcuts
    // Typed as Event (not KeyboardEvent) because Angular 21's typeCheckHostBindings
    // infers $event as Event for key-specific bindings (angular/angular#40778).
    // =========================================================================

    private shouldProcessKeyboardEvent(event: Event): boolean {
        if (this.dialog.openDialogs.length > 0) {
            return false;
        }
        const target = event.target as HTMLElement;
        if (target?.classList.contains('search-input')) {
            return false;
        }
        const tagName = target?.tagName?.toLowerCase();
        return !['input', 'textarea', 'select'].includes(tagName);
    }

    returnToConnectorListing(): void {
        this.router.navigate(['/connectors']);
    }
}
