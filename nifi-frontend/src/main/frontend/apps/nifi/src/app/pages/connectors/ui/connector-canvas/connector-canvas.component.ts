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

import { Component, computed, OnDestroy, OnInit, DestroyRef, inject, HostListener, viewChild } from '@angular/core';
import { CommonModule } from '@angular/common';
import { Router } from '@angular/router';
import { MatButton } from '@angular/material/button';
import { MatDialog } from '@angular/material/dialog';
import { MatSidenav, MatSidenavContainer, MatSidenavContent } from '@angular/material/sidenav';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { Store } from '@ngrx/store';
import { ComponentType, selectRouteParams, selectUrl, Storage } from '@nifi/shared';
import { DocumentedType, RegistryClientEntity } from '../../../../state/shared';
import { combineLatest, distinctUntilChanged, filter, map, Observable, of } from 'rxjs';
import { NiFiState } from '../../../../state';
import { selectCurrentUser } from '../../../../state/current-user/current-user.selectors';
import { CanvasConfiguration } from '../../../../state/canvas-ui';
import { setConfiguration } from '../../../../state/canvas-ui/canvas-ui.actions';
import { CanvasComponent } from '../../../../ui/common/canvas/canvas.component';
import { ContextMenuContext } from '../../../../ui/common/canvas/canvas.types';
import {
    ContextMenuDefinition,
    ContextMenuDefinitionProvider,
    ContextMenuItemDefinition
} from '../../../../ui/common/context-menu/context-menu.component';
import { Navigation } from '../../../../ui/common/navigation/navigation.component';
import { ConnectorCanvasHeaderBarComponent } from './header-bar/connector-canvas-header-bar.component';
import { ConnectorCanvasFooterComponent } from './footer/footer.component';
import { ConnectorGraphControls } from './graph-controls/connector-graph-controls.component';
import { ContextErrorBanner } from '../../../../ui/common/context-error-banner/context-error-banner.component';
import { ErrorContextKey } from '../../../../state/error';
import * as ConnectorCanvasActions from '../../state/connector-canvas/connector-canvas.actions';
import * as ConnectorCanvasSelectors from '../../state/connector-canvas/connector-canvas.selectors';
import * as ConnectorCanvasEntityActions from '../../state/connector-canvas-entity/connector-canvas-entity.actions';
import {
    selectConnectorCanvasEntity,
    selectConnectorCanvasEntitySaving
} from '../../state/connector-canvas-entity/connector-canvas-entity.selectors';
import { getComponentStateAndOpenDialog } from '../../../../state/component-state/component-state.actions';

@Component({
    selector: 'connector-canvas',
    standalone: true,
    imports: [
        CommonModule,
        CanvasComponent,
        MatButton,
        MatSidenav,
        MatSidenavContainer,
        MatSidenavContent,
        Navigation,
        ConnectorCanvasHeaderBarComponent,
        ConnectorCanvasFooterComponent,
        ConnectorGraphControls,
        ContextErrorBanner
    ],
    templateUrl: './connector-canvas.component.html'
})
export class ConnectorCanvasComponent implements OnInit, OnDestroy {
    private store = inject(Store<NiFiState>);
    private destroyRef = inject(DestroyRef);
    private router = inject(Router);
    private dialog = inject(MatDialog);
    private storage = inject(Storage);

    private static readonly CONTROL_VISIBILITY_KEY = 'graph-control-visibility';
    private static readonly GRAPH_CONTROL_KEY = 'connector-graph-controls';

    canvasComponent = viewChild.required(CanvasComponent);

    currentConnectorId = '';
    currentProcessGroupId: string | null = null;
    selectedComponentIds: string[] = [];
    canNavigateToParent = false;
    skipTransform = this.store.selectSignal(ConnectorCanvasSelectors.selectSkipTransform);
    graphControlsOpen = true;

    connectorEntity = this.store.selectSignal(selectConnectorCanvasEntity);
    entitySaving = this.store.selectSignal(selectConnectorCanvasEntitySaving);

    protected readonly ErrorContextKey = ErrorContextKey;

    constructor() {
        try {
            const item: { [key: string]: boolean } | null = this.storage.getItem(
                ConnectorCanvasComponent.CONTROL_VISIBILITY_KEY
            );
            if (item) {
                this.graphControlsOpen = item[ConnectorCanvasComponent.GRAPH_CONTROL_KEY] === true;
            }
        } catch (_e) {
            // likely could not parse item... ignoring
        }
    }

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

    private currentUser = this.store.selectSignal(selectCurrentUser);
    canAccessProvenance = computed(() => this.currentUser().provenancePermissions.canRead);

    // =========================================================================
    // Context Menu
    // =========================================================================

    private currentContextMenuContext: ContextMenuContext | null = null;

    contextMenuProvider: ContextMenuDefinitionProvider = {
        getMenu: (menuId: string): ContextMenuDefinition | undefined => {
            if (menuId !== 'root') {
                return undefined;
            }

            const context = this.currentContextMenuContext;
            let menuItems: ContextMenuItemDefinition[] = [];

            if (context?.targetType === 'canvas') {
                menuItems = [
                    {
                        text: 'Refresh',
                        clazz: 'fa fa-refresh',
                        condition: () => true,
                        action: () => this.refreshAction(),
                        shortcut: { control: true, code: 'R' }
                    },
                    {
                        text: 'Leave Group',
                        clazz: 'fa fa-level-up',
                        condition: () => this.canNavigateToParent,
                        action: () => this.leaveGroupAction(),
                        shortcut: { code: 'ESC' }
                    }
                ];
            } else if (context?.targetType === 'component') {
                const clicked = context.clickedComponent;
                const isSingleSelection = context.selectedComponents.length <= 1;
                const isProcessGroup = clicked?.ui.componentType === ComponentType.ProcessGroup;
                const isConnection = clicked?.ui.componentType === ComponentType.Connection;
                const isProcessor = clicked?.ui.componentType === ComponentType.Processor;
                const isFunnel = clicked?.ui.componentType === ComponentType.Funnel;
                const isProvenanceTarget =
                    !!clicked &&
                    isSingleSelection &&
                    !isProcessGroup &&
                    !isConnection &&
                    clicked.ui.componentType !== ComponentType.RemoteProcessGroup &&
                    clicked.ui.componentType !== ComponentType.Label;
                const isViewConfigurationTarget =
                    !!clicked && isSingleSelection && !isFunnel && clicked.entity.permissions.canRead === true;

                menuItems = [
                    {
                        text: 'View Configuration',
                        clazz: 'fa fa-gear',
                        condition: () => isViewConfigurationTarget,
                        action: () => this.viewConfigurationAction(clicked!.entity, clicked!.ui.componentType)
                    },
                    {
                        text: 'Enter Group',
                        clazz: 'fa fa-sign-in',
                        condition: () => isProcessGroup && isSingleSelection,
                        action: () => this.enterGroupAction(clicked!.entity.id)
                    },
                    { isSeparator: true },
                    {
                        text: 'View Data Provenance',
                        clazz: 'icon icon-provenance',
                        condition: () => isProvenanceTarget && this.canAccessProvenance(),
                        action: () => this.viewDataProvenanceAction(clicked!.entity.id, clicked!.ui.componentType)
                    },
                    {
                        text: 'View State',
                        clazz: 'fa fa-tasks',
                        condition: () =>
                            isProcessor &&
                            isSingleSelection &&
                            clicked!.entity.component?.persistsState === true &&
                            clicked!.entity.permissions.canRead === true &&
                            clicked!.entity.permissions.canWrite === true,
                        action: () => this.viewProcessorStateAction(clicked!.entity)
                    },
                    { isSeparator: true },
                    this.getCenterInViewMenuItem()
                ];
            }

            return { id: menuId, menuItems };
        },
        filterMenuItem: (menuItem: ContextMenuItemDefinition): boolean => {
            return menuItem.condition ? menuItem.condition(null) : true;
        },
        menuItemClicked: (menuItem: ContextMenuItemDefinition, event: MouseEvent): void => {
            menuItem.action?.(null, event);
        }
    };

    onContextMenuOpened(context: ContextMenuContext): void {
        this.currentContextMenuContext = context;
    }

    private getCenterInViewMenuItem(): ContextMenuItemDefinition {
        return {
            text: 'Center In View',
            clazz: 'fa fa-crosshairs',
            condition: () => true,
            action: () => this.centerInViewAction()
        };
    }

    // Data ready signal for canvas - true when flow data is successfully loaded
    dataReady$: Observable<boolean> = this.store
        .select(ConnectorCanvasSelectors.selectLoadingStatus)
        .pipe(map((status) => status === 'success'));

    // Error state - true when flow load failed
    hasError$: Observable<boolean> = this.store
        .select(ConnectorCanvasSelectors.selectLoadingStatus)
        .pipe(map((status) => status === 'error'));

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

        // Subscribe to route params for selection tracking
        this.store
            .select(selectRouteParams)
            .pipe(takeUntilDestroyed(this.destroyRef))
            .subscribe((params) => {
                if (params?.['type'] && params?.['componentId']) {
                    this.selectedComponentIds = [params['componentId']];
                } else if (params?.['ids']) {
                    this.selectedComponentIds = params['ids'].split(',');
                } else {
                    this.selectedComponentIds = [];
                }
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

    onComponentDoubleClick(event: { entity: any; componentType: ComponentType }): void {
        if (event.entity?.permissions?.canRead === true) {
            this.viewConfigurationAction(event.entity, event.componentType);
        }
    }

    viewConfigurationAction(entity: any, componentType: ComponentType): void {
        this.store.dispatch(
            ConnectorCanvasActions.viewComponentConfiguration({
                request: { entity, componentType }
            })
        );
    }

    onSelectComponents(components: Array<{ id: string; type: ComponentType }>): void {
        this.store.dispatch(
            ConnectorCanvasActions.selectComponents({
                request: {
                    components: components.map((c) => ({
                        id: c.id,
                        componentType: c.type
                    }))
                }
            })
        );
    }

    onDeselectAll(): void {
        this.store.dispatch(ConnectorCanvasActions.deselectAllComponents());
    }

    onCanvasInitialized(): void {
        if (this.selectedComponentIds.length > 0) {
            if (this.skipTransform()) {
                this.store.dispatch(ConnectorCanvasActions.setSkipTransform({ skipTransform: false }));
            } else {
                this.canvasComponent().centerOnSelection(false, 1);
            }
        }
    }

    // =========================================================================
    // Shared Actions (used by both context menu and keyboard shortcuts)
    // =========================================================================

    refreshAction(): void {
        if (this.currentConnectorId && this.currentProcessGroupId) {
            this.store.dispatch(
                ConnectorCanvasActions.loadConnectorFlow({
                    connectorId: this.currentConnectorId,
                    processGroupId: this.currentProcessGroupId
                })
            );
        }
    }

    leaveGroupAction(): void {
        if (this.canNavigateToParent) {
            this.store.dispatch(ConnectorCanvasActions.leaveProcessGroup());
        }
    }

    enterGroupAction(processGroupId: string): void {
        this.store.dispatch(ConnectorCanvasActions.enterProcessGroup({ request: { id: processGroupId } }));
    }

    viewDataProvenanceAction(componentId: string, componentType: ComponentType): void {
        this.store.dispatch(
            ConnectorCanvasActions.navigateToProvenanceForComponent({ id: componentId, componentType })
        );
    }

    viewProcessorStateAction(processorEntity: any): void {
        this.store.dispatch(
            getComponentStateAndOpenDialog({
                request: {
                    componentName: processorEntity.component.name,
                    componentId: processorEntity.id,
                    componentType: ComponentType.Processor,
                    canClear: this.canClearProcessorState(processorEntity),
                    connectorId: this.currentConnectorId
                }
            })
        );
    }

    private canClearProcessorState(processorEntity: any): boolean {
        const runStatus = processorEntity.status?.aggregateSnapshot?.runStatus;
        const activeThreadCount = processorEntity.status?.aggregateSnapshot?.activeThreadCount || 0;
        return runStatus !== 'Running' && activeThreadCount === 0;
    }

    centerInViewAction(): void {
        this.canvasComponent().centerOnSelection(true);
    }

    // =========================================================================
    // Keyboard Shortcuts
    // Typed as Event (not KeyboardEvent) because Angular 21's typeCheckHostBindings
    // infers $event as Event for key-specific bindings (angular/angular#40778).
    // =========================================================================

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
            this.refreshAction();
        }
    }

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

    toggleGraphControls(): void {
        this.graphControlsOpen = !this.graphControlsOpen;

        let item: { [key: string]: boolean } | null = this.storage.getItem(
            ConnectorCanvasComponent.CONTROL_VISIBILITY_KEY
        );
        if (item == null) {
            item = {};
        }

        item[ConnectorCanvasComponent.GRAPH_CONTROL_KEY] = this.graphControlsOpen;
        this.storage.setItem(ConnectorCanvasComponent.CONTROL_VISIBILITY_KEY, item);
    }

    onSearchGoToComponent(event: { id: string; type: ComponentType; groupId: string }): void {
        if (event.type === ComponentType.ParameterProvider) {
            this.router.navigate(['/settings', 'parameter-providers', event.id]);
            return;
        }

        if (event.groupId === this.currentProcessGroupId) {
            this.onSelectComponents([{ id: event.id, type: event.type }]);
            this.canvasComponent().centerOnComponent(event.id, event.type);
        } else {
            this.store.dispatch(ConnectorCanvasActions.setSkipTransform({ skipTransform: false }));
            this.router.navigate([
                '/connectors',
                this.currentConnectorId,
                'canvas',
                event.groupId,
                event.type,
                event.id
            ]);
        }
    }

    returnToConnectorListing(): void {
        this.router.navigate(['/connectors']);
    }
}
