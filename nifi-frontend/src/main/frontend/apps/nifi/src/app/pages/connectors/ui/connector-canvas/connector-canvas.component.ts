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
    Component,
    computed,
    OnDestroy,
    OnInit,
    DestroyRef,
    inject,
    HostListener,
    signal,
    viewChild
} from '@angular/core';
import { CommonModule } from '@angular/common';
import { CdkDragEnd } from '@angular/cdk/drag-drop';
import { Router } from '@angular/router';
import { MatButton } from '@angular/material/button';
import { MatDialog } from '@angular/material/dialog';
import { MatSidenav, MatSidenavContainer, MatSidenavContent } from '@angular/material/sidenav';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { Actions, ofType } from '@ngrx/effects';
import { Store } from '@ngrx/store';
import {
    ComponentType,
    ConnectionEntity,
    FunnelEntity,
    LabelEntity,
    PortEntity,
    Position,
    ProcessGroupEntity,
    ProcessorEntity,
    RemoteProcessGroupEntity,
    RevisionRequest,
    selectRouteParams,
    selectUrl,
    Storage
} from '@nifi/shared';
import { RegistryClientEntity } from '../../../../state/shared';
import { combineLatest, distinctUntilChanged, filter, map, Observable } from 'rxjs';
import { NiFiState } from '../../../../state';
import { CanvasConfiguration } from '../../../../state/canvas-ui';
import { setConfiguration } from '../../../../state/canvas-ui/canvas-ui.actions';
import { CanvasComponent } from '../../../../ui/common/canvas/canvas.component';
import { BirdseyeComponentData, BirdseyeTransform } from '../../../../ui/common/birdseye/birdseye.types';
import { ContextMenuContext, Dimension, DragEndBaselineItem } from '../../../../ui/common/canvas/canvas.types';
import { Navigation } from '../../../../ui/common/navigation/navigation.component';
import { ConnectorCanvasHeaderBarComponent } from './header-bar/connector-canvas-header-bar.component';
import { ConnectorCanvasFooterComponent } from './footer/footer.component';
import { ConnectorGraphControls } from './graph-controls/connector-graph-controls.component';
import { ContextErrorBanner } from '../../../../ui/common/context-error-banner/context-error-banner.component';
import { ErrorContextKey } from '../../../../state/error';
import { selectAbout } from '../../../../state/about/about.selectors';
import { selectClusterSummary } from '../../../../state/cluster-summary/cluster-summary.selectors';
import { ProvenanceEvent } from '../../../../state/shared';
import * as ConnectorCanvasActions from '../../state/connector-canvas/connector-canvas.actions';
import * as ConnectorCanvasSelectors from '../../state/connector-canvas/connector-canvas.selectors';
import * as ConnectorCanvasEntityActions from '../../state/connector-canvas-entity/connector-canvas-entity.actions';
import * as ConnectorProvenanceActions from '../../state/connector-provenance-preview/connector-provenance-preview.actions';
import {
    selectConnectorProvenanceError,
    selectConnectorProvenanceEvents,
    selectConnectorProvenanceStatus
} from '../../state/connector-provenance-preview/connector-provenance-preview.selectors';
import {
    DownloadContentRequest,
    ReplayEventRequest,
    ViewContentRequest
} from '../../state/connector-provenance-preview';
import {
    selectConnectorCanvasEntity,
    selectConnectorCanvasEntitySaving
} from '../../state/connector-canvas-entity/connector-canvas-entity.selectors';
import { Client } from '../../../../service/client.service';
import { ConnectablePolicy, CreateConnectionPayload } from '../../../../ui/common/canvas/connectable-behavior.helper';
import {
    ConnectionEndpointReconnectDestination,
    CreateComponentRequest,
    CreateConnectionRequest
} from '../../../../state/flow-shared';
import { ConnectorConnectablePolicy } from './connector-connectable.policy';
import { ConnectorCanvasCommands } from './connector-canvas-commands.service';
import * as ExtensionTypesActions from '../../../../state/extension-types/extension-types.actions';
import { ConnectorCanvasContextMenu } from './connector-canvas-context-menu.service';
import { NewCanvasItem } from '../../../../ui/common/canvas/new-canvas-item/new-canvas-item.component';

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
        ContextErrorBanner,
        NewCanvasItem
    ],
    templateUrl: './connector-canvas.component.html',
    styleUrl: './connector-canvas.component.scss',
    providers: [ConnectorCanvasCommands, ConnectorCanvasContextMenu]
})
export class ConnectorCanvasComponent implements OnInit, OnDestroy {
    private store = inject(Store<NiFiState>);
    private actions$ = inject(Actions);
    private destroyRef = inject(DestroyRef);
    private router = inject(Router);
    private dialog = inject(MatDialog);
    private storage = inject(Storage);
    private client = inject(Client);
    protected readonly commands = inject(ConnectorCanvasCommands);
    readonly contextMenu = inject(ConnectorCanvasContextMenu);

    private static readonly CONTROL_VISIBILITY_KEY = 'graph-control-visibility';
    private static readonly GRAPH_CONTROL_KEY = 'connector-graph-controls';

    canvasComponent = viewChild.required(CanvasComponent);

    currentConnectorId = '';
    currentProcessGroupId: string | null = null;
    selectedComponentIds: string[] = [];
    canNavigateToParent = false;
    skipTransform = this.store.selectSignal(ConnectorCanvasSelectors.selectSkipTransform);
    graphControlsOpen = true;

    birdseyeComponents = signal<BirdseyeComponentData[]>([]);
    birdseyeTransform = signal<BirdseyeTransform>({ translate: { x: 0, y: 0 }, scale: 1 });
    canvasDimensions = signal<Dimension>({ width: 0, height: 0 });

    private canvasReady = false;

    connectorEntity = this.store.selectSignal(selectConnectorCanvasEntity);
    entitySaving = this.store.selectSignal(selectConnectorCanvasEntitySaving);
    inTroubleshooting = this.contextMenu.inTroubleshooting;
    canEdit = this.contextMenu.canEdit;
    dragging = this.store.selectSignal(ConnectorCanvasSelectors.selectConnectorCanvasDragging);
    operationBreadcrumb = this.store.selectSignal(ConnectorCanvasSelectors.selectBreadcrumbs);
    operationProcessGroupRevision = this.store.selectSignal(ConnectorCanvasSelectors.selectProcessGroupRevision);
    operationSelection = this.store.selectSignal(ConnectorCanvasSelectors.selectConnectorCanvasSelectedComponents);
    private readonly connectorConnectablePolicy = new ConnectorConnectablePolicy(
        () => this.canvasComponent().canvasRootResolver(),
        () => this.currentProcessGroupId
    );
    connectablePolicy = computed<ConnectablePolicy | null>(() =>
        this.connectorEntity()?.component.state === 'TROUBLESHOOTING' ? this.connectorConnectablePolicy : null
    );

    provenanceEvents = this.store.selectSignal(selectConnectorProvenanceEvents);
    provenanceStatus = this.store.selectSignal(selectConnectorProvenanceStatus);
    provenanceError = this.store.selectSignal(selectConnectorProvenanceError);

    private clusterSummary = this.store.selectSignal(selectClusterSummary);
    private about = this.store.selectSignal(selectAbout);

    connectedToCluster = computed(() => this.clusterSummary()?.connectedToCluster ?? false);
    contentViewerAvailable = computed(() => (this.about()?.contentViewerUrl ?? null) != null);

    private provenanceComponentId = signal<string | null>(null);
    private provenanceCollapsed = true;
    private lastDeferredComponentId: string | null = null;

    protected readonly ErrorContextKey = ErrorContextKey;
    protected readonly ComponentType = ComponentType;

    constructor() {
        this.contextMenu.setHost({
            connectorId: () => this.currentConnectorId,
            processGroupId: () => this.currentProcessGroupId,
            canNavigateToParent: () => this.canNavigateToParent,
            centerOnSelection: () => this.canvasComponent().centerOnSelection(true)
        });

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

    labels$: Observable<LabelEntity[]> = this.store.select(ConnectorCanvasSelectors.selectLabels);
    processors$: Observable<ProcessorEntity[]> = this.store.select(ConnectorCanvasSelectors.selectProcessors);
    funnels$: Observable<FunnelEntity[]> = this.store.select(ConnectorCanvasSelectors.selectFunnels);
    allPorts$: Observable<PortEntity[]> = this.store.select(ConnectorCanvasSelectors.selectAllPorts);
    remoteProcessGroups$: Observable<RemoteProcessGroupEntity[]> = this.store.select(
        ConnectorCanvasSelectors.selectRemoteProcessGroups
    );
    processGroups$: Observable<ProcessGroupEntity[]> = this.store.select(ConnectorCanvasSelectors.selectProcessGroups);
    connections$: Observable<ConnectionEntity[]> = this.store.select(ConnectorCanvasSelectors.selectConnections);
    private connections = this.store.selectSignal(ConnectorCanvasSelectors.selectConnections);
    registryClients$: Observable<RegistryClientEntity[]> = this.store.select(
        ConnectorCanvasSelectors.selectRegistryClients
    );

    canAccessProvenance = this.contextMenu.canAccessProvenance;

    // =========================================================================
    // Context Menu
    // =========================================================================

    onContextMenuOpened(context: ContextMenuContext): void {
        this.contextMenu.setContext(context);
    }

    // Once loaded, data remains ready while same-canvas refreshes run or fail.
    dataReady$: Observable<boolean> = this.store.select(ConnectorCanvasSelectors.selectHasLoadedFlow);

    // Error state - true when flow load failed
    hasError$: Observable<boolean> = this.store
        .select(ConnectorCanvasSelectors.selectLoadingStatus)
        .pipe(map((status) => status === 'error'));

    ngOnInit(): void {
        this.store.dispatch(ExtensionTypesActions.loadExtensionTypesForCanvas());
        // Configure global canvas UI state - read-only view (no editing)
        const config: CanvasConfiguration = {
            features: {
                canEdit: false,
                canSelect: true
            }
        };
        this.store.dispatch(setConfiguration({ configuration: config }));

        // Begin periodic refresh so bulletins, queue counts, and run status reflect
        // server-side changes without requiring the user to manually navigate.
        this.store.dispatch(ConnectorCanvasActions.startConnectorCanvasPolling());

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

        // Refresh birdseye geometry whenever a flow load, create, or delete completes.
        // Without this, the minimap would only reflect the data captured during the very first
        // canvas initialization and would go stale after subsequent refreshes, edits, or
        // process-group navigations performed against an already-mounted canvas.
        this.actions$
            .pipe(
                ofType(
                    ConnectorCanvasActions.loadConnectorFlowSuccess,
                    ConnectorCanvasActions.createCanvasComponentSuccess,
                    ConnectorCanvasActions.deleteSelectedComponentsSuccess
                ),
                takeUntilDestroyed(this.destroyRef)
            )
            .subscribe(() => {
                // Defer one tick so the entity arrays flow through async pipes into the canvas
                // and the canvas's internal signals (read by getBirdseyeComponentData) update.
                queueMicrotask(() => this.refreshBirdseye());
            });

        this.actions$
            .pipe(ofType(ConnectorCanvasActions.updateComponentPositionSuccess), takeUntilDestroyed(this.destroyRef))
            .subscribe(({ componentId, componentType }) => this.confirmComponentPosition(componentId, componentType));

        this.actions$
            .pipe(ofType(ConnectorCanvasActions.updateComponentPositionFailure), takeUntilDestroyed(this.destroyRef))
            .subscribe(({ componentId, componentType }) => this.revertComponentPosition(componentId, componentType));

        this.actions$
            .pipe(ofType(ConnectorCanvasActions.updateConnectionBendPointsSuccess), takeUntilDestroyed(this.destroyRef))
            .subscribe(({ connectionId }) => this.canvasComponent().confirmConnectionBendPoints(connectionId));

        this.actions$
            .pipe(ofType(ConnectorCanvasActions.updateConnectionBendPointsFailure), takeUntilDestroyed(this.destroyRef))
            .subscribe(({ connectionId }) => this.canvasComponent().revertConnectionBendPoints(connectionId));

        this.actions$
            .pipe(ofType(ConnectorCanvasActions.updateConnectionLabelIndexSuccess), takeUntilDestroyed(this.destroyRef))
            .subscribe(({ connectionId }) => this.canvasComponent().confirmConnectionLabelIndex(connectionId));

        this.actions$
            .pipe(ofType(ConnectorCanvasActions.updateConnectionLabelIndexFailure), takeUntilDestroyed(this.destroyRef))
            .subscribe(({ connectionId }) => this.canvasComponent().revertConnectionLabelIndex(connectionId));

        this.actions$
            .pipe(
                ofType(ConnectorCanvasActions.updateConnectionDestinationSuccess),
                takeUntilDestroyed(this.destroyRef)
            )
            .subscribe(({ connectionId }) => this.canvasComponent().confirmConnectionDestination(connectionId));

        this.actions$
            .pipe(
                ofType(ConnectorCanvasActions.updateConnectionDestinationFailure),
                takeUntilDestroyed(this.destroyRef)
            )
            .subscribe(({ connectionId, dialogOpen }) => {
                if (!dialogOpen) {
                    this.canvasComponent().revertConnectionDestination(connectionId);
                }
            });

        this.actions$
            .pipe(ofType(ConnectorCanvasActions.editConnectionDialogClosed), takeUntilDestroyed(this.destroyRef))
            .subscribe(({ connectionId, cancelled }) => {
                if (cancelled) {
                    this.canvasComponent().revertConnectionDestination(connectionId);
                }
            });

        this.actions$
            .pipe(ofType(ConnectorCanvasActions.resizeLabelSuccess), takeUntilDestroyed(this.destroyRef))
            .subscribe(({ labelId }) => this.canvasComponent().confirmLabelResize(labelId));

        this.actions$
            .pipe(ofType(ConnectorCanvasActions.resizeLabelFailure), takeUntilDestroyed(this.destroyRef))
            .subscribe(({ labelId }) => this.canvasComponent().revertLabelDimensions(labelId));

        this.actions$
            .pipe(
                ofType(
                    ConnectorCanvasActions.createConnectionSuccess,
                    ConnectorCanvasActions.createConnectionFailure,
                    ConnectorCanvasActions.createConnectionDialogClosed
                ),
                takeUntilDestroyed(this.destroyRef)
            )
            .subscribe(() => this.canvasComponent().clearConnectionPreview());

        // Derive provenance eligibility from the route + loaded canvas entities. The eligible
        // set covers Processor / Connection / RemoteProcessGroup, plus InputPort / OutputPort
        // gated on allowRemoteAccess. All other types (Funnel, Label, ProcessGroup, multi-select,
        // and no selection) clear the preview.
        combineLatest([
            this.store.select(selectRouteParams),
            this.store.select(ConnectorCanvasSelectors.selectInputPorts),
            this.store.select(ConnectorCanvasSelectors.selectOutputPorts)
        ])
            .pipe(
                map(([params, inputPorts, outputPorts]) =>
                    this.computeEligibleProvenanceId(params, inputPorts, outputPorts)
                ),
                distinctUntilChanged(),
                takeUntilDestroyed(this.destroyRef)
            )
            .subscribe((eligibleId) => this.handleProvenanceEligibilityChange(eligibleId));
    }

    ngOnDestroy(): void {
        this.store.dispatch(ConnectorCanvasActions.stopConnectorCanvasPolling());
        this.store.dispatch(ConnectorCanvasActions.resetConnectorCanvasState());
        this.store.dispatch(ConnectorCanvasEntityActions.resetConnectorCanvasEntityState());
        this.store.dispatch(ConnectorProvenanceActions.resetState());
    }

    @HostListener('window:resize')
    handleWindowResize(): void {
        if (this.canvasReady) {
            this.canvasDimensions.set(this.canvasComponent().getCanvasDimensions());
        }
    }

    onTransformChange(event: BirdseyeTransform): void {
        this.birdseyeTransform.set(event);
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
        this.contextMenu.viewConfigurationAction(entity, componentType);
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

    onComponentsDragEnd(event: { items: DragEndBaselineItem[]; delta: Position; targetGroupId?: string }): void {
        if (!this.currentProcessGroupId || event.items.length === 0) {
            return;
        }

        if (event.targetGroupId) {
            this.store.dispatch(ConnectorCanvasActions.deselectAllComponents());
            event.items.forEach((item) => this.confirmComponentPosition(item.id, item.type));
            this.store.dispatch(
                ConnectorCanvasActions.moveCanvasComponents({
                    processGroupId: this.currentProcessGroupId,
                    targetGroupId: event.targetGroupId,
                    components: event.items.map((item) => ({
                        id: item.id,
                        type: item.type,
                        revision: this.withLocalClientId(item.revision)
                    }))
                })
            );
            return;
        }

        this.store.dispatch(
            ConnectorCanvasActions.updateComponentPositionsBulk({
                processGroupId: this.currentProcessGroupId,
                updates: event.items.map((item) => this.buildBulkUpdate(item, event.delta)),
                delta: event.delta
            })
        );
    }

    onConnectionBendPointsUpdate(event: {
        id: string;
        bends: Position[];
        labelIndex?: number;
        revision: RevisionRequest;
    }): void {
        if (!this.currentProcessGroupId) return;
        this.store.dispatch(
            ConnectorCanvasActions.updateConnectionBendPoints({
                processGroupId: this.currentProcessGroupId,
                update: {
                    id: event.id,
                    bends: event.bends,
                    labelIndex: event.labelIndex,
                    revision: this.withLocalClientId(event.revision)
                }
            })
        );
    }

    onConnectionLabelDragEnd(event: { id: string; labelIndex: number; revision: RevisionRequest }): void {
        if (!this.currentProcessGroupId) return;
        this.store.dispatch(
            ConnectorCanvasActions.updateConnectionLabelIndex({
                processGroupId: this.currentProcessGroupId,
                connectionId: event.id,
                labelIndex: event.labelIndex,
                revision: this.withLocalClientId(event.revision)
            })
        );
    }

    onLabelResizeEnd(event: {
        id: string;
        dimensions: { width: number; height: number };
        revision: RevisionRequest;
    }): void {
        if (!this.currentProcessGroupId) return;
        this.store.dispatch(
            ConnectorCanvasActions.resizeLabel({
                processGroupId: this.currentProcessGroupId,
                labelId: event.id,
                dimensions: event.dimensions,
                revision: this.withLocalClientId(event.revision)
            })
        );
    }

    onChipDragStarted(): void {
        this.store.dispatch(ConnectorCanvasActions.setDragging({ dragging: true }));
    }

    onChipDragEnded(type: ComponentType, event: CdkDragEnd): void {
        const position = this.canEdit() ? this.canvasComponent().getCanvasPosition(event.dropPoint) : null;
        if (!position || !this.currentProcessGroupId) {
            this.store.dispatch(ConnectorCanvasActions.setDragging({ dragging: false }));
            return;
        }

        const request: CreateComponentRequest = {
            revision: { clientId: this.client.getClientId(), version: 0 },
            type,
            position
        };
        this.store.dispatch(ConnectorCanvasActions.createComponentRequest({ request }));
    }

    onCreateConnectionRequested(payload: CreateConnectionPayload): void {
        if (!this.currentProcessGroupId) return;
        this.store.dispatch(
            ConnectorCanvasActions.createConnectionRequested({
                processGroupId: this.currentProcessGroupId,
                request: {
                    source: {
                        id: payload.source.id,
                        componentType: payload.source.type,
                        entity: payload.source.entity
                    },
                    destination: {
                        id: payload.destination.id,
                        componentType: payload.destination.type,
                        entity: payload.destination.entity
                    },
                    bends: payload.bends
                } as CreateConnectionRequest
            })
        );
    }

    onConnectionDestinationChangeRequested(event: {
        id: string;
        revision: RevisionRequest;
        newDestination: ConnectionEndpointReconnectDestination;
        bends?: Position[];
    }): void {
        if (!this.currentProcessGroupId) {
            this.canvasComponent().revertConnectionDestination(event.id);
            return;
        }

        const destinationType = event.newDestination.componentType;
        if (destinationType === ComponentType.ProcessGroup || destinationType === ComponentType.RemoteProcessGroup) {
            const connection = this.connections().find((candidate) => candidate.id === event.id);
            if (!connection) {
                this.canvasComponent().revertConnectionDestination(event.id);
                return;
            }

            const destinationComponent = event.newDestination.entity.component;
            const destinationName =
                destinationComponent && 'name' in destinationComponent && typeof destinationComponent.name === 'string'
                    ? destinationComponent.name
                    : undefined;
            this.store.dispatch(
                ConnectorCanvasActions.openReconnectConnectionDialog({
                    request: {
                        type: ComponentType.Connection,
                        uri: connection.uri,
                        entity: event.bends ? { ...connection, bends: event.bends } : connection,
                        revision: this.withLocalClientId(event.revision),
                        newDestination: {
                            type: destinationType,
                            groupId: event.newDestination.id,
                            name: event.newDestination.entity.permissions.canRead
                                ? (destinationName ?? event.newDestination.id)
                                : event.newDestination.id
                        }
                    }
                })
            );
            return;
        }

        this.store.dispatch(
            ConnectorCanvasActions.updateConnectionDestination({
                processGroupId: this.currentProcessGroupId,
                connectionId: event.id,
                revision: this.withLocalClientId(event.revision),
                newDestination: event.newDestination,
                bends: event.bends
            })
        );
    }

    private buildBulkUpdate(item: DragEndBaselineItem, delta: Position): ConnectorCanvasActions.BulkPositionUpdate {
        if (item.type === ComponentType.Connection) {
            return {
                componentId: item.id,
                componentType: item.type,
                revision: this.withLocalClientId(item.revision),
                bends: item.bends.map((bend) => ({ x: bend.x + delta.x, y: bend.y + delta.y })),
                labelIndex: item.labelIndex
            };
        }
        return {
            componentId: item.id,
            componentType: item.type,
            revision: this.withLocalClientId(item.revision),
            position: { x: item.position.x + delta.x, y: item.position.y + delta.y }
        };
    }

    private withLocalClientId(revision: RevisionRequest): RevisionRequest {
        return { version: revision.version, clientId: this.client.getClientId() };
    }

    private confirmComponentPosition(componentId: string, componentType: ComponentType): void {
        switch (componentType) {
            case ComponentType.Processor:
                this.canvasComponent().confirmProcessorPosition(componentId);
                break;
            case ComponentType.InputPort:
            case ComponentType.OutputPort:
                this.canvasComponent().confirmPortPosition(componentId);
                break;
            case ComponentType.RemoteProcessGroup:
                this.canvasComponent().confirmRemoteProcessGroupPosition(componentId);
                break;
            case ComponentType.ProcessGroup:
                this.canvasComponent().confirmProcessGroupPosition(componentId);
                break;
            case ComponentType.Funnel:
                this.canvasComponent().confirmFunnelPosition(componentId);
                break;
            case ComponentType.Label:
                this.canvasComponent().confirmLabelPosition(componentId);
                break;
            case ComponentType.Connection:
                this.canvasComponent().confirmConnectionBendPoints(componentId);
                break;
        }
    }

    private revertComponentPosition(componentId: string, componentType: ComponentType): void {
        switch (componentType) {
            case ComponentType.Processor:
                this.canvasComponent().revertProcessorPosition(componentId);
                break;
            case ComponentType.InputPort:
            case ComponentType.OutputPort:
                this.canvasComponent().revertPortPosition(componentId);
                break;
            case ComponentType.RemoteProcessGroup:
                this.canvasComponent().revertRemoteProcessGroupPosition(componentId);
                break;
            case ComponentType.ProcessGroup:
                this.canvasComponent().revertProcessGroupPosition(componentId);
                break;
            case ComponentType.Funnel:
                this.canvasComponent().revertFunnelPosition(componentId);
                break;
            case ComponentType.Label:
                this.canvasComponent().revertLabelPosition(componentId);
                break;
            case ComponentType.Connection:
                this.canvasComponent().revertConnectionBendPoints(componentId);
                break;
        }
    }

    onCanvasInitialized(): void {
        this.canvasReady = true;
        this.refreshBirdseye();

        if (this.selectedComponentIds.length > 0) {
            if (this.skipTransform()) {
                this.store.dispatch(ConnectorCanvasActions.setSkipTransform({ skipTransform: false }));
            } else {
                this.canvasComponent().centerOnSelection(false, 1);
            }
        }
    }

    private refreshBirdseye(): void {
        if (!this.canvasReady) {
            return;
        }
        const canvas = this.canvasComponent();
        this.birdseyeComponents.set(canvas.getBirdseyeComponentData());
        this.canvasDimensions.set(canvas.getCanvasDimensions());
    }

    // =========================================================================
    // Navigation Control / Birdseye delegation
    // =========================================================================

    onNavigationZoomIn(): void {
        this.canvasComponent().onZoomIn();
    }

    onNavigationZoomOut(): void {
        this.canvasComponent().onZoomOut();
    }

    onNavigationZoomFit(): void {
        this.canvasComponent().onZoomFit();
    }

    onNavigationZoomActual(): void {
        this.canvasComponent().onZoomActual();
    }

    onNavigationLeaveGroup(): void {
        this.leaveGroupAction();
    }

    onBirdseyeViewportChange(event: Position): void {
        this.canvasComponent().setViewportPosition(event.x, event.y, false);
    }

    onBirdseyeDragStart(): void {
        this.canvasComponent().birdseyeDragStart();
    }

    onBirdseyeDragEnd(): void {
        this.canvasComponent().birdseyeDragEnd();
    }

    // =========================================================================
    // Provenance Preview orchestration
    // =========================================================================

    private computeEligibleProvenanceId(
        params: { [key: string]: string } | null,
        inputPorts: any[],
        outputPorts: any[]
    ): string | null {
        if (!params || !params['type'] || !params['componentId']) {
            return null;
        }

        const componentId = params['componentId'];
        const componentType = params['type'] as ComponentType;

        switch (componentType) {
            case ComponentType.Processor:
            case ComponentType.Connection:
            case ComponentType.RemoteProcessGroup:
                return componentId;
            case ComponentType.InputPort: {
                const port = inputPorts.find((p: any) => p.id === componentId);
                return port?.component?.allowRemoteAccess === true ? componentId : null;
            }
            case ComponentType.OutputPort: {
                const port = outputPorts.find((p: any) => p.id === componentId);
                return port?.component?.allowRemoteAccess === true ? componentId : null;
            }
            default:
                return null;
        }
    }

    private handleProvenanceEligibilityChange(eligibleId: string | null): void {
        this.provenanceComponentId.set(eligibleId);

        if (eligibleId === null) {
            this.lastDeferredComponentId = null;
            this.store.dispatch(ConnectorProvenanceActions.resetState());
            return;
        }

        if (this.graphControlsOpen && !this.provenanceCollapsed) {
            this.lastDeferredComponentId = null;
            this.store.dispatch(ConnectorProvenanceActions.loadLatestEventsForComponent({ componentId: eligibleId }));
        } else {
            this.lastDeferredComponentId = eligibleId;
        }
    }

    private flushDeferredProvenanceLoad(): void {
        if (!this.graphControlsOpen || this.provenanceCollapsed) {
            return;
        }

        const pending = this.lastDeferredComponentId;
        if (pending) {
            this.lastDeferredComponentId = null;
            this.store.dispatch(ConnectorProvenanceActions.loadLatestEventsForComponent({ componentId: pending }));
        }
    }

    onProvenanceCollapsedChange(collapsed: boolean): void {
        this.provenanceCollapsed = collapsed;
        if (!collapsed) {
            this.flushDeferredProvenanceLoad();
        }
    }

    onProvenanceRefresh(): void {
        const id = this.provenanceComponentId();
        if (id) {
            this.store.dispatch(ConnectorProvenanceActions.loadLatestEventsForComponent({ componentId: id }));
        }
    }

    onProvenanceViewDetails(event: ProvenanceEvent): void {
        this.store.dispatch(
            ConnectorProvenanceActions.openProvenanceEventDialog({
                request: { event }
            })
        );
    }

    onProvenanceDownloadContent(request: DownloadContentRequest): void {
        this.store.dispatch(ConnectorProvenanceActions.downloadContent({ request }));
    }

    onProvenanceViewContent(request: ViewContentRequest): void {
        this.store.dispatch(ConnectorProvenanceActions.viewContent({ request }));
    }

    onProvenanceReplayEvent(event: ProvenanceEvent): void {
        const request: ReplayEventRequest = { event };
        this.store.dispatch(ConnectorProvenanceActions.replayEvent({ request }));
    }

    // =========================================================================
    // Shared Actions (used by both context menu and keyboard shortcuts)
    // =========================================================================

    refreshAction(): void {
        this.contextMenu.refreshAction();
    }

    leaveGroupAction(): void {
        this.contextMenu.leaveGroupAction();
    }

    viewProcessorStateAction(processorEntity: any): void {
        this.contextMenu.viewProcessorStateAction(processorEntity);
    }

    canDrain(): boolean {
        return this.contextMenu.canDrain();
    }

    canCancelDrain(): boolean {
        return this.contextMenu.canCancelDrain();
    }

    drainConnectorAction(): void {
        this.contextMenu.drainConnectorAction();
    }

    cancelDrainConnectorAction(): void {
        this.contextMenu.cancelDrainConnectorAction();
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

    @HostListener('window:keydown.delete', ['$event'])
    @HostListener('window:keydown.backspace', ['$event'])
    handleDeleteShortcut(event: Event): void {
        if (!this.canEdit() || !this.shouldProcessKeyboardEvent(event)) {
            return;
        }
        const selection = this.operationSelection();
        if (!this.contextMenu.hasDeletableSelection(selection)) {
            return;
        }
        event.preventDefault();
        this.commands.delete(selection);
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

        if (this.graphControlsOpen) {
            this.flushDeferredProvenanceLoad();
        }
    }

    onSearchGoToComponent(event: { id: string; type: ComponentType; groupId: string }): void {
        if (event.type === ComponentType.ControllerService) {
            this.store.dispatch(
                ConnectorCanvasActions.navigateToControllerService({
                    processGroupId: event.groupId,
                    serviceId: event.id
                })
            );
            return;
        }

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
