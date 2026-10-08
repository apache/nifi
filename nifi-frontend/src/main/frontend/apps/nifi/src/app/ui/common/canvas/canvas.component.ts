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
    OnInit,
    AfterViewInit,
    OnDestroy,
    ElementRef,
    ChangeDetectorRef,
    inject,
    input,
    output,
    viewChild,
    computed,
    signal,
    effect,
    untracked
} from '@angular/core';
import { CommonModule } from '@angular/common';
import { Store } from '@ngrx/store';
import { Observable, Subject, fromEvent } from 'rxjs';
import { map, takeUntil, take, debounceTime } from 'rxjs/operators';
import * as d3 from 'd3';
import {
    clampScale,
    ComponentType,
    isFiniteInBound,
    isScaleInBound,
    MAX_ABS_COORD,
    MAX_ABS_TRANSLATE,
    NiFiCommon,
    Position,
    ConnectionEntity,
    FunnelEntity,
    LabelEntity,
    PortEntity,
    ProcessGroupEntity,
    ProcessorEntity,
    RemoteProcessGroupEntity,
    RevisionRequest
} from '@nifi/shared';
import { DocumentedType, RegistryClientEntity } from '../../../state/shared';
import { NiFiState } from '../../../state';
import {
    CanvasLabel,
    CanvasProcessor,
    CanvasFunnel,
    CanvasPort,
    CanvasRemoteProcessGroup,
    CanvasProcessGroup,
    CanvasConnection,
    CanvasDatum,
    CanvasEntity,
    CanvasRootResolver,
    CanvasRootSelection,
    ComponentDoubleClickEvent,
    DragEndBaselineItem,
    Dimension,
    ContextMenuContext,
    isConnectionDatum
} from './canvas.types';
import { CdkContextMenuTrigger } from '@angular/cdk/menu';
import { ContextMenu, ContextMenuDefinitionProvider } from '../context-menu/context-menu.component';
import { selectScale, selectTransform, selectCanvasFeatures } from '../../../state/canvas-ui/canvas-ui.selectors';
import { setTransform } from '../../../state/canvas-ui/canvas-ui.actions';
import { LabelLayerComponent } from './layers/label-layer/label-layer.component';
import { ProcessorLayerComponent } from './layers/processor-layer/processor-layer.component';
import { TextEllipsisUtils } from './utils/text-ellipsis.utils';
import { CanvasFormatUtils } from './canvas-format-utils.service';
import { CanvasComponentUtils } from './canvas-component-utils.service';
import { FunnelLayerComponent } from './layers/funnel-layer/funnel-layer.component';
import { PortLayerComponent } from './layers/port-layer/port-layer.component';
import { RemoteProcessGroupLayerComponent } from './layers/remote-process-group-layer/remote-process-group-layer.component';
import { ProcessGroupLayerComponent } from './layers/process-group-layer/process-group-layer.component';
import { ConnectionLayerComponent } from './layers/connection-layer/connection-layer.component';
import { MatSnackBar } from '@angular/material/snack-bar';
import { CanvasConstants } from './canvas.constants';
import {
    detectOverlappingConnections,
    OverlappingConnectionGroup,
    OverlapDetectionConnection,
    wouldRemovalCauseOverlap
} from '../overlap-detection.utils';
import { BirdseyeComponentData, BirdseyeTransform } from '../birdseye/birdseye.types';
import { CanvasComponentRef, ConnectionEndpointReconnectDestination } from '../../../state/flow-shared';
import {
    ConnectableBehaviorHelper,
    ConnectablePolicy,
    CreateConnectionPayload,
    DEFAULT_GEOMETRY
} from './connectable-behavior.helper';
import { ConnectionReconnectContext } from './layers/render-context.types';
import { DragUtils } from './utils/drag.utils';

/**
 * Interface for viewport transform storage
 */
interface StorageTransform {
    scale: number;
    translateX: number;
    translateY: number;
}

interface SelectionBoxData {
    x: number;
    y: number;
    width: number;
    height: number;
}

interface ComponentDragStateCarrier {
    entity: { id: string; revision: { version: number } };
    ui: {
        dragDelta?: Position;
        dragMovingIds?: Set<string>;
        dragStartPosition?: Position;
        currentPosition?: Position;
        dragStartEntity?: CanvasEntity;
        dragStartRevision?: RevisionRequest;
        dimensions?: { width: number; height: number };
    };
}

/**
 * Canvas Component (Reusable)
 *
 * This is the coordinator component for the new reusable canvas architecture.
 * It demonstrates the pattern of coordinating multiple layer components.
 *
 * Responsibilities:
 * - Subscribe to canvas state
 * - Pass data to layer components
 * - Handle events from layers (dispatch actions)
 * - Apply canvas transform to all layers
 *
 * Current Implementation:
 * - Single layer (LabelLayer) for testing
 * - Basic transform controls
 * - Selection handling
 *
 * Future:
 * - Add more layers (processors, connections, etc.)
 * - Add drag-and-drop
 * - Add context menu
 * - Add grid/rulers
 */
@Component({
    selector: 'reusable-canvas',
    standalone: true,
    imports: [
        CommonModule,
        LabelLayerComponent,
        ProcessorLayerComponent,
        FunnelLayerComponent,
        PortLayerComponent,
        RemoteProcessGroupLayerComponent,
        ProcessGroupLayerComponent,
        ConnectionLayerComponent,
        CdkContextMenuTrigger,
        ContextMenu
    ],
    templateUrl: './canvas.component.html',
    styleUrls: ['./canvas.component.scss']
})
export class CanvasComponent implements OnInit, AfterViewInit, OnDestroy {
    private static readonly VIEW_PREFIX: string = 'nifi-view-';
    private static readonly STORAGE_MILLIS_PER_DAY = 86400000;

    private static readViewportTransform(key: string): StorageTransform | null {
        const raw = localStorage.getItem(key);
        if (!raw) {
            return null;
        }
        try {
            const parsed = JSON.parse(raw) as { expires?: number; item?: StorageTransform } & Partial<StorageTransform>;
            if (
                parsed &&
                typeof parsed === 'object' &&
                typeof parsed.expires === 'number' &&
                new Date(parsed.expires).valueOf() < Date.now()
            ) {
                localStorage.removeItem(key);
                return null;
            }
            const item: StorageTransform | null =
                parsed &&
                typeof parsed === 'object' &&
                'item' in parsed &&
                parsed.item &&
                typeof parsed.item === 'object'
                    ? parsed.item
                    : parsed &&
                        typeof parsed === 'object' &&
                        'scale' in parsed &&
                        'translateX' in parsed &&
                        'translateY' in parsed
                      ? (parsed as StorageTransform)
                      : null;
            if (
                !item ||
                !isFiniteInBound(item.translateX, MAX_ABS_TRANSLATE) ||
                !isFiniteInBound(item.translateY, MAX_ABS_TRANSLATE) ||
                !isScaleInBound(item.scale)
            ) {
                return null;
            }
            return item;
        } catch {
            return null;
        }
    }

    private preserveComponentDragState<T extends ComponentDragStateCarrier>(
        previous: ReadonlyArray<T>,
        next: ReadonlyArray<T>,
        savingIds: ReadonlySet<string>
    ): void {
        this.preserveInFlightUiState(previous, next, CanvasComponent.DRAG_BASELINE_FIELDS_COMPONENT);
        this.preserveInFlightUiState(previous, next, CanvasComponent.DRAG_VISUAL_FIELDS_COMPONENT, (prior, fresh) =>
            this.shouldPreserveComponentUi(prior, fresh.entity, savingIds)
        );
    }

    private shouldPreserveComponentUi(
        existing: ComponentDragStateCarrier,
        incoming: { id: string; revision: { version: number } },
        savingIds: ReadonlySet<string>
    ): boolean {
        const isInFlight =
            existing.ui.dragStartEntity !== undefined ||
            existing.ui.dragStartRevision !== undefined ||
            existing.ui.currentPosition !== undefined ||
            savingIds.has(incoming.id);
        return isInFlight && !(incoming.revision.version > existing.entity.revision.version);
    }

    private refreshPositionableEntity<T extends ComponentDragStateCarrier>(
        existing: T,
        incoming: T['entity'],
        savingIds: ReadonlySet<string>
    ): boolean {
        const preserve = this.shouldPreserveComponentUi(existing, incoming, savingIds);
        existing.entity = incoming;
        if (!preserve) {
            delete existing.ui.currentPosition;
            delete existing.ui.dragDelta;
            delete existing.ui.dragMovingIds;
        }
        return preserve;
    }

    private store = inject<Store<NiFiState>>(Store);
    private elementRef = inject(ElementRef);
    private cdr = inject(ChangeDetectorRef);

    // ViewChild as signal
    canvasSvg = viewChild.required<ElementRef<SVGSVGElement>>('canvasSvg');

    // Layer component references for calling pan() methods
    labelLayer = viewChild<LabelLayerComponent>('labelLayer');
    remoteProcessGroupLayer = viewChild<RemoteProcessGroupLayerComponent>('remoteProcessGroupLayer');
    processGroupLayer = viewChild<ProcessGroupLayerComponent>('processGroupLayer');
    processorLayer = viewChild<ProcessorLayerComponent>('processorLayer');
    portLayer = viewChild<PortLayerComponent>('portLayer');
    funnelLayer = viewChild<FunnelLayerComponent>('funnelLayer');
    connectionLayer = viewChild<ConnectionLayerComponent>('connectionLayer');

    // Text ellipsis utility (shared across all layers)
    // Cache is cleared on process group navigation
    textEllipsisUtils = new TextEllipsisUtils();

    // Format utilities (shared across all layers)
    formatUtils = inject(CanvasFormatUtils);
    componentUtils = inject(CanvasComponentUtils);
    nifiCommon = inject(NiFiCommon);
    private snackBar = inject(MatSnackBar);

    // Flow data inputs (raw backend entities provided by parent) - using signals
    labels = input<LabelEntity[]>([]);
    processors = input<ProcessorEntity[]>([]);
    funnels = input<FunnelEntity[]>([]);
    ports = input<PortEntity[]>([]);
    remoteProcessGroups = input<RemoteProcessGroupEntity[]>([]);
    processGroups = input<ProcessGroupEntity[]>([]);
    connections = input<ConnectionEntity[]>([]);

    // Preview extensions (processors with Preview tag)
    previewExtensions = input<DocumentedType[]>([]);

    // Registry clients (for version control tooltip)
    registryClients = input<RegistryClientEntity[]>([]);

    // Process group context (null when not yet resolved from route)
    processGroupId = input<string | null>(null);

    // Selection input (from parent, driven by route)
    selectedComponentIds = input<string[]>([]);

    // Data ready signal - parent sets this to true when flow data is loaded
    // Canvas will automatically initialize (updateCanvasVisibility + restoreViewportFromStorage)
    // when both canvas is ready AND dataReady is true
    dataReady = input<boolean>(false);

    /**
     * When true, the canvas will skip centering on selected components during
     * initialization and instead restore the viewport from storage. Used to
     * prevent the viewport from jumping when the selection change was user-initiated
     * (e.g., clicking a component) rather than navigation-initiated (deep link).
     */
    skipInitialCenter = input<boolean>(false);

    // Context menu provider - if provided, enables context menu functionality
    // Parent provides a ContextMenuDefinitionProvider implementation
    menuProvider = input<ContextMenuDefinitionProvider | undefined>(undefined);

    connectable = input<ConnectablePolicy | null>(null);

    // Render trigger - incremented to force layer re-rendering (e.g., after fonts load)
    private renderTriggerValue = signal(0);
    renderTrigger = computed(() => this.renderTriggerValue());

    // Internal canvas ready state (set after fonts load and initial render)
    private internalCanvasReady = signal(false);

    // Track if we've already initialized for the current process group.
    // Polling flips dataReady false -> true, but must not reset the viewport.
    private hasInitialized = signal(false);
    private initProcessGroupId = signal<string | null>(null);

    // Controls canvas content visibility during PG transitions to prevent
    // a brief flash of components at the wrong viewport position
    viewportReady = signal(true);

    // Track which labels are currently saving (disabled during save)
    private savingLabels = signal<Set<string>>(new Set());
    disabledLabelIds = computed(() => this.savingLabels());

    // Track which connections are currently saving bend points (disabled during save)
    private savingConnections = signal<Set<string>>(new Set());
    disabledConnectionIds = computed(() => this.savingConnections());

    // Track which processors are currently saving position (disabled during save)
    private savingProcessors = signal<Set<string>>(new Set());
    disabledProcessorIds = computed(() => this.savingProcessors());

    // Track which ports are currently saving position (disabled during save)
    private savingPorts = signal<Set<string>>(new Set());
    disabledPortIds = computed(() => this.savingPorts());

    connectableBehavior = computed<ConnectableBehaviorHelper | null>(() => {
        const policy = this.connectable();
        if (!policy) {
            return null;
        }
        return new ConnectableBehaviorHelper(
            policy,
            {
                onSelectSource: (source) => this.selectComponents.emit([source]),
                onConnectionRequested: (payload) => this.createConnectionRequested.emit(payload),
                isDisabled: (id) =>
                    this.disabledProcessorIds().has(id) ||
                    this.disabledFunnelIds().has(id) ||
                    this.disabledPortIds().has(id) ||
                    this.disabledProcessGroupIds().has(id) ||
                    this.disabledRemoteProcessGroupIds().has(id)
            },
            this.canvasRootResolver
        );
    });

    connectionReconnect = computed<ConnectionReconnectContext | null>(() => {
        const policy = this.connectable();
        if (!policy) {
            return null;
        }
        const geometry = { ...DEFAULT_GEOMETRY, ...policy.geometry };
        return {
            isValidConnectionDestination: (selection) => policy.isValidConnectionDestination(selection),
            getPerimeterPoint: geometry.getPerimeterPoint,
            selfLoopXOffset: geometry.selfLoopXOffset,
            selfLoopYOffset: geometry.selfLoopYOffset
        };
    });

    readonly canvasRootResolver: CanvasRootResolver = () =>
        this.canvasGroup ?? d3.select<SVGGElement, unknown>(null as unknown as SVGGElement);

    readonly isDropAllowedFn = (): boolean => true;

    // Track which remote process groups are currently saving position (disabled during save)
    private savingRemoteProcessGroups = signal<Set<string>>(new Set());
    disabledRemoteProcessGroupIds = computed(() => this.savingRemoteProcessGroups());

    // Track which process groups are currently saving position (disabled during save)
    private savingProcessGroups = signal<Set<string>>(new Set());
    disabledProcessGroupIds = computed(() => this.savingProcessGroups());

    // Track which funnels are currently saving position (disabled during save)
    private savingFunnels = signal<Set<string>>(new Set());
    disabledFunnelIds = computed(() => this.savingFunnels());

    // Selection events (for parent to handle routing) - using output signals
    // Canvas handles multi-select logic and emits high-level selection intent
    // Parent updates routes → parent reads route → parent passes selection as @Input
    selectComponents = output<CanvasComponentRef[]>();
    deselectAll = output<void>();

    // Process group navigation event (for parent to handle routing)
    processGroupDoubleClick = output<{ processGroupId: string }>();

    // Label resize events
    labelResizeEnd = output<{ id: string; dimensions: Dimension; revision: RevisionRequest }>();

    // Connection bend point update event (unified for drag, add, remove)
    connectionBendPointsUpdate = output<{
        id: string;
        bends: Position[];
        labelIndex?: number;
        revision: RevisionRequest;
    }>();

    // Connection label drag event (when label is moved between bend points)
    connectionLabelDragEnd = output<{ id: string; labelIndex: number; revision: RevisionRequest }>();

    // Component position drag event (for processors, ports, process groups, funnels, labels, remote process groups)
    componentsDragEnd = output<{ items: DragEndBaselineItem[]; delta: Position; targetGroupId?: string }>();
    componentDragEnd = output<{ id: string; type: ComponentType; position: Position }>();
    createConnectionRequested = output<CreateConnectionPayload>();
    connectionDestinationChangeRequested = output<{
        id: string;
        revision: RevisionRequest;
        newDestination: ConnectionEndpointReconnectDestination;
        bends?: Position[];
    }>();

    // Transform change event (for birdseye synchronization)
    transformChange = output<{ translate: { x: number; y: number }; scale: number }>();

    // Initialized event - emitted after canvas is fully ready with data
    // Canvas automatically handles updateCanvasVisibility() and restoreViewportFromStorage()
    // Parent only needs to update birdseye when this fires
    initialized = output<void>();

    // Context menu - emits context when right-click happens so parent can store it
    contextMenuOpened = output<ContextMenuContext>();

    // Emitted when a configurable component is double-clicked (Processor, Port, Connection, RPG)
    componentDoubleClick = output<ComponentDoubleClickEvent>();
    contextMenuComponent = viewChild<ContextMenu>('contextMenuComponent');

    overlappingConnectionGroups = computed<OverlappingConnectionGroup[]>(() => {
        const connections = this.internalConnections();
        const mapped: OverlapDetectionConnection[] = connections.map((c) => ({
            id: c.entity.id,
            sourceId: c.entity.sourceId,
            destinationId: c.entity.destinationId,
            sourceGroupId: c.entity.sourceGroupId,
            destinationGroupId: c.entity.destinationGroupId,
            bends: c.entity.bends ?? []
        }));
        return detectOverlappingConnections(mapped, this.processGroupId() ?? '');
    });

    // Internal Canvas* types (with UI metadata) - computed signals from inputs
    //
    // Labels keep optimistic dimensions and position only while a resize, move, or
    // pending save is in flight and the incoming revision has not advanced. A newer
    // server revision shows the server geometry immediately. The gesture baseline
    // stays until the gesture ends.
    private _internalLabels: CanvasLabel[] = [];
    internalLabels = computed(() => {
        const inputLabels = this.labels();

        // Check if labels array changed (by comparing IDs as sets)
        const inputIdSet = new Set(inputLabels.map((l) => l.id));
        const currentIdSet = new Set(this._internalLabels.map((l) => l.entity.id));

        const labelsChanged =
            inputIdSet.size !== currentIdSet.size || !Array.from(inputIdSet).every((id) => currentIdSet.has(id));

        if (labelsChanged) {
            // Labels changed (added/removed) - rebuild the array via mapLabels,
            // then preserve in-flight drag state on surviving wrappers. Gesture
            // baselines always carry over. Optimistic dimensions and position
            // carry over only while the revision has not advanced.
            const previous = this._internalLabels;
            this._internalLabels = this.mapLabels(inputLabels);
            this.preserveComponentDragState(previous, this._internalLabels, this.savingLabels());
        } else {
            // Same labels. Resync dimensions unless the resize or move is still
            // in flight at the same revision. A newer revision replaces both
            // the dimensions and the move preview.
            inputLabels.forEach((inputEntity) => {
                const existingLabel = this._internalLabels.find((l) => l.entity.id === inputEntity.id);
                if (existingLabel) {
                    const preserve = this.refreshPositionableEntity(existingLabel, inputEntity, this.savingLabels());
                    if (!preserve) {
                        existingLabel.ui.dimensions = {
                            width: inputEntity.dimensions?.width || 148,
                            height: inputEntity.dimensions?.height || 148
                        };
                    }
                }
            });
            // Return new array reference to trigger re-render of layers
            this._internalLabels = [...this._internalLabels];
        }

        return this._internalLabels;
    });

    // Processors keep ui.currentPosition only for a same-revision drag or pending
    // save. A newer server revision clears that preview and leaves the gesture baseline.
    private _internalProcessors: CanvasProcessor[] = [];
    internalProcessors = computed(() => {
        const inputProcessors = this.processors();

        // Check if processors array changed (by comparing IDs as sets)
        const inputIdSet = new Set(inputProcessors.map((p) => p.id));
        const currentIdSet = new Set(this._internalProcessors.map((p) => p.entity.id));

        const processorsChanged =
            inputIdSet.size !== currentIdSet.size || !Array.from(inputIdSet).every((id) => currentIdSet.has(id));

        if (processorsChanged) {
            // Processors changed (added/removed) - rebuild the array via
            // mapProcessors, then preserve in-flight drag state on
            // surviving wrappers so a sibling add/remove mid-gesture
            // doesn't strip the gesture baseline. See
            // `preserveInFlightUiState` for the contract.
            const previous = this._internalProcessors;
            this._internalProcessors = this.mapProcessors(inputProcessors);
            this.preserveComponentDragState(previous, this._internalProcessors, this.savingProcessors());
        } else {
            inputProcessors.forEach((inputEntity) => {
                const existingProcessor = this._internalProcessors.find((p) => p.entity.id === inputEntity.id);
                if (existingProcessor) {
                    this.refreshPositionableEntity(existingProcessor, inputEntity, this.savingProcessors());
                }
            });
            // Return new array reference to trigger re-render of layers
            this._internalProcessors = [...this._internalProcessors];
        }

        return this._internalProcessors;
    });

    // Funnels keep ui.currentPosition only for a same-revision drag or pending save.
    // A newer server revision clears that preview and leaves the gesture baseline.
    private _internalFunnels: CanvasFunnel[] = [];
    internalFunnels = computed(() => {
        const inputFunnels = this.funnels();
        const inputIdSet = new Set(inputFunnels.map((f) => f.id));
        const currentIdSet = new Set(this._internalFunnels.map((f) => f.entity.id));

        const funnelsChanged =
            inputIdSet.size !== currentIdSet.size || !Array.from(inputIdSet).every((id) => currentIdSet.has(id));

        if (funnelsChanged) {
            // Funnels changed (added/removed) - rebuild then preserve any
            // in-flight drag state on surviving wrappers so a sibling
            // add/remove mid-gesture doesn't strip the gesture baseline.
            const previous = this._internalFunnels;
            this._internalFunnels = this.mapFunnels(inputFunnels);
            this.preserveComponentDragState(previous, this._internalFunnels, this.savingFunnels());
        } else {
            inputFunnels.forEach((inputEntity) => {
                const existingFunnel = this._internalFunnels.find((f) => f.entity.id === inputEntity.id);
                if (existingFunnel) {
                    this.refreshPositionableEntity(existingFunnel, inputEntity, this.savingFunnels());
                }
            });
            // Return new array reference to trigger re-render of layers
            this._internalFunnels = [...this._internalFunnels];
        }

        return this._internalFunnels;
    });

    // Ports keep ui.currentPosition only for a same-revision drag or pending save.
    // A newer server revision clears that preview and leaves the gesture baseline.
    private _internalPorts: CanvasPort[] = [];
    internalPorts = computed(() => {
        const inputPorts = this.ports();
        const inputIdSet = new Set(inputPorts.map((p) => p.id));
        const currentIdSet = new Set(this._internalPorts.map((p) => p.entity.id));

        const portsChanged =
            inputIdSet.size !== currentIdSet.size || !Array.from(inputIdSet).every((id) => currentIdSet.has(id));

        if (portsChanged) {
            // Ports changed (added/removed) - rebuild then preserve any
            // in-flight drag state on surviving wrappers so a sibling
            // add/remove mid-gesture doesn't strip the gesture baseline.
            const previous = this._internalPorts;
            this._internalPorts = this.mapPorts(inputPorts);
            this.preserveComponentDragState(previous, this._internalPorts, this.savingPorts());
        } else {
            inputPorts.forEach((inputEntity) => {
                const existingPort = this._internalPorts.find((p) => p.entity.id === inputEntity.id);
                if (existingPort) {
                    this.refreshPositionableEntity(existingPort, inputEntity, this.savingPorts());
                }
            });
            // Return new array reference to trigger re-render of layers
            this._internalPorts = [...this._internalPorts];
        }
        return this._internalPorts;
    });

    // Remote process groups keep ui.currentPosition only for a same-revision drag or
    // pending save. A newer server revision clears that preview and leaves the gesture baseline.
    private _internalRemoteProcessGroups: CanvasRemoteProcessGroup[] = [];
    internalRemoteProcessGroups = computed(() => {
        const inputRpgs = this.remoteProcessGroups();
        const inputIdSet = new Set(inputRpgs.map((r) => r.id));
        const currentIdSet = new Set(this._internalRemoteProcessGroups.map((r) => r.entity.id));

        const rpgsChanged =
            inputIdSet.size !== currentIdSet.size || !Array.from(inputIdSet).every((id) => currentIdSet.has(id));

        if (rpgsChanged) {
            // RPGs changed (added/removed) - rebuild then preserve any
            // in-flight drag state on surviving wrappers so a sibling
            // add/remove mid-gesture doesn't strip the gesture baseline.
            const previous = this._internalRemoteProcessGroups;
            this._internalRemoteProcessGroups = this.mapRemoteProcessGroups(inputRpgs);
            this.preserveComponentDragState(
                previous,
                this._internalRemoteProcessGroups,
                this.savingRemoteProcessGroups()
            );
        } else {
            inputRpgs.forEach((inputEntity) => {
                const existingRpg = this._internalRemoteProcessGroups.find((r) => r.entity.id === inputEntity.id);
                if (existingRpg) {
                    this.refreshPositionableEntity(existingRpg, inputEntity, this.savingRemoteProcessGroups());
                }
            });
            // Return new array reference to trigger re-render of layers
            this._internalRemoteProcessGroups = [...this._internalRemoteProcessGroups];
        }
        return this._internalRemoteProcessGroups;
    });

    // Process groups keep ui.currentPosition only for a same-revision drag or pending
    // save. A newer server revision clears that preview and leaves the gesture baseline.
    private _internalProcessGroups: CanvasProcessGroup[] = [];
    internalProcessGroups = computed(() => {
        const inputPgs = this.processGroups();
        const inputIdSet = new Set(inputPgs.map((pg) => pg.id));
        const currentIdSet = new Set(this._internalProcessGroups.map((pg) => pg.entity.id));

        const pgsChanged =
            inputIdSet.size !== currentIdSet.size || !Array.from(inputIdSet).every((id) => currentIdSet.has(id));

        if (pgsChanged) {
            // Process groups changed (added/removed) - rebuild then preserve
            // any in-flight drag state on surviving wrappers so a sibling
            // add/remove mid-gesture doesn't strip the gesture baseline.
            const previous = this._internalProcessGroups;
            this._internalProcessGroups = this.mapProcessGroups(inputPgs);
            this.preserveComponentDragState(previous, this._internalProcessGroups, this.savingProcessGroups());
        } else {
            inputPgs.forEach((inputEntity) => {
                const existingPg = this._internalProcessGroups.find((pg) => pg.entity.id === inputEntity.id);
                if (existingPg) {
                    this.refreshPositionableEntity(existingPg, inputEntity, this.savingProcessGroups());
                }
            });
            // Return new array reference to trigger re-render of layers
            this._internalProcessGroups = [...this._internalProcessGroups];
        }
        return this._internalProcessGroups;
    });

    // Connections keep optimistic bends only while a drag or pending save is in
    // flight and the incoming revision has not advanced. A newer server revision
    // shows the server bends immediately. The gesture baseline stays until the
    // gesture ends.
    private _internalConnections: CanvasConnection[] = [];
    internalConnections = computed(() => {
        const inputConnections = this.connections();

        // Check if connections array changed (by comparing IDs as sets)
        const inputIdSet = new Set(inputConnections.map((c) => c.id));
        const currentIdSet = new Set(this._internalConnections.map((c) => c.entity.id));

        const connectionsChanged =
            inputIdSet.size !== currentIdSet.size || !Array.from(inputIdSet).every((id) => currentIdSet.has(id));

        if (connectionsChanged) {
            // Connections changed (added/removed) - rebuild the array via
            // mapConnections, then preserve in-flight drag state on surviving
            // wrappers. Gesture baselines always carry over. Optimistic bends
            // carry over only while the revision has not advanced. See
            // `preserveInFlightUiState`.
            const previous = this._internalConnections;
            this._internalConnections = this.mapConnections(inputConnections);
            this.preserveInFlightUiState(
                previous,
                this._internalConnections,
                CanvasComponent.DRAG_BASELINE_FIELDS_CONNECTION
            );
            this.preserveInFlightUiState(
                previous,
                this._internalConnections,
                CanvasComponent.DRAG_VISUAL_FIELDS_CONNECTION,
                (prior, fresh) => this.shouldPreserveConnectionUi(prior, fresh.entity)
            );
            const previousById = new Map(previous.map((connection) => [connection.entity.id, connection]));
            for (const connection of this._internalConnections) {
                const prior = previousById.get(connection.entity.id);
                if (!prior || !this.shouldPreserveConnectionUi(prior, connection.entity)) {
                    connection.ui.bends = (connection.entity.bends ?? []).map((bend) => ({ ...bend }));
                }
            }
        } else {
            // Same connections - refresh entity data and resync ui.bends.
            // `calculatePath` initializes `ui.bends` only when it is unset, so
            // a polling update of `entity.bends` would never reach the renderer
            // without this resync. Mirrors the `ui.dimensions` resync for labels.
            //
            // `shouldPreserveConnectionUi` keeps optimistic bends only while the
            // connection is in flight and the incoming revision has not advanced.
            // A newer revision replaces the visual preview immediately. The
            // gesture baseline in `ui.dragStartEntity` / `ui.dragStartRevision`
            // stays until gesture end, so the eventual write still carries the
            // revision the user acted on.
            inputConnections.forEach((inputEntity) => {
                const existingConnection = this._internalConnections.find((c) => c.entity.id === inputEntity.id);
                if (existingConnection) {
                    const preserveUi = this.shouldPreserveConnectionUi(existingConnection, inputEntity);
                    existingConnection.entity = inputEntity;
                    if (!preserveUi) {
                        existingConnection.ui.bends = (inputEntity.bends ?? []).map((bend) => ({ ...bend }));
                        delete existingConnection.ui.endPointDragging;
                        delete existingConnection.ui.reconnectDestinationId;
                        delete existingConnection.ui.tempLabelIndex;
                        existingConnection.ui.dragging = false;
                    }
                }
            });
            // Return new array reference to trigger re-render of layers
            this._internalConnections = [...this._internalConnections];
        }

        return this._internalConnections;
    });

    private shouldPreserveConnectionUi(
        existingConnection: CanvasConnection,
        incomingEntity: ConnectionEntity
    ): boolean {
        const isInFlight =
            existingConnection.ui.dragging === true ||
            existingConnection.ui.dragStartEntity !== undefined ||
            existingConnection.ui.dragStartRevision !== undefined ||
            this.savingConnections().has(incomingEntity.id);
        const isNewerRevision = incomingEntity.revision.version > existingConnection.entity.revision.version;
        return isInFlight && !isNewerRevision;
    }

    private destroy$ = new Subject<void>();
    private destroyed = false; // Flag to prevent emitting after component is destroyed
    private zoom!: d3.ZoomBehavior<SVGSVGElement, unknown>;
    private zoomInitialized = signal(false); // Signal to track when zoom is initialized
    private svg!: d3.Selection<SVGSVGElement, unknown, null, undefined>;
    private canvasGroup!: CanvasRootSelection;
    private currentScale = 1; // Track current scale for selection box stroke width
    private x = 0; // Current transform x (for dispatching to store)
    private y = 0; // Current transform y (for dispatching to store)

    // Box selection state
    private canvasClicked = false; // Track if canvas was clicked (vs component clicked)
    private panning = false; // Track if user is actually panning (vs simple click)
    private zoomHappened = false; // Track if zoom event fired (to distinguish real zoom from spurious ends)
    private isSettingInitialTransform = false; // Guard to prevent dispatch loop during initialization
    private birdseyeTranslateInProgress = false; // Track if birdseye is currently dragging (skip visibility updates)

    // UI state subscriptions (internal - from global canvas state)
    // Note: We use a signal for scale instead of the store observable to avoid async pipe delays
    // The currentScale is updated synchronously during zoom events
    scale = signal<number>(1);
    scale$: Observable<number> = this.store.select(selectScale); // Keep for backwards compatibility
    canSelect$: Observable<boolean> = this.store
        .select(selectCanvasFeatures)
        .pipe(map((features) => features.canSelect));
    canEdit$: Observable<boolean> = this.store.select(selectCanvasFeatures).pipe(map((features) => features.canEdit));

    // Signals for synchronous access in event handlers
    private canSelectSignal = signal<boolean>(true);
    private canEditSignal = signal<boolean>(true);

    // selectedIds as computed signal (derived from input)
    selectedIds = computed(() => this.selectedComponentIds());

    // Component type map (ID → ComponentType) for handling mixed-type selections
    private componentTypeMap = computed(() => {
        const map = new Map<string, ComponentType>();
        this.internalLabels().forEach((label) => map.set(label.entity.id, label.ui.componentType));
        this.internalProcessors().forEach((proc) => map.set(proc.entity.id, proc.ui.componentType));
        this.internalFunnels().forEach((funnel) => map.set(funnel.entity.id, funnel.ui.componentType));
        this.internalPorts().forEach((port) => map.set(port.entity.id, port.ui.componentType));
        this.internalRemoteProcessGroups().forEach((rpg) => map.set(rpg.entity.id, rpg.ui.componentType));
        this.internalProcessGroups().forEach((pg) => map.set(pg.entity.id, pg.ui.componentType));
        this.internalConnections().forEach((conn) => map.set(conn.entity.id, conn.ui.componentType));
        return map;
    });

    constructor() {
        /**
         * Auto-initialization Effect
         *
         * This effect coordinates the initialization timing between the canvas DOM being ready
         * and the flow data being loaded. The problem it solves:
         *
         * 1. D3-based rendering requires SVG elements to exist in the DOM before D3 can select them
         * 2. Flow data may arrive before or after the DOM is ready (race condition)
         * 3. Fonts must be loaded before text measurements are accurate
         *
         * The solution:
         * - `internalCanvasReady` becomes true after fonts load and initial render completes
         * - `dataReady` input is set by parent when flow data is successfully loaded
         * - When BOTH are true, we initialize visibility and viewport
         * - `hasInitialized` prevents duplicate initialization
         *
         * The setTimeout(0) is necessary because Angular's change detection may not have
         * propagated the new data to child layer components yet. Deferring to the next
         * microtask ensures Angular has completed its rendering cycle.
         */
        effect(() => {
            const canvasReady = this.internalCanvasReady();
            const dataReady = this.dataReady();
            const processGroupId = this.processGroupId();
            const previousProcessGroupId = untracked(() => this.initProcessGroupId());

            if (processGroupId !== previousProcessGroupId) {
                untracked(() => {
                    this.initProcessGroupId.set(processGroupId);
                    this.hasInitialized.set(false);
                    this.viewportReady.set(false);
                });
            }

            const hasInitialized = untracked(() => this.hasInitialized());
            if (!canvasReady || !dataReady || hasInitialized) {
                return;
            }

            untracked(() => this.hasInitialized.set(true));

            // Defer to next microtask to ensure Angular has propagated data to child components
            setTimeout(() => {
                this.updateCanvasVisibility();
                if (this.selectedComponentIds().length > 0 && !this.skipInitialCenter()) {
                    this.centerOnSelection(false);
                } else {
                    this.restoreViewportFromStorage();
                }
                this.viewportReady.set(true);
                this.initialized.emit();
            }, 0);
        });
    }

    ngOnInit(): void {
        // Subscribe to canvas features to update signals for synchronous access
        this.canSelect$.pipe(takeUntil(this.destroy$)).subscribe((canSelect) => {
            this.canSelectSignal.set(canSelect);
        });
        this.canEdit$.pipe(takeUntil(this.destroy$)).subscribe((canEdit) => {
            this.canEditSignal.set(canEdit);
        });

        // Update component visibility on window resize (debounced for performance)
        fromEvent(window, 'resize')
            .pipe(debounceTime(300), takeUntil(this.destroy$))
            .subscribe(() => {
                if (this.zoomInitialized()) {
                    this.updateCanvasVisibility();
                }
            });
    }

    ngAfterViewInit(): void {
        // Only initialize D3 once (ngAfterViewInit can be called multiple times by Angular)
        if (this.zoomInitialized()) {
            return;
        }
        this.zoomInitialized.set(true);

        // Initialize D3 selections for the SVG canvas
        this.svg = d3.select(this.canvasSvg().nativeElement);
        this.canvasGroup = this.svg.select('g.canvas');

        // Set up box selection behavior BEFORE zoom to ensure selection handlers take priority
        this.setupBoxSelection();

        // Set up D3 zoom/pan behavior
        this.setupD3Zoom();

        // Set up context menu handling (if callback is provided)
        this.setupContextMenu();

        /**
         * Font Loading & Canvas Ready Sequence
         *
         * Text measurements in SVG depend on fonts being fully loaded. The browser's
         * document.fonts.ready promise resolves when fonts are loaded, but the actual
         * rendering may still be pending. We use requestAnimationFrame + setTimeout to
         * ensure the browser has completed the paint cycle with the new fonts.
         *
         * The 50ms delay is empirically determined to be sufficient for most browsers
         * to complete font rendering. Without this delay, text truncation calculations
         * may use incorrect measurements.
         *
         * Once fonts are ready and rendered:
         * 1. Increment renderTrigger to force layer re-rendering with correct text measurements
         * 2. Run change detection to propagate the trigger to child layers
         * 3. Mark canvas as internally ready, which triggers auto-initialization (see constructor effect)
         */
        const fontsReady = document.fonts?.ready || Promise.resolve();
        fontsReady.then(() => {
            requestAnimationFrame(() => {
                setTimeout(() => {
                    // Force re-render with correct font measurements
                    this.renderTriggerValue.set(this.renderTriggerValue() + 1);
                    this.cdr.detectChanges();

                    // Signal that canvas DOM is ready for D3 operations
                    // This triggers the auto-initialization effect if dataReady is already true
                    this.internalCanvasReady.set(true);
                }, 50);
            });
        });
    }

    /**
     * Initial visibility is handled by the zoom handler's updateCanvasVisibility() call.
     *
     * Flow:
     * 1. Parent component waits for data loading to complete (loadingStatus === 'success')
     * 2. Parent calls canvasComponent().restoreViewportFromStorage()
     * 3. restoreViewportFromStorage() applies a transform, triggering D3 zoom event
     * 4. Zoom handler calls updateCanvasVisibility() → initial visibility calculated
     *
     * Visibility is calculated during the initial viewport restoration, not via a separate effect.
     *
     * No effect needed - the zoom handler's updateCanvasVisibility() call handles both:
     * - Initial visibility (during viewport restoration)
     * - Ongoing visibility updates (during user zoom/pan)
     */

    ngOnDestroy(): void {
        this.destroyed = true;
        this.destroy$.next();
        this.destroy$.complete();

        // Remove D3 zoom behavior to prevent events firing after component is destroyed
        if (this.svg) {
            this.svg.on('.zoom', null);
        }
    }

    /**
     * Field sets that `preserveInFlightUiState` carries from a pre-rebuild
     * wrapper to its freshly-constructed counterpart when the input id-set
     * changes. The baseline contract published by `componentsDragEnd` /
     * `labelResizeEnd` / `connectionBendPointsUpdate` /
     * `connectionLabelDragEnd` depends on `ui.dragStartEntity` (for the
     * multi-component drag) and `ui.dragStartRevision` (for the three
     * single-gesture emit paths) surviving any backend push that adds or
     * removes a sibling of the same type during a gesture; without that,
     * the fresh wrapper's `ui` would be initialized empty by `mapXxx` and
     * the emit handler would silently fall back to the post-push
     * `wrapper.entity.revision`, re-introducing the optimistic-locking
     * silent-overwrite the gesture-baseline contract is designed to
     * prevent.
     *
     * Gesture baselines survive a newer server revision so the eventual
     * request still carries the revision the user acted on and can be
     * rejected correctly. Optimistic connection, label, and component-move
     * visuals survive only while the server revision remains unchanged.
     */
    private static readonly DRAG_BASELINE_FIELDS_COMPONENT: ReadonlyArray<string> = [
        'dragStartPosition',
        'dragStartEntity',
        'dragStartRevision'
    ];
    private static readonly DRAG_VISUAL_FIELDS_COMPONENT: ReadonlyArray<string> = [
        'currentPosition',
        'dragDelta',
        'dragMovingIds',
        'dimensions'
    ];
    private static readonly DRAG_BASELINE_FIELDS_CONNECTION: ReadonlyArray<string> = [
        'dragStartBends',
        'dragStartEntity',
        'dragStartRevision'
    ];
    private static readonly DRAG_VISUAL_FIELDS_CONNECTION: ReadonlyArray<string> = [
        'bends',
        'dragging',
        'endPointDragging',
        'end',
        'tempLabelIndex',
        // In-flight endpoint-reconnect target must survive a mid-save sibling
        // add/remove rebuild, or the optimistic re-route would snap back to the
        // entity destination before the server confirms.
        'reconnectDestinationId'
    ];

    /**
     * After a same-type input change rebuilds an `_internalXxx` array via
     * `mapXxx`, copy the listed in-flight `ui` fields from the pre-rebuild
     * wrapper to the freshly-constructed wrapper for every id that survived
     * the rebuild. See the `DRAG_*_FIELDS_*` constants for the rationale and
     * canonical field sets.
     *
     * The check is `!== undefined` so we only overwrite when the prior
     * wrapper actually carried in-flight state — wrappers that were idle at
     * the time of the rebuild get the fresh `mapXxx` defaults untouched.
     */
    private preserveInFlightUiState<W extends { entity: { id: string }; ui: object }>(
        previous: ReadonlyArray<W>,
        next: ReadonlyArray<W>,
        fields: ReadonlyArray<string>,
        shouldPreserve: (prior: W, fresh: W) => boolean = () => true
    ): void {
        if (previous.length === 0 || next.length === 0) {
            return;
        }
        const previousById = new Map(previous.map((w) => [w.entity.id, w]));
        for (const fresh of next) {
            const prior = previousById.get(fresh.entity.id);
            if (!prior || !shouldPreserve(prior, fresh)) {
                continue;
            }
            const priorUi = prior.ui as Record<string, unknown>;
            const freshUi = fresh.ui as Record<string, unknown>;
            for (const field of fields) {
                const value = priorUi[field];
                if (value !== undefined) {
                    freshUi[field] = value;
                }
            }
        }
    }

    /**
     * Mapping methods: Convert raw backend entities to Canvas* types with UI metadata
     */

    private mapLabels(labelEntities: LabelEntity[]): CanvasLabel[] {
        return labelEntities.map((entity) => ({
            entity,
            ui: {
                componentType: ComponentType.Label,
                dimensions: {
                    width: entity.dimensions?.width || 148,
                    height: entity.dimensions?.height || 148
                }
            }
        }));
    }

    private mapProcessors(processorEntities: ProcessorEntity[]): CanvasProcessor[] {
        return processorEntities.map((entity) => ({
            entity,
            ui: {
                componentType: ComponentType.Processor,
                dimensions: { ...CanvasConstants.PROCESSOR }
            }
        }));
    }

    private mapFunnels(funnelEntities: FunnelEntity[]): CanvasFunnel[] {
        return funnelEntities.map((entity) => ({
            entity,
            ui: {
                componentType: ComponentType.Funnel,
                dimensions: { ...CanvasConstants.FUNNEL }
            }
        }));
    }

    private mapPorts(portEntities: PortEntity[]): CanvasPort[] {
        return portEntities.map((entity) => {
            const isInputPort = entity.portType === 'INPUT_PORT';
            const dimensions = entity.allowRemoteAccess
                ? { ...CanvasConstants.REMOTE_PORT }
                : { ...CanvasConstants.PORT };
            return {
                entity,
                ui: {
                    componentType: isInputPort ? ComponentType.InputPort : ComponentType.OutputPort,
                    dimensions
                }
            };
        });
    }

    private mapRemoteProcessGroups(rpgEntities: RemoteProcessGroupEntity[]): CanvasRemoteProcessGroup[] {
        return rpgEntities.map((entity) => {
            return {
                entity,
                ui: {
                    componentType: ComponentType.RemoteProcessGroup,
                    dimensions: { ...CanvasConstants.REMOTE_PROCESS_GROUP }
                }
            };
        });
    }

    private mapProcessGroups(pgEntities: ProcessGroupEntity[]): CanvasProcessGroup[] {
        return pgEntities.map((entity) => ({
            entity,
            ui: {
                componentType: ComponentType.ProcessGroup,
                dimensions: { ...CanvasConstants.PROCESS_GROUP }
            }
        }));
    }

    private mapConnections(connectionEntities: ConnectionEntity[]): CanvasConnection[] {
        return connectionEntities.map((entity) => ({
            entity,
            ui: {
                componentType: ComponentType.Connection,
                // start and end will be set by calculatePath() during rendering
                start: { x: 0, y: 0 },
                end: { x: 0, y: 0 }
            }
        }));
    }

    /**
     * Calculate viewport bounds for culling (with 1 screen padding in each direction)
     */
    public getViewportBounds(): { left: number; top: number; right: number; bottom: number } | null {
        const canvasContainer = this.elementRef.nativeElement;
        if (!canvasContainer) {
            return null;
        }

        // Get current transform
        let translate = [this.x, this.y];
        const scale = this.currentScale;

        // Scale the translation
        translate = [translate[0] / scale, translate[1] / scale];

        // Get the normalized screen width and height
        const screenWidth = canvasContainer.offsetWidth / scale;
        const screenHeight = canvasContainer.offsetHeight / scale;

        // Calculate the screen bounds one screen's worth in each direction (padding)
        const screenLeft = -translate[0] - screenWidth;
        const screenTop = -translate[1] - screenHeight;
        const screenRight = screenLeft + screenWidth * 3;
        const screenBottom = screenTop + screenHeight * 3;

        return { left: screenLeft, top: screenTop, right: screenRight, bottom: screenBottom };
    }

    /**
     * Check if a component is visible in the viewport
     */
    public isComponentVisible(
        position: { x: number; y: number },
        dimensions: { width: number; height: number }
    ): boolean {
        const bounds = this.getViewportBounds();
        if (!bounds) {
            return true; // If we can't calculate bounds, show everything
        }

        // Check scale threshold (MIN_SCALE_TO_RENDER = 0.4)
        if (this.currentScale < 0.4) {
            return false;
        }

        const left = position.x;
        const top = position.y;
        const right = left + dimensions.width;
        const bottom = top + dimensions.height;

        // Component is visible if it intersects with viewport bounds
        return bounds.left < right && bounds.right > left && bounds.top < bottom && bounds.bottom > top;
    }

    /**
     * Convert screen coordinates to canvas coordinates
     *
     * Used by paste operations to determine where to paste components.
     *
     * @param screenPosition - Position in screen coordinates (e.g., from MouseEvent.pageX/pageY)
     * @returns Position in canvas coordinates, or null if canvas not initialized
     */
    public getCanvasPosition(screenPosition: Position): Position | null {
        const rect = this.getCanvasBoundingClientRect();
        if (!rect) {
            return null;
        }

        const transform = this.getTransform();
        const x = (screenPosition.x - rect.left - transform.translate.x) / transform.scale;
        const y = (screenPosition.y - rect.top - transform.translate.y) / transform.scale;

        // Reject coordinates produced by a corrupted transform (e.g. near-zero scale that
        // causes division overflow) before they can be used to create a POST request that
        // re-poisons the backend with an out-of-range position.
        if (!isFiniteInBound(x, MAX_ABS_COORD) || !isFiniteInBound(y, MAX_ABS_COORD)) {
            return null;
        }

        return { x, y };
    }

    /**
     * Get the bounding client rect of the canvas element
     *
     * Used by paste operations for centering calculations.
     *
     * @returns DOMRect of the canvas element, or null if not initialized
     */
    public getCanvasBoundingClientRect(): DOMRect | null {
        const canvasElement = this.elementRef?.nativeElement;
        if (!canvasElement) {
            return null;
        }
        return canvasElement.getBoundingClientRect();
    }

    /**
     * Update Visibility Classes for Canvas Components
     *
     * This method implements viewport culling - components outside the visible viewport
     * are marked as not visible to avoid rendering their details (improving performance).
     *
     * The visibility system uses three CSS classes:
     * - `visible`: Component is currently in the viewport
     * - `entering`: Component just became visible (was hidden, now visible)
     * - `leaving`: Component just became hidden (was visible, now hidden)
     *
     * The entering/leaving classes allow layer components to efficiently update only
     * components that changed visibility, rather than re-rendering everything.
     *
     * Called during:
     * - Initial canvas load (via auto-initialization effect)
     * - User zoom/pan operations (via D3 zoom end handler)
     * - Window resize events
     */
    public updateCanvasVisibility(): void {
        // Helper to update visibility classes for a single component
        const updateVisibility = (
            selection: d3.Selection<SVGGElement, unknown, d3.BaseType, unknown>,
            isVisible: boolean
        ) => {
            const wasVisible = selection.classed('visible');
            selection
                .classed('visible', isVisible)
                .classed('entering', () => isVisible && !wasVisible)
                .classed('leaving', () => !isVisible && wasVisible);
        };

        // Update visibility for all component types
        // Select all components and update their visibility classes
        const canvasGroup = d3.select(this.elementRef.nativeElement).select('g.canvas');

        // Processors
        canvasGroup.selectAll<SVGGElement, CanvasProcessor>('g.processor').each((d, index, nodes) => {
            const processor = d3.select(nodes[index]);
            const isVisible = this.isComponentVisible(d.entity.position, d.ui.dimensions);
            updateVisibility(processor, isVisible);
        });

        // Process Groups
        canvasGroup.selectAll<SVGGElement, CanvasProcessGroup>('g.process-group').each((d, index, nodes) => {
            const pg = d3.select(nodes[index]);
            const isVisible = this.isComponentVisible(d.entity.position, d.ui.dimensions);
            updateVisibility(pg, isVisible);
        });

        // Remote Process Groups
        canvasGroup
            .selectAll<SVGGElement, CanvasRemoteProcessGroup>('g.remote-process-group')
            .each((d, index, nodes) => {
                const rpg = d3.select(nodes[index]);
                const isVisible = this.isComponentVisible(d.entity.position, d.ui.dimensions);
                updateVisibility(rpg, isVisible);
            });

        // Connections
        canvasGroup.selectAll<SVGGElement, CanvasConnection>('g.connection').each((d, index, nodes) => {
            const connection = d3.select(nodes[index]);

            // Check if connection label position is visible in viewport
            // Get the position where the label would be centered (on bend point or midpoint)
            const labelPosition = this.getConnectionLabelPosition(d);

            // Use a small dimension for the label check (just checking if the center point is visible)
            // The actual label is 240px wide x ~40px tall, but we check center point visibility
            const isVisible = this.isComponentVisible(labelPosition, { width: 240, height: 40 });

            updateVisibility(connection, isVisible);
        });

        // Ports (input and output)
        canvasGroup.selectAll<SVGGElement, CanvasPort>('g.input-port, g.output-port').each((d, index, nodes) => {
            const port = d3.select(nodes[index]);
            const isVisible = this.isComponentVisible(d.entity.position, d.ui.dimensions);
            updateVisibility(port, isVisible);
        });

        // Funnels
        canvasGroup.selectAll<SVGGElement, CanvasFunnel>('g.funnel').each((d, index, nodes) => {
            const funnel = d3.select(nodes[index]);
            const isVisible = this.isComponentVisible(d.entity.position, d.ui.dimensions);
            updateVisibility(funnel, isVisible);
        });

        // Labels
        canvasGroup.selectAll<SVGGElement, CanvasLabel>('g.label').each((d, index, nodes) => {
            const label = d3.select(nodes[index]);
            const isVisible = this.isComponentVisible(d.entity.position, d.ui.dimensions);
            updateVisibility(label, isVisible);
        });

        // Trigger pan on all layers to update entering/leaving components
        // This ensures components with 'entering' or 'leaving' classes get their details rendered/removed
        this.triggerPan();
    }

    /**
     * Get the position where a connection label would be centered
     * Matches existing CanvasUtils.getPositionForCenteringConnection logic
     */
    private getConnectionLabelPosition(connection: CanvasConnection): Position {
        const bends = connection.ui?.bends || connection.entity?.bends || [];
        const labelIndex = connection.entity?.component?.labelIndex || 0;

        let x: number;
        let y: number;

        if (bends.length > 0) {
            const i = Math.min(Math.max(0, labelIndex), bends.length - 1);
            x = bends[i].x;
            y = bends[i].y;
        } else {
            const start = connection.ui?.start;
            const end = connection.ui?.end;

            if (start && end) {
                x = (start.x + end.x) / 2;
                y = (start.y + end.y) / 2;
            } else {
                // Fallback if start/end not available
                x = 0;
                y = 0;
            }
        }

        return { x, y };
    }

    /**
     * Trigger Pan Update on Layer Components
     *
     * This method is called after updateCanvasVisibility() to efficiently update
     * only the components that changed visibility state. Rather than re-rendering
     * all components, each layer's pan() method processes only components with
     * 'entering' or 'leaving' CSS classes.
     *
     * - Entering components: Just became visible, need their details rendered
     * - Leaving components: Just became hidden, can have their details removed
     *
     * All D3 selections are collected first, then pan() methods are called
     * synchronously to ensure consistent rendering across all component types.
     */
    private triggerPan(): void {
        const canvasGroup = d3.select(this.elementRef.nativeElement).select('g.canvas');

        // Collect all entering/leaving selections for each component type
        const enteringLeavingLabels = canvasGroup.selectAll<SVGGElement, CanvasLabel>(
            'g.label.entering, g.label.leaving'
        );
        const enteringLeavingProcessors = canvasGroup.selectAll<SVGGElement, CanvasProcessor>(
            'g.processor.entering, g.processor.leaving'
        );
        const enteringLeavingPorts = canvasGroup.selectAll<SVGGElement, CanvasPort>(
            'g.input-port.entering, g.input-port.leaving, g.output-port.entering, g.output-port.leaving'
        );
        const enteringLeavingFunnels = canvasGroup.selectAll<SVGGElement, CanvasFunnel>(
            'g.funnel.entering, g.funnel.leaving'
        );
        const enteringLeavingRPGs = canvasGroup.selectAll<SVGGElement, CanvasRemoteProcessGroup>(
            'g.remote-process-group.entering, g.remote-process-group.leaving'
        );
        const enteringLeavingPGs = canvasGroup.selectAll<SVGGElement, CanvasProcessGroup>(
            'g.process-group.entering, g.process-group.leaving'
        );
        const enteringLeavingConnections = canvasGroup.selectAll<SVGGElement, CanvasConnection>(
            'g.connection.entering, g.connection.leaving'
        );

        // Get layer component references
        const lblLayer = this.labelLayer();
        const procLayer = this.processorLayer();
        const portLayer = this.portLayer();
        const funnelLayer = this.funnelLayer();
        const rpgLayer = this.remoteProcessGroupLayer();
        const pgLayer = this.processGroupLayer();
        const connLayer = this.connectionLayer();

        // Call pan() on each layer with its entering/leaving selection
        // D3 selections handle empty selections gracefully
        if (lblLayer && !enteringLeavingLabels.empty()) {
            lblLayer.pan(enteringLeavingLabels);
        }
        if (procLayer && !enteringLeavingProcessors.empty()) {
            procLayer.pan(enteringLeavingProcessors);
        }
        if (portLayer && !enteringLeavingPorts.empty()) {
            portLayer.pan(enteringLeavingPorts);
        }
        if (funnelLayer && !enteringLeavingFunnels.empty()) {
            funnelLayer.pan(enteringLeavingFunnels);
        }
        if (rpgLayer && !enteringLeavingRPGs.empty()) {
            rpgLayer.pan(enteringLeavingRPGs);
        }
        if (pgLayer && !enteringLeavingPGs.empty()) {
            pgLayer.pan(enteringLeavingPGs);
        }
        if (connLayer && !enteringLeavingConnections.empty()) {
            connLayer.pan(enteringLeavingConnections);
        }
    }

    /**
     * Set up SVG filters and markers (drop shadow, connection arrows, etc.)
     * Create SVG filter and marker definitions
     */

    /**
     * Set up D3 zoom behavior
     *
     */
    private setupD3Zoom(): void {
        // Create D3 zoom behavior (svg and canvasGroup already initialized in ngAfterViewInit)
        this.zoom = d3
            .zoom<SVGSVGElement, unknown>()
            .scaleExtent([0.2, 8]) // Min/max zoom levels
            .filter((event) => {
                // Prevent zoom/pan when:
                // 1. Not a mouse event (touch events, etc.)
                // 2. Right-click or middle-click (only allow left-click for pan)
                // 3. Mousedown originates from a draggable component (has 'component' class)
                //    Note: wheel events are always allowed (scroll zoom works everywhere)

                // Only process mouse events
                if (event.type !== 'mousedown' && event.type !== 'wheel' && event.type !== 'dblclick') {
                    return true;
                }

                // For mousedown events, check button (0 = left, 1 = middle, 2 = right)
                if (event.type === 'mousedown' && event.button !== 0) {
                    return false;
                }

                // For mousedown events, check if clicking on a component
                // This prevents canvas pan when dragging a component
                // Wheel events are NOT blocked (scroll zoom works over components)
                if (event.type === 'mousedown') {
                    let target = event.target as Element;
                    while (target && target !== this.svg.node()) {
                        if (target.classList && target.classList.contains('component')) {
                            // Clicking on a component - don't allow pan
                            return false;
                        }
                        target = target.parentElement as Element;
                    }
                }

                // Allow zoom/pan for canvas background clicks and all wheel events
                return true;
            })
            .on('zoom', (event) => {
                // Guard against events firing after component destruction
                if (this.destroyed) {
                    return;
                }

                // All-or-nothing magnitude+scale guard: reject the entire event when any
                // component is degenerate. Using all-or-nothing here (unlike the per-axis
                // partial-commit in flow-designer's CanvasView) because the reusable canvas
                // emits a single {translate, scale} event and partial-commit semantics would
                // silently desync the emitted state from the rendered SVG transform.
                if (
                    !isFiniteInBound(event.transform.x, MAX_ABS_TRANSLATE) ||
                    !isFiniteInBound(event.transform.y, MAX_ABS_TRANSLATE) ||
                    !isScaleInBound(event.transform.k)
                ) {
                    return;
                }

                // Apply transform to canvas group
                this.canvasGroup.attr('transform', event.transform);

                // Track current transform values
                this.x = event.transform.x;
                this.y = event.transform.y;
                this.currentScale = event.transform.k;

                // Update scale signal synchronously for immediate label rendering
                this.scale.set(this.currentScale);

                // Emit transform change for birdseye synchronization
                this.transformChange.emit({
                    translate: { x: this.x, y: this.y },
                    scale: this.currentScale
                });

                // Indicate that we are panning to prevent deselection in 'end' below
                this.panning = true;

                // Track that zoom happened (to filter spurious ends)
                this.zoomHappened = true;

                // Note: updateCanvasVisibility() is NOT called here during zoom/pan.
                // It's only called on zoom 'end' to avoid expensive re-renders during drag.
                // This matches the existing canvas pattern.
            })
            .on('end', () => {
                // Guard against events firing after component destruction
                if (this.destroyed) {
                    return;
                }

                // Skip processing during birdseye drag - birdseyeDragEnd() will handle visibility update
                if (this.birdseyeTranslateInProgress) {
                    return;
                }

                // Only process if a zoom event actually fired
                // Skip dispatch during initial transform setup to prevent infinite loop
                if (this.zoomHappened && !this.isSettingInitialTransform) {
                    // Dispatch transform state update
                    this.store.dispatch(
                        setTransform({
                            transform: {
                                translate: {
                                    x: this.x,
                                    y: this.y
                                },
                                scale: this.currentScale,
                                transition: false
                            }
                        })
                    );

                    // Persist viewport to localStorage
                    this.persistViewport();

                    // Update visibility after pan/zoom completes (not during drag for performance)
                    this.updateCanvasVisibility();

                    // Reset zoom flag
                    this.zoomHappened = false;
                }

                // Deselection logic: Only deselect if there was an actual canvas click
                // This filters out spurious zoom.end events from change detection cycles
                if (!this.panning && this.canvasClicked) {
                    this.deselectAll.emit();
                    this.canvasClicked = false; // Reset after handling
                }

                // Reset the panning flag
                this.panning = false;
            });

        // Apply zoom behavior to SVG
        this.svg.call(this.zoom);

        // Set initial transform from state
        // Must update both canvas group AND D3 zoom's internal state for correct zoom-to-point
        this.store
            .select(selectTransform)
            .pipe(take(1))
            .subscribe((transform) => {
                this.x = transform.translate.x;
                this.y = transform.translate.y;
                this.currentScale = transform.scale;

                // Set flag to prevent dispatch loop during initialization
                this.isSettingInitialTransform = true;

                // Create D3 transform identity and apply initial values
                const initialTransform = d3.zoomIdentity
                    .translate(transform.translate.x, transform.translate.y)
                    .scale(transform.scale);

                // Update D3 zoom's internal state (will trigger zoom/end events, but dispatch is suppressed)
                // This is critical for correct zoom-to-point behavior on first mouse wheel zoom
                this.svg.call(this.zoom.transform, initialTransform);

                // Reset flag after D3 has processed the transform
                this.isSettingInitialTransform = false;
                this.zoomHappened = false; // Also reset this since we don't want to process this zoom
            });
    }

    /**
     * Set up context menu handling
     *
     * Listens for contextmenu events on the SVG and emits the context
     * so the parent can store it before the menu opens.
     */
    private setupContextMenu(): void {
        // Only set up if provider is provided
        if (!this.menuProvider()) {
            return;
        }

        this.svg.on('contextmenu', (event: MouseEvent) => {
            // Determine what was right-clicked
            const target = event.target as Element;
            // Check for both .component (processors, PGs, etc.) and .connection
            const componentElement = target.closest('.component') || target.closest('g.connection');

            let clickedComponent: CanvasDatum | undefined;
            let targetType: 'canvas' | 'component' = 'canvas';

            if (componentElement) {
                // Right-clicked on a component or connection
                targetType = 'component';

                // Get the component from D3 datum
                const datum = d3.select(componentElement).datum() as CanvasDatum | undefined;
                if (datum) {
                    clickedComponent = datum;

                    // If right-clicked component is not selected, select it
                    // This matches the flow-designer behavior
                    if (!this.selectedComponentIds().includes(datum.entity.id)) {
                        this.handleComponentSelection(datum.entity.id, datum.ui.componentType, false);
                    }
                }
            } else {
                // Right-clicked on canvas background - clear selection
                if (this.selectedComponentIds().length > 0) {
                    this.deselectAll.emit();
                }
            }

            // Build array of selected components with full data
            const selectedIds = this.selectedComponentIds();
            const selectedComponents: CanvasDatum[] = [];

            // Gather selected components from all internal arrays
            this.internalLabels().forEach((c) => {
                if (selectedIds.includes(c.entity.id)) selectedComponents.push(c);
            });
            this.internalProcessors().forEach((c) => {
                if (selectedIds.includes(c.entity.id)) selectedComponents.push(c);
            });
            this.internalFunnels().forEach((c) => {
                if (selectedIds.includes(c.entity.id)) selectedComponents.push(c);
            });
            this.internalPorts().forEach((c) => {
                if (selectedIds.includes(c.entity.id)) selectedComponents.push(c);
            });
            this.internalRemoteProcessGroups().forEach((c) => {
                if (selectedIds.includes(c.entity.id)) selectedComponents.push(c);
            });
            this.internalProcessGroups().forEach((c) => {
                if (selectedIds.includes(c.entity.id)) selectedComponents.push(c);
            });
            this.internalConnections().forEach((c) => {
                if (selectedIds.includes(c.entity.id)) selectedComponents.push(c);
            });

            // Build context and emit it so parent can store it
            const context: ContextMenuContext = {
                processGroupId: this.processGroupId(),
                targetType,
                selectedComponents,
                clickedComponent,
                allConnections: this.internalConnections()
            };

            // Emit the context so parent can update its provider's state
            this.contextMenuOpened.emit(context);
        });
    }

    /**
     * Set up box selection behavior (Shift+Drag)
     * Allows selecting multiple components by dragging a selection rectangle
     */
    private setupBoxSelection(): void {
        // Attach box selection handlers to SVG (same element as zoom)
        // Zoom filter prevents zoom on Shift+mousedown, allowing selection to work
        this.svg
            .on('mousedown.selection', (event: MouseEvent) => {
                this.canvasClicked = true;

                // Only handle left mouse button
                if (event.button !== 0) {
                    return;
                }

                // Show selection box if shift is held down
                if (event.shiftKey) {
                    const position: [number, number] = d3.pointer(event, this.canvasGroup.node());
                    this.canvasGroup
                        .append('rect')
                        .attr('rx', 6)
                        .attr('ry', 6)
                        .attr('x', position[0])
                        .attr('y', position[1])
                        .attr('class', 'component-selection')
                        .attr('width', 0)
                        .attr('height', 0)
                        .attr('stroke-width', () => 1 / this.currentScale)
                        .attr('stroke-dasharray', () => 4 / this.currentScale)
                        .datum(position);

                    // Prevent further propagation to zoom handlers
                    event.stopImmediatePropagation();

                    // Prevent text selection
                    event.preventDefault();
                }
            })
            .on('mousemove.selection', (event: MouseEvent) => {
                // Update selection box if shift is held down
                if (event.shiftKey) {
                    const selectionBox = this.canvasGroup.select<SVGRectElement>('rect.component-selection');
                    if (!selectionBox.empty()) {
                        const originalPosition = selectionBox.datum() as [number, number];
                        const position: [number, number] = d3.pointer(event, this.canvasGroup.node());

                        const d: SelectionBoxData = { x: 0, y: 0, width: 0, height: 0 };
                        if (originalPosition[0] < position[0]) {
                            d.x = originalPosition[0];
                            d.width = position[0] - originalPosition[0];
                        } else {
                            d.x = position[0];
                            d.width = originalPosition[0] - position[0];
                        }

                        if (originalPosition[1] < position[1]) {
                            d.y = originalPosition[1];
                            d.height = position[1] - originalPosition[1];
                        } else {
                            d.y = position[1];
                            d.height = originalPosition[1] - position[1];
                        }

                        // Update the selection box
                        selectionBox.attr('width', d.width).attr('height', d.height).attr('x', d.x).attr('y', d.y);

                        // Prevent further propagation
                        event.stopPropagation();
                    }
                }
            })
            .on('mouseup.selection', (_event: MouseEvent) => {
                // Ensure this originated from clicking the canvas, not a component
                if (!this.canvasClicked) {
                    return;
                }

                // Reset the canvas click flag
                this.canvasClicked = false;

                // Get the selection box
                const selectionBox = this.canvasGroup.select<SVGRectElement>('rect.component-selection');
                if (!selectionBox.empty()) {
                    const selectionBoundingBox = {
                        x: parseInt(selectionBox.attr('x'), 10),
                        y: parseInt(selectionBox.attr('y'), 10),
                        width: parseInt(selectionBox.attr('width'), 10),
                        height: parseInt(selectionBox.attr('height'), 10)
                    };

                    // Find all components within the selection box
                    const selectedComponents = this.getComponentsInSelectionBox(selectionBoundingBox);

                    // Emit selection event
                    if (selectedComponents.length > 0) {
                        this.selectComponents.emit(selectedComponents);
                    } else {
                        this.deselectAll.emit();
                    }

                    // Remove the selection box
                    selectionBox.remove();
                }
                // Note: Canvas background click deselection is handled by D3 zoom 'end' event
            });
    }

    /**
     * Get all components that intersect with the selection box
     * Includes already selected components + components within bounds
     */
    private getComponentsInSelectionBox(box: {
        x: number;
        y: number;
        width: number;
        height: number;
    }): Array<{ id: string; type: ComponentType }> {
        const selected: Array<{ id: string; type: ComponentType }> = [];
        const currentSelection = this.selectedComponentIds();

        // Helper to check if component is within selection box
        const isInBox = (x: number, y: number, width: number, height: number): boolean => {
            return x >= box.x && x + width <= box.x + box.width && y >= box.y && y + height <= box.y + box.height;
        };

        // Check labels
        this.internalLabels().forEach((label) => {
            const alreadySelected = currentSelection.includes(label.entity.id);
            const inBox = isInBox(
                label.entity.position.x,
                label.entity.position.y,
                label.ui.dimensions.width,
                label.ui.dimensions.height
            );
            if (alreadySelected || inBox) {
                selected.push({ id: label.entity.id, type: label.ui.componentType });
            }
        });

        // Check processors
        this.internalProcessors().forEach((processor) => {
            const alreadySelected = currentSelection.includes(processor.entity.id);
            const inBox = isInBox(
                processor.entity.position.x,
                processor.entity.position.y,
                processor.ui.dimensions.width,
                processor.ui.dimensions.height
            );
            if (alreadySelected || inBox) {
                selected.push({ id: processor.entity.id, type: processor.ui.componentType });
            }
        });

        // Check funnels
        this.internalFunnels().forEach((funnel) => {
            const alreadySelected = currentSelection.includes(funnel.entity.id);
            const inBox = isInBox(
                funnel.entity.position.x,
                funnel.entity.position.y,
                funnel.ui.dimensions.width,
                funnel.ui.dimensions.height
            );
            if (alreadySelected || inBox) {
                selected.push({ id: funnel.entity.id, type: funnel.ui.componentType });
            }
        });

        // Check ports
        this.internalPorts().forEach((port) => {
            const alreadySelected = currentSelection.includes(port.entity.id);
            const inBox = isInBox(
                port.entity.position.x,
                port.entity.position.y,
                port.ui.dimensions.width,
                port.ui.dimensions.height
            );
            if (alreadySelected || inBox) {
                selected.push({ id: port.entity.id, type: port.ui.componentType });
            }
        });

        // Check remote process groups
        this.internalRemoteProcessGroups().forEach((rpg) => {
            const alreadySelected = currentSelection.includes(rpg.entity.id);
            const inBox = isInBox(
                rpg.entity.position.x,
                rpg.entity.position.y,
                rpg.ui.dimensions.width,
                rpg.ui.dimensions.height
            );
            if (alreadySelected || inBox) {
                selected.push({ id: rpg.entity.id, type: rpg.ui.componentType });
            }
        });

        // Check process groups
        this.internalProcessGroups().forEach((pg) => {
            const alreadySelected = currentSelection.includes(pg.entity.id);
            const inBox = isInBox(
                pg.entity.position.x,
                pg.entity.position.y,
                pg.ui.dimensions.width,
                pg.ui.dimensions.height
            );
            if (alreadySelected || inBox) {
                selected.push({ id: pg.entity.id, type: pg.ui.componentType });
            }
        });

        // Check connections (using bounding box of all points)
        // Connection renderer calculates and stores start/end/bends in connection.ui
        this.internalConnections().forEach((connection) => {
            const alreadySelected = currentSelection.includes(connection.entity.id);

            // Get calculated positions from connection.ui (set by renderer)
            const start = connection.ui.start;
            const end = connection.ui.end;
            const bends = connection.ui.bends || [];

            // Skip if positions not yet calculated (connection not rendered)
            if (!start || !end) {
                // If already selected, still include it
                if (alreadySelected) {
                    selected.push({ id: connection.entity.id, type: connection.ui.componentType });
                }
                return;
            }

            // Build points array: start → bends → end
            const points: Array<{ x: number; y: number }> = [start, ...bends, end];

            // Calculate connection bounding box
            const xs = points.map((p) => p.x);
            const ys = points.map((p) => p.y);
            const minX = Math.min(...xs);
            const maxX = Math.max(...xs);
            const minY = Math.min(...ys);
            const maxY = Math.max(...ys);

            // Check if connection is within selection box
            const inBox = minX >= box.x && maxX <= box.x + box.width && minY >= box.y && maxY <= box.y + box.height;

            if (alreadySelected || inBox) {
                selected.push({ id: connection.entity.id, type: connection.ui.componentType });
            }
        });

        return selected;
    }

    /**
     * Helper method to handle component selection
     * Supports mixed-type selections (standard NiFi behavior)
     */
    private handleComponentSelection(componentId: string, componentType: ComponentType, isShiftKey: boolean): void {
        const currentSelection = this.selectedComponentIds();
        const isAlreadySelected = currentSelection.includes(componentId);
        const typeMap = this.componentTypeMap();

        // Reset canvasClicked since this is a component click, not a canvas background click
        // This prevents spurious zoom.end events from incorrectly triggering deselection
        this.canvasClicked = false;

        if (isShiftKey) {
            // Shift+Click: Add or remove from selection
            if (isAlreadySelected) {
                // Remove this component
                const newSelection = currentSelection.filter((id) => id !== componentId);
                if (newSelection.length === 0) {
                    this.deselectAll.emit();
                } else {
                    // Build array with correct type for each ID
                    const components = newSelection
                        .map((id) => {
                            const type = typeMap.get(id);
                            return type ? { id, type } : null;
                        })
                        .filter((c) => c !== null) as Array<{ id: string; type: ComponentType }>;
                    this.selectComponents.emit(components);
                }
            } else {
                // Add to selection
                const newSelection = [...currentSelection, componentId];
                // Build array with correct type for each ID
                const components = newSelection
                    .map((id) => {
                        const type = typeMap.get(id);
                        return type ? { id, type } : null;
                    })
                    .filter((c) => c !== null) as Array<{ id: string; type: ComponentType }>;
                this.selectComponents.emit(components);
            }
        } else {
            // Regular click: Replace selection
            this.selectComponents.emit([{ id: componentId, type: componentType }]);
        }
    }

    /**
     * Handle label click - dispatch selection action and emit event for parent
     */
    onLabelClick({ label, event }: { label: CanvasLabel; event: MouseEvent }): void {
        if (!this.canSelectSignal()) return;
        this.handleComponentSelection(label.entity.id, label.ui.componentType, event.shiftKey);
    }

    /**
     * Handle label double-click - could navigate to edit
     */
    onLabelDoubleClick(_event: { label: CanvasLabel; event: MouseEvent }): void {
        // Labels are edited inline - no dialog needed
    }

    /**
     * Handle label resize end - update UI state and emit to parent for server save
     */
    onLabelResizeEnd(event: { label: CanvasLabel; dimensions: { width: number; height: number } }): void {
        const labelId = event.label.entity.id;

        // Update ui.dimensions immediately (ui.dimensions is already updated by resize behavior)
        // Mark as saving (this will disable further resizes)
        this.savingLabels.update((labels) => new Set(labels).add(labelId));

        // Emit to parent for server save
        this.labelResizeEnd.emit({
            id: labelId,
            dimensions: event.dimensions,
            revision: event.label.ui.dragStartRevision ?? event.label.entity.revision
        });
        delete event.label.ui.dragStartRevision;
    }

    /**
     * Handle label drag end - add to saving set and emit to parent for API call
     */
    onLabelDragEnd({ label, newPosition }: { label: CanvasLabel; newPosition: Position }): void {
        const labelId = label.entity.id;

        // Add to saving set to disable during API call
        this.savingLabels.update((labels) => new Set(labels).add(labelId));

        this.componentDragEnd.emit({
            id: labelId,
            type: label.ui.componentType,
            position: newPosition
        });
    }

    /**
     * Public API: Called by parent when server confirms save success
     * Clears pending state and re-enables resize
     * Note: This is called AFTER the reducer has updated the state and Angular has re-rendered
     * with the new entity.dimensions from the server
     */
    public confirmLabelResize(labelId: string): void {
        // Clear saving flag (re-enables resize)
        this.savingLabels.update((labels) => {
            const newLabels = new Set(labels);
            newLabels.delete(labelId);
            return newLabels;
        });

        // No need to update ui.dimensions - the computed signal will update it from entity.dimensions
        // when the next render cycle runs with the updated entity from the reducer
    }

    /**
     * Public API: Called by parent when server save fails
     * Reverts to entity dimensions (original server dimensions) and re-enables resize
     */
    public revertLabelDimensions(labelId: string): void {
        // Clear saving flag (re-enables resize)
        this.savingLabels.update((labels) => {
            const newLabels = new Set(labels);
            newLabels.delete(labelId);
            return newLabels;
        });

        // Revert ui.dimensions to entity.dimensions (source of truth)
        const label = this._internalLabels.find((l) => l.entity.id === labelId);
        if (label) {
            label.ui.dimensions = {
                width: label.entity.dimensions?.width || 148,
                height: label.entity.dimensions?.height || 148
            };
        }

        // Force a re-render to show the reverted dimensions
        this.renderTriggerValue.update((v) => v + 1);
    }

    /**
     * Public API: Called by parent when server confirms label position update success
     * Clears pending state and re-enables drag
     */
    public confirmLabelPosition(labelId: string): void {
        // Clear saving flag (re-enables drag)
        this.savingLabels.update((labels) => {
            const newLabels = new Set(labels);
            newLabels.delete(labelId);
            return newLabels;
        });

        // Clean up currentPosition from UI state (no longer needed)
        const label = this._internalLabels.find((l) => l.entity.id === labelId);
        if (label) {
            delete label.ui.currentPosition;
        }
    }

    /**
     * Public API: Called by parent when server label position update fails
     * Clears pending state and re-enables drag (position revert handled by reducer)
     */
    public revertLabelPosition(labelId: string): void {
        // Clear saving flag (re-enables drag)
        this.savingLabels.update((labels) => {
            const newLabels = new Set(labels);
            newLabels.delete(labelId);
            return newLabels;
        });

        // Clean up currentPosition from UI state (revert to entity.position)
        const label = this._internalLabels.find((l) => l.entity.id === labelId);
        if (label) {
            delete label.ui.currentPosition;
        }

        // Force a re-render to show the reverted position
        this.renderTriggerValue.update((v) => v + 1);
    }

    /**
     * Public API: Called by parent when server confirms processor position update success
     * Clears pending state and re-enables drag
     */
    public confirmProcessorPosition(processorId: string): void {
        // Clear saving flag (re-enables drag)
        this.savingProcessors.update((processors) => {
            const newProcessors = new Set(processors);
            newProcessors.delete(processorId);
            return newProcessors;
        });

        // Clean up currentPosition from UI state (no longer needed)
        const processor = this._internalProcessors.find((p) => p.entity.id === processorId);
        if (processor) {
            delete processor.ui.currentPosition;
        }
    }

    /**
     * Public API: Called by parent when server processor position update fails
     * Clears pending state and re-enables drag (position revert handled by reducer)
     */
    public revertProcessorPosition(processorId: string): void {
        // Clear saving flag (re-enables drag)
        this.savingProcessors.update((processors) => {
            const newProcessors = new Set(processors);
            newProcessors.delete(processorId);
            return newProcessors;
        });

        // Clean up currentPosition from UI state (revert to entity.position)
        const processor = this._internalProcessors.find((p) => p.entity.id === processorId);
        if (processor) {
            delete processor.ui.currentPosition;
        }

        // Force a re-render to show the reverted position
        this.renderTriggerValue.update((v) => v + 1);
    }

    /**
     * Public API: Called by parent when server confirms port position update success
     * Clears pending state and re-enables drag
     */
    public confirmPortPosition(portId: string): void {
        // Clear saving flag (re-enables drag)
        this.savingPorts.update((ports) => {
            const newPorts = new Set(ports);
            newPorts.delete(portId);
            return newPorts;
        });

        // Clean up currentPosition from UI state (no longer needed)
        const port = this.internalPorts().find((p) => p.entity.id === portId);
        if (port) {
            delete port.ui.currentPosition;
        }
    }

    /**
     * Public API: Called by parent when server port position update fails
     * Clears pending state and re-enables drag (position revert handled by reducer)
     */
    public revertPortPosition(portId: string): void {
        // Clear saving flag (re-enables drag)
        this.savingPorts.update((ports) => {
            const newPorts = new Set(ports);
            newPorts.delete(portId);
            return newPorts;
        });

        // Clean up currentPosition from UI state (revert to entity.position)
        const port = this.internalPorts().find((p) => p.entity.id === portId);
        if (port) {
            delete port.ui.currentPosition;
        }

        // Force a re-render to show the reverted position
        this.renderTriggerValue.update((v) => v + 1);
    }

    /**
     * Public API: Called by parent when server confirms remote process group position update success
     * Clears pending state and re-enables drag
     */
    public confirmRemoteProcessGroupPosition(rpgId: string): void {
        // Clear saving flag (re-enables drag)
        this.savingRemoteProcessGroups.update((rpgs) => {
            const newRpgs = new Set(rpgs);
            newRpgs.delete(rpgId);
            return newRpgs;
        });

        // Clean up currentPosition from UI state (no longer needed)
        const rpg = this.internalRemoteProcessGroups().find((r) => r.entity.id === rpgId);
        if (rpg) {
            delete rpg.ui.currentPosition;
        }
    }

    /**
     * Public API: Called by parent when server remote process group position update fails
     * Clears pending state and re-enables drag (position revert handled by reducer)
     */
    public revertRemoteProcessGroupPosition(rpgId: string): void {
        // Clear saving flag (re-enables drag)
        this.savingRemoteProcessGroups.update((rpgs) => {
            const newRpgs = new Set(rpgs);
            newRpgs.delete(rpgId);
            return newRpgs;
        });

        // Clean up currentPosition from UI state (revert to entity.position)
        const rpg = this.internalRemoteProcessGroups().find((r) => r.entity.id === rpgId);
        if (rpg) {
            delete rpg.ui.currentPosition;
        }

        // Force a re-render to show the reverted position
        this.renderTriggerValue.update((v) => v + 1);
    }

    /**
     * Public API: Called by parent when server confirms process group position update success
     * Clears pending state and re-enables drag
     */
    public confirmProcessGroupPosition(pgId: string): void {
        // Clear saving flag (re-enables drag)
        this.savingProcessGroups.update((pgs) => {
            const newPgs = new Set(pgs);
            newPgs.delete(pgId);
            return newPgs;
        });

        // Clean up currentPosition from UI state (no longer needed)
        const pg = this.internalProcessGroups().find((p) => p.entity.id === pgId);
        if (pg) {
            delete pg.ui.currentPosition;
        }
    }

    /**
     * Public API: Called by parent when server process group position update fails
     * Clears pending state and re-enables drag (position revert handled by reducer)
     */
    public revertProcessGroupPosition(pgId: string): void {
        // Clear saving flag (re-enables drag)
        this.savingProcessGroups.update((pgs) => {
            const newPgs = new Set(pgs);
            newPgs.delete(pgId);
            return newPgs;
        });

        // Clean up currentPosition from UI state (revert to entity.position)
        const pg = this.internalProcessGroups().find((p) => p.entity.id === pgId);
        if (pg) {
            delete pg.ui.currentPosition;
        }

        // Force a re-render to show the reverted position
        this.renderTriggerValue.update((v) => v + 1);
    }

    /**
     * Public API: Called by parent when server funnel position update succeeds
     * Clears pending state and re-enables drag
     */
    public confirmFunnelPosition(funnelId: string): void {
        // Clear saving flag (re-enables drag)
        this.savingFunnels.update((funnels) => {
            const newFunnels = new Set(funnels);
            newFunnels.delete(funnelId);
            return newFunnels;
        });

        // Clean up currentPosition from UI state (no longer needed)
        const funnel = this._internalFunnels.find((f) => f.entity.id === funnelId);
        if (funnel) {
            delete funnel.ui.currentPosition;
        }
    }

    /**
     * Public API: Called by parent when server funnel position update fails
     * Clears pending state and re-enables drag (position revert handled by reducer)
     */
    public revertFunnelPosition(funnelId: string): void {
        // Clear saving flag (re-enables drag)
        this.savingFunnels.update((funnels) => {
            const newFunnels = new Set(funnels);
            newFunnels.delete(funnelId);
            return newFunnels;
        });

        // Clean up currentPosition from UI state (revert to entity.position)
        const funnel = this._internalFunnels.find((f) => f.entity.id === funnelId);
        if (funnel) {
            delete funnel.ui.currentPosition;
        }

        // Force a re-render to show the reverted position
        this.renderTriggerValue.update((v) => v + 1);
    }

    /**
     * Handle connection bend point drag end - update UI state and emit to parent for server save
     */
    onConnectionBendPointDragEnd(event: {
        connection: CanvasConnection;
        bends: Array<{ x: number; y: number }>;
    }): void {
        const connectionId = event.connection.entity.id;

        // Update UI state immediately (ui.bends is already updated by drag behavior)
        // Mark as saving (this will disable further bend point drags)
        this.savingConnections.update((connections) => new Set(connections).add(connectionId));

        // Emit to parent for server save
        this.connectionBendPointsUpdate.emit({
            id: connectionId,
            bends: event.bends,
            revision: event.connection.ui.dragStartRevision ?? event.connection.entity.revision
        });
        delete event.connection.ui.dragStartRevision;
    }

    /**
     * Handle bend point add - update UI state and emit to parent for server save
     */
    onConnectionBendPointAdd(event: {
        connection: CanvasConnection;
        point: { x: number; y: number; index: number };
    }): void {
        const connectionId = event.connection.entity.id;

        // UI state is already updated by the renderer
        // Mark as saving (this will disable further bend point operations)
        this.savingConnections.update((connections) => new Set(connections).add(connectionId));

        // Emit current ui.bends to parent for server save (unified event)
        this.connectionBendPointsUpdate.emit({
            id: connectionId,
            bends: event.connection.ui.bends || [],
            revision: event.connection.entity.revision
        });
    }

    /**
     * Handle bend point remove - update UI state and emit to parent for server save.
     * If removing the last bend point would cause a visual overlap with another
     * straight-line connection, the removal is reverted to prevent hidden connections.
     */
    onConnectionBendPointRemove(event: { connection: CanvasConnection; index: number }): void {
        const connectionId = event.connection.entity.id;
        const currentBends = event.connection.ui.bends || [];

        if (currentBends.length === 0) {
            const entity = event.connection.entity;
            const mapped: OverlapDetectionConnection[] = this.internalConnections().map((c) => ({
                id: c.entity.id,
                sourceId: c.entity.sourceId,
                destinationId: c.entity.destinationId,
                sourceGroupId: c.entity.sourceGroupId,
                destinationGroupId: c.entity.destinationGroupId,
                bends: c.entity.id === connectionId ? [] : (c.entity.bends ?? [])
            }));

            if (wouldRemovalCauseOverlap(connectionId, mapped, this.processGroupId() ?? '')) {
                event.connection.ui.bends = entity.bends ? [...entity.bends] : [];
                this.renderTriggerValue.update((v) => v + 1);
                this.snackBar.open('Bend point required to prevent overlapping connections.', undefined, {
                    duration: 4000,
                    verticalPosition: 'top'
                });
                return;
            }
        }

        this.savingConnections.update((connections) => new Set(connections).add(connectionId));

        this.connectionBendPointsUpdate.emit({
            id: connectionId,
            bends: currentBends,
            revision: event.connection.entity.revision
        });
    }

    /**
     * Handle connection label drag - emit to parent for server save
     */
    onConnectionLabelDragEnd(event: { connection: CanvasConnection; labelIndex: number }): void {
        const connectionId = event.connection.entity.id;

        // Mark as saving (this will disable further label dragging)
        this.savingConnections.update((connections) => new Set(connections).add(connectionId));

        // Emit label index change to parent for server save
        this.connectionLabelDragEnd.emit({
            id: connectionId,
            labelIndex: event.labelIndex,
            revision: event.connection.ui.dragStartRevision ?? event.connection.entity.revision
        });
        delete event.connection.ui.dragStartRevision;
    }

    onConnectionEndpointReconnect(event: {
        connection: CanvasConnection;
        newDestination: ConnectionEndpointReconnectDestination;
        bends?: Position[];
    }): void {
        const connectionId = event.connection.entity.id;
        this.savingConnections.update((connections) => new Set(connections).add(connectionId));
        const revision = event.connection.ui.dragStartRevision ?? event.connection.entity.revision;
        delete event.connection.ui.dragStartRevision;

        if (event.bends) {
            event.connection.ui.bends = event.bends.map((bend) => ({ ...bend }));
        }
        event.connection.ui.reconnectDestinationId = event.newDestination.id;

        this.connectionDestinationChangeRequested.emit({
            id: connectionId,
            revision,
            newDestination: event.newDestination,
            bends: event.bends
        });
    }

    public confirmConnectionDestination(connectionId: string): void {
        this.savingConnections.update((connections) => {
            const updatedConnections = new Set(connections);
            updatedConnections.delete(connectionId);
            return updatedConnections;
        });

        const connection = this._internalConnections.find((item) => item.entity.id === connectionId);
        if (connection) {
            connection.ui.bends = undefined;
            delete connection.ui.reconnectDestinationId;
        }
    }

    public revertConnectionDestination(connectionId: string): void {
        this.savingConnections.update((connections) => {
            const updatedConnections = new Set(connections);
            updatedConnections.delete(connectionId);
            return updatedConnections;
        });

        const connection = this._internalConnections.find((item) => item.entity.id === connectionId);
        if (connection) {
            connection.ui.bends = undefined;
            delete connection.ui.endPointDragging;
            delete connection.ui.reconnectDestinationId;
        }

        this.renderTriggerValue.update((value) => value + 1);
    }

    /**
     * Public API: Called by parent when server confirms bend point save success
     * Clears pending state and re-enables bend point dragging
     * Note: This is called AFTER the reducer has updated the state and Angular has re-rendered
     * with the new entity.bends from the server
     */
    public confirmConnectionBendPoints(connectionId: string): void {
        // Clear saving flag (re-enables bend point dragging)
        this.savingConnections.update((connections) => {
            const newConnections = new Set(connections);
            newConnections.delete(connectionId);
            return newConnections;
        });

        // Clear ui.bends so it will be re-initialized from the updated entity.bends on next render
        // At this point, entity.bends has already been updated by the reducer
        const connection = this._internalConnections.find((c) => c.entity.id === connectionId);
        if (connection) {
            connection.ui.bends = undefined;
        }

        // Force a re-render to pick up the updated entity.bends
        this.renderTriggerValue.update((v) => v + 1);
    }

    /**
     * Public API: Called by parent when server confirms label index save success
     * Clears pending state and re-enables label dragging
     */
    public confirmConnectionLabelIndex(connectionId: string): void {
        // Clear saving flag (re-enables label dragging)
        this.savingConnections.update((connections) => {
            const newConnections = new Set(connections);
            newConnections.delete(connectionId);
            return newConnections;
        });

        // Clear tempLabelIndex now that save is confirmed
        // The label will now use the updated entity.component.labelIndex from the reducer
        const connection = this._internalConnections.find((c) => c.entity.id === connectionId);
        if (connection) {
            delete connection.ui.tempLabelIndex;
        }

        // Force a re-render to pick up the updated labelIndex
        this.renderTriggerValue.update((v) => v + 1);
    }

    /**
     * Public API: Called by parent when label index save fails
     * Reverts label position to previous state
     */
    public revertConnectionLabelIndex(connectionId: string): void {
        // Clear saving flag
        this.savingConnections.update((connections) => {
            const newConnections = new Set(connections);
            newConnections.delete(connectionId);
            return newConnections;
        });

        // Clear tempLabelIndex if it exists
        const connection = this._internalConnections.find((c) => c.entity.id === connectionId);
        if (connection) {
            delete connection.ui.tempLabelIndex;
        }

        // Force a re-render to revert to original labelIndex
        this.renderTriggerValue.update((v) => v + 1);
    }

    /**
     * Public API: Called by parent when server save fails
     * Reverts UI state to entity state (source of truth)
     */
    public revertConnectionBendPoints(connectionId: string): void {
        // Clear saving flag
        this.savingConnections.update((connections) => {
            const newConnections = new Set(connections);
            newConnections.delete(connectionId);
            return newConnections;
        });

        // Revert ui.bends to entity.bends (source of truth)
        const connection = this._internalConnections.find((c) => c.entity.id === connectionId);
        if (connection) {
            connection.ui.bends = connection.entity.bends ? [...connection.entity.bends] : [];
            // Force re-render to show reverted bends
            this.renderTriggerValue.update((v) => v + 1);
        }
    }

    /**
     * Handle processor click
     * Canvas determines multi-select logic and emits selection intent to parent
     */
    onProcessorClick(processor: CanvasProcessor, event: MouseEvent): void {
        if (!this.canSelectSignal()) return;
        this.handleComponentSelection(processor.entity.id, processor.ui.componentType, event.shiftKey);
    }

    /**
     * Handle processor double-click - could navigate to edit
     */
    onProcessorDoubleClick({ processor }: { processor: CanvasProcessor; event: MouseEvent }): void {
        this.componentDoubleClick.emit({ entity: processor.entity, componentType: ComponentType.Processor });
    }

    onDragEnd({ delta, movingIds }: { delta: Position; movingIds: Set<string> }): void {
        const targetGroupId = DragUtils.readDropTarget(this.canvasGroup) ?? undefined;
        if (delta.x === 0 && delta.y === 0 && !targetGroupId) {
            this.clearNoOpDragState(movingIds);
            return;
        }

        const items: DragEndBaselineItem[] = [];
        const includedIds = new Set<string>();
        for (const id of movingIds) {
            const type = this.componentTypeMap().get(id);
            if (type === undefined) {
                continue;
            }
            const wrapper = this.findWrapperByType(id, type);
            if (!wrapper) {
                continue;
            }
            const source = wrapper.ui.dragStartEntity ?? wrapper.entity;
            items.push(this.projectBaselineItem(id, type as DragEndBaselineItem['type'], source));
            includedIds.add(id);
            delete wrapper.ui.dragStartEntity;
        }
        for (const connection of this.internalConnections()) {
            const entity = connection.entity;
            if (
                entity.sourceId === entity.destinationId &&
                movingIds.has(entity.sourceId) &&
                !includedIds.has(entity.id)
            ) {
                const source = connection.ui.dragStartEntity ?? entity;
                items.push(this.projectBaselineItem(entity.id, ComponentType.Connection, source));
                delete connection.ui.dragStartEntity;
            }
        }

        if (items.length === 0) {
            return;
        }
        this.markItemsSaving(items);
        this.componentsDragEnd.emit(targetGroupId ? { items, delta, targetGroupId } : { items, delta });
    }

    private clearNoOpDragState(movingIds: Set<string>): void {
        for (const id of movingIds) {
            const type = this.componentTypeMap().get(id);
            if (type === undefined) {
                continue;
            }
            const wrapper = this.findWrapperByType(id, type);
            if (!wrapper) {
                continue;
            }
            delete wrapper.ui.dragStartEntity;
            if (!isConnectionDatum(wrapper)) {
                delete wrapper.ui.dragDelta;
                delete wrapper.ui.dragMovingIds;
                delete wrapper.ui.dragStartPosition;
                delete wrapper.ui.currentPosition;
            } else {
                delete wrapper.ui.dragStartBends;
                wrapper.ui.bends = (wrapper.entity.bends ?? []).map((bend) => ({ ...bend }));
            }
        }

        for (const connection of this.internalConnections()) {
            if (
                connection.entity.sourceId === connection.entity.destinationId &&
                movingIds.has(connection.entity.sourceId)
            ) {
                delete connection.ui.dragStartEntity;
                delete connection.ui.dragStartBends;
                connection.ui.bends = (connection.entity.bends ?? []).map((bend) => ({ ...bend }));
            }
        }
    }

    private projectBaselineItem(
        id: string,
        type: DragEndBaselineItem['type'],
        source: CanvasEntity
    ): DragEndBaselineItem {
        const revision: RevisionRequest = {
            version: source.revision.version,
            ...(source.revision.clientId !== undefined && { clientId: source.revision.clientId })
        };
        if (type === ComponentType.Connection) {
            const connection = source as ConnectionEntity;
            return {
                id,
                type,
                bends: (connection.bends ?? []).map((bend) => ({ ...bend })),
                revision,
                ...(connection.component?.labelIndex !== undefined && {
                    labelIndex: connection.component.labelIndex
                })
            };
        }
        const position = 'position' in source ? source.position : { x: 0, y: 0 };
        return { id, type, position: { ...position }, revision };
    }

    private findWrapperByType(id: string, type: ComponentType): CanvasDatum | undefined {
        switch (type) {
            case ComponentType.Processor:
                return this._internalProcessors.find((item) => item.entity.id === id);
            case ComponentType.Funnel:
                return this._internalFunnels.find((item) => item.entity.id === id);
            case ComponentType.InputPort:
            case ComponentType.OutputPort:
                return this._internalPorts.find((item) => item.entity.id === id);
            case ComponentType.ProcessGroup:
                return this._internalProcessGroups.find((item) => item.entity.id === id);
            case ComponentType.RemoteProcessGroup:
                return this._internalRemoteProcessGroups.find((item) => item.entity.id === id);
            case ComponentType.Label:
                return this._internalLabels.find((item) => item.entity.id === id);
            case ComponentType.Connection:
                return this._internalConnections.find((item) => item.entity.id === id);
            default:
                return undefined;
        }
    }

    private markItemsSaving(items: CanvasComponentRef[]): void {
        for (const item of items) {
            const update = (value: Set<string>) => new Set(value).add(item.id);
            switch (item.type) {
                case ComponentType.Processor:
                    this.savingProcessors.update(update);
                    break;
                case ComponentType.Funnel:
                    this.savingFunnels.update(update);
                    break;
                case ComponentType.InputPort:
                case ComponentType.OutputPort:
                    this.savingPorts.update(update);
                    break;
                case ComponentType.ProcessGroup:
                    this.savingProcessGroups.update(update);
                    break;
                case ComponentType.RemoteProcessGroup:
                    this.savingRemoteProcessGroups.update(update);
                    break;
                case ComponentType.Label:
                    this.savingLabels.update(update);
                    break;
                case ComponentType.Connection:
                    this.savingConnections.update(update);
                    break;
            }
        }
    }

    /**
     * Handle processor drag end - emit to parent for API call
     */
    onProcessorDragEnd({ processor, newPosition }: { processor: CanvasProcessor; newPosition: Position }): void {
        const processorId = processor.entity.id;

        // Add to saving set to disable during API call
        this.savingProcessors.update((processors) => new Set(processors).add(processorId));

        this.componentDragEnd.emit({
            id: processorId,
            type: processor.ui.componentType,
            position: newPosition
        });
    }

    /**
     * Handle port drag end - add to saving set and emit to parent for API call
     */
    onPortDragEnd({ port, newPosition }: { port: CanvasPort; newPosition: Position }): void {
        const portId = port.entity.id;

        // Add to saving set to disable during API call
        this.savingPorts.update((ports) => new Set(ports).add(portId));

        this.componentDragEnd.emit({
            id: portId,
            type: port.ui.componentType,
            position: newPosition
        });
    }

    /**
     * Handle remote process group drag end - add to saving set and emit to parent for API call
     */
    onRemoteProcessGroupDragEnd({ rpg, newPosition }: { rpg: CanvasRemoteProcessGroup; newPosition: Position }): void {
        const rpgId = rpg.entity.id;

        // Add to saving set to disable during API call
        this.savingRemoteProcessGroups.update((rpgs) => new Set(rpgs).add(rpgId));

        this.componentDragEnd.emit({
            id: rpgId,
            type: rpg.ui.componentType,
            position: newPosition
        });
    }

    /**
     * Handle process group drag end - add to saving set and emit to parent for API call
     */
    onProcessGroupDragEnd({ pg, newPosition }: { pg: CanvasProcessGroup; newPosition: Position }): void {
        const pgId = pg.entity.id;

        // Add to saving set to disable during API call
        this.savingProcessGroups.update((pgs) => new Set(pgs).add(pgId));

        this.componentDragEnd.emit({
            id: pgId,
            type: pg.ui.componentType,
            position: newPosition
        });
    }

    /**
     * Handle funnel click
     */
    onFunnelClick({ funnel, event }: { funnel: CanvasFunnel; event: MouseEvent }): void {
        if (!this.canSelectSignal()) return;
        this.handleComponentSelection(funnel.entity.id, funnel.ui.componentType, event.shiftKey);
    }

    /**
     * Handle funnel double-click - could navigate to edit
     */
    onFunnelDoubleClick(_event: { funnel: CanvasFunnel; event: MouseEvent }): void {
        // Funnels have no configurable properties - intentional no-op
    }

    /**
     * Handle funnel drag end - update UI state and emit to parent for server save
     */
    onFunnelDragEnd({ funnel, newPosition }: { funnel: CanvasFunnel; newPosition: Position }): void {
        const funnelId = funnel.entity.id;

        // Add to saving set to disable during API call
        this.savingFunnels.update((funnels) => new Set(funnels).add(funnelId));

        this.componentDragEnd.emit({
            id: funnelId,
            type: ComponentType.Funnel,
            position: newPosition
        });
    }

    /**
     * Handle port click
     */
    onPortClick({ port, event }: { port: CanvasPort; event: MouseEvent }): void {
        if (!this.canSelectSignal()) return;
        this.handleComponentSelection(port.entity.id, port.ui.componentType, event.shiftKey);
    }

    /**
     * Handle port double-click - could navigate to edit
     */
    onPortDoubleClick({ port }: { port: CanvasPort; event: MouseEvent }): void {
        this.componentDoubleClick.emit({ entity: port.entity, componentType: port.ui.componentType });
    }

    /**
     * Handle remote process group click
     */
    onRemoteProcessGroupClick({ rpg, event }: { rpg: CanvasRemoteProcessGroup; event: MouseEvent }): void {
        if (!this.canSelectSignal()) return;
        this.handleComponentSelection(rpg.entity.id, rpg.ui.componentType, event.shiftKey);
    }

    /**
     * Handle remote process group double-click - could navigate to edit
     */
    onRemoteProcessGroupDoubleClick({ rpg }: { rpg: CanvasRemoteProcessGroup; event: MouseEvent }): void {
        this.componentDoubleClick.emit({ entity: rpg.entity, componentType: ComponentType.RemoteProcessGroup });
    }

    /**
     * Handle process group click
     */
    onProcessGroupClick({ pg, event }: { pg: CanvasProcessGroup; event: MouseEvent }): void {
        if (!this.canSelectSignal()) return;
        this.handleComponentSelection(pg.entity.id, pg.ui.componentType, event.shiftKey);
    }

    /**
     * Handle process group double-click - navigate into group
     * Emits event for parent to handle routing
     */
    onProcessGroupDoubleClick({ pg }: { pg: CanvasProcessGroup; event: MouseEvent }): void {
        this.processGroupDoubleClick.emit({ processGroupId: pg.entity.id });
    }

    /**
     * Handle connection click
     */
    onConnectionClick({ connection, event }: { connection: CanvasConnection; event: MouseEvent }): void {
        if (!this.canSelectSignal()) return;
        this.handleComponentSelection(connection.entity.id, connection.ui.componentType, event.shiftKey);
    }

    /**
     * Handle connection double-click - could show details
     */
    onConnectionDoubleClick({ connection }: { connection: CanvasConnection; event: MouseEvent }): void {
        this.componentDoubleClick.emit({ entity: connection.entity, componentType: ComponentType.Connection });
    }

    /**
     * Persist current viewport to localStorage
     *
     */
    private persistViewport(): void {
        const processGroupId = this.processGroupId();
        if (!processGroupId) {
            return;
        }

        const name: string = CanvasComponent.VIEW_PREFIX + processGroupId;

        // Create the item to store
        const item: StorageTransform = {
            scale: this.currentScale,
            translateX: this.x,
            translateY: this.y
        };

        const expires = Date.now() + CanvasComponent.STORAGE_MILLIS_PER_DAY * 2;
        // Store the item
        localStorage.setItem(name, JSON.stringify({ expires, item }));
    }

    /**
     * Restore Viewport from LocalStorage
     *
     * Attempts to restore the user's previous viewport position (pan/zoom) from localStorage.
     * Each process group has its own stored viewport, keyed by process group ID.
     *
     * If no stored viewport exists or the stored data is invalid, falls back to fitContent()
     * which automatically fits all canvas content in the viewport.
     *
     * This is called internally by the auto-initialization effect after both canvas and data are ready.
     */
    public restoreViewportFromStorage(): void {
        const processGroupId = this.processGroupId();
        if (!processGroupId) {
            // No process group context - zoom to fit all content
            this.fitContent(false);
            return;
        }

        try {
            const name: string = CanvasComponent.VIEW_PREFIX + processGroupId;
            const item: StorageTransform | null = CanvasComponent.readViewportTransform(name);

            if (item) {
                // Restore previous viewport position
                this.applyTransform(item.translateX, item.translateY, item.scale, false);
            } else {
                // Either no entry exists, or it failed magnitude/range/expiry validation (e.g. a
                // catastrophic-finite ~9e307 written before this guard existed). Evict it so the
                // page-refresh loop cannot replay corrupted data (removeItem is a no-op if absent).
                localStorage.removeItem(name);
                this.fitContent(false);
            }
        } catch (_e) {
            // Storage read error - fall back to zoom to fit
            this.fitContent(false);
        }
    }

    /**
     * Fit all canvas content into the viewport.
     *
     * Calculates the bounding box of all components (processors, process groups,
     * labels, funnels, ports, remote process groups) using their positions and
     * dimensions from the internal computed signals. Then computes the optimal
     * scale and translation to center the content within the SVG viewport.
     *
     * If no components exist (empty canvas), applies an identity transform.
     *
     * @param transition - Whether to animate the viewport change
     */
    private fitContent(transition = false): void {
        const svg = this.canvasSvg()?.nativeElement;
        if (!svg) {
            return;
        }

        const svgRect = svg.getBoundingClientRect();
        const padding = 50;
        const canvasWidth = svgRect.width - padding;
        const canvasHeight = svgRect.height - padding;

        // Guard against zero-size container (not rendered yet)
        if (canvasWidth <= 0 || canvasHeight <= 0) {
            return;
        }

        // Calculate bounding box of all components
        let minX = Infinity;
        let minY = Infinity;
        let maxX = -Infinity;
        let maxY = -Infinity;

        const expandBounds = (x: number, y: number, width: number, height: number) => {
            minX = Math.min(minX, x);
            minY = Math.min(minY, y);
            maxX = Math.max(maxX, x + width);
            maxY = Math.max(maxY, y + height);
        };

        // Include all component types
        this.internalProcessors().forEach((p) => {
            expandBounds(p.entity.position.x, p.entity.position.y, p.ui.dimensions.width, p.ui.dimensions.height);
        });
        this.internalProcessGroups().forEach((pg) => {
            expandBounds(pg.entity.position.x, pg.entity.position.y, pg.ui.dimensions.width, pg.ui.dimensions.height);
        });
        this.internalRemoteProcessGroups().forEach((rpg) => {
            expandBounds(
                rpg.entity.position.x,
                rpg.entity.position.y,
                rpg.ui.dimensions.width,
                rpg.ui.dimensions.height
            );
        });
        this.internalLabels().forEach((l) => {
            expandBounds(l.entity.position.x, l.entity.position.y, l.ui.dimensions.width, l.ui.dimensions.height);
        });
        this.internalFunnels().forEach((f) => {
            expandBounds(f.entity.position.x, f.entity.position.y, f.ui.dimensions.width, f.ui.dimensions.height);
        });
        this.internalPorts().forEach((port) => {
            expandBounds(
                port.entity.position.x,
                port.entity.position.y,
                port.ui.dimensions.width,
                port.ui.dimensions.height
            );
        });

        // No components - apply identity transform
        if (minX === Infinity) {
            this.applyTransform(0, 0, 1, transition);
            return;
        }

        const graphWidth = maxX - minX;
        const graphHeight = maxY - minY;

        let newScale: number;

        if (graphWidth > canvasWidth || graphHeight > canvasHeight) {
            // Content is larger than viewport - scale down to fit
            newScale = Math.min(canvasWidth / graphWidth, canvasHeight / graphHeight);

            // clampScale handles NaN (from 0/0 when graph is zero-size) and out-of-range values
            newScale = clampScale(newScale);
        } else {
            // Content fits within viewport at 1:1 - use scale 1
            newScale = 1;
        }

        // Center the content within the viewport (accounting for padding)
        const halfPadding = padding / 2;
        const translateX = halfPadding + (canvasWidth - graphWidth * newScale) / 2 - minX * newScale;
        const translateY = halfPadding + (canvasHeight - graphHeight * newScale) / 2 - minY * newScale;

        this.applyTransform(translateX, translateY, newScale, transition);
    }

    /**
     * Apply transform programmatically
     * Used for viewport restoration and zoom actions
     */
    private applyTransform(x: number, y: number, scale: number, transition = false): void {
        if (!this.zoom || !this.svg) {
            return;
        }

        // Last-line-of-defense magnitude guard: reject degenerate inputs from any of the
        // many internal callers (fitContent, restoreViewportFromStorage, centerOnComponent,
        // etc.) before they can write a corrupted value into d3's internal zoom state.
        if (
            !isFiniteInBound(x, MAX_ABS_TRANSLATE) ||
            !isFiniteInBound(y, MAX_ABS_TRANSLATE) ||
            !isScaleInBound(scale)
        ) {
            return;
        }

        // Set flag to allow zoom.end to process and persist viewport
        // (We want to persist programmatic transforms like centerOnComponent)
        this.isSettingInitialTransform = false;
        this.zoomHappened = true; // Mark that a zoom happened so end event processes it

        // Create D3 transform
        const transform = d3.zoomIdentity.translate(x, y).scale(scale);

        // Apply transform with or without transition
        if (transition) {
            this.svg.transition().duration(400).call(this.zoom.transform, transform);
        } else {
            this.svg.call(this.zoom.transform, transform);
        }
    }

    /**
     * Center viewport on a specific component
     * Public method for external use (e.g., search, go-to-component)
     *
     * @param componentId - ID of the component to center on
     * @param componentType - Type of the component
     * @param transition - Whether to animate the pan (default: true for same process group, false for cross-process-group)
     */
    public centerOnComponent(componentId: string, componentType: ComponentType, transition = true): void {
        const position = this.getComponentPosition(componentId, componentType);

        if (position) {
            const svg = this.canvasSvg().nativeElement;
            const svgRect = svg.getBoundingClientRect();

            const centerX = svgRect.width / 2;
            const centerY = svgRect.height / 2;

            const currentScale = this.scale();
            const translateX = centerX - position.x * currentScale;
            const translateY = centerY - position.y * currentScale;

            // Apply transform with or without transition
            this.applyTransform(translateX, translateY, currentScale, transition);
        }
    }

    /**
     * Center the viewport on all currently selected components
     *
     * Calculates the bounding box of all selected components and pans
     * the viewport to center on that area.
     *
     * @param transition - Whether to animate the pan (default: true)
     */
    public centerOnSelection(transition = true, targetScale?: number): void {
        const selectedIds = this.selectedComponentIds();
        if (selectedIds.length === 0) {
            return;
        }

        // Collect bounding boxes of all selected components
        let minX = Infinity;
        let minY = Infinity;
        let maxX = -Infinity;
        let maxY = -Infinity;

        const updateBounds = (position: Position, dimensions: Dimension) => {
            minX = Math.min(minX, position.x);
            minY = Math.min(minY, position.y);
            maxX = Math.max(maxX, position.x + dimensions.width);
            maxY = Math.max(maxY, position.y + dimensions.height);
        };

        // Check all component types
        this.internalProcessors().forEach((c) => {
            if (selectedIds.includes(c.entity.id)) {
                updateBounds(c.entity.position, c.ui.dimensions);
            }
        });
        this.internalProcessGroups().forEach((c) => {
            if (selectedIds.includes(c.entity.id)) {
                updateBounds(c.entity.position, c.ui.dimensions);
            }
        });
        this.internalLabels().forEach((c) => {
            if (selectedIds.includes(c.entity.id)) {
                updateBounds(c.entity.position, c.ui.dimensions);
            }
        });
        this.internalFunnels().forEach((c) => {
            if (selectedIds.includes(c.entity.id)) {
                updateBounds(c.entity.position, c.ui.dimensions);
            }
        });
        this.internalPorts().forEach((c) => {
            if (selectedIds.includes(c.entity.id)) {
                updateBounds(c.entity.position, c.ui.dimensions);
            }
        });
        this.internalRemoteProcessGroups().forEach((c) => {
            if (selectedIds.includes(c.entity.id)) {
                updateBounds(c.entity.position, c.ui.dimensions);
            }
        });
        this.internalConnections().forEach((c) => {
            if (selectedIds.includes(c.entity.id)) {
                const centerPoint = this.getConnectionCenterPoint(c);
                if (centerPoint) {
                    // For connections, use a small bounding box around center
                    updateBounds({ x: centerPoint.x - 50, y: centerPoint.y - 50 }, { width: 100, height: 100 });
                }
            }
        });

        // Calculate center of bounding box
        if (minX !== Infinity) {
            const centerX = (minX + maxX) / 2;
            const centerY = (minY + maxY) / 2;

            const svg = this.canvasSvg().nativeElement;
            const svgRect = svg.getBoundingClientRect();

            const viewCenterX = svgRect.width / 2;
            const viewCenterY = svgRect.height / 2;

            const currentScale = targetScale ?? this.scale();
            const translateX = viewCenterX - centerX * currentScale;
            const translateY = viewCenterY - centerY * currentScale;

            this.applyTransform(translateX, translateY, currentScale, transition);
        }
    }

    /**
     * Get position of a component by ID and type
     * Returns the center point of the component for centering
     */
    private getComponentPosition(componentId: string, componentType: ComponentType): Position | null {
        switch (componentType) {
            case ComponentType.Processor: {
                const processor = this.internalProcessors().find((p) => p.entity.id === componentId);
                if (processor) {
                    // Return center of processor
                    return {
                        x: processor.entity.position.x + processor.ui.dimensions.width / 2,
                        y: processor.entity.position.y + processor.ui.dimensions.height / 2
                    };
                }
                break;
            }

            case ComponentType.Connection: {
                const connection = this.internalConnections().find((c) => c.entity.id === componentId);
                if (connection) {
                    // Return center point of connection (midpoint of path)
                    return this.getConnectionCenterPoint(connection);
                }
                break;
            }

            case ComponentType.Label: {
                const label = this.internalLabels().find((l) => l.entity.id === componentId);
                if (label) {
                    // Return center of label
                    return {
                        x: label.entity.position.x + label.ui.dimensions.width / 2,
                        y: label.entity.position.y + label.ui.dimensions.height / 2
                    };
                }
                break;
            }

            case ComponentType.Funnel: {
                const funnel = this.internalFunnels().find((f) => f.entity.id === componentId);
                if (funnel) {
                    // Return center of funnel
                    return {
                        x: funnel.entity.position.x + funnel.ui.dimensions.width / 2,
                        y: funnel.entity.position.y + funnel.ui.dimensions.height / 2
                    };
                }
                break;
            }

            case ComponentType.InputPort:
            case ComponentType.OutputPort: {
                const port = this.internalPorts().find((p) => p.entity.id === componentId);
                if (port) {
                    // Return center of port
                    return {
                        x: port.entity.position.x + port.ui.dimensions.width / 2,
                        y: port.entity.position.y + port.ui.dimensions.height / 2
                    };
                }
                break;
            }

            case ComponentType.ProcessGroup: {
                const processGroup = this.internalProcessGroups().find((pg) => pg.entity.id === componentId);
                if (processGroup) {
                    // Return center of process group
                    return {
                        x: processGroup.entity.position.x + processGroup.ui.dimensions.width / 2,
                        y: processGroup.entity.position.y + processGroup.ui.dimensions.height / 2
                    };
                }
                break;
            }

            case ComponentType.RemoteProcessGroup: {
                const remoteProcessGroup = this.internalRemoteProcessGroups().find(
                    (rpg) => rpg.entity.id === componentId
                );
                if (remoteProcessGroup) {
                    // Return center of remote process group
                    return {
                        x: remoteProcessGroup.entity.position.x + remoteProcessGroup.ui.dimensions.width / 2,
                        y: remoteProcessGroup.entity.position.y + remoteProcessGroup.ui.dimensions.height / 2
                    };
                }
                break;
            }
        }

        return null;
    }

    /**
     * Get center point of a connection
     * Uses the midpoint of the connection path
     */
    private getConnectionCenterPoint(connection: CanvasConnection): Position {
        const bends = connection.ui.bends || [];

        if (bends.length === 0) {
            // No bend points, use midpoint between start and end
            return {
                x: (connection.ui.start.x + connection.ui.end.x) / 2,
                y: (connection.ui.start.y + connection.ui.end.y) / 2
            };
        } else {
            // Use the middle bend point
            const middleIndex = Math.floor(bends.length / 2);
            return {
                x: bends[middleIndex].x,
                y: bends[middleIndex].y
            };
        }
    }

    /**
     * Zoom increment factor for button-based zoom in/out.
     * Matches the flow designer's CanvasView.INCREMENT (1.2x per step).
     */
    private static readonly ZOOM_INCREMENT: number = 1.2;

    /**
     * Zoom controls - all operate directly on the D3 zoom behavior.
     *
     * These methods are the public API for programmatic zoom control.
     * Scroll wheel zoom is handled separately by D3's built-in wheel handler.
     */
    onZoomIn(): void {
        if (!this.zoom || !this.svg) {
            return;
        }
        this.svg.transition().duration(200).call(this.zoom.scaleBy, CanvasComponent.ZOOM_INCREMENT);
    }

    onZoomOut(): void {
        if (!this.zoom || !this.svg) {
            return;
        }
        this.svg
            .transition()
            .duration(200)
            .call(this.zoom.scaleBy, 1 / CanvasComponent.ZOOM_INCREMENT);
    }

    onZoomFit(): void {
        this.fitContent(true);
    }

    /**
     * Zoom to actual size (1:1 scale).
     *
     * Centers the viewport on all canvas content at scale 1.
     * If there are selected components, centers on the selection instead.
     */
    onZoomActual(): void {
        const svg = this.canvasSvg()?.nativeElement;
        if (!svg) {
            return;
        }

        const svgRect = svg.getBoundingClientRect();
        const selectedIds = this.selectedComponentIds();

        // Determine bounding box: selected components, or all components
        let minX = Infinity;
        let minY = Infinity;
        let maxX = -Infinity;
        let maxY = -Infinity;

        const expandBounds = (x: number, y: number, width: number, height: number) => {
            minX = Math.min(minX, x);
            minY = Math.min(minY, y);
            maxX = Math.max(maxX, x + width);
            maxY = Math.max(maxY, y + height);
        };

        const hasSelection = selectedIds.length > 0;

        // Collect bounds from all component types, filtering by selection if applicable
        this.internalProcessors().forEach((p) => {
            if (!hasSelection || selectedIds.includes(p.entity.id)) {
                expandBounds(p.entity.position.x, p.entity.position.y, p.ui.dimensions.width, p.ui.dimensions.height);
            }
        });
        this.internalProcessGroups().forEach((pg) => {
            if (!hasSelection || selectedIds.includes(pg.entity.id)) {
                expandBounds(
                    pg.entity.position.x,
                    pg.entity.position.y,
                    pg.ui.dimensions.width,
                    pg.ui.dimensions.height
                );
            }
        });
        this.internalRemoteProcessGroups().forEach((rpg) => {
            if (!hasSelection || selectedIds.includes(rpg.entity.id)) {
                expandBounds(
                    rpg.entity.position.x,
                    rpg.entity.position.y,
                    rpg.ui.dimensions.width,
                    rpg.ui.dimensions.height
                );
            }
        });
        this.internalLabels().forEach((l) => {
            if (!hasSelection || selectedIds.includes(l.entity.id)) {
                expandBounds(l.entity.position.x, l.entity.position.y, l.ui.dimensions.width, l.ui.dimensions.height);
            }
        });
        this.internalFunnels().forEach((f) => {
            if (!hasSelection || selectedIds.includes(f.entity.id)) {
                expandBounds(f.entity.position.x, f.entity.position.y, f.ui.dimensions.width, f.ui.dimensions.height);
            }
        });
        this.internalPorts().forEach((port) => {
            if (!hasSelection || selectedIds.includes(port.entity.id)) {
                expandBounds(
                    port.entity.position.x,
                    port.entity.position.y,
                    port.ui.dimensions.width,
                    port.ui.dimensions.height
                );
            }
        });

        // No components found - apply identity transform
        if (minX === Infinity) {
            this.applyTransform(0, 0, 1, true);
            return;
        }

        // Center the bounding box at scale 1
        const centerX = (minX + maxX) / 2;
        const centerY = (minY + maxY) / 2;
        const translateX = svgRect.width / 2 - centerX;
        const translateY = svgRect.height / 2 - centerY;

        this.applyTransform(translateX, translateY, 1, true);
    }

    // =========================================================================
    // Public API for Birdseye Integration
    // =========================================================================

    /**
     * Get the current canvas transform for birdseye synchronization
     * Returns the current translate and scale values
     */
    public getTransform(): BirdseyeTransform {
        return {
            translate: { x: this.x, y: this.y },
            scale: this.currentScale
        };
    }

    /**
     * Get the canvas viewport dimensions
     * Returns the width and height of the SVG element
     */
    public getCanvasDimensions(): Dimension {
        const svg = this.canvasSvg()?.nativeElement;
        if (!svg) {
            return { width: 0, height: 0 };
        }
        const rect = svg.getBoundingClientRect();
        return { width: rect.width, height: rect.height };
    }

    /**
     * Get all component data for birdseye rendering
     * Returns simplified component data with positions and dimensions
     */
    public getBirdseyeComponentData(): BirdseyeComponentData[] {
        const components: BirdseyeComponentData[] = [];

        // Add processors
        this.internalProcessors().forEach((p) => {
            // Without canRead the caller is not authorized to view the component definition
            // (including its configured style), so fall back to the default palette fill rather
            // than leaking the user-configured background color through the minimap.
            const fillColor = p.entity.permissions?.canRead
                ? p.entity.component?.style?.['background-color'] || undefined
                : undefined;
            components.push({
                id: p.entity.id,
                type: ComponentType.Processor,
                position: { x: p.entity.position.x, y: p.entity.position.y },
                dimensions: { width: p.ui.dimensions.width, height: p.ui.dimensions.height },
                fillColor
            });
        });

        // Add process groups
        this.internalProcessGroups().forEach((pg) => {
            components.push({
                id: pg.entity.id,
                type: ComponentType.ProcessGroup,
                position: { x: pg.entity.position.x, y: pg.entity.position.y },
                dimensions: { width: pg.ui.dimensions.width, height: pg.ui.dimensions.height }
            });
        });

        // Add remote process groups
        this.internalRemoteProcessGroups().forEach((rpg) => {
            components.push({
                id: rpg.entity.id,
                type: ComponentType.RemoteProcessGroup,
                position: { x: rpg.entity.position.x, y: rpg.entity.position.y },
                dimensions: { width: rpg.ui.dimensions.width, height: rpg.ui.dimensions.height }
            });
        });

        // Add ports
        this.internalPorts().forEach((port) => {
            const type = port.entity.portType === 'INPUT_PORT' ? ComponentType.InputPort : ComponentType.OutputPort;
            components.push({
                id: port.entity.id,
                type,
                position: { x: port.entity.position.x, y: port.entity.position.y },
                dimensions: { width: port.ui.dimensions.width, height: port.ui.dimensions.height }
            });
        });

        // Add funnels
        this.internalFunnels().forEach((f) => {
            components.push({
                id: f.entity.id,
                type: ComponentType.Funnel,
                position: { x: f.entity.position.x, y: f.entity.position.y },
                dimensions: { width: f.ui.dimensions.width, height: f.ui.dimensions.height }
            });
        });

        // Add labels
        this.internalLabels().forEach((l) => {
            // Same authorization rule as processors above: a caller without canRead has no
            // authority to view the label's configured style, so fall back to the default
            // palette fill rather than leaking the user-configured background color.
            const fillColor = l.entity.permissions?.canRead
                ? l.entity.component?.style?.['background-color'] || undefined
                : undefined;
            components.push({
                id: l.entity.id,
                type: ComponentType.Label,
                position: { x: l.entity.position.x, y: l.entity.position.y },
                dimensions: { width: l.ui.dimensions.width, height: l.ui.dimensions.height },
                fillColor
            });
        });

        return components;
    }

    /**
     * Set viewport position programmatically
     * Used for birdseye navigation, go-to-component, and other viewport changes
     *
     * @param x - New translate X value
     * @param y - New translate Y value
     * @param transition - Whether to animate the transition
     */
    public setViewportPosition(x: number, y: number, transition = false): void {
        this.applyTransform(x, y, this.currentScale, transition);
    }

    /**
     * Signal that birdseye drag has started
     * This prevents expensive updateCanvasVisibility calls during drag
     */
    public birdseyeDragStart(): void {
        this.birdseyeTranslateInProgress = true;
    }

    /**
     * Signal that birdseye drag has ended
     * This triggers updateCanvasVisibility to update component details
     */
    public birdseyeDragEnd(): void {
        this.birdseyeTranslateInProgress = false;
        this.updateCanvasVisibility();
    }
}
