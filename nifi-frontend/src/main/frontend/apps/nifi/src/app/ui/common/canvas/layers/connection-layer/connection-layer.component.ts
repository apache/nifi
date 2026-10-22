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
    AfterViewInit,
    ElementRef,
    inject,
    input,
    output,
    effect,
    computed,
    untracked,
    ChangeDetectionStrategy
} from '@angular/core';
import * as d3 from 'd3';
import { CanvasConnection, CanvasRootResolver, CanvasRootSelection } from '../../canvas.types';
import { ConnectionRenderer } from './connection-renderer';
import { ConnectionReconnectContext, ConnectionRenderContext } from '../render-context.types';
import { TextEllipsisUtils } from '../../utils/text-ellipsis.utils';
import { CanvasFormatUtils } from '../../canvas-format-utils.service';
import { CanvasComponentUtils } from '../../canvas-component-utils.service';
import { NiFiCommon } from '@nifi/shared';
import { ConnectionEndpointReconnectDestination } from '../../../../../state/flow-shared';
import { ViewportRect } from '../../utils/visibility.utils';

/**
 * ConnectionLayerComponent
 *
 * Angular wrapper for ConnectionRenderer that manages connection lifecycle and interactions.
 * Handles connections (paths between components) and their labels.
 *
 * Pattern:
 * 1. Receives connection data via input signals from parent canvas
 * 2. Owns D3 container element (SVG <g> via attribute selector)
 * 3. Delegates pure rendering logic to ConnectionRenderer class
 * 4. Emits user interaction events (click, double-click, bend point manipulation, label drag)
 * 5. Parent canvas handles events (updates state, dispatches actions)
 *
 * Renders:
 * - Connection paths with bend points (source → bends → destination)
 * - Connection labels (source, destination, relationships, queued stats)
 * - Backpressure indicators (object count and data size bars with predictions)
 * - Status indicators (expiration, load balance, retry, penalized icons)
 * - Interactive behaviors (bend point add/remove/drag, label positioning)
 */
@Component({
    // Attribute selector required: this component renders as an SVG <g> element, not a custom HTML element
    // eslint-disable-next-line @angular-eslint/component-selector
    selector: '[canvas-connection-layer]',
    standalone: true,
    imports: [],
    template: '',
    host: {
        class: 'connections'
    },
    changeDetection: ChangeDetectionStrategy.OnPush
})
export class ConnectionLayerComponent implements AfterViewInit {
    private elementRef = inject(ElementRef);

    /**
     * Connection data to render
     */
    connections = input<CanvasConnection[]>([]);

    /**
     * Current canvas scale factor.
     */
    scale = input<number>(1);

    /**
     * Padded viewport rectangle in canvas-space coordinates published by the canvas.
     * Null until the canvas is measurable.
     */
    viewportBounds = input<ViewportRect | null>(null);

    /**
     * Currently selected component IDs
     */
    selectedIds = input<string[]>([]);

    /**
     * Whether this layer is interactive (can click/select)
     */
    canSelect = input<boolean>(true);

    /**
     * Text ellipsis utility for truncating long text in labels
     */
    textEllipsis = input.required<TextEllipsisUtils>();

    /**
     * Format utilities (shared across all layers)
     */
    formatUtils = input.required<CanvasFormatUtils>();

    /**
     * NiFi common utilities (for compareNumber, etc.)
     */
    nifiCommon = input.required<NiFiCommon>();

    /**
     * Component utilities for tooltips and formatting
     */
    componentUtils = input.required<CanvasComponentUtils>();

    /**
     * Whether editing operations are allowed (bend points, label drag)
     */
    canEdit = input<boolean>(true);

    /**
     * Trigger value to force re-render
     */
    renderTrigger = input<number>(0);

    /**
     * Current process group ID for determining connection label content
     */
    processGroupId = input<string | null>(null);

    /**
     * Set of connection IDs that are disabled (e.g., while saving bend points)
     */
    disabledConnectionIds = input<Set<string>>(new Set());
    canvasRootResolver = input<CanvasRootResolver | null>(null);
    reconnect = input<ConnectionReconnectContext | null>(null);

    /**
     * Emitted when connection is clicked
     */
    connectionClick = output<{ connection: CanvasConnection; event: MouseEvent }>();

    /**
     * Emitted when connection is double-clicked
     */
    connectionDoubleClick = output<{ connection: CanvasConnection; event: MouseEvent }>();

    /**
     * Emitted when bend point drag ends (save to backend)
     */
    bendPointDragEnd = output<{ connection: CanvasConnection; bends: Array<{ x: number; y: number }> }>();

    /**
     * Emitted when a new bend point is added via context menu
     */
    bendPointAdd = output<{ connection: CanvasConnection; point: { x: number; y: number; index: number } }>();

    /**
     * Emitted when a bend point is removed via context menu
     */
    bendPointRemove = output<{ connection: CanvasConnection; index: number }>();

    /**
     * Emitted when a connection label is dragged to a new bend point
     */
    labelDragEnd = output<{ connection: CanvasConnection; labelIndex: number }>();
    endpointReconnect = output<{
        connection: CanvasConnection;
        newDestination: ConnectionEndpointReconnectDestination;
        bends?: Array<{ x: number; y: number }>;
    }>();

    /**
     * D3 selection of the layer group
     */
    private containerSelection: CanvasRootSelection | null = null;
    private readonly defaultCanvasRootResolver: CanvasRootResolver = () => {
        const node = this.containerSelection?.node();
        const canvasNode = (node?.closest('g.canvas') as SVGGElement | null) ?? node;
        return canvasNode
            ? d3.select<SVGGElement, unknown>(canvasNode)
            : d3.select<SVGGElement, unknown>(null as unknown as SVGGElement);
    };
    /**
     * Stable callback object to prevent unnecessary D3 event handler re-binding
     */
    private readonly callbacks: ConnectionRenderContext['callbacks'] = {
        onClick: (connection, event) => {
            this.connectionClick.emit({ connection, event });
        },
        onDoubleClick: (connection, event) => {
            this.connectionDoubleClick.emit({ connection, event });
        },
        onBendPointDragEnd: (connection, bends: Array<{ x: number; y: number }>) => {
            this.bendPointDragEnd.emit({ connection, bends });
        },
        onBendPointAdd: (connection, point: { x: number; y: number; index: number }) => {
            this.bendPointAdd.emit({ connection, point });
        },
        onBendPointRemove: (connection, index: number) => {
            this.bendPointRemove.emit({ connection, index });
        },
        onLabelDragEnd: (connection, labelIndex: number) => {
            this.labelDragEnd.emit({ connection, labelIndex });
        },
        onEndpointReconnect: (connection, newDestination, bends) => {
            this.endpointReconnect.emit({ connection, newDestination, bends });
        }
    };

    /**
     * Memoized Set of selected component IDs for O(1) lookup. Used by
     * `applySelectionStyling` to keep the per-connection `classed()` lookup
     * cost flat as the selection grows. Unlike the six component-side layers,
     * connections are NOT a drag-gesture source for `dragMovingIds` (their
     * bend translation rides the moving component's gesture via the
     * canvas's `getComponentConnections` self-loop discovery), so this set
     * does not need to be bridged through `renderContext.getSelectedIds()`.
     */
    private selectedIdsSet = computed<Set<string>>(() => new Set(this.selectedIds()));

    /**
     * Computed render context - encapsulates all data needed by ConnectionRenderer
     */
    private renderContext = computed<ConnectionRenderContext>(() => ({
        containerSelection: this.containerSelection!,
        textEllipsis: this.textEllipsis(),
        formatUtils: this.formatUtils(),
        nifiCommon: this.nifiCommon(),
        // untracked: scale is passive — don't re-render on zoom ticks.
        scale: untracked(() => this.scale()),
        viewportBounds: this.viewportBounds(),
        componentUtils: this.componentUtils(),
        getCanEdit: () => this.canEdit(),
        getCanSelect: () => this.canSelect(),
        getSelectedIds: () => this.selectedIdsSet(),
        canvasRootResolver: this.canvasRootResolver() ?? this.defaultCanvasRootResolver,
        connections: this.connections(),
        processGroupId: this.processGroupId(),
        canSelect: this.canSelect(),
        disabledConnectionIds: this.disabledConnectionIds(),
        getDisabledConnectionIds: () => this.disabledConnectionIds(),
        reconnect: this.reconnect() ?? undefined,
        getReconnect: () => this.reconnect() ?? undefined,
        callbacks: this.callbacks
    }));

    /**
     * Effect to re-render when connection data changes
     * NOTE: selectedIds is NOT tracked here to avoid unnecessary re-renders
     */
    private renderEffect = effect(() => {
        const _connections = this.connections();
        const _renderTrigger = this.renderTrigger();
        const _processGroupId = this.processGroupId();
        // Track `canSelect`, `canEdit`, and `disabledConnectionIds` so
        // render-time reads (cursor styling on the connection group itself and
        // on bend-point midpoints, opacity, the `disabled` class) refresh on
        // runtime toggles. Fire-time gates inside handlers read the same
        // values via `getCanSelect()` / `getCanEdit()` / `getDisabledConnectionIds()`
        // and don't require these deps; tracking here exists purely so that
        // when the saving set clears, the disabled visual treatment lifts
        // immediately without the consumer having to bump `renderTrigger`
        // from a confirm handler.
        const _canSelect = this.canSelect();
        const _canEdit = this.canEdit();
        const _disabledConnectionIds = this.disabledConnectionIds();
        // Track viewportBounds so the renderer re-classifies visibility after
        // zoom end, resize, or birdseye drag end.
        const _viewportBounds = this.viewportBounds();

        if (this.containerSelection) {
            this.render();
        }
    });

    /**
     * Effect to update selection styling efficiently without full re-render
     */
    private selectionEffect = effect(() => {
        this.applySelectionStyling();
    });

    ngAfterViewInit(): void {
        const nativeElement = this.elementRef.nativeElement;
        this.containerSelection = d3.select<SVGGElement, unknown>(nativeElement);

        // Initial render if data arrived before view was ready
        if (this.connections().length > 0) {
            this.render();
        }
    }

    /**
     * Apply current selection styling to connections
     */
    private applySelectionStyling(): void {
        if (!this.containerSelection) {
            return;
        }

        const selectedIdsSet = this.selectedIdsSet();
        const canEdit = this.canEdit();
        const canSelect = this.canSelect();

        this.containerSelection
            .selectAll<SVGGElement, CanvasConnection>('g.connection')
            .classed('selected', (d) => selectedIdsSet.has(d.entity.id))
            .each(function (d) {
                const connection = d3.select(this);
                const isSelected = selectedIdsSet.has(d.entity.id);
                const labelContainer = connection.select('g.connection-label-container');

                if (!labelContainer.empty()) {
                    const bends = d.ui.bends || [];
                    let cursor: string;

                    if (isSelected && canEdit && bends.length > 1) {
                        // Selected with multiple bend points: show move cursor to indicate draggable
                        cursor = 'move';
                    } else if (canSelect) {
                        // Not selected or not draggable: show pointer cursor for selection
                        cursor = 'pointer';
                    } else {
                        cursor = 'default';
                    }

                    labelContainer.style('cursor', cursor);
                }
            });
    }

    /**
     * Core rendering method
     */
    private render(): void {
        if (!this.containerSelection) {
            return;
        }

        ConnectionRenderer.render(this.renderContext());
        this.applySelectionStyling();
    }
}
