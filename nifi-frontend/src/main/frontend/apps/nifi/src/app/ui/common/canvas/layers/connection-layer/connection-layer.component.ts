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
    ChangeDetectionStrategy
} from '@angular/core';
import * as d3 from 'd3';
import { CanvasConnection } from '../../canvas.types';
import { ConnectionRenderer } from './connection-renderer';
import { ConnectionRenderContext } from '../render-context.types';
import { TextEllipsisUtils } from '../../utils/text-ellipsis.utils';
import { CanvasFormatUtils } from '../../canvas-format-utils.service';
import { CanvasComponentUtils } from '../../canvas-component-utils.service';
import { NiFiCommon } from '@nifi/shared';

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
     * Scale factor (not used in rendering, kept for compatibility)
     */
    scale = input<number>(1);

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

    /**
     * D3 selection of the layer group
     */
    private containerSelection: d3.Selection<any, any, any, any> | null = null;

    /**
     * Stable callback object to prevent unnecessary D3 event handler re-binding
     */
    private readonly callbacks = {
        onClick: (connection: any, event: MouseEvent) => {
            this.connectionClick.emit({ connection, event });
        },
        onDoubleClick: (connection: any, event: MouseEvent) => {
            this.connectionDoubleClick.emit({ connection, event });
        },
        onBendPointDragEnd: (connection: any, bends: Array<{ x: number; y: number }>) => {
            this.bendPointDragEnd.emit({ connection, bends });
        },
        onBendPointAdd: (connection: any, point: { x: number; y: number; index: number }) => {
            this.bendPointAdd.emit({ connection, point });
        },
        onBendPointRemove: (connection: any, index: number) => {
            this.bendPointRemove.emit({ connection, index });
        },
        onLabelDragEnd: (connection: any, labelIndex: number) => {
            this.labelDragEnd.emit({ connection, labelIndex });
        }
    };

    /**
     * Computed render context - encapsulates all data needed by ConnectionRenderer
     */
    private renderContext = computed<ConnectionRenderContext>(() => ({
        containerSelection: this.containerSelection!,
        textEllipsis: this.textEllipsis(),
        formatUtils: this.formatUtils(),
        nifiCommon: this.nifiCommon(),
        componentUtils: this.componentUtils(),
        getCanEdit: () => this.canEdit(),
        connections: this.connections(),
        processGroupId: this.processGroupId(),
        canSelect: this.canSelect(),
        disabledConnectionIds: this.disabledConnectionIds(),
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
        this.containerSelection = d3.select(nativeElement);

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

        const selectedIds = this.selectedIds();
        const canEdit = this.canEdit();
        const canSelect = this.canSelect();

        this.containerSelection
            .selectAll<SVGGElement, CanvasConnection>('g.connection')
            .classed('selected', (d) => selectedIds.includes(d.entity.id))
            .each(function (d) {
                const connection = d3.select(this);
                const isSelected = selectedIds.includes(d.entity.id);
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

    /**
     * Pan update for entering/leaving connections (called from canvas during zoom/pan)
     */
    public pan(selection: d3.Selection<any, any, any, any>): void {
        ConnectionRenderer.pan(selection, this.renderContext());
        this.applySelectionStyling();
    }
}
