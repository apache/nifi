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
    DestroyRef,
    inject,
    input,
    output,
    effect,
    computed,
    ChangeDetectionStrategy
} from '@angular/core';
import * as d3 from 'd3';
import { CanvasProcessGroup, CanvasRootResolver, CanvasRootSelection, CanvasSelection } from '../../canvas.types';
import { ProcessGroupRenderer } from './process-group-renderer';
import { TextEllipsisUtils } from '../../utils/text-ellipsis.utils';
import { CanvasFormatUtils } from '../../canvas-format-utils.service';
import { CanvasComponentUtils } from '../../canvas-component-utils.service';
import { NiFiCommon, Position } from '@nifi/shared';
import { RegistryClientEntity } from '../../../../../state/shared';
import { ProcessGroupRenderContext } from '../render-context.types';
import { ConnectableBehaviorHelper } from '../../connectable-behavior.helper';

/**
 * ProcessGroupLayerComponent
 *
 * Angular wrapper for ProcessGroupRenderer that manages process group lifecycle and interactions.
 * Handles process groups (nested containers with aggregated statistics).
 *
 * Pattern:
 * 1. Receives process group data via input signals from parent canvas
 * 2. Owns D3 container element (SVG <g> via attribute selector)
 * 3. Delegates pure rendering logic to ProcessGroupRenderer class
 * 4. Emits user interaction events (click, double-click, drag)
 * 5. Parent canvas handles events (updates state, dispatches actions)
 *
 * Renders:
 * - Process group border and body
 * - Name and version control information
 * - Aggregated statistics (Queued, In, Read/Write, Out, Sent, Received)
 * - Status indicators (run status, bulletins, validation errors, active threads, version control)
 * - Interactive drag behavior for repositioning
 */
@Component({
    // Attribute selector required: this component renders as an SVG <g> element, not a custom HTML element
    // eslint-disable-next-line @angular-eslint/component-selector
    selector: '[canvas-process-group-layer]',
    standalone: true,
    template: '',
    host: {
        class: 'process-groups'
    },
    changeDetection: ChangeDetectionStrategy.OnPush
})
export class ProcessGroupLayerComponent implements AfterViewInit {
    private elementRef = inject(ElementRef);
    private destroyRef = inject(DestroyRef);

    /**
     * Process group data to render
     */
    processGroups = input<CanvasProcessGroup[]>([]);

    /**
     * Scale factor for responsive rendering
     */
    scale = input<number>(1);

    /**
     * Currently selected component IDs
     */
    selectedIds = input<string[]>([]);

    /**
     * Trigger value to force re-render
     */
    renderTrigger = input<number>(0);

    /**
     * Whether this layer is interactive (can click/select)
     */
    canSelect = input<boolean>(true);

    /**
     * Text ellipsis utility for truncating long text
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
     * Whether editing operations are allowed (drag)
     */
    canEdit = input<boolean>(true);

    /**
     * Component utilities (active thread counts, bulletins, etc)
     */
    componentUtils = input.required<CanvasComponentUtils>();

    /**
     * IDs of process groups currently being saved (disabled during save)
     */
    disabledProcessGroupIds = input<Set<string>>(new Set());

    /**
     * Registry clients for version control tooltip
     */
    registryClients = input<RegistryClientEntity[]>([]);
    connectableBehavior = input<ConnectableBehaviorHelper | null>(null);
    canvasRootResolver = input<CanvasRootResolver | null>(null);
    isDropAllowed = input<() => boolean>(() => true);

    /**
     * Emitted when PG is clicked
     */
    processGroupClick = output<{ pg: CanvasProcessGroup; event: MouseEvent }>();

    /**
     * Emitted when PG is double-clicked
     */
    processGroupDoubleClick = output<{ pg: CanvasProcessGroup; event: MouseEvent }>();

    /**
     * Emitted when PG drag ends
     */
    dragEnd = output<{ delta: Position; movingIds: Set<string> }>();

    /**
     * D3 selection of the layer group
     */
    private containerSelection: CanvasRootSelection | null = null;

    constructor() {
        this.destroyRef.onDestroy(() => {
            const helper = this.connectableBehavior();
            if (helper && this.containerSelection) {
                helper.deactivate(
                    this.containerSelection.selectAll<SVGGElement, CanvasProcessGroup>('g.process-group')
                );
            }
        });
    }

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
    private readonly callbacks: ProcessGroupRenderContext['callbacks'] = {
        onClick: (pg, event) => {
            this.processGroupClick.emit({ pg, event });
        },
        onDoubleClick: (pg, event) => {
            this.processGroupDoubleClick.emit({ pg, event });
        },
        onDragEnd: (delta, movingIds) => {
            this.dragEnd.emit({ delta, movingIds });
        }
    };
    private selectedIdsSet = computed(() => new Set(this.selectedIds()));

    /**
     * Computed render context - encapsulates all data needed by ProcessGroupRenderer
     */
    private renderContext = computed<ProcessGroupRenderContext>(() => ({
        containerSelection: this.containerSelection!,
        textEllipsis: this.textEllipsis(),
        formatUtils: this.formatUtils(),
        nifiCommon: this.nifiCommon(),
        getCanEdit: () => this.canEdit(),
        getCanSelect: () => this.canSelect(),
        getSelectedIds: () => this.selectedIdsSet(),
        componentUtils: this.componentUtils(),
        processGroups: this.processGroups(),
        disabledProcessGroupIds: this.disabledProcessGroupIds(),
        getDisabledProcessGroupIds: () => this.disabledProcessGroupIds(),
        getIsDropAllowed: this.isDropAllowed(),
        canvasRootResolver: this.canvasRootResolver() ?? this.defaultCanvasRootResolver,
        registryClients: this.registryClients(),
        canSelect: this.canSelect(),
        callbacks: this.callbacks
    }));

    /**
     * Effect to re-render when process group data changes
     */
    private renderEffect = effect(() => {
        const _processGroups = this.processGroups();
        const _renderTrigger = this.renderTrigger();

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

    /**
     * Attach or detach the connection handle when the helper, canEdit, or
     * process group set changes. activate is idempotent, so a data refresh
     * rewires newly entered groups without tearing down an in-flight connection.
     * deactivate runs only when editing is turned off.
     */
    private connectableEffect = effect((onCleanup) => {
        this.processGroups();
        this.renderTrigger();
        const helper = this.connectableBehavior();
        const canEdit = this.canEdit();
        if (!this.containerSelection) return;
        const groups = this.containerSelection.selectAll<SVGGElement, CanvasProcessGroup>('g.process-group');
        if (helper && canEdit) {
            helper.activate(groups);
            onCleanup(() => {
                if (this.connectableBehavior() !== helper || !this.canEdit()) {
                    helper.deactivate(groups);
                }
            });
        } else if (helper) {
            helper.deactivate(groups);
        }
    });

    ngAfterViewInit(): void {
        const nativeElement = this.elementRef.nativeElement;
        this.containerSelection = d3.select<SVGGElement, unknown>(nativeElement);

        // Initial render if data arrived before view was ready
        if (this.processGroups().length > 0) {
            this.render();
        }
    }

    /**
     * Apply selection styling to process groups
     */
    private applySelectionStyling(): void {
        if (!this.containerSelection) {
            return;
        }

        const selectedIds = this.selectedIds();

        this.containerSelection
            .selectAll<SVGGElement, CanvasProcessGroup>('g.process-group')
            .classed('selected', (d) => selectedIds.includes(d.entity.id));
    }

    /**
     * Core rendering method
     */
    private render(): void {
        if (!this.containerSelection) {
            return;
        }

        ProcessGroupRenderer.render(this.renderContext());
        this.applySelectionStyling();
    }

    /**
     * Pan update for entering/leaving PGs (called from canvas during zoom/pan)
     */
    public pan(selection: CanvasSelection<CanvasProcessGroup>): void {
        ProcessGroupRenderer.pan(selection, this.renderContext());
        this.applySelectionStyling();
    }
}
