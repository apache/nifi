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
    ChangeDetectionStrategy,
    AfterViewInit,
    ElementRef,
    inject,
    input,
    output,
    effect,
    computed
} from '@angular/core';
import * as d3 from 'd3';
import { CanvasProcessor } from '../../canvas.types';
import { ProcessorRenderer } from './processor-renderer';
import { TextEllipsisUtils } from '../../utils/text-ellipsis.utils';
import { CanvasFormatUtils } from '../../canvas-format-utils.service';
import { CanvasComponentUtils } from '../../canvas-component-utils.service';
import { NiFiCommon } from '@nifi/shared';
import { DocumentedType } from '../../../../../state/shared';
import { ProcessorRenderContext } from '../render-context.types';

/**
 * Processor Layer Component
 *
 * Angular wrapper for ProcessorRenderer that manages processor lifecycle and interactions.
 *
 * Pattern:
 * 1. Receives processor data via input signals from parent canvas
 * 2. Owns D3 container element (SVG <g> via attribute selector)
 * 3. Delegates pure rendering logic to ProcessorRenderer class
 * 4. Emits user interaction events (click, double-click, drag)
 * 5. Parent canvas handles events (updates state, dispatches actions)
 *
 * Renders:
 * - Processor border and body with status colors
 * - Processor icon and restricted/primary badges
 * - Name, type, bundle, and version information
 * - Statistics (In, Read/Write, Out, Tasks/Time)
 * - Status indicators (run status, bulletins, validation errors, active threads)
 * - Interactive drag behavior for repositioning
 */
@Component({
    // Attribute selector required: this component renders as an SVG <g> element, not a custom HTML element
    // eslint-disable-next-line @angular-eslint/component-selector
    selector: '[canvas-processor-layer]',
    template: '',
    changeDetection: ChangeDetectionStrategy.OnPush,
    standalone: true,
    host: {
        class: 'processors'
    }
})
export class ProcessorLayerComponent implements AfterViewInit {
    /**
     * Processors to render (from state)
     */
    processors = input<CanvasProcessor[]>([]);

    /**
     * Current canvas scale (affects rendering)
     */
    scale = input<number>(1);

    /**
     * IDs of selected processors (for visual highlighting)
     */
    selectedIds = input<string[]>([]);

    /**
     * Render trigger for forcing re-renders (e.g., after font loading or visibility updates)
     */
    renderTrigger = input<number>(0);

    /**
     * Enable/disable user interactions
     */
    canSelect = input<boolean>(true);

    /**
     * Text ellipsis utility (shared across all layers)
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
     * Preview extension types (processors with Preview tag)
     */
    previewExtensions = input<DocumentedType[]>([]);

    /**
     * IDs of processors that are currently saving (disabled during save)
     */
    disabledProcessorIds = input<Set<string>>(new Set());

    /**
     * Emitted when processor is clicked
     */
    processorClick = output<{ processor: CanvasProcessor; event: MouseEvent }>();

    /**
     * Emitted when processor is double-clicked (typically for editing)
     */
    processorDoubleClick = output<{ processor: CanvasProcessor; event: MouseEvent }>();

    /**
     * Emitted when processor drag ends (for position updates)
     */
    processorDragEnd = output<{
        processor: CanvasProcessor;
        newPosition: { x: number; y: number };
        previousPosition: { x: number; y: number };
    }>();

    private elementRef = inject(ElementRef);
    private containerSelection: d3.Selection<any, any, any, any> | null = null;

    /**
     * Stable callback references - defined once, not recreated on every render
     */
    private readonly callbacks = {
        onClick: (processor: CanvasProcessor, event: MouseEvent) => {
            this.processorClick.emit({ processor, event });
        },
        onDoubleClick: (processor: CanvasProcessor, event: MouseEvent) => {
            this.processorDoubleClick.emit({ processor, event });
        },
        onDragEnd: (
            processor: CanvasProcessor,
            newPosition: { x: number; y: number },
            previousPosition: { x: number; y: number }
        ) => {
            this.processorDragEnd.emit({ processor, newPosition, previousPosition });
        }
    };

    /**
     * Computed render context - single source of truth for all rendering
     * NOTE: selectedIds is NOT included here - it's handled separately by selectionEffect
     * NOTE: callbacks are stable references defined once, not recreated on every render
     */
    private renderContext = computed<ProcessorRenderContext>(() => {
        return {
            containerSelection: this.containerSelection!,
            textEllipsis: this.textEllipsis(),
            formatUtils: this.formatUtils(),
            nifiCommon: this.nifiCommon(),
            getCanEdit: () => this.canEdit(), // Function that returns current value
            componentUtils: this.componentUtils(),
            processors: this.processors(),
            previewExtensions: this.previewExtensions(),
            disabledProcessorIds: this.disabledProcessorIds(),
            canSelect: this.canSelect(),
            callbacks: this.callbacks // Stable reference
        };
    });

    /**
     * Effect to re-render when processor data changes
     * NOTE: selectedIds is NOT tracked here to avoid unnecessary re-renders
     */
    private renderEffect = effect(() => {
        // Access signals to track dependencies
        const _processors = this.processors();
        const _renderTrigger = this.renderTrigger();
        const _previewExtensions = this.previewExtensions();

        if (this.containerSelection) {
            this.renderProcessors();
        }
    });

    /**
     * Effect to update selection styling when selected IDs change
     */
    private selectionEffect = effect(() => {
        this.applySelectionStyling();
    });

    ngAfterViewInit(): void {
        const nativeElement = this.elementRef.nativeElement;

        // The component IS the <g> element with attribute selector
        this.containerSelection = d3.select(nativeElement);

        // Initial render if data arrived before view was ready
        if (this.processors().length > 0) {
            this.renderProcessors();
        }
    }

    /**
     * Apply current selection styling to processor elements
     */
    private applySelectionStyling(): void {
        if (!this.containerSelection) {
            return;
        }

        const selectedIds = this.selectedIds();

        this.containerSelection
            .selectAll<SVGGElement, CanvasProcessor>('g.processor')
            .classed('selected', (d) => selectedIds.includes(d.entity.id));
    }

    /**
     * Core rendering method
     */
    private renderProcessors(): void {
        if (!this.containerSelection) {
            return;
        }

        ProcessorRenderer.render(this.renderContext());
        this.applySelectionStyling();
    }

    /**
     * Pan update for entering/leaving processors (called from canvas during zoom/pan)
     */
    public pan(selection: d3.Selection<any, any, any, any>): void {
        ProcessorRenderer.pan(selection, this.renderContext());
        this.applySelectionStyling();
    }
}
