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
    AfterViewInit,
    ChangeDetectionStrategy,
    Component,
    ElementRef,
    OnDestroy,
    effect,
    input,
    output,
    viewChild
} from '@angular/core';
import * as d3 from 'd3';
import { ComponentType } from '@nifi/shared';
import { Dimension, Position } from '../canvas/canvas.types';
import { BirdseyeBounds, BirdseyeComponentData, BirdseyeTransform } from './birdseye.types';

/**
 * Canvas Birdseye Component
 *
 * A minimap component that provides an overview of the entire canvas
 * and allows users to navigate by dragging the viewport brush.
 *
 * Architecture: Hybrid Canvas + SVG
 * - Canvas: Renders component representations (efficient for thousands of components)
 * - SVG: Renders the interactive viewport brush (easy drag handling)
 *
 * This hybrid approach provides optimal performance:
 * - Canvas handles static component rendering without DOM overhead
 * - SVG handles interactive brush with native D3 drag behavior
 *
 * Features:
 * - Renders simplified representations of all canvas components
 * - Shows current viewport position as a draggable brush
 * - Drag brush to pan the canvas (delta-based translation)
 * - Automatically scales to fit all components
 * - Scales efficiently to thousands of components
 *
 * Usage:
 * ```html
 * <canvas-birdseye
 *     [components]="birdseyeComponents()"
 *     [transform]="canvasTransform()"
 *     [canvasDimensions]="{ width: canvasWidth, height: canvasHeight }"
 *     (viewportChange)="onBirdseyeViewportChange($event)">
 * </canvas-birdseye>
 * ```
 */
@Component({
    selector: 'canvas-birdseye',
    standalone: true,
    templateUrl: './birdseye.component.html',
    styleUrls: ['./birdseye.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush
})
export class CanvasBirdseyeComponent implements AfterViewInit, OnDestroy {
    private birdseyeContainer = viewChild.required<ElementRef<HTMLDivElement>>('birdseyeContainer');

    components = input.required<BirdseyeComponentData[]>();

    transform = input.required<BirdseyeTransform>();

    canvasDimensions = input.required<Dimension>();

    birdseyeDimensions = input<Dimension>({ width: 200, height: 150 });

    // Emitted during drag with the new translate the canvas should adopt.
    viewportChange = output<Position>();

    // Parent should call canvas.birdseyeDragStart() to skip visibility updates.
    dragStart = output<void>();

    // Parent should call canvas.birdseyeDragEnd() to update visibility.
    dragEnd = output<void>();

    private canvas: HTMLCanvasElement | null = null;
    private canvasContext: CanvasRenderingContext2D | null = null;

    private svg: d3.Selection<SVGSVGElement, unknown, null, undefined> | null = null;
    private brushGroup: d3.Selection<SVGGElement, unknown, null, undefined> | null = null;
    private brush: d3.Selection<SVGRectElement, { x: number; y: number }, null, undefined> | null = null;

    private readonly DPR = window.devicePixelRatio || 1;

    private birdseyeScale = 1;
    private offsetX = 0;
    private offsetY = 0;
    private bounds: BirdseyeBounds = { minX: 0, minY: 0, maxX: 100, maxY: 100 };

    private isDragging = false;

    private destroyed = false;

    private resizeObserver: ResizeObserver | null = null;

    private effectiveWidth = 200;

    // Default palette mapped per ComponentType. Process groups and remote process groups share a
    // fill so the two visually merge into a single "group" band on the minimap. Only processors
    // and labels render a stroke because they are the only component types whose fill can be
    // overridden by a user-configured background color; the stroke is derived from the resolved
    // fill so it keeps adequate contrast against any choice. Connection is intentionally omitted
    // because getBirdseyeComponentData() does not emit connections, so any unmapped type
    // (current or future) falls through to DEFAULT_COLOR.
    private readonly COMPONENT_COLORS: Partial<Record<ComponentType, { fill: string; hasStroke: boolean }>> = {
        [ComponentType.Processor]: { fill: '#dde4eb', hasStroke: true },
        [ComponentType.ProcessGroup]: { fill: '#728e9b', hasStroke: false },
        [ComponentType.RemoteProcessGroup]: { fill: '#728e9b', hasStroke: false },
        [ComponentType.InputPort]: { fill: '#bbdcde', hasStroke: false },
        [ComponentType.OutputPort]: { fill: '#bbdcde', hasStroke: false },
        [ComponentType.Funnel]: { fill: '#ad9897', hasStroke: false },
        [ComponentType.Label]: { fill: '#fff7d7', hasStroke: true }
    };

    private readonly DEFAULT_COLOR = { fill: '#dde4eb', hasStroke: false };

    constructor() {
        effect(() => {
            const components = this.components();
            if (this.canvasContext && components && !this.isDragging) {
                this.renderComponents(components);
            }
        });

        // Recalculates bounds to account for viewport position beyond flow bounds.
        // Skipped during drag because the brush position is updated directly in the drag handler.
        effect(() => {
            const transform = this.transform();
            const canvasDims = this.canvasDimensions();
            if (this.canvasContext && this.brush && transform && canvasDims && !this.isDragging) {
                this.refresh();
            }
        });

        effect(() => {
            const dims = this.birdseyeDimensions();
            if (this.canvas && this.svg && this.canvasContext) {
                this.updateBirdseyeSize({ width: this.effectiveWidth, height: dims.height });
            }
        });
    }

    ngAfterViewInit(): void {
        this.initializeBirdseye();

        const container = this.birdseyeContainer().nativeElement;
        this.resizeObserver = new ResizeObserver((entries) => {
            for (const entry of entries) {
                const width = entry.contentRect.width;
                if (width > 0 && this.canvas && this.svg && this.canvasContext) {
                    this.updateBirdseyeSize({ width, height: this.birdseyeDimensions().height });
                }
            }
        });
        this.resizeObserver.observe(container);
    }

    ngOnDestroy(): void {
        this.destroyed = true;

        if (this.resizeObserver) {
            this.resizeObserver.disconnect();
            this.resizeObserver = null;
        }

        // Remove the D3 drag behavior so events do not fire after the component is destroyed.
        if (this.brush) {
            this.brush.on('.drag', null);
        }

        if (this.svg) {
            this.svg.remove();
            this.svg = null;
        }
        if (this.canvas) {
            this.canvas.remove();
            this.canvas = null;
        }
    }

    /**
     * Initialize the birdseye view with hybrid Canvas + SVG approach
     */
    private initializeBirdseye(): void {
        const container = this.birdseyeContainer().nativeElement;

        // Use the container's actual width (CSS-driven) instead of the input default.
        const containerWidth = container.clientWidth || this.birdseyeDimensions().width;
        this.effectiveWidth = containerWidth;
        const height = this.birdseyeDimensions().height;

        this.canvas = document.createElement('canvas');
        this.canvas.width = containerWidth * this.DPR;
        this.canvas.height = height * this.DPR;
        this.canvas.style.width = `${containerWidth}px`;
        this.canvas.style.height = `${height}px`;
        this.canvas.className = 'birdseye-canvas';
        container.appendChild(this.canvas);

        this.canvasContext = this.canvas.getContext('2d');
        if (this.canvasContext) {
            this.canvasContext.scale(this.DPR, this.DPR);
        }

        this.svg = d3
            .select(container)
            .append('svg')
            .attr('class', 'birdseye-svg')
            .attr('width', containerWidth)
            .attr('height', height);

        this.brushGroup = this.svg.append('g').attr('class', 'birdseye-brush-container');

        this.createBrush();

        this.renderComponents(this.components());
        this.updateBrush(this.transform(), this.canvasDimensions());
    }

    /**
     * Create the draggable viewport brush. Translation is delta-based: each drag event applies
     * an incremental offset to the current canvas translate rather than computing an absolute
     * viewport position. This keeps the brush in lock-step with incremental canvas pans and
     * avoids cumulative rounding drift when the user holds and drags continuously.
     */
    private createBrush(): void {
        if (!this.brushGroup) return;

        const drag = d3
            .drag<SVGRectElement, { x: number; y: number }>()
            .subject((_event, d) => {
                return { x: d.x, y: d.y };
            })
            .on('start', (_event, _d) => {
                if (this.destroyed) {
                    return;
                }
                this.isDragging = true;
                this.dragStart.emit();
            })
            .on('drag', (event, d) => {
                if (this.destroyed) {
                    return;
                }

                d.x += event.dx;
                d.y += event.dy;

                if (this.brush) {
                    this.brush.attr('transform', `translate(${d.x}, ${d.y})`);
                }

                // Translate the canvas by the inverse delta scaled into canvas coordinates.
                const currentTransform = this.transform();
                const scaledDx = event.dx / this.birdseyeScale;
                const scaledDy = event.dy / this.birdseyeScale;

                const newTranslateX = currentTransform.translate.x - scaledDx * currentTransform.scale;
                const newTranslateY = currentTransform.translate.y - scaledDy * currentTransform.scale;

                this.viewportChange.emit({
                    x: newTranslateX,
                    y: newTranslateY
                });
            })
            .on('end', (_event, _d) => {
                if (this.destroyed) {
                    return;
                }

                this.isDragging = false;

                this.dragEnd.emit();

                // Recalculate bounds so the brush stays within the birdseye area after dragging.
                this.refresh();
            });

        const brushContainer = this.brushGroup
            .append('g')
            .attr('pointer-events', 'all')
            .attr('class', 'birdseye-brush-container');

        this.brush = brushContainer.append('rect').attr('class', 'birdseye-brush').datum({ x: 0, y: 0 }).call(drag);
    }

    /**
     * Returns the effective birdseye dimensions, using the container's actual width
     * (auto-sized via CSS) and the configured height from the input.
     */
    private getEffectiveDimensions(): Dimension {
        return { width: this.effectiveWidth, height: this.birdseyeDimensions().height };
    }

    /**
     * Render component representations using Canvas (efficient for many components).
     */
    private renderComponents(components: BirdseyeComponentData[]): void {
        if (!this.canvasContext) return;

        const ctx = this.canvasContext;

        const dims = this.getEffectiveDimensions();
        ctx.clearRect(0, 0, dims.width, dims.height);

        if (!components || components.length === 0) {
            return;
        }

        const componentBounds = this.calculateComponentBounds(components);

        // Calculate viewport bounds in canvas coordinates.
        const transform = this.transform();
        const canvasDims = this.canvasDimensions();
        const viewportLeft = -transform.translate.x / transform.scale;
        const viewportTop = -transform.translate.y / transform.scale;
        const viewportRight = viewportLeft + canvasDims.width / transform.scale;
        const viewportBottom = viewportTop + canvasDims.height / transform.scale;

        // Total bounds = union of component bounds and viewport bounds so the birdseye
        // shows both content AND the viewport even if the viewport is panned away.
        this.bounds = {
            minX: Math.min(componentBounds.minX, viewportLeft),
            minY: Math.min(componentBounds.minY, viewportTop),
            maxX: Math.max(componentBounds.maxX, viewportRight),
            maxY: Math.max(componentBounds.maxY, viewportBottom)
        };

        const contentWidth = this.bounds.maxX - this.bounds.minX;
        const contentHeight = this.bounds.maxY - this.bounds.minY;

        const padding = 5;
        const availableWidth = dims.width - padding * 2;
        const availableHeight = dims.height - padding * 2;

        const scaleX = contentWidth > 0 ? availableWidth / contentWidth : 1;
        const scaleY = contentHeight > 0 ? availableHeight / contentHeight : 1;
        // Don't scale up beyond 1.
        this.birdseyeScale = Math.min(scaleX, scaleY, 1);

        const scaledWidth = contentWidth * this.birdseyeScale;
        const scaledHeight = contentHeight * this.birdseyeScale;
        this.offsetX = (dims.width - scaledWidth) / 2;
        this.offsetY = (dims.height - scaledHeight) / 2;

        ctx.save();

        // Apply the transformation chain in this order:
        //   translate(offset) -> scale(birdseyeScale) -> translate(-bounds.min)
        // Scaling before drawing means strokes scale uniformly with the content, so a single
        // strokeStyle assignment produces a visually consistent border for every component.
        // Translating by -bounds.min lets the loop pass each component's raw canvas position
        // and dimensions without the caller having to map them into birdseye coordinates.
        ctx.translate(this.offsetX, this.offsetY);
        ctx.scale(this.birdseyeScale, this.birdseyeScale);
        ctx.translate(-this.bounds.minX, -this.bounds.minY);

        for (const component of components) {
            const color = this.COMPONENT_COLORS[component.type] || this.DEFAULT_COLOR;
            // Honor a user-configured background color (set by Change Color on processors and
            // labels) when present; otherwise fall back to the type-based palette entry.
            const fill = component.fillColor || color.fill;

            ctx.fillStyle = fill;
            ctx.fillRect(
                component.position.x,
                component.position.y,
                component.dimensions.width,
                component.dimensions.height
            );

            if (color.hasStroke) {
                ctx.strokeStyle = this.determineContrastColor(fill);
                ctx.strokeRect(
                    component.position.x,
                    component.position.y,
                    component.dimensions.width,
                    component.dimensions.height
                );
            }
        }

        ctx.restore();
    }

    /**
     * Returns black or white depending on whether the supplied hex color is light or dark, so
     * the stroke remains legible against any user-configured fill. Uses a simple luminance
     * threshold (`parseInt(hex, 16) > 0xffffff / 1.5`) which is cheap to compute on every
     * paint and gives correct contrast for the limited fill palette this component renders.
     */
    private determineContrastColor(fill: string): string {
        const hex = fill.startsWith('#') ? fill.substring(1) : fill;
        if (parseInt(hex, 16) > 0xffffff / 1.5) {
            return '#000000';
        }
        return '#ffffff';
    }

    /**
     * Update the viewport brush position and size.
     */
    private updateBrush(transform: BirdseyeTransform, canvasDimensions: Dimension): void {
        if (!this.brush) return;

        const components = this.components();
        if (!components || components.length === 0) return;

        const viewportX = -transform.translate.x / transform.scale;
        const viewportY = -transform.translate.y / transform.scale;

        const viewportWidth = canvasDimensions.width / transform.scale;
        const viewportHeight = canvasDimensions.height / transform.scale;

        const brushX = (viewportX - this.bounds.minX) * this.birdseyeScale + this.offsetX;
        const brushY = (viewportY - this.bounds.minY) * this.birdseyeScale + this.offsetY;
        const brushWidth = viewportWidth * this.birdseyeScale;
        const brushHeight = viewportHeight * this.birdseyeScale;

        this.brush.datum({ x: brushX, y: brushY });

        this.brush
            .attr('transform', `translate(${brushX}, ${brushY})`)
            .attr('width', Math.max(brushWidth, 10))
            .attr('height', Math.max(brushHeight, 10));
    }

    /**
     * Calculate the bounding box of all components.
     */
    private calculateComponentBounds(components: BirdseyeComponentData[]): BirdseyeBounds {
        if (!components || components.length === 0) {
            return { minX: 0, minY: 0, maxX: 100, maxY: 100 };
        }

        let minX = Infinity;
        let minY = Infinity;
        let maxX = -Infinity;
        let maxY = -Infinity;

        for (const component of components) {
            minX = Math.min(minX, component.position.x);
            minY = Math.min(minY, component.position.y);
            maxX = Math.max(maxX, component.position.x + component.dimensions.width);
            maxY = Math.max(maxY, component.position.y + component.dimensions.height);
        }

        return { minX, minY, maxX, maxY };
    }

    /**
     * Recalculate bounds and redraw everything. Called after drag ends so the brush
     * stays within the bounds of the new viewport.
     */
    private refresh(): void {
        this.renderComponents(this.components());
        this.updateBrush(this.transform(), this.canvasDimensions());
    }

    /**
     * Update birdseye canvas and SVG sizes when dimensions change.
     */
    private updateBirdseyeSize(dims: Dimension): void {
        if (!this.canvas || !this.svg || !this.canvasContext) return;

        this.effectiveWidth = dims.width;

        this.canvas.width = dims.width * this.DPR;
        this.canvas.height = dims.height * this.DPR;
        this.canvas.style.width = `${dims.width}px`;
        this.canvas.style.height = `${dims.height}px`;

        this.canvasContext.setTransform(1, 0, 0, 1, 0, 0);
        this.canvasContext.scale(this.DPR, this.DPR);

        this.svg.attr('width', dims.width).attr('height', dims.height);

        this.refresh();
    }
}
