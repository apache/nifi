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
    ChangeDetectionStrategy,
    inject,
    input,
    output,
    effect,
    computed
} from '@angular/core';
import * as d3 from 'd3';
import { CanvasLabel, CanvasRootResolver, CanvasRootSelection, CanvasSelection } from '../../canvas.types';
import { LabelRenderer } from './label-renderer';
import { TextEllipsisUtils } from '../../utils/text-ellipsis.utils';
import { CanvasFormatUtils } from '../../canvas-format-utils.service';
import { NiFiCommon, Position } from '@nifi/shared';
import { LabelRenderContext } from '../render-context.types';

@Component({
    // Attribute selector required: this component renders as an SVG <g> element, not a custom HTML element
    // eslint-disable-next-line @angular-eslint/component-selector
    selector: '[canvas-label-layer]',
    template: '',
    changeDetection: ChangeDetectionStrategy.OnPush,
    standalone: true,
    host: {
        class: 'labels'
    }
})
export class LabelLayerComponent implements AfterViewInit {
    labels = input<CanvasLabel[]>([]);

    scale = input<number>(1);

    selectedIds = input<string[]>([]);

    canSelect = input<boolean>(true);

    textEllipsis = input.required<TextEllipsisUtils>();

    formatUtils = input.required<CanvasFormatUtils>();

    nifiCommon = input.required<NiFiCommon>();

    canEdit = input<boolean>(true);

    disabledLabelIds = input<Set<string>>(new Set());
    canvasRootResolver = input<CanvasRootResolver | null>(null);

    labelClick = output<{ label: CanvasLabel; event: MouseEvent }>();

    labelDoubleClick = output<{ label: CanvasLabel; event: MouseEvent }>();

    labelResizeEnd = output<{ label: CanvasLabel; dimensions: { width: number; height: number } }>();

    dragEnd = output<{ delta: Position; movingIds: Set<string> }>();

    private elementRef = inject(ElementRef);
    private containerSelection: CanvasRootSelection | null = null;
    private readonly defaultCanvasRootResolver: CanvasRootResolver = () => {
        const node = this.containerSelection?.node();
        const canvasNode = (node?.closest('g.canvas') as SVGGElement | null) ?? node;
        return canvasNode
            ? d3.select<SVGGElement, unknown>(canvasNode)
            : d3.select<SVGGElement, unknown>(null as unknown as SVGGElement);
    };

    private readonly callbacks: LabelRenderContext['callbacks'] = {
        onClick: (label, event) => {
            this.labelClick.emit({ label, event });
        },
        onDoubleClick: (label, event) => {
            this.labelDoubleClick.emit({ label, event });
        },
        onResizeEnd: (label, dimensions: { width: number; height: number }) => {
            this.labelResizeEnd.emit({ label, dimensions });
        },
        onDragEnd: (delta, movingIds) => {
            this.dragEnd.emit({ delta, movingIds });
        }
    };
    private selectedIdsSet = computed(() => new Set(this.selectedIds()));

    private renderContext = computed<LabelRenderContext>(() => ({
        containerSelection: this.containerSelection!,
        scale: this.scale(),
        textEllipsis: this.textEllipsis(),
        formatUtils: this.formatUtils(),
        nifiCommon: this.nifiCommon(),
        getCanEdit: () => this.canEdit(),
        getCanSelect: () => this.canSelect(),
        getSelectedIds: () => this.selectedIdsSet(),
        labels: this.labels(),
        disabledLabelIds: this.disabledLabelIds(),
        getDisabledLabelIds: () => this.disabledLabelIds(),
        canvasRootResolver: this.canvasRootResolver() ?? this.defaultCanvasRootResolver,
        canSelect: this.canSelect(),
        callbacks: this.callbacks
    }));

    private renderEffect = effect(() => {
        // Access signals to track dependencies
        const _labels = this.labels();
        const _disabledLabelIds = this.disabledLabelIds();

        // Render if container is initialized
        if (this.containerSelection) {
            this.renderLabels();
        }
    });

    private selectionEffect = effect(() => {
        this.applySelectionStyling();
    });

    ngAfterViewInit(): void {
        const nativeElement = this.elementRef.nativeElement;

        // The component IS the <g> element with attribute selector
        this.containerSelection = d3.select<SVGGElement, unknown>(nativeElement);

        // Initial render if data arrived before view was ready
        if (this.labels().length > 0) {
            this.renderLabels();
        }
    }

    private applySelectionStyling(): void {
        if (!this.containerSelection) {
            return;
        }

        const selectedIds = this.selectedIds();

        this.containerSelection
            .selectAll<SVGGElement, CanvasLabel>('g.label')
            .classed('selected', (d) => selectedIds.includes(d.entity.id));
    }

    private renderLabels(): void {
        if (!this.containerSelection) {
            return;
        }

        // Call static renderer with computed context
        LabelRenderer.render(this.renderContext());
        this.applySelectionStyling();
    }

    public pan(selection: CanvasSelection<CanvasLabel>): void {
        LabelRenderer.pan(selection, this.renderContext());
        this.applySelectionStyling();
    }
}
