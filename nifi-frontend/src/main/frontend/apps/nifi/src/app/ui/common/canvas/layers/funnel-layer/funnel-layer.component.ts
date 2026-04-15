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
import { CanvasFunnel } from '../../canvas.types';
import { FunnelRenderer } from './funnel-renderer';
import { TextEllipsisUtils } from '../../utils/text-ellipsis.utils';
import { CanvasFormatUtils } from '../../canvas-format-utils.service';
import { FunnelRenderContext } from '../render-context.types';

@Component({
    // Attribute selector required: this component renders as an SVG <g> element, not a custom HTML element
    // eslint-disable-next-line @angular-eslint/component-selector
    selector: '[canvas-funnel-layer]',
    standalone: true,
    template: '',
    host: {
        class: 'funnels'
    },
    changeDetection: ChangeDetectionStrategy.OnPush
})
export class FunnelLayerComponent implements AfterViewInit {
    private elementRef = inject(ElementRef);

    funnels = input<CanvasFunnel[]>([]);

    scale = input<number>(1);

    selectedIds = input<string[]>([]);

    canSelect = input<boolean>(true);

    disabledFunnelIds = input<Set<string>>(new Set());

    canEdit = input<boolean>(true);

    textEllipsis = input.required<TextEllipsisUtils>();

    formatUtils = input.required<CanvasFormatUtils>();

    funnelClick = output<{ funnel: CanvasFunnel; event: MouseEvent }>();

    funnelDoubleClick = output<{ funnel: CanvasFunnel; event: MouseEvent }>();

    funnelDragEnd = output<{ funnel: CanvasFunnel; newPosition: any; previousPosition: any }>();

    private containerSelection: d3.Selection<any, any, any, any> | null = null;

    private readonly callbacks = {
        onClick: (funnel: any, event: MouseEvent) => {
            this.funnelClick.emit({ funnel, event });
        },
        onDoubleClick: (funnel: any, event: MouseEvent) => {
            this.funnelDoubleClick.emit({ funnel, event });
        },
        onDragEnd: (funnel: any, newPosition: any, previousPosition: any) => {
            this.funnelDragEnd.emit({ funnel, newPosition, previousPosition });
        }
    };

    private renderContext = computed<FunnelRenderContext>(() => ({
        containerSelection: this.containerSelection!,
        textEllipsis: this.textEllipsis(),
        formatUtils: this.formatUtils(),
        funnels: this.funnels(),
        canSelect: this.canSelect(),
        getCanEdit: () => this.canEdit(),
        disabledFunnelIds: this.disabledFunnelIds(),
        callbacks: this.callbacks
    }));

    private renderEffect = effect(() => {
        const _funnels = this.funnels();

        if (this.containerSelection) {
            this.renderFunnels();
        }
    });

    private selectionEffect = effect(() => {
        this.applySelectionStyling();
    });

    ngAfterViewInit(): void {
        const nativeElement = this.elementRef.nativeElement;
        this.containerSelection = d3.select(nativeElement);

        // Initial render if data arrived before view was ready
        if (this.funnels().length > 0) {
            this.renderFunnels();
        }
    }

    private applySelectionStyling(): void {
        if (!this.containerSelection) {
            return;
        }

        const selectedIds = this.selectedIds();

        this.containerSelection
            .selectAll<SVGGElement, CanvasFunnel>('g.funnel')
            .classed('selected', (d) => selectedIds.includes(d.entity.id));
    }

    private renderFunnels(): void {
        if (!this.containerSelection) {
            return;
        }

        FunnelRenderer.render(this.renderContext());
        this.applySelectionStyling();
    }

    public pan(selection: d3.Selection<any, any, any, any>): void {
        FunnelRenderer.pan(selection, this.renderContext());
        this.applySelectionStyling();
    }
}
