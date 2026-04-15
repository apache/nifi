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
import { CanvasPort } from '../../canvas.types';
import { PortRenderer } from './port-renderer';
import { TextEllipsisUtils } from '../../utils/text-ellipsis.utils';
import { CanvasFormatUtils } from '../../canvas-format-utils.service';
import { CanvasComponentUtils } from '../../canvas-component-utils.service';
import { PortRenderContext } from '../render-context.types';

@Component({
    // Attribute selector required: this component renders as an SVG <g> element, not a custom HTML element
    // eslint-disable-next-line @angular-eslint/component-selector
    selector: '[canvas-port-layer]',
    standalone: true,
    template: '',
    host: {
        class: 'ports'
    },
    changeDetection: ChangeDetectionStrategy.OnPush
})
export class PortLayerComponent implements AfterViewInit {
    private elementRef = inject(ElementRef);

    ports = input<CanvasPort[]>([]);
    scale = input<number>(1);
    selectedIds = input<string[]>([]);
    textEllipsis = input.required<TextEllipsisUtils>();
    formatUtils = input.required<CanvasFormatUtils>();
    componentUtils = input.required<CanvasComponentUtils>();
    canEdit = input<boolean>(true);
    disabledPortIds = input<Set<string>>(new Set());
    canSelect = input<boolean>(true);

    portClick = output<{ port: CanvasPort; event: MouseEvent }>();
    portDoubleClick = output<{ port: CanvasPort; event: MouseEvent }>();
    portDragEnd = output<{
        port: CanvasPort;
        newPosition: { x: number; y: number };
        previousPosition: { x: number; y: number };
    }>();

    private containerSelection: d3.Selection<any, any, any, any> | null = null;

    private readonly callbacks = {
        onClick: (port: any, event: MouseEvent) => {
            this.portClick.emit({ port, event });
        },
        onDoubleClick: (port: any, event: MouseEvent) => {
            this.portDoubleClick.emit({ port, event });
        },
        onDragEnd: (port: any, newPosition: any, previousPosition: any) => {
            this.portDragEnd.emit({ port, newPosition, previousPosition });
        }
    };

    private renderContext = computed<PortRenderContext>(() => ({
        containerSelection: this.containerSelection!,
        textEllipsis: this.textEllipsis(),
        formatUtils: this.formatUtils(),
        componentUtils: this.componentUtils(),
        getCanEdit: () => this.canEdit(),
        ports: this.ports(),
        disabledPortIds: this.disabledPortIds(),
        canSelect: this.canSelect(),
        callbacks: this.callbacks
    }));

    private renderEffect = effect(() => {
        const _ports = this.ports();

        if (this.containerSelection) {
            this.renderPorts();
        }
    });

    private selectionEffect = effect(() => {
        this.applySelectionStyling();
    });

    ngAfterViewInit(): void {
        const nativeElement = this.elementRef.nativeElement;
        this.containerSelection = d3.select(nativeElement);

        // Initial render if data arrived before view was ready
        if (this.ports().length > 0) {
            this.renderPorts();
        }
    }

    private applySelectionStyling(): void {
        if (!this.containerSelection) {
            return;
        }

        const selectedIds = this.selectedIds();

        this.containerSelection
            .selectAll<SVGGElement, CanvasPort>('g.input-port, g.output-port')
            .classed('selected', (d) => selectedIds.includes(d.entity.id));
    }

    private renderPorts(): void {
        if (!this.containerSelection) {
            return;
        }

        PortRenderer.render(this.renderContext());
        this.applySelectionStyling();
    }

    public pan(selection: d3.Selection<any, any, any, any>): void {
        PortRenderer.pan(selection, this.renderContext());
        this.applySelectionStyling();
    }
}
