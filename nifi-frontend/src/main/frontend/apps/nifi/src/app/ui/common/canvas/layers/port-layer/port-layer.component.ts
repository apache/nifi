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
import { CanvasPort, CanvasRootResolver, CanvasRootSelection, CanvasSelection } from '../../canvas.types';
import { PortRenderer } from './port-renderer';
import { TextEllipsisUtils } from '../../utils/text-ellipsis.utils';
import { CanvasFormatUtils } from '../../canvas-format-utils.service';
import { CanvasComponentUtils } from '../../canvas-component-utils.service';
import { PortRenderContext } from '../render-context.types';
import { NiFiCommon, Position } from '@nifi/shared';
import { ConnectableBehaviorHelper } from '../../connectable-behavior.helper';

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
    private destroyRef = inject(DestroyRef);

    ports = input<CanvasPort[]>([]);
    scale = input<number>(1);
    selectedIds = input<string[]>([]);
    textEllipsis = input.required<TextEllipsisUtils>();
    formatUtils = input.required<CanvasFormatUtils>();
    componentUtils = input.required<CanvasComponentUtils>();
    nifiCommon = input.required<NiFiCommon>();
    canEdit = input<boolean>(true);
    disabledPortIds = input<Set<string>>(new Set());
    canSelect = input<boolean>(true);
    connectableBehavior = input<ConnectableBehaviorHelper | null>(null);
    canvasRootResolver = input<CanvasRootResolver | null>(null);

    portClick = output<{ port: CanvasPort; event: MouseEvent }>();
    portDoubleClick = output<{ port: CanvasPort; event: MouseEvent }>();
    dragEnd = output<{ delta: Position; movingIds: Set<string> }>();

    private containerSelection: CanvasRootSelection | null = null;

    constructor() {
        this.destroyRef.onDestroy(() => {
            const helper = this.connectableBehavior();
            if (helper && this.containerSelection) {
                helper.deactivate(
                    this.containerSelection.selectAll<SVGGElement, CanvasPort>('g.input-port, g.output-port')
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

    private readonly callbacks: PortRenderContext['callbacks'] = {
        onClick: (port, event) => {
            this.portClick.emit({ port, event });
        },
        onDoubleClick: (port, event) => {
            this.portDoubleClick.emit({ port, event });
        },
        onDragEnd: (delta, movingIds) => {
            this.dragEnd.emit({ delta, movingIds });
        }
    };
    private selectedIdsSet = computed(() => new Set(this.selectedIds()));

    private renderContext = computed<PortRenderContext>(() => ({
        containerSelection: this.containerSelection!,
        textEllipsis: this.textEllipsis(),
        formatUtils: this.formatUtils(),
        nifiCommon: this.nifiCommon(),
        componentUtils: this.componentUtils(),
        getCanEdit: () => this.canEdit(),
        getCanSelect: () => this.canSelect(),
        getSelectedIds: () => this.selectedIdsSet(),
        ports: this.ports(),
        disabledPortIds: this.disabledPortIds(),
        getDisabledPortIds: () => this.disabledPortIds(),
        canvasRootResolver: this.canvasRootResolver() ?? this.defaultCanvasRootResolver,
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

    /**
     * Attach or detach the connection handle when the helper, canEdit, or
     * port set changes. activate is idempotent, so a data refresh rewires
     * newly entered ports without tearing down an in-flight connection.
     * deactivate runs only when editing is turned off.
     */
    private connectableEffect = effect((onCleanup) => {
        this.ports();
        const helper = this.connectableBehavior();
        const canEdit = this.canEdit();
        if (!this.containerSelection) return;
        const groups = this.containerSelection.selectAll<SVGGElement, CanvasPort>('g.input-port, g.output-port');
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

    public pan(selection: CanvasSelection<CanvasPort>): void {
        PortRenderer.pan(selection, this.renderContext());
        this.applySelectionStyling();
    }
}
