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
import { NiFiCommon } from '@nifi/shared';
import { CanvasRemoteProcessGroup } from '../../canvas.types';
import { RemoteProcessGroupRenderer } from './remote-process-group-renderer';
import { TextEllipsisUtils } from '../../utils/text-ellipsis.utils';
import { CanvasFormatUtils } from '../../canvas-format-utils.service';
import { CanvasComponentUtils } from '../../canvas-component-utils.service';
import { RemoteProcessGroupRenderContext } from '../render-context.types';

@Component({
    // Attribute selector required: this component renders as an SVG <g> element, not a custom HTML element
    // eslint-disable-next-line @angular-eslint/component-selector
    selector: '[canvas-remote-process-group-layer]',
    standalone: true,
    template: '',
    host: {
        class: 'remote-process-groups'
    },
    changeDetection: ChangeDetectionStrategy.OnPush
})
export class RemoteProcessGroupLayerComponent implements AfterViewInit {
    private elementRef = inject(ElementRef);

    remoteProcessGroups = input<CanvasRemoteProcessGroup[]>([]);

    scale = input<number>(1);

    selectedIds = input<string[]>([]);

    renderTrigger = input<number>(0);

    canSelect = input<boolean>(true);

    textEllipsis = input.required<TextEllipsisUtils>();

    formatUtils = input.required<CanvasFormatUtils>();

    nifiCommon = input.required<NiFiCommon>();

    canEdit = input<boolean>(true);

    componentUtils = input.required<CanvasComponentUtils>();

    disabledRemoteProcessGroupIds = input<Set<string>>(new Set());

    remoteProcessGroupClick = output<{ rpg: CanvasRemoteProcessGroup; event: MouseEvent }>();

    remoteProcessGroupDoubleClick = output<{ rpg: CanvasRemoteProcessGroup; event: MouseEvent }>();

    remoteProcessGroupDragEnd = output<{
        rpg: CanvasRemoteProcessGroup;
        newPosition: { x: number; y: number };
        previousPosition: { x: number; y: number };
    }>();

    private containerSelection: d3.Selection<any, any, any, any> | null = null;

    private readonly callbacks = {
        onClick: (rpg: any, event: MouseEvent) => {
            this.remoteProcessGroupClick.emit({ rpg, event });
        },
        onDoubleClick: (rpg: any, event: MouseEvent) => {
            this.remoteProcessGroupDoubleClick.emit({ rpg, event });
        },
        onDragEnd: (rpg: any, newPosition: any, previousPosition: any) => {
            this.remoteProcessGroupDragEnd.emit({ rpg, newPosition, previousPosition });
        }
    };

    private renderContext = computed<RemoteProcessGroupRenderContext>(() => ({
        containerSelection: this.containerSelection!,
        textEllipsis: this.textEllipsis(),
        formatUtils: this.formatUtils(),
        nifiCommon: this.nifiCommon(),
        getCanEdit: () => this.canEdit(),
        componentUtils: this.componentUtils(),
        remoteProcessGroups: this.remoteProcessGroups(),
        disabledRemoteProcessGroupIds: this.disabledRemoteProcessGroupIds(),
        canSelect: this.canSelect(),
        callbacks: this.callbacks
    }));

    private renderEffect = effect(() => {
        const _remoteProcessGroups = this.remoteProcessGroups();
        const _renderTrigger = this.renderTrigger();

        if (this.containerSelection) {
            this.renderRemoteProcessGroups();
        }
    });

    private selectionEffect = effect(() => {
        this.applySelectionStyling();
    });

    ngAfterViewInit(): void {
        const nativeElement = this.elementRef.nativeElement;
        this.containerSelection = d3.select(nativeElement);

        // Initial render if data arrived before view was ready
        if (this.remoteProcessGroups().length > 0) {
            this.renderRemoteProcessGroups();
        }
    }

    private applySelectionStyling(): void {
        if (!this.containerSelection) {
            return;
        }

        const selectedIds = this.selectedIds();

        const groups = this.containerSelection.selectAll<SVGGElement, CanvasRemoteProcessGroup>(
            'g.remote-process-group'
        );

        groups.classed('selected', (d) => selectedIds.includes(d.entity.id));

        groups.select('rect.border').attr('stroke', (d) => {
            if (d.entity.permissions?.canRead === false) {
                return '#ba554a';
            }
            return selectedIds.includes(d.entity.id) ? '#004ba0' : 'transparent';
        });
    }

    private renderRemoteProcessGroups(): void {
        if (!this.containerSelection) {
            return;
        }

        RemoteProcessGroupRenderer.render(this.renderContext());
        this.applySelectionStyling();
    }

    public pan(selection: d3.Selection<any, CanvasRemoteProcessGroup, any, any>): void {
        RemoteProcessGroupRenderer.pan(selection, this.renderContext());
        this.applySelectionStyling();
    }
}
