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
import { NiFiCommon, Position } from '@nifi/shared';
import { CanvasRemoteProcessGroup, CanvasRootResolver, CanvasRootSelection, CanvasSelection } from '../../canvas.types';
import { RemoteProcessGroupRenderer } from './remote-process-group-renderer';
import { TextEllipsisUtils } from '../../utils/text-ellipsis.utils';
import { CanvasFormatUtils } from '../../canvas-format-utils.service';
import { CanvasComponentUtils } from '../../canvas-component-utils.service';
import { RemoteProcessGroupRenderContext } from '../render-context.types';
import { ConnectableBehaviorHelper } from '../../connectable-behavior.helper';

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
    private destroyRef = inject(DestroyRef);

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
    connectableBehavior = input<ConnectableBehaviorHelper | null>(null);
    canvasRootResolver = input<CanvasRootResolver | null>(null);

    remoteProcessGroupClick = output<{ rpg: CanvasRemoteProcessGroup; event: MouseEvent }>();

    remoteProcessGroupDoubleClick = output<{ rpg: CanvasRemoteProcessGroup; event: MouseEvent }>();

    dragEnd = output<{ delta: Position; movingIds: Set<string> }>();

    private containerSelection: CanvasRootSelection | null = null;

    constructor() {
        this.destroyRef.onDestroy(() => {
            const helper = this.connectableBehavior();
            if (helper && this.containerSelection) {
                helper.deactivate(
                    this.containerSelection.selectAll<SVGGElement, CanvasRemoteProcessGroup>('g.remote-process-group')
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

    private readonly callbacks: RemoteProcessGroupRenderContext['callbacks'] = {
        onClick: (rpg, event) => {
            this.remoteProcessGroupClick.emit({ rpg, event });
        },
        onDoubleClick: (rpg, event) => {
            this.remoteProcessGroupDoubleClick.emit({ rpg, event });
        },
        onDragEnd: (delta, movingIds) => {
            this.dragEnd.emit({ delta, movingIds });
        }
    };
    private selectedIdsSet = computed(() => new Set(this.selectedIds()));

    private renderContext = computed<RemoteProcessGroupRenderContext>(() => ({
        containerSelection: this.containerSelection!,
        textEllipsis: this.textEllipsis(),
        formatUtils: this.formatUtils(),
        nifiCommon: this.nifiCommon(),
        getCanEdit: () => this.canEdit(),
        getCanSelect: () => this.canSelect(),
        getSelectedIds: () => this.selectedIdsSet(),
        componentUtils: this.componentUtils(),
        remoteProcessGroups: this.remoteProcessGroups(),
        disabledRemoteProcessGroupIds: this.disabledRemoteProcessGroupIds(),
        getDisabledRemoteProcessGroupIds: () => this.disabledRemoteProcessGroupIds(),
        canvasRootResolver: this.canvasRootResolver() ?? this.defaultCanvasRootResolver,
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

    /**
     * Attach or detach the connection handle when the helper, canEdit, or
     * remote process group set changes. activate is idempotent, so a data
     * refresh rewires newly entered groups without tearing down an in-flight
     * connection. deactivate runs only when editing is turned off.
     */
    private connectableEffect = effect((onCleanup) => {
        this.remoteProcessGroups();
        this.renderTrigger();
        const helper = this.connectableBehavior();
        const canEdit = this.canEdit();
        if (!this.containerSelection) return;
        const groups = this.containerSelection.selectAll<SVGGElement, CanvasRemoteProcessGroup>(
            'g.remote-process-group'
        );
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

    public pan(selection: CanvasSelection<CanvasRemoteProcessGroup>): void {
        RemoteProcessGroupRenderer.pan(selection, this.renderContext());
        this.applySelectionStyling();
    }
}
