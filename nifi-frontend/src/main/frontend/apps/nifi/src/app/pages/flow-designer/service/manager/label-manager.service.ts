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

import { Injectable, OnDestroy, inject } from '@angular/core';
import { Store } from '@ngrx/store';
import { CanvasState } from '../../state';
import { CanvasUtils } from '../canvas-utils.service';
import { PositionBehavior } from '../behavior/position-behavior.service';
import { SelectableBehavior } from '../behavior/selectable-behavior.service';
import { EditableBehavior } from '../behavior/editable-behavior.service';
import * as d3 from 'd3';
import {
    selectFlowLoadingStatus,
    selectLabels,
    selectAnySelectedComponentIds,
    selectTransitionRequired
} from '../../state/flow/flow.selectors';
import { Client } from '../../../../service/client.service';
import { updateComponent } from '../../state/flow/flow.actions';
import { QuickSelectBehavior } from '../behavior/quick-select-behavior.service';
import { UpdateComponentRequest } from '../../state/flow';
import { filter, Subject, switchMap, takeUntil } from 'rxjs';
import { ComponentType, NiFiCommon } from '@nifi/shared';
import { ClusterConnectionService } from '../../../../service/cluster-connection.service';

@Injectable({
    providedIn: 'root'
})
export class LabelManager implements OnDestroy {
    private store = inject<Store<CanvasState>>(Store);
    private canvasUtils = inject(CanvasUtils);
    private nifiCommon = inject(NiFiCommon);
    private client = inject(Client);
    private clusterConnectionService = inject(ClusterConnectionService);
    private positionBehavior = inject(PositionBehavior);
    private selectableBehavior = inject(SelectableBehavior);
    private quickSelectBehavior = inject(QuickSelectBehavior);
    private editableBehavior = inject(EditableBehavior);

    private destroyed$: Subject<boolean> = new Subject();

    public static readonly INITIAL_WIDTH: number = 148;
    public static readonly INITIAL_HEIGHT: number = 148;
    private static readonly MIN_HEIGHT: number = 24;
    private static readonly MIN_WIDTH: number = 64;
    private static readonly SNAP_ALIGNMENT_PIXELS: number = 8;

    private labels: [] = [];
    private labelContainer: any = null;
    private transitionRequired = false;

    private labelPointDrag: any;
    private snapEnabled = true;

    constructor() {
        // handle bend point drag events
        this.labelPointDrag = d3
            .drag()
            .subject(function (this: Element) {
                // Capture DOM element via subject; currentTarget is valid during the initiating mousedown.
                return { element: this };
            })
            .on('start', (event: any) => {
                // stop further propagation
                event.sourceEvent.stopPropagation();

                // indicate dragging start
                const label = d3.select(event.subject.element.parentNode as Element);
                const labelData: any = label.datum();
                labelData.dragging = true;
            })
            .on('drag', (event: any) => {
                const label = d3.select(event.subject.element.parentNode as Element);
                const labelData: any = label.datum();

                if (labelData.dragging) {
                    // update the dimensions and ensure they are still within bounds
                    // snap between aligned sizes unless the user is holding shift
                    this.snapEnabled = !event.sourceEvent.shiftKey;
                    labelData.dimensions.width = Math.max(
                        LabelManager.MIN_WIDTH,
                        this.snapEnabled
                            ? Math.round(event.x / LabelManager.SNAP_ALIGNMENT_PIXELS) *
                                  LabelManager.SNAP_ALIGNMENT_PIXELS
                            : event.x
                    );
                    labelData.dimensions.height = Math.max(
                        LabelManager.MIN_HEIGHT,
                        this.snapEnabled
                            ? Math.round(event.y / LabelManager.SNAP_ALIGNMENT_PIXELS) *
                                  LabelManager.SNAP_ALIGNMENT_PIXELS
                            : event.y
                    );

                    // redraw this connection
                    this.updateLabels(label);
                }
            })
            .on('end', (event: any) => {
                const label = d3.select(event.subject.element.parentNode as Element);
                const labelData: any = label.datum();

                if (labelData.dragging) {
                    // determine if the width has changed
                    let different = false;
                    const widthSet = !!labelData.component.width;
                    if (widthSet || labelData.dimensions.width !== labelData.component.width) {
                        different = true;
                    }

                    // determine if the height has changed
                    const heightSet = !!labelData.component.height;
                    if ((!different && heightSet) || labelData.dimensions.height !== labelData.component.height) {
                        different = true;
                    }

                    // only save the updated dimensions if necessary
                    if (different) {
                        const updateLabel: UpdateComponentRequest = {
                            id: labelData.id,
                            type: ComponentType.Label,
                            uri: labelData.uri,
                            payload: {
                                revision: this.client.getRevision(labelData),
                                disconnectedNodeAcknowledged:
                                    this.clusterConnectionService.isDisconnectionAcknowledged(),
                                component: {
                                    id: labelData.id,
                                    width: labelData.dimensions.width,
                                    height: labelData.dimensions.height
                                }
                            },
                            restoreOnFailure: {
                                dimensions: {
                                    width: widthSet ? labelData.component.width : LabelManager.INITIAL_WIDTH,
                                    height: heightSet ? labelData.component.height : LabelManager.INITIAL_HEIGHT
                                }
                            },
                            errorStrategy: 'snackbar'
                        };

                        this.store.dispatch(
                            updateComponent({
                                request: updateLabel
                            })
                        );
                    }
                }

                // stop further propagation
                event.sourceEvent.stopPropagation();

                // indicate dragging complete
                labelData.dragging = false;
            });
    }

    private select() {
        return this.labelContainer.selectAll('g.label').data(this.labels, function (d: any) {
            return d.id;
        });
    }

    /**
     * Sorts the specified labels according to the z index.
     *
     * @param {type} labels
     */
    private sort(labels: any[]): void {
        labels.sort((a, b) => {
            return this.nifiCommon.compareNumber(a.zIndex, b.zIndex);
        });
    }

    private renderLabels(entered: any) {
        if (entered.empty()) {
            return entered;
        }

        const label = entered
            .append('g')
            .attr('id', function (d: any) {
                return 'id-' + d.id;
            })
            .attr('class', 'label component');

        // label border
        label.append('rect').attr('class', 'border').attr('fill', 'transparent').attr('stroke', 'transparent');

        // label
        label
            .append('rect')
            .attr('class', 'body')
            .attr('filter', 'url(#component-drop-shadow)')
            .attr('stroke-width', 0);

        // label value
        label
            .append('text')
            .attr('x', 10)
            .attr('xml:space', 'preserve')
            .attr('font-weight', 'bold')
            .attr('fill', 'black')
            .attr('class', 'label-value');

        this.selectableBehavior.activate(label);
        this.quickSelectBehavior.activate(label);

        return label;
    }

    private updateLabels(updated: any) {
        if (updated.empty()) {
            return;
        }

        // update the border using the configured color
        updated
            .select('rect.border')
            .attr('width', (d: any) => d.dimensions.width)
            .attr('height', (d: any) => d.dimensions.height)
            .classed('unauthorized', (d: any) => d.permissions.canRead === false);

        // update the body fill using the configured color
        updated
            .select('rect.body')
            .attr('width', (d: any) => d.dimensions.width)
            .attr('height', (d: any) => d.dimensions.height)
            .style('fill', (d: any) => {
                if (!d.permissions.canRead) {
                    return null;
                }

                let color = this.defaultColor();

                // use the specified color if appropriate
                if (d.component.style['background-color']) {
                    color = d.component.style['background-color'];
                }

                return color;
            })
            .classed('unauthorized', (d: any) => d.permissions.canRead === false);

        // go through each label being updated
        updated.each((d: any, i: number, nodes: Element[]) => {
            const label = d3.select(nodes[i]);

            // update the component behavior as appropriate
            this.editableBehavior.editable(label);

            // update the label
            const labelText = label.select('text.label-value');
            const labelPoint = label.selectAll('path.labelpoint');
            if (d.permissions.canRead) {
                // update the font size
                labelText.attr('font-size', () => {
                    let fontSize = '12px';

                    // use the specified color if appropriate
                    if (d.component.style['font-size']) {
                        fontSize = d.component.style['font-size'];
                    }

                    return fontSize;
                });

                // remove the previous label value
                labelText.selectAll('tspan').remove();

                // parse the lines in this label
                let lines = [];
                if (d.component.label) {
                    lines = d.component.label.split('\n');
                } else {
                    lines.push('');
                }

                let color = this.defaultColor();

                // use the specified color if appropriate
                if (d.component.style['background-color']) {
                    color = d.component.style['background-color'];
                }

                // add label value
                const textWidth = d.dimensions.width - 15;
                const textHeight = d.dimensions.height;
                this.canvasUtils.boundedMultilineEllipsis(
                    labelText,
                    textWidth,
                    textHeight,
                    lines,
                    `label-text.${d.id}.width.${textWidth}`
                );
                labelText.selectAll('tspan').style('fill', () => {
                    return this.canvasUtils.determineContrastColor(this.nifiCommon.substringAfterLast(color, '#'));
                });

                // -----------
                // labelpoints
                // -----------

                if (d.permissions.canWrite) {
                    const pointData = [{ x: d.dimensions.width, y: d.dimensions.height }];
                    const points = labelPoint.data(pointData);

                    // create a point for the end
                    const pointsEntered: any = points
                        .enter()
                        .append('path')
                        .attr('class', 'labelpoint resizable-triangle')
                        .attr('d', 'm0,0 l0,8 l-8,0 z')
                        .call(this.labelPointDrag);

                    // update the midpoints
                    points.merge(pointsEntered).attr('transform', (p: any) => {
                        return 'translate(' + (p.x - 2) + ', ' + (p.y - 10) + ')';
                    });

                    // remove old items
                    points.exit().remove();
                }
            } else {
                // remove the previous label value
                labelText.selectAll('tspan').remove();

                // remove the label points
                labelPoint.remove();
            }
        });
    }

    private defaultColor(): string {
        return '#fff7d7';
    }

    private removeLabels(removed: any) {
        removed.remove();
    }

    public init(): void {
        this.labelContainer = d3.select('#canvas').append('g').attr('pointer-events', 'all').attr('class', 'labels');

        this.store
            .select(selectLabels)
            .pipe(
                filter(() => this.labelContainer !== null),
                takeUntil(this.destroyed$)
            )
            .subscribe((labels) => {
                this.set(labels);
            });

        this.store
            .select(selectFlowLoadingStatus)
            .pipe(
                filter((status) => status === 'success'),
                filter(() => this.labelContainer !== null),
                switchMap(() => this.store.select(selectAnySelectedComponentIds)),
                takeUntil(this.destroyed$)
            )
            .subscribe((selected) => {
                this.labelContainer.selectAll('g.label').classed('selected', function (d: any) {
                    return selected.includes(d.id);
                });
            });

        this.store
            .select(selectTransitionRequired)
            .pipe(takeUntil(this.destroyed$))
            .subscribe((transitionRequired) => {
                this.transitionRequired = transitionRequired;
            });
    }

    public destroy(): void {
        this.labelContainer = null;
        this.destroyed$.next(true);
    }

    ngOnDestroy(): void {
        this.destroyed$.complete();
    }

    private set(labels: any): void {
        // update the labels
        this.labels = labels.map((label: any) => {
            const currentLabel: any = this.labels.find((l: any) => l.id === label.id);

            // only consider newer when the version is greater which indicates the component configuration has changed.
            // when this happens we should override the current dragging action so that the new changes can be realized.
            const isNewerRevision = label.revision.version > currentLabel?.revision.version;

            let dragging = false;
            if (currentLabel?.dragging && !isNewerRevision) {
                dragging = true;
            }

            return {
                ...label,
                type: ComponentType.Label,
                dragging,
                dimensions: {
                    ...(dragging ? currentLabel.dimensions : label.dimensions)
                }
            };
        });

        // select
        const selection = this.select();

        // enter
        const entered = this.renderLabels(selection.enter());

        // update
        const updated = selection.merge(entered);
        this.updateLabels(updated);
        this.sort(updated);

        // position
        this.positionBehavior.position(updated, this.transitionRequired);

        // exit
        this.removeLabels(selection.exit());
    }

    public selectAll(): any {
        return this.labelContainer.selectAll('g.label');
    }

    public render(): void {
        this.updateLabels(this.selectAll());
    }

    public pan(): void {
        this.updateLabels(this.labelContainer.selectAll('g.label.entering, g.label.leaving'));
    }
}
