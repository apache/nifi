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

import { DestroyRef, inject, Injectable } from '@angular/core';
import * as d3 from 'd3';
import { INITIAL_SCALE, INITIAL_TRANSLATE } from '../state/transform/transform.reducer';
import { Store } from '@ngrx/store';
import { CanvasState } from '../state';
import { selectTransform } from '../state/transform/transform.selectors';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { CanvasView } from './canvas-view.service';
import { ProcessorManager } from './manager/processor-manager.service';
import { ProcessGroupManager } from './manager/process-group-manager.service';
import { RemoteProcessGroupManager } from './manager/remote-process-group-manager.service';
import { PortManager } from './manager/port-manager.service';
import { FunnelManager } from './manager/funnel-manager.service';
import { LabelManager } from './manager/label-manager.service';
import { selectNavigationCollapsed } from '../state/flow/flow.selectors';
import { initialState } from '../state/flow/flow.reducer';
import { CanvasUtils } from './canvas-utils.service';
import { NiFiCommon } from '@nifi/shared';
import { ComponentEntityWithDimensions } from '../state/flow';

@Injectable({
    providedIn: 'root'
})
export class BirdseyeView {
    private destroyRef = inject(DestroyRef);

    private birdseyeGroup: any = null;
    private componentGroup: any = null;
    private navigationCollapsed: boolean = initialState.navigationCollapsed;

    private k: number = INITIAL_SCALE;
    private x: number = INITIAL_TRANSLATE.x;
    private y: number = INITIAL_TRANSLATE.y;

    private initialized = false;

    constructor(
        private store: Store<CanvasState>,
        private canvasView: CanvasView,
        private nifiCommon: NiFiCommon,
        private canvasUtils: CanvasUtils,
        private processorManager: ProcessorManager,
        private processGroupManager: ProcessGroupManager,
        private remoteProcessGroupManager: RemoteProcessGroupManager,
        private portManager: PortManager,
        private funnelManager: FunnelManager,
        private labelManager: LabelManager
    ) {
        this.store
            .select(selectTransform)
            .pipe(takeUntilDestroyed(this.destroyRef))
            .subscribe((transform) => {
                this.k = transform.scale;
                this.x = transform.translate.x;
                this.y = transform.translate.y;
            });

        this.store
            .select(selectNavigationCollapsed)
            .pipe(takeUntilDestroyed(this.destroyRef))
            .subscribe((navigationCollapsed) => {
                this.navigationCollapsed = navigationCollapsed;
            });
    }

    public init(birdseye: any) {
        const self: BirdseyeView = this;
        const birdseyeBoundingBox: any = birdseye.getBoundingClientRect();

        d3.select(birdseye)
            .append('canvas')
            .attr('id', 'birdseye-canvas')
            .attr('width', birdseyeBoundingBox.width - 2)
            .attr('height', birdseyeBoundingBox.height - 2);

        // build the birdseye svg
        const svg = d3
            .select(birdseye)
            .append('svg')
            .attr('class', 'birdseye-svg')
            .attr('width', birdseyeBoundingBox.width - 2)
            .attr('height', birdseyeBoundingBox.height - 2);

        // group birdseye components together
        this.birdseyeGroup = svg.append('g').attr('class', 'birdseye');

        // processor in the birdseye
        this.componentGroup = this.birdseyeGroup.append('g').attr('pointer-events', 'none');

        // define the brush drag behavior
        const brush = d3
            .drag()
            .subject(function (d) {
                return {
                    x: d.x,
                    y: d.y,
                    source: 'birdseye'
                };
            })
            .on('start', function () {
                self.canvasView.birdseyeDragStart();
            })
            .on('drag', function (this: any, event, d: any) {
                d.x += event.dx;
                d.y += event.dy;

                // update the location of the brush
                d3.select(this).attr('transform', function () {
                    return 'translate(' + d.x + ', ' + d.y + ')';
                });

                // transform the canvas
                self.canvasView.translate([-event.dx, -event.dy]);
            })
            .on('end', function () {
                self.canvasView.birdseyeDragEnd();

                // refresh the birdseye
                self.refresh();
            });

        // context area
        this.birdseyeGroup
            .append('g')
            .attr('pointer-events', 'all')
            .attr('class', 'birdseye-brush-container')
            .append('rect')
            .attr('class', 'birdseye-brush pointer')
            .datum({
                x: 0,
                y: 0
            })
            .call(brush);

        this.initialized = true;
    }

    public refresh(): void {
        // do not refresh if the component is not initialized or navigation is collapsed
        if (!this.initialized || this.navigationCollapsed) {
            return;
        }

        // scale the translation
        const translate: [number, number] = [this.x / this.k, this.y / this.k];

        const canvasContainer: any = document.getElementById('canvas-container');
        const canvasBoundingBox: any = canvasContainer.getBoundingClientRect();
        const canvas: any = d3.select('#canvas').node();

        // get the bounding box for the graph and convert into canvas space
        const graphBox = canvas.getBoundingClientRect();
        const graphLeft: number = graphBox.left / this.k - translate[0];
        const graphTop: number = (graphBox.top - canvasBoundingBox.top) / this.k - translate[1];
        const graphRight: number = graphBox.right / this.k - translate[0];
        const graphBottom: number = graphBox.bottom / this.k - translate[1];

        // get the screen bounding box and convert into canvas space
        const screenWidth: number = canvasBoundingBox.width / this.k;
        const screenHeight: number = canvasBoundingBox.height / this.k;
        const screenLeft: number = -translate[0];
        const screenTop: number = -translate[1];
        const screenRight: number = screenLeft + screenWidth;
        const screenBottom: number = screenTop + screenHeight;

        // determine the maximum size of the canvas given the graph and the visible portion on the screen
        const canvasLeft: number = Math.min(graphLeft, screenLeft);
        const canvasTop: number = Math.min(graphTop, screenTop);
        const canvasRight: number = Math.max(graphRight, screenRight);
        const canvasBottom: number = Math.max(graphBottom, screenBottom);
        const canvasWidth: number = canvasRight - canvasLeft;
        const canvasHeight: number = canvasBottom - canvasTop;

        // get the width/height we have to work with
        const birdseye: any = document.getElementById('birdseye');
        const birdseyeBox: any = birdseye.getBoundingClientRect();
        const birdseyeWidth: number = birdseyeBox.width;
        const birdseyeHeight: number = birdseyeBox.height;

        // determine the appropriate scale for the birdseye (min scale to accommodate total width and height)
        const birdseyeWidthScale: number = birdseyeWidth / canvasWidth;
        const birdseyeHeightScale: number = birdseyeHeight / canvasHeight;
        const birdseyeScale: number = Math.min(birdseyeWidthScale, birdseyeHeightScale);

        // calculate the translation for the birdseye and the brush
        let birdseyeTranslate: [number, number] = [0, 0];
        let brushTranslate: [number, number] = [0, 0];
        if (translate[0] < 0 && translate[1] < 0) {
            birdseyeTranslate = [0, 0];
            brushTranslate = [-translate[0], -translate[1]];
        } else if (translate[0] >= 0 && translate[1] < 0) {
            birdseyeTranslate = [translate[0], 0];
            brushTranslate = [0, -translate[1]];
        } else if (translate[0] < 0 && translate[1] >= 0) {
            birdseyeTranslate = [0, translate[1]];
            brushTranslate = [-translate[0], 0];
        } else {
            birdseyeTranslate = [translate[0], translate[1]];
            brushTranslate = [0, 0];
        }

        // offset in case the graph has positive/negative coordinates and panning appropriately
        let offsetX = 0;
        const left: number = -graphLeft;
        if (translate[0] < 0) {
            if (translate[0] < left) {
                offsetX = left;
            } else {
                offsetX = left - (left - translate[0]);
            }
        } else {
            if (translate[0] < left) {
                offsetX = left - translate[0];
            }
        }
        let offsetY = 0;
        const top: number = -graphTop;
        if (translate[1] < 0) {
            if (translate[1] < top) {
                offsetY = top;
            } else {
                offsetY = top - (top - translate[1]);
            }
        } else {
            if (translate[1] < top) {
                offsetY = top - translate[1];
            }
        }

        // adjust the translations of the birdseye and brush to account for the offset
        birdseyeTranslate = [birdseyeTranslate[0] + offsetX, birdseyeTranslate[1] + offsetY];
        brushTranslate = [brushTranslate[0] + offsetX, brushTranslate[1] + offsetY];

        // update the birdseye
        this.birdseyeGroup.attr('transform', 'scale(' + birdseyeScale + ')');
        this.componentGroup.attr('transform', 'translate(' + birdseyeTranslate + ')');

        // update the brush
        d3.select('rect.birdseye-brush')
            .attr('width', screenWidth)
            .attr('height', screenHeight)
            .attr('stroke-width', 2 / birdseyeScale)
            .attr('transform', function (d: any) {
                d.x = brushTranslate[0];
                d.y = brushTranslate[1];

                return 'translate(' + brushTranslate + ')';
            });

        // redraw the canvas
        const canvasElement: any = d3.select('#birdseye-canvas').node();
        const context = canvasElement.getContext('2d');
        context.save();

        // clear the current canvas
        context.setTransform(1, 0, 0, 1, 0, 0);
        context.clearRect(0, 0, canvasElement.width, canvasElement.height);

        context.restore();
        context.save();

        // apply the current transformation
        context.translate(birdseyeTranslate[0] * birdseyeScale, birdseyeTranslate[1] * birdseyeScale);
        context.scale(birdseyeScale, birdseyeScale);

        // labels
        this.labelManager.selectAll().each((d: ComponentEntityWithDimensions) => {
            // default color
            let color = '#fff7d7';

            if (d.permissions.canRead) {
                // use the specified color if appropriate
                if (d.component.style['background-color']) {
                    color = d.component.style['background-color'];
                }
            }

            // determine border color
            const strokeColor: string = this.canvasUtils.determineContrastColor(
                this.nifiCommon.substringAfterLast(color, '#')
            );

            context.fillStyle = color;
            context.fillRect(d.position.x, d.position.y, d.dimensions.width, d.dimensions.height);
            context.strokeStyle = strokeColor;
            context.strokeRect(d.position.x, d.position.y, d.dimensions.width, d.dimensions.height);
        });

        // funnels
        context.fillStyle = '#ad9897';
        this.funnelManager.selectAll().each(function (d: any) {
            context.fillRect(d.position.x, d.position.y, d.dimensions.width, d.dimensions.height);
        });

        // ports
        context.fillStyle = '#bbdcde';
        this.portManager.selectAll().each(function (d: any) {
            context.fillRect(d.position.x, d.position.y, d.dimensions.width, d.dimensions.height);
        });

        // remote process groups
        context.fillStyle = '#728e9b';
        this.remoteProcessGroupManager.selectAll().each(function (d: any) {
            context.fillRect(d.position.x, d.position.y, d.dimensions.width, d.dimensions.height);
        });

        // process groups
        this.processGroupManager.selectAll().each(function (d: any) {
            context.fillRect(d.position.x, d.position.y, d.dimensions.width, d.dimensions.height);
        });

        // processors
        this.processorManager.selectAll().each((d: ComponentEntityWithDimensions) => {
            // default color
            let color = '#dde4eb';

            if (d.permissions.canRead) {
                // use the specified color if appropriate
                if (d.component.style['background-color']) {
                    color = d.component.style['background-color'];
                }
            }

            // determine border color
            const strokeColor: string = this.canvasUtils.determineContrastColor(
                this.nifiCommon.substringAfterLast(color, '#')
            );

            context.fillStyle = color;
            context.fillRect(d.position.x, d.position.y, d.dimensions.width, d.dimensions.height);
            context.strokeStyle = strokeColor;
            context.strokeRect(d.position.x, d.position.y, d.dimensions.width, d.dimensions.height);
        });

        context.restore();
    }

    public destroy(): void {
        this.initialized = false;

        this.navigationCollapsed = initialState.navigationCollapsed;

        this.k = INITIAL_SCALE;
        this.x = INITIAL_TRANSLATE.x;
        this.y = INITIAL_TRANSLATE.y;

        this.birdseyeGroup = null;
        this.componentGroup = null;
    }
}
