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

import { Component, OnInit } from '@angular/core';
import { CanvasState } from '../../state';
import { Store } from '@ngrx/store';
import { enterProcessGroup } from '../../state/flow/flow.actions';
import * as d3 from 'd3';
import { CanvasView } from '../../service/canvas-view.service';
import { INITIAL_SCALE, INITIAL_TRANSLATE } from '../../state/transform/transform.reducer';

@Component({
    selector: 'fd-canvas',
    templateUrl: './canvas.component.html',
    styleUrls: ['./canvas.component.scss']
})
export class CanvasComponent implements OnInit {
    private svg: any;
    private canvas: any;

    constructor(
        private store: Store<CanvasState>,
        private canvasView: CanvasView
    ) {}

    ngOnInit(): void {
        // initialize the canvas svg
        this.createSvg();
        this.canvasView.init(this.svg, this.canvas);

        // enter the root process group
        this.store.dispatch(
            enterProcessGroup({
                request: {
                    id: 'root',
                    selection: []
                }
            })
        );
    }

    private createSvg(): void {
        this.svg = d3.select('#canvas-container').append('svg').attr('class', 'canvas-svg');

        this.createDefs();

        const t = [INITIAL_TRANSLATE.x, INITIAL_TRANSLATE.y];

        // create the canvas element
        this.canvas = this.svg
            .append('g')
            .attr('transform', 'translate(' + t + ') scale(' + INITIAL_SCALE + ')')
            .attr('pointer-events', 'all')
            .attr('id', 'canvas');
    }

    private createDefs(): void {
        // create the definitions element
        const defs = this.svg.append('defs');

        // create arrow definitions for the various line types
        defs.selectAll('marker')
            .data(['normal', 'ghost', 'unauthorized', 'full'])
            .enter()
            .append('marker')
            .attr('id', function (d: string) {
                return d;
            })
            .attr('viewBox', '0 0 6 6')
            .attr('refX', 5)
            .attr('refY', 3)
            .attr('markerWidth', 6)
            .attr('markerHeight', 6)
            .attr('orient', 'auto')
            .attr('fill', function (d: string) {
                if (d === 'ghost') {
                    return '#aaaaaa';
                } else if (d === 'unauthorized') {
                    return '#ba554a';
                } else if (d === 'full') {
                    return '#ba554a';
                } else {
                    return '#000000';
                }
            })
            .append('path')
            .attr('d', 'M2,3 L0,6 L6,3 L0,0 z');

        // filter for drop shadow
        const componentDropShadowFilter = defs
            .append('filter')
            .attr('id', 'component-drop-shadow')
            .attr('height', '140%')
            .attr('y', '-20%');

        // blur
        componentDropShadowFilter
            .append('feGaussianBlur')
            .attr('in', 'SourceAlpha')
            .attr('stdDeviation', 3)
            .attr('result', 'blur');

        // offset
        componentDropShadowFilter
            .append('feOffset')
            .attr('in', 'blur')
            .attr('dx', 0)
            .attr('dy', 1)
            .attr('result', 'offsetBlur');

        // color/opacity
        componentDropShadowFilter
            .append('feFlood')
            .attr('flood-color', '#000000')
            .attr('flood-opacity', 0.4)
            .attr('result', 'offsetColor');

        // combine
        componentDropShadowFilter
            .append('feComposite')
            .attr('in', 'offsetColor')
            .attr('in2', 'offsetBlur')
            .attr('operator', 'in')
            .attr('result', 'offsetColorBlur');

        // stack the effect under the source graph
        const componentDropShadowFeMerge = componentDropShadowFilter.append('feMerge');
        componentDropShadowFeMerge.append('feMergeNode').attr('in', 'offsetColorBlur');
        componentDropShadowFeMerge.append('feMergeNode').attr('in', 'SourceGraphic');

        // filter for drop shadow
        const connectionFullDropShadowFilter = defs
            .append('filter')
            .attr('id', 'connection-full-drop-shadow')
            .attr('height', '140%')
            .attr('y', '-20%');

        // blur
        connectionFullDropShadowFilter
            .append('feGaussianBlur')
            .attr('in', 'SourceAlpha')
            .attr('stdDeviation', 3)
            .attr('result', 'blur');

        // offset
        connectionFullDropShadowFilter
            .append('feOffset')
            .attr('in', 'blur')
            .attr('dx', 0)
            .attr('dy', 1)
            .attr('result', 'offsetBlur');

        // color/opacity
        connectionFullDropShadowFilter
            .append('feFlood')
            .attr('flood-color', '#ba554a')
            .attr('flood-opacity', 1)
            .attr('result', 'offsetColor');

        // combine
        connectionFullDropShadowFilter
            .append('feComposite')
            .attr('in', 'offsetColor')
            .attr('in2', 'offsetBlur')
            .attr('operator', 'in')
            .attr('result', 'offsetColorBlur');

        // stack the effect under the source graph
        const connectionFullFeMerge = connectionFullDropShadowFilter.append('feMerge');
        connectionFullFeMerge.append('feMergeNode').attr('in', 'offsetColorBlur');
        connectionFullFeMerge.append('feMergeNode').attr('in', 'SourceGraphic');
    }
}
