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

import { Injectable } from '@angular/core';
import * as d3 from 'd3';
import * as WebFont from 'webfontloader';
import { select, Store } from '@ngrx/store';
import { CanvasState } from '../state';
import { setTransform } from '../state/transform/transform.actions';
import { INITIAL_SCALE, INITIAL_TRANSLATE } from '../state/transform/transform.reducer';
import { ProcessGroupManager } from './manager/process-group-manager.service';
import { FunnelManager } from './manager/funnel-manager.service';
import { selectRenderRequired } from '../state/flow/flow.selectors';

@Injectable({
    providedIn: 'root'
})
export class CanvasView {
    private static readonly INCREMENT: number = 1.2;
    private static readonly MAX_SCALE: number = 8;
    private static readonly MIN_SCALE: number = 0.2;
    private static readonly MIN_SCALE_TO_RENDER: number = 0.6;

    private svg: any;
    private canvas: any;

    private k: number = INITIAL_SCALE;
    private x: number = INITIAL_TRANSLATE.x;
    private y: number = INITIAL_TRANSLATE.y;

    private behavior: any;

    constructor(
        private store: Store<CanvasState>,
        private processGroupManager: ProcessGroupManager,
        private funnelManager: FunnelManager
    ) {
        this.store.pipe(select(selectRenderRequired)).subscribe((renderRequired) => {
            if (renderRequired) {
                this.updateCanvasVisibility();
            }
        });
    }

    public init(svg: any, canvas: any): void {
        WebFont.load({
            custom: {
                families: ['Roboto', 'Roboto Slab', 'flowfont', 'FontAwesome']
            },
            active: function () {
                // re-render once the fonts have loaded, without the fonts
                // positions of elements on the canvas may be incorrect
                self.processGroupManager.render();
                self.funnelManager.render();
            }
        });

        this.svg = svg;
        this.canvas = canvas;

        this.processGroupManager.init();
        this.funnelManager.init();

        const self: CanvasView = this;
        let refreshed: Promise<void> | null;
        let panning: boolean = false;

        // define the behavior
        this.behavior = d3
            .zoom()
            .scaleExtent([CanvasView.MIN_SCALE, CanvasView.MAX_SCALE])
            .on('start', function () {
                // hide the context menu
                // nfContextMenu.hide();
            })
            .on('zoom', function (event) {
                // update the local translation and scale
                if (!isNaN(event.transform.x)) {
                    self.x = event.transform.x;
                }
                if (!isNaN(event.transform.y)) {
                    self.y = event.transform.y;
                }
                if (!isNaN(event.transform.k)) {
                    self.k = event.transform.k;
                }

                // indicate that we are panning to prevent deselection in zoom.end below
                panning = true;

                // refresh the canvas
                refreshed = self.refresh({
                    persist: false,
                    transition: self.shouldTransition(event.sourceEvent),
                    refreshComponents: false,
                    refreshBirdseye: false
                });
            })
            .on('end', function (event) {
                if (!self.isBirdseyeEvent(event.sourceEvent)) {
                    // ensure the canvas was actually refreshed
                    if (refreshed) {
                        self.updateCanvasVisibility();

                        // TODO
                        // refresh the birdseye
                        refreshed.then(function () {
                            //   nfBirdseye.refresh();
                        });

                        // TODO
                        // persist the users view
                        // nfCanvasUtils.persistUserView();

                        // reset the refreshed deferred
                        refreshed = null;
                    }

                    if (!panning) {
                        // TODO - get selection util
                        // deselect as necessary if we are not panning
                        d3.selectAll('g.component.selected, g.connection.selected').classed('selected', false);

                        // TODO
                        // update URL deep linking params
                        // nfCanvasUtils.setURLParameters();

                        // TODO
                        // inform Angular app values have changed
                        // nfNgBridge.digest();
                    }
                }

                // reset the panning flag
                panning = false;

                // dispatch the current transform
                self.store.dispatch(
                    setTransform({
                        transform: {
                            translate: {
                                x: self.x,
                                y: self.y
                            },
                            scale: self.k
                        }
                    })
                );
            });

        // add the behavior to the canvas and disable dbl click zoom
        this.svg.call(this.behavior).on('dblclick.zoom', null);
    }

    // filters zoom events as programmatically modifying the translate or scale now triggers the handlers
    private isBirdseyeEvent(sourceEvent: any): boolean {
        if (sourceEvent?.subject) {
            return sourceEvent.subject.source === 'birdseye';
        } else {
            return false;
        }
    }

    // see if the scale has changed during this zoom event,
    // we want to only transition when zooming in/out as running
    // the transitions during pan events is undesirable
    private shouldTransition(sourceEvent: any): boolean {
        if (sourceEvent) {
            if (this.isBirdseyeEvent(sourceEvent)) {
                return false;
            }

            return sourceEvent.type === 'wheel' || sourceEvent.type === 'mousewheel';
        } else {
            return true;
        }
    }

    private updateCanvasVisibility(): void {
        const self: CanvasView = this;
        const canvasContainer: any = document.getElementById('canvas-container');
        let translate = [this.x, this.y];
        const scale = this.k;

        // scale the translation
        translate = [translate[0] / scale, translate[1] / scale];

        // get the normalized screen width and height
        const screenWidth = canvasContainer.offsetWidth / scale;
        const screenHeight = canvasContainer.offsetHeight / scale;

        // calculate the screen bounds one screens worth in each direction
        const screenLeft = -translate[0] - screenWidth;
        const screenTop = -translate[1] - screenHeight;
        const screenRight = screenLeft + screenWidth * 3;
        const screenBottom = screenTop + screenHeight * 3;

        // detects whether a component is visible and should be rendered
        const isComponentVisible = function (d: any) {
            if (!self.shouldRenderPerScale()) {
                return false;
            }

            var left = d.position.x;
            var top = d.position.y;
            var right = left + d.dimensions.width;
            var bottom = top + d.dimensions.height;

            // determine if the component is now visible
            return screenLeft < right && screenRight > left && screenTop < bottom && screenBottom > top;
        };

        // detects whether a connection is visible and should be rendered
        const isConnectionVisible = function (d: any) {
            if (!self.shouldRenderPerScale()) {
                return false;
            }

            var x, y;
            if (d.bends.length > 0) {
                var i = Math.min(Math.max(0, d.labelIndex), d.bends.length - 1);
                x = d.bends[i].x;
                y = d.bends[i].y;
            } else {
                x = (d.start.x + d.end.x) / 2;
                y = (d.start.y + d.end.y) / 2;
            }

            return screenLeft < x && screenRight > x && screenTop < y && screenBottom > y;
        };

        // marks the specific component as visible and determines if its entering or leaving visibility
        const updateVisibility = function (selection: any, d: any, isVisible: Function) {
            var visible = isVisible(d);
            var wasVisible = selection.classed('visible');

            // mark the selection as appropriate
            selection
                .classed('visible', visible)
                .classed('entering', function () {
                    return visible && !wasVisible;
                })
                .classed('leaving', function () {
                    return !visible && wasVisible;
                });
        };

        // update the visibility
        this.processGroupManager.selectAll().each(function (this: any, d: any) {
            updateVisibility(d3.select(this), d, isComponentVisible);
        });
        this.funnelManager.selectAll().each(function (this: any, d: any) {
            updateVisibility(d3.select(this), d, isComponentVisible);
        });

        // trigger pan
        this.processGroupManager.pan();
        this.funnelManager.pan();
    }

    /**
     * Whether or not a component should be rendered based solely on the current scale.
     *
     * @returns {Boolean}
     */
    private shouldRenderPerScale(): boolean {
        return this.k >= CanvasView.MIN_SCALE_TO_RENDER;
    }

    /**
     * Translates by the specified translation.
     *
     * @param translate
     */
    public translate(translate: any): void {
        this.behavior.translateBy(this.svg, translate[0], translate[1]);
    }

    /**
     * Scales by the specified scale.
     *
     * @param {number} scale        The factor to scale by
     */
    public scale(scale: any): void {
        this.behavior.scaleBy(this.svg, scale);
    }

    /**
     * Sets the current transform.
     *
     * @param translate
     * @param scale
     */
    public transform(translate: any, scale: any): void {
        this.behavior.transform(this.svg, d3.zoomIdentity.translate(translate[0], translate[1]).scale(scale));
    }

    /**
     * Zooms in a single zoom increment.
     */
    public zoomIn(): void {
        this.scale(CanvasView.INCREMENT);
    }

    /**
     * Zooms out a single zoom increment.
     */
    public zoomOut(): void {
        this.scale(1 / CanvasView.INCREMENT);
    }

    /**
     * Refreshes the view based on the configured translation and scale.
     *
     * @param {object} options Options for the refresh operation
     */
    private async refresh({
        persist = true,
        transition = false,
        refreshComponents = true,
        refreshBirdseye = true
    }: {
        persist?: boolean;
        transition?: boolean;
        refreshComponents?: boolean;
        refreshBirdseye?: boolean;
    } = {}): Promise<void> {
        const self: CanvasView = this;

        await new Promise<void>(function (resolve) {
            // TODO
            // update component visibility
            if (refreshComponents) {
                // nfGraph.updateVisibility();
            }

            // TODO
            // persist if appropriate
            if (persist) {
                // nfCanvasUtils.persistUserView();
            }

            const t = [self.x, self.y];
            const s = self.k;

            // update the canvas
            if (transition) {
                self.canvas
                    .transition()
                    .duration(500)
                    .attr('transform', function () {
                        return 'translate(' + t + ') scale(' + s + ')';
                    })
                    .on('end', function () {
                        // TODO
                        // refresh birdseye if appropriate
                        if (refreshBirdseye) {
                            //   nfBirdseye.refresh();
                        }

                        resolve();
                    });
            } else {
                self.canvas.attr('transform', function () {
                    return 'translate(' + t + ') scale(' + s + ')';
                });

                // TODO
                // refresh birdseye if appropriate
                if (refreshBirdseye) {
                    //   nfBirdseye.refresh();
                }

                resolve();
            }
        });
    }
}
