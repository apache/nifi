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
import { Store } from '@ngrx/store';
import { CanvasState } from '../state';
import { refreshBirdseyeView, transformComplete } from '../state/transform/transform.actions';
import { INITIAL_SCALE, INITIAL_TRANSLATE } from '../state/transform/transform.reducer';
import { ProcessGroupManager } from './manager/process-group-manager.service';
import { FunnelManager } from './manager/funnel-manager.service';
import { LabelManager } from './manager/label-manager.service';
import { ProcessorManager } from './manager/processor-manager.service';
import { PortManager } from './manager/port-manager.service';
import { RemoteProcessGroupManager } from './manager/remote-process-group-manager.service';
import { ConnectionManager } from './manager/connection-manager.service';
import { deselectAllComponents } from '../state/flow/flow.actions';
import { CanvasUtils } from './canvas-utils.service';
import { Position } from '../state/shared';

@Injectable({
    providedIn: 'root'
})
export class CanvasView {
    private static readonly INCREMENT: number = 1.2;
    private static readonly MAX_SCALE: number = 8;
    private static readonly MIN_SCALE: number = 0.2;
    private static readonly MIN_SCALE_TO_RENDER: number = 0.4;

    private svg: any;
    private canvas: any;

    private k: number = INITIAL_SCALE;
    private x: number = INITIAL_TRANSLATE.x;
    private y: number = INITIAL_TRANSLATE.y;

    private behavior: any;

    private birdseyeTranslateInProgress = false;
    private allowTransition = false;

    private canvasInitialized: boolean = false;

    constructor(
        private store: Store<CanvasState>,
        private canvasUtils: CanvasUtils,
        private processorManager: ProcessorManager,
        private processGroupManager: ProcessGroupManager,
        private remoteProcessGroupManager: RemoteProcessGroupManager,
        private portManager: PortManager,
        private funnelManager: FunnelManager,
        private labelManager: LabelManager,
        private connectionManager: ConnectionManager
    ) {
        const self: CanvasView = this;
        let refreshed: Promise<void> | null;
        let panning = false;

        // define the behavior
        this.behavior = d3
            .zoom()
            .scaleExtent([CanvasView.MIN_SCALE, CanvasView.MAX_SCALE])
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
                    transition: self.shouldTransition(),
                    refreshComponents: false,
                    refreshBirdseye: false
                });
            })
            .on('end', function () {
                if (!self.isBirdseyeEvent()) {
                    // ensure the canvas was actually refreshed
                    if (refreshed) {
                        // dispatch the current transform
                        self.store.dispatch(
                            transformComplete({
                                transform: {
                                    translate: {
                                        x: self.x,
                                        y: self.y
                                    },
                                    scale: self.k
                                }
                            })
                        );

                        self.updateCanvasVisibility();

                        // refresh the birdseye
                        refreshed.then(function () {
                            self.store.dispatch(refreshBirdseyeView());
                        });

                        // reset the refreshed deferred
                        refreshed = null;
                    }

                    if (!panning) {
                        // deselect as necessary if we are not panning
                        self.store.dispatch(deselectAllComponents());
                    }
                }

                // reset the panning flag
                panning = false;
            });
    }

    public init(svg: any, canvas: any): void {
        const self: CanvasView = this;

        WebFont.load({
            custom: {
                families: ['Inter', 'flowfont', 'FontAwesome']
            },
            active: function () {
                // re-render once the fonts have loaded, without the fonts
                // positions of elements on the canvas may be incorrect
                self.processorManager.render();
                self.processGroupManager.render();
                self.remoteProcessGroupManager.render();
                self.portManager.render();
                self.labelManager.render();
                self.funnelManager.render();
                self.connectionManager.render();
            }
        });

        this.svg = svg;
        this.canvas = canvas;

        this.k = INITIAL_SCALE;
        this.x = INITIAL_TRANSLATE.x;
        this.y = INITIAL_TRANSLATE.y;

        this.labelManager.init();
        this.funnelManager.init();
        this.portManager.init();
        this.remoteProcessGroupManager.init();
        this.processGroupManager.init();
        this.processorManager.init();
        this.connectionManager.init();

        // add the behavior to the canvas and disable dbl click zoom
        this.svg.call(this.behavior).on('dblclick.zoom', null);

        this.canvasInitialized = true;
    }

    public getCanvasBoundingClientRect(): DOMRect | null {
        const canvasContainer: any = document.getElementById('canvas-container');
        if (canvasContainer == null) {
            return null;
        }

        return canvasContainer.getBoundingClientRect() as DOMRect;
    }

    // filters zoom events as programmatically modifying the translate or scale now triggers the handlers
    private isBirdseyeEvent(): boolean {
        return this.birdseyeTranslateInProgress;
    }

    private shouldTransition(): boolean {
        if (this.birdseyeTranslateInProgress) {
            return false;
        }

        return this.allowTransition;
    }

    public isSelectedComponentOnScreen(): boolean {
        const canvasContainer: any = document.getElementById('canvas-container');
        if (canvasContainer == null) {
            return false;
        }

        const selection: any = this.canvasUtils.getSelection();
        if (selection.size() !== 1) {
            return false;
        }
        const d = selection.datum();

        let translate = [this.x, this.y];
        const scale = this.k;

        // scale the translation
        translate = [translate[0] / scale, translate[1] / scale];

        // get the normalized screen width and height
        const screenWidth = canvasContainer.offsetWidth / scale;
        const screenHeight = canvasContainer.offsetHeight / scale;

        // calculate the screen bounds one screens worth in each direction
        const screenLeft = -translate[0];
        const screenTop = -translate[1];
        const screenRight = screenLeft + screenWidth;
        const screenBottom = screenTop + screenHeight;

        if (this.canvasUtils.isConnection(selection)) {
            let connectionX, connectionY;
            if (d.bends.length > 0) {
                const i: number = Math.min(Math.max(0, d.labelIndex), d.bends.length - 1);
                connectionX = d.bends[i].x;
                connectionY = d.bends[i].y;
            } else {
                connectionX = (d.start.x + d.end.x) / 2;
                connectionY = (d.start.y + d.end.y) / 2;
            }

            return (
                screenLeft < connectionX &&
                screenRight > connectionX &&
                screenTop < connectionY &&
                screenBottom > connectionY
            );
        } else {
            const componentLeft: number = d.position.x;
            const componentTop: number = d.position.y;
            const componentRight: number = componentLeft + d.dimensions.width;
            const componentBottom: number = componentTop + d.dimensions.height;

            // determine if the component is now visible
            return (
                screenLeft < componentRight &&
                screenRight > componentLeft &&
                screenTop < componentBottom &&
                screenBottom > componentTop
            );
        }
    }

    /**
     * Determines if a bounding box is in the current viewable canvas area.
     *
     * @param {type} boundingBox       Bounding box to check.
     * @param {boolean} strict         If true, the entire bounding box must be in the viewport.
     *                                 If false, only part of the bounding box must be in the viewport.
     * @returns {boolean}
     */
    public isBoundingBoxInViewport(boundingBox: any, strict: boolean): boolean {
        const canvasContainer: any = document.getElementById('canvas-container');
        if (!canvasContainer) {
            return false;
        }

        // scale the translation
        const translate = [this.x / this.k, this.y / this.k];

        // get the normalized screen width and height
        const screenWidth = canvasContainer.offsetWidth / this.k;
        const screenHeight = canvasContainer.offsetHeight / this.k;

        // calculate the screen bounds one screens worth in each direction
        const screenLeft = -translate[0];
        const screenTop = -translate[1];
        const screenRight = screenLeft + screenWidth;
        const screenBottom = screenTop + screenHeight;

        const left = Math.ceil(boundingBox.x);
        const right = Math.floor(boundingBox.x + boundingBox.width);
        const top = Math.ceil(boundingBox.y);
        const bottom = Math.floor(boundingBox.y + boundingBox.height);

        if (strict) {
            return !(left < screenLeft || right > screenRight || top < screenTop || bottom > screenBottom);
        } else {
            return (
                ((left > screenLeft && left < screenRight) || (right < screenRight && right > screenLeft)) &&
                ((top > screenTop && top < screenBottom) || (bottom < screenBottom && bottom > screenTop))
            );
        }
    }

    public updateCanvasVisibility(): void {
        const self: CanvasView = this;
        const canvasContainer: any = document.getElementById('canvas-container');

        if (canvasContainer == null) {
            return;
        }

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

            const left: number = d.position.x;
            const top: number = d.position.y;
            const right: number = left + d.dimensions.width;
            const bottom: number = top + d.dimensions.height;

            // determine if the component is now visible
            return screenLeft < right && screenRight > left && screenTop < bottom && screenBottom > top;
        };

        // detects whether a connection is visible and should be rendered
        const isConnectionVisible = function (d: any) {
            if (!self.shouldRenderPerScale()) {
                return false;
            }

            const { x, y } = self.canvasUtils.getPositionForCenteringConnection(d);

            return screenLeft < x && screenRight > x && screenTop < y && screenBottom > y;
        };

        // marks the specific component as visible and determines if its entering or leaving visibility
        const updateVisibility = function (selection: any, d: any, isVisible: (d: any) => boolean) {
            const visible: boolean = isVisible(d);
            const wasVisible: boolean = selection.classed('visible');

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
        this.processorManager.selectAll().each(function (this: any, d: any) {
            updateVisibility(d3.select(this), d, isComponentVisible);
        });
        this.processGroupManager.selectAll().each(function (this: any, d: any) {
            updateVisibility(d3.select(this), d, isComponentVisible);
        });
        this.remoteProcessGroupManager.selectAll().each(function (this: any, d: any) {
            updateVisibility(d3.select(this), d, isComponentVisible);
        });
        this.portManager.selectAll().each(function (this: any, d: any) {
            updateVisibility(d3.select(this), d, isComponentVisible);
        });
        this.labelManager.selectAll().each(function (this: any, d: any) {
            updateVisibility(d3.select(this), d, isComponentVisible);
        });
        this.funnelManager.selectAll().each(function (this: any, d: any) {
            updateVisibility(d3.select(this), d, isComponentVisible);
        });
        this.connectionManager.selectAll().each(function (this: any, d: any) {
            updateVisibility(d3.select(this), d, isConnectionVisible);
        });

        // trigger pan
        this.processorManager.pan();
        this.processGroupManager.pan();
        this.remoteProcessGroupManager.pan();
        this.portManager.pan();
        this.labelManager.pan();
        this.funnelManager.pan();
        this.connectionManager.pan();
    }

    /**
     * Whether a component should be rendered based solely on the current scale.
     *
     * @returns {Boolean}
     */
    private shouldRenderPerScale(): boolean {
        return this.k >= CanvasView.MIN_SCALE_TO_RENDER;
    }

    public centerSelectedComponents(allowTransition: boolean): void {
        const selection: any = this.canvasUtils.getSelection();
        if (selection.empty()) {
            return;
        }

        let bbox;
        if (selection.size() === 1) {
            bbox = this.getSingleSelectionBoundingClientRect(selection);
        } else {
            bbox = this.getSelectionBoundingClientRect(selection);
        }

        this.allowTransition = allowTransition;
        this.centerBoundingBox(bbox);
        this.allowTransition = false;
    }

    private getSingleSelectionBoundingClientRect(selection: any): any {
        let bbox;
        if (this.canvasUtils.isConnection(selection)) {
            const d = selection.datum();

            // get the position of the connection label
            const { x, y } = this.canvasUtils.getPositionForCenteringConnection(d);

            bbox = {
                x: x,
                y: y,
                width: 1,
                height: 1
            };
        } else {
            const selectionData = selection.datum();
            const selectionPosition = selectionData.position;

            bbox = {
                x: selectionPosition.x,
                y: selectionPosition.y,
                width: selectionData.dimensions.width,
                height: selectionData.dimensions.height
            };
        }

        return bbox;
    }

    /**
     * Get a BoundingClientRect, normalized to the canvas, that encompasses all nodes in a given selection.
     */
    public getSelectionBoundingClientRect(selection: any): any {
        let yOffset = 0;

        const canvasContainer: any = document.getElementById('canvas-container');
        if (canvasContainer) {
            yOffset = canvasContainer.getBoundingClientRect().top;
        }

        const initialBBox: any = {
            x: Number.MAX_VALUE,
            y: Number.MAX_VALUE,
            right: Number.MIN_VALUE,
            bottom: Number.MIN_VALUE
        };

        const bbox = selection.nodes().reduce((aggregateBBox: any, node: any) => {
            const rect = node.getBoundingClientRect();
            aggregateBBox.x = Math.min(rect.x, aggregateBBox.x);
            aggregateBBox.y = Math.min(rect.y, aggregateBBox.y);
            aggregateBBox.right = Math.max(rect.right, aggregateBBox.right);
            aggregateBBox.bottom = Math.max(rect.bottom, aggregateBBox.bottom);

            return aggregateBBox;
        }, initialBBox);

        // normalize the bounding box with scale and translate
        bbox.x = (bbox.x - this.x) / this.k;
        bbox.y = (bbox.y - yOffset - this.y) / this.k;
        bbox.right = (bbox.right - this.x) / this.k;
        bbox.bottom = (bbox.bottom - yOffset - this.y) / this.k;

        bbox.width = bbox.right - bbox.x;
        bbox.height = bbox.bottom - bbox.y;
        bbox.top = bbox.y;
        bbox.left = bbox.x;

        return bbox;
    }

    /**
     * Translates a position to the space visible on the canvas
     *
     * @param position
     *
     * @returns {Position | null}
     */
    public getCanvasPosition(position: Position): Position | null {
        const canvasContainer: any = document.getElementById('canvas-container');
        if (!canvasContainer) {
            return null;
        }

        const rect = canvasContainer.getBoundingClientRect();

        // translate the point onto the canvas
        const canvasDropPoint = {
            x: position.x - rect.left,
            y: position.y - rect.top
        };

        // if the position is over the canvas fire an event to add the new item
        if (
            canvasDropPoint.x >= 0 &&
            canvasDropPoint.x < rect.width &&
            canvasDropPoint.y >= 0 &&
            canvasDropPoint.y < rect.height
        ) {
            // adjust the x and y coordinates accordingly
            const x = canvasDropPoint.x / this.k - this.x / this.k;
            const y = canvasDropPoint.y / this.k - this.y / this.k;

            return { x, y };
        }

        return null;
    }

    /**
     * Centers the canvas to a bounding box. If a scale is provided, it will zoom to that scale.
     * @param {type} boundingBox
     */
    public centerBoundingBox(boundingBox: any): void {
        let scale: number = this.k;
        if (boundingBox.scale != null) {
            scale = boundingBox.scale;
        }

        const center: number[] = this.getCenterForBoundingBox(boundingBox);

        // calculate the difference between the center point and the position of this component and convert to screen space
        this.transform([(center[0] - boundingBox.x) * scale, (center[1] - boundingBox.y) * scale], scale);
    }

    /**
     * Gets the coordinates necessary to center a bounding box on the screen.
     *
     * @param {type} boundingBox
     * @returns {number[]}
     */
    public getCenterForBoundingBox(boundingBox: any): number[] {
        let scale: number = this.k;
        if (boundingBox.scale != null) {
            scale = boundingBox.scale;
        }

        // get the canvas normalized width and height
        const canvasContainer: any = document.getElementById('canvas-container');
        const screenWidth: number = canvasContainer.offsetWidth / scale;
        const screenHeight: number = canvasContainer.offsetHeight / scale;

        // determine the center location for this component in canvas space
        return [screenWidth / 2 - boundingBox.width / 2, screenHeight / 2 - boundingBox.height / 2];
    }

    /**
     * Translates by the specified translation.
     *
     * @param translate
     */
    public translate(translate: [number, number]): void {
        this.behavior.translateBy(this.svg, translate[0], translate[1]);
    }

    public birdseyeDragStart(): void {
        this.birdseyeTranslateInProgress = true;
    }

    public birdseyeDragEnd(): void {
        this.birdseyeTranslateInProgress = false;

        this.updateCanvasVisibility();

        // dispatch the current transform
        this.store.dispatch(
            transformComplete({
                transform: {
                    translate: {
                        x: this.x,
                        y: this.y
                    },
                    scale: this.k
                }
            })
        );
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
        this.allowTransition = true;
        this.scale(CanvasView.INCREMENT);
        this.allowTransition = false;
    }

    /**
     * Zooms out a single zoom increment.
     */
    public zoomOut(): void {
        this.allowTransition = true;
        this.scale(1 / CanvasView.INCREMENT);
        this.allowTransition = false;
    }

    /**
     * Zooms to fit the entire graph on the canvas.
     */
    public fit(allowTransition: boolean): void {
        const translate = [this.x, this.y];
        const scale: number = this.k;
        let newScale: number;

        // get the canvas normalized width and height
        const canvasContainer: any = document.getElementById('canvas-container');
        const canvasBoundingBox: any = canvasContainer.getBoundingClientRect();
        const canvasWidth = canvasBoundingBox.width - 50;
        const canvasHeight = canvasBoundingBox.height - 50;

        // get the bounding box for the graph
        const graph: any = d3.select('#canvas');
        const graphBox = graph.node().getBoundingClientRect();
        const graphWidth: number = graphBox.width / scale;
        const graphHeight: number = graphBox.height / scale;
        let graphLeft: number = graphBox.left / scale;
        let graphTop: number = (graphBox.top - canvasBoundingBox.top) / scale;
        const x = translate[0] / scale;
        const y = translate[1] / scale;

        // adjust the scale to ensure the entire graph is visible
        if (graphWidth > canvasWidth || graphHeight > canvasHeight) {
            newScale = Math.min(canvasWidth / graphWidth, canvasHeight / graphHeight);

            // ensure the scale is within bounds
            newScale = Math.min(Math.max(newScale, CanvasView.MIN_SCALE), CanvasView.MAX_SCALE);
        } else {
            newScale = 1;

            graphLeft -= (canvasWidth - graphWidth) / 2;
            graphTop -= (canvasHeight - graphHeight) / 2;
        }

        this.allowTransition = allowTransition;
        this.centerBoundingBox({
            x: graphLeft - x,
            y: graphTop - y,
            width: canvasWidth / newScale,
            height: canvasHeight / newScale,
            scale: newScale
        });
        this.allowTransition = false;
    }

    /**
     * Zooms to the actual size (1 to 1).
     */
    actualSize(): void {
        const translate = [this.x, this.y];
        const scale: number = this.k;

        // get the first selected component
        const selection: any = this.canvasUtils.getSelection();

        // box to zoom towards
        let box;

        const canvasContainer: any = document.getElementById('canvas-container');
        const canvasBoundingBox: any = canvasContainer.getBoundingClientRect();

        // if components have been selected position the view accordingly
        if (!selection.empty()) {
            // gets the data for the first component
            const selectionBox = selection.node().getBoundingClientRect();

            // get the bounding box for the selected components
            box = {
                x: selectionBox.left / scale - translate[0] / scale,
                y: (selectionBox.top - canvasBoundingBox.top) / scale - translate[1] / scale,
                width: selectionBox.width / scale,
                height: selectionBox.height / scale,
                scale: 1
            };
        } else {
            // get the canvas normalized width and height
            const screenWidth: number = canvasBoundingBox.width / scale;
            const screenHeight: number = canvasBoundingBox.height / scale;

            // center around the center of the screen accounting for the translation accordingly
            box = {
                x: screenWidth / 2 - translate[0] / scale,
                y: screenHeight / 2 - translate[1] / scale,
                width: 1,
                height: 1,
                scale: 1
            };
        }

        this.allowTransition = true;
        this.centerBoundingBox(box);
        this.allowTransition = false;
    }

    /**
     * Refreshes the view based on the configured translation and scale.
     *
     * @param {object} options Options for the refresh operation
     */
    private async refresh({
        transition = false,
        refreshComponents = true,
        refreshBirdseye = true
    }: {
        transition?: boolean;
        refreshComponents?: boolean;
        refreshBirdseye?: boolean;
    } = {}): Promise<void> {
        const self: CanvasView = this;

        await new Promise<void>(function (resolve) {
            // update component visibility
            if (refreshComponents) {
                self.updateCanvasVisibility();
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
                        // refresh birdseye if appropriate
                        if (refreshBirdseye) {
                            self.store.dispatch(refreshBirdseyeView());
                        }

                        resolve();
                    });
            } else {
                self.canvas.attr('transform', function () {
                    return 'translate(' + t + ') scale(' + s + ')';
                });

                // refresh birdseye if appropriate
                if (refreshBirdseye) {
                    self.store.dispatch(refreshBirdseyeView());
                }

                resolve();
            }
        });
    }

    public isCanvasInitialized(): boolean {
        return this.canvasInitialized;
    }

    public destroy(): void {
        this.canvasInitialized = false;

        this.labelManager.destroy();
        this.funnelManager.destroy();
        this.portManager.destroy();
        this.remoteProcessGroupManager.destroy();
        this.processGroupManager.destroy();
        this.processorManager.destroy();
        this.connectionManager.destroy();

        this.k = INITIAL_SCALE;
        this.x = INITIAL_TRANSLATE.x;
        this.y = INITIAL_TRANSLATE.y;

        this.svg = null;
        this.canvas = null;
    }
}
