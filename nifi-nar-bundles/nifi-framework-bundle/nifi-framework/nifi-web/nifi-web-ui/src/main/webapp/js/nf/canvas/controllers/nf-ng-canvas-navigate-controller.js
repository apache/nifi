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

/* global nf, d3 */

nf.ng.Canvas.NavigateCtrl = function () {
    'use strict';

    function NavigateCtrl() {

        /**
         * Zoom in on the canvas.
         */
        this.zoomIn = function () {
            nf.Canvas.View.zoomIn();

            // hide the context menu
            nf.ContextMenu.hide();

            // refresh the canvas
            nf.Canvas.View.refresh({
                transition: true
            });
        };

        /**
         * Zoom out on the canvas.
         */
        this.zoomOut = function () {
            nf.Canvas.View.zoomOut();

            // hide the context menu
            nf.ContextMenu.hide();

            // refresh the canvas
            nf.Canvas.View.refresh({
                transition: true
            });
        };

        /**
         * Zoom fit on the canvas.
         */
        this.zoomFit = function () {
            nf.Canvas.View.fit();

            // hide the context menu
            nf.ContextMenu.hide();

            // refresh the canvas
            nf.Canvas.View.refresh({
                transition: true
            });
        };

        /**
         * Zoom actual size on the canvas.
         */
        this.zoomActualSize = function () {
            nf.Canvas.View.actualSize();

            // hide the context menu
            nf.ContextMenu.hide();

            // refresh the canvas
            nf.Canvas.View.refresh({
                transition: true
            });
        };
    }
    NavigateCtrl.prototype = {
        constructor: NavigateCtrl
    }

    var navigateCtrl = new NavigateCtrl();
    return navigateCtrl;
};