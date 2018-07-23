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

/* global define, module, require, exports */

(function (root, factory) {
    if (typeof define === 'function' && define.amd) {
        define(['nf.CanvasUtils',
                'nf.ContextMenu'],
            function (nfCanvasUtils, nfContextMenu) {
                return (nf.ng.Canvas.NavigateCtrl = factory(nfCanvasUtils, nfContextMenu));
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.ng.Canvas.NavigateCtrl =
            factory(require('nf.CanvasUtils'),
                require('nf.ContextMenu')));
    } else {
        nf.ng.Canvas.NavigateCtrl = factory(root.nf.CanvasUtils,
            root.nf.ContextMenu);
    }
}(this, function (nfCanvasUtils, nfContextMenu) {
    'use strict';

    return function ($timeout) {
        'use strict';

        function NavigateCtrl() {

            /**
             * Zoom in on the canvas.
             */
            this.zoomIn = function () {
                $timeout(function () {
                    nfCanvasUtils.zoomInCanvas();
                }, 0);
            };

            /**
             * Zoom out on the canvas.
             */
            this.zoomOut = function () {
                $timeout(function () {
                    nfCanvasUtils.zoomOutCanvas();
                }, 0);
            };

            /**
             * Zoom fit on the canvas.
             */
            this.zoomFit = function () {
                $timeout(function () {
                    nfCanvasUtils.fitCanvas();
                }, 0);
            };

            /**
             * Zoom actual size on the canvas.
             */
            this.zoomActualSize = function () {
                $timeout(function () {
                    nfCanvasUtils.actualSizeCanvas();
                }, 0);
            };
        }

        NavigateCtrl.prototype = {
            constructor: NavigateCtrl
        };

        var navigateCtrl = new NavigateCtrl();
        return navigateCtrl;
    };
}));