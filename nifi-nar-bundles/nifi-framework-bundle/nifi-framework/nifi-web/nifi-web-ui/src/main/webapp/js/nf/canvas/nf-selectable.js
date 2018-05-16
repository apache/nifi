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
        define(['d3',
                'nf.ng.Bridge',
                'nf.ContextMenu',
                'nf.CanvasUtils'],
            function (d3, nfNgBridge, nfContextMenu, nfCanvasUtils) {
                return (nf.Selectable = factory(d3, nfNgBridge, nfContextMenu, nfCanvasUtils));
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.Selectable =
            factory(require('d3'),
                require('nf.ng.Bridge'),
                require('nf.ContextMenu'),
                require('nf.CanvasUtils')));
    } else {
        nf.Selectable = factory(root.d3,
            root.nf.ng.Bridge,
            root.nf.ContextMenu,
            root.nf.CanvasUtils);
    }
}(this, function (d3, nfNgBridge, nfContextMenu, nfCanvasUtils) {
    'use strict';

    var nfSelectable = {

        select: function (g) {
            // hide any context menus as necessary
            nfContextMenu.hide();

            // only need to update selection if necessary
            if (!g.classed('selected')) {
                // since we're not appending, deselect everything else
                if (!d3.event.shiftKey) {
                    d3.selectAll('g.selected').classed('selected', false);
                }

                // update the selection
                g.classed('selected', true);
            } else {
                // we are currently selected, if shift key the deselect
                if (d3.event.shiftKey) {
                    g.classed('selected', false);
                }
            }

            // inform Angular app that values have changed since the
            // enabled operate palette buttons are based off of the selection
            nfNgBridge.digest();

            // stop propagation
            d3.event.stopPropagation();
        },

        /**
         * Activates the select behavior for the components in the specified selection.
         *
         * @param {selection} components
         */
        activate: function (components) {
            components.on('mousedown.selection', function () {
                // get the clicked component to update selection
                nfSelectable.select(d3.select(this));

                // update URL deep linking params
                nfCanvasUtils.setURLParameters();
            });
        }
    };

    return nfSelectable;
}));