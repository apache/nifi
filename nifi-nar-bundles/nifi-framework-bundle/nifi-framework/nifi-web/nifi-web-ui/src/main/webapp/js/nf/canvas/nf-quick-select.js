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
                'nf.CanvasUtils'],
            function (d3, nfCanvasUtils) {
                return (nf.QuickSelect = factory(d3, nfCanvasUtils));
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.QuickSelect =
            factory(require('d3'),
                require('nf.CanvasUtils')));
    } else {
        nf.QuickSelect = factory(
            root.d3,
            root.nf.CanvasUtils);
    }
}(this, function (d3, nfCanvasUtils) {
    'use strict';

    var nfActions;

    var nfQuickSelect = {
        /**
         * Initialize the context menu.
         *
         * @param nfActionsRef   The nfActions module.
         */
        init: function (nfActionsRef) {
            nfActions = nfActionsRef;
        },

        /**
         * Attempts to show configuration or details dialog for the specified slection.
         */
        quickSelect: function () {
            var selection = nfCanvasUtils.getSelection();

            if (nfCanvasUtils.isConfigurable(selection)) {
                nfActions.showConfiguration(selection);
            } else if (nfCanvasUtils.hasDetails(selection)) {
                nfActions.showDetails(selection);
            }

            // stop propagation and prevent default
            d3.event.preventDefault();
            d3.event.stopPropagation();
        },

        /**
         * Activates the quick select behavior for the components in the specified selection.
         *
         * @param {selection} components
         */
        activate: function (components) {
            components.on('dblclick', function () {
                // get the clicked component to update selection
                nfQuickSelect.quickSelect();
            });
        }
    };

    return nfQuickSelect;
}));