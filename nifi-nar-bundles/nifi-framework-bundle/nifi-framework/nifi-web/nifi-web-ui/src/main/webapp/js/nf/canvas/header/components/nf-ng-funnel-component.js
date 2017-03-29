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
        define(['jquery',
                'nf.Client',
                'nf.Birdseye',
                'nf.Graph',
                'nf.CanvasUtils',
                'nf.ErrorHandler'],
            function ($, nfClient, nfBirdseye, nfGraph, nfCanvasUtils, nfErrorHandler) {
                return (nf.ng.FunnelComponent = factory($, nfClient, nfBirdseye, nfGraph, nfCanvasUtils, nfErrorHandler));
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.ng.FunnelComponent =
            factory(require('jquery'),
                require('nf.Client'),
                require('nf.Birdseye'),
                require('nf.Graph'),
                require('nf.CanvasUtils'),
                require('nf.ErrorHandler')));
    } else {
        nf.ng.FunnelComponent = factory(root.$,
            root.nf.Client,
            root.nf.Birdseye,
            root.nf.Graph,
            root.nf.CanvasUtils,
            root.nf.ErrorHandler);
    }
}(this, function ($, nfClient, nfBirdseye, nfGraph, nfCanvasUtils, nfErrorHandler) {
    'use strict';

    return function (serviceProvider) {
        'use strict';

        function FunnelComponent() {
            this.icon = 'icon icon-funnel';

            this.hoverIcon = 'icon icon-funnel-add';
        }

        FunnelComponent.prototype = {
            constructor: FunnelComponent,

            /**
             * Gets the component.
             *
             * @returns {*|jQuery|HTMLElement}
             */
            getElement: function () {
                return $('#funnel-component');
            },

            /**
             * Enable the component.
             */
            enabled: function () {
                this.getElement().attr('disabled', false);
            },

            /**
             * Disable the component.
             */
            disabled: function () {
                this.getElement().attr('disabled', true);
            },

            /**
             * Handler function for when component is dropped on the canvas.
             *
             * @argument {object} pt        The point that the component was dropped.
             */
            dropHandler: function (pt) {
                this.createFunnel(pt);
            },

            /**
             * The drag icon for the toolbox component.
             *
             * @param event
             * @returns {*|jQuery|HTMLElement}
             */
            dragIcon: function (event) {
                return $('<div class="icon icon-funnel-add"></div>');
            },

            /**
             * Creates a new funnel at the specified point.
             *
             * @argument {object} pt        The point that the funnel was dropped.
             */
            createFunnel: function (pt) {
                var outputPortEntity = {
                    'revision': nfClient.getRevision({
                        'revision': {
                            'version': 0
                        }
                    }),
                    'component': {
                        'position': {
                            'x': pt.x,
                            'y': pt.y
                        }
                    }
                };

                // create a new funnel
                $.ajax({
                    type: 'POST',
                    url: serviceProvider.headerCtrl.toolboxCtrl.config.urls.api + '/process-groups/' + encodeURIComponent(nfCanvasUtils.getGroupId()) + '/funnels',
                    data: JSON.stringify(outputPortEntity),
                    dataType: 'json',
                    contentType: 'application/json'
                }).done(function (response) {
                    // add the funnel to the graph
                    nfGraph.add({
                        'funnels': [response]
                    }, {
                        'selectAll': true
                    });

                    // update component visibility
                    nfGraph.updateVisibility();

                    // update the birdseye
                    nfBirdseye.refresh();
                }).fail(nfErrorHandler.handleAjaxError);
            }
        }

        var funnelComponent = new FunnelComponent();
        return funnelComponent;
    };
}));