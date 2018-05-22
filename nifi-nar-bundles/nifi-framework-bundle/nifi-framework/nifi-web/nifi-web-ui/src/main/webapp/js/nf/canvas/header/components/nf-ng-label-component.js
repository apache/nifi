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
                'nf.Storage',
                'nf.Graph',
                'nf.CanvasUtils',
                'nf.ErrorHandler',
                'nf.Label'],
            function ($, nfClient, nfBirdseye, nfStorage, nfGraph, nfCanvasUtils, nfErrorHandler, nfLabel) {
                return (nf.ng.LabelComponent = factory($, nfClient, nfBirdseye, nfStorage, nfGraph, nfCanvasUtils, nfErrorHandler, nfLabel));
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.ng.LabelComponent =
            factory(require('jquery'),
                require('nf.Client'),
                require('nf.Birdseye'),
                require('nf.Storage'),
                require('nf.Graph'),
                require('nf.CanvasUtils'),
                require('nf.ErrorHandler'),
                require('nf.Label')));
    } else {
        nf.ng.LabelComponent = factory(root.$,
            root.nf.Client,
            root.nf.Birdseye,
            root.nf.Storage,
            root.nf.Graph,
            root.nf.CanvasUtils,
            root.nf.ErrorHandler,
            root.nf.Label);
    }
}(this, function ($, nfClient, nfBirdseye, nfStorage, nfGraph, nfCanvasUtils, nfErrorHandler, nfLabel) {
    'use strict';

    return function (serviceProvider) {
        'use strict';

        function LabelComponent() {
            this.icon = 'icon icon-label';

            this.hoverIcon = 'icon icon-label-add';
        }

        LabelComponent.prototype = {
            constructor: LabelComponent,

            /**
             * Gets the component.
             *
             * @returns {*|jQuery|HTMLElement}
             */
            getElement: function () {
                return $('#label-component');
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
                this.createLabel(pt);
            },

            /**
             * The drag icon for the toolbox component.
             *
             * @param event
             * @returns {*|jQuery|HTMLElement}
             */
            dragIcon: function (event) {
                return $('<div class="icon icon-label-add"></div>');
            },

            /**
             * Create the label and add to the graph.
             *
             * @argument {object} pt        The point that the label was dropped.
             */
            createLabel: function (pt) {
                var labelEntity = {
                    'revision': nfClient.getRevision({
                        'revision': {
                            'version': 0
                        }
                    }),
                    'disconnectedNodeAcknowledged': nfStorage.isDisconnectionAcknowledged(),
                    'component': {
                        'width': nfLabel.config.width,
                        'height': nfLabel.config.height,
                        'position': {
                            'x': pt.x,
                            'y': pt.y
                        }
                    }
                };

                // create a new label
                $.ajax({
                    type: 'POST',
                    url: serviceProvider.headerCtrl.toolboxCtrl.config.urls.api + '/process-groups/' + encodeURIComponent(nfCanvasUtils.getGroupId()) + '/labels',
                    data: JSON.stringify(labelEntity),
                    dataType: 'json',
                    contentType: 'application/json'
                }).done(function (response) {
                    // add the label to the graph
                    nfGraph.add({
                        'labels': [response]
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

        var labelComponent = new LabelComponent();
        return labelComponent;
    };
}));