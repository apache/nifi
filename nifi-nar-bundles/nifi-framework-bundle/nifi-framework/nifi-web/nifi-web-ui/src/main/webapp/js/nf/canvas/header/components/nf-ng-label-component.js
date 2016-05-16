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

nf.ng.LabelComponent = function (serviceProvider) {
    'use strict';

    function LabelComponent() {
    }
    LabelComponent.prototype = {
        constructor: LabelComponent,

        /**
         * Gets the component.
         *
         * @returns {*|jQuery|HTMLElement}
         */
        getElement: function() {
            return $('#label-component');
        },

        /**
         * Enable the component.
         */
        enabled: function() {
            this.getElement().attr('disabled', false);
        },

        /**
         * Disable the component.
         */
        disabled: function() {
            this.getElement().attr('disabled', true);
        },

        /**
         * Handler function for when component is dropped on the canvas.
         *
         * @argument {object} pt        The point that the component was dropped.
         */
        dropHandler: function(pt) {
            this.createLabel(pt);
        },

        /**
         * Create the label and add to the graph.
         *
         * @argument {object} pt        The point that the label was dropped.
         */
        createLabel: function(pt) {
            var labelEntity = {
                'component': {
                    'width': nf.Label.config.width,
                    'height': nf.Label.config.height,
                    'position': {
                        'x': pt.x,
                        'y': pt.y
                    }
                }
            };

            // create a new label
            $.ajax({
                type: 'POST',
                url: serviceProvider.headerCtrl.toolboxCtrl.config.urls.api + '/process-groups/' + encodeURIComponent(nf.Canvas.getGroupId()) + '/labels',
                data: JSON.stringify(labelEntity),
                dataType: 'json',
                contentType: 'application/json'
            }).done(function (response) {
                if (nf.Common.isDefinedAndNotNull(response.component)) {
                    // add the label to the graph
                    nf.Graph.add({
                        'labels': [response]
                    }, {
                        'selectAll': true
                    });

                    // update the birdseye
                    nf.Birdseye.refresh();
                }
            }).fail(nf.Common.handleAjaxError);
        }
    }

    var labelComponent = new LabelComponent();
    return labelComponent;
};