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

nf.Funnel = (function () {

    var dimensions = {
        width: 61,
        height: 61
    };

    // -----------------------------
    // funnels currently on the graph
    // -----------------------------

    var funnelMap;

    // --------------------
    // component containers
    // --------------------

    var funnelContainer;

    // --------------------------
    // privately scoped functions
    // --------------------------

    /**
     * Selects the funnel elements against the current funnel map.
     */
    var select = function () {
        return funnelContainer.selectAll('g.funnel').data(funnelMap.values(), function (d) {
            return d.component.id;
        });
    };

    /**
     * Renders the funnels in the specified selection.
     * 
     * @param {selection} entered           The selection of funnels to be rendered
     * @param {boolean} selected             Whether the element should be selected
     */
    var renderFunnels = function (entered, selected) {
        if (entered.empty()) {
            return entered;
        }

        var funnel = entered.append('g')
                .attr({
                    'id': function (d) {
                        return 'id-' + d.component.id;
                    },
                    'class': 'funnel component'
                })
                .classed('selected', selected)
                .call(nf.CanvasUtils.position);

        // funnel border
        funnel.append('rect')
                .attr({
                    'class': 'border',
                    'width': function (d) {
                        return d.dimensions.width;
                    },
                    'height': function (d) {
                        return d.dimensions.height;
                    },
                    'fill': 'transparent',
                    'stroke-opacity': 0.8,
                    'stroke-width': 1
                });

        // processor icon
        funnel.append('image')
                .call(nf.CanvasUtils.disableImageHref)
                .attr({
                    'xlink:href': 'images/iconFunnel.png',
                    'width': 41,
                    'height': 41,
                    'x': 10,
                    'y': 10
                });

        // always support selection
        funnel.call(nf.Selectable.activate).call(nf.ContextMenu.activate);

        // only support dragging and connecting when appropriate
        if (nf.Common.isDFM()) {
            funnel.call(nf.Draggable.activate).call(nf.Connectable.activate);
        }

        return funnel;
    };

    /**
     * Updates the funnels in the specified selection.
     * 
     * @param {selection} updated               The funnels to be updated
     */
    var updateFunnels = function (updated) {
    };

    /**
     * Removes the funnels in the specified selection.
     * 
     * @param {selection} removed               The funnels to be removed
     */
    var removeFunnels = function (removed) {
        removed.remove();
    };

    return {
        /**
         * Initializes of the Processor handler.
         */
        init: function () {
            funnelMap = d3.map();

            // create the funnel container
            funnelContainer = d3.select('#canvas').append('g')
                    .attr({
                        'pointer-events': 'all',
                        'class': 'funnels'
                    });
        },
        
        /**
         * Populates the graph with the specified funnels.
         * 
         * @argument {object | array} funnels                    The funnels to add
         * @argument {boolean} selectAll                Whether or not to select the new contents
         */
        add: function (funnels, selectAll) {
            selectAll = nf.Common.isDefinedAndNotNull(selectAll) ? selectAll : false;

            var add = function (funnel) {
                // add the funnel
                funnelMap.set(funnel.id, {
                    type: 'Funnel',
                    component: funnel,
                    dimensions: dimensions
                });
            };

            // determine how to handle the specified funnel status
            if ($.isArray(funnels)) {
                $.each(funnels, function (_, funnel) {
                    add(funnel);
                });
            } else {
                add(funnels);
            }

            // apply the selection and handle all new processors
            select().enter().call(renderFunnels, selectAll);
        },
        
        /**
         * If the funnel id is specified it is returned. If no funnel id
         * specified, all funnels are returned.
         * 
         * @param {string} id
         */
        get: function (id) {
            if (nf.Common.isUndefined(id)) {
                return funnelMap.values();
            } else {
                return funnelMap.get(id);
            }
        },
        
        /**
         * If the funnel id is specified it is refresh according to the current 
         * state. If not funnel id is specified, all funnels are refreshed.
         * 
         * @param {string} id      Optional
         */
        refresh: function (id) {
            if (nf.Common.isDefinedAndNotNull(id)) {
                d3.select('#id-' + id).call(updateFunnels);
            } else {
                d3.selectAll('g.funnel').call(updateFunnels);
            }
        },
        
        /**
         * Reloads the funnel state from the server and refreshes the UI.
         * If the funnel is currently unknown, this function just returns.
         * 
         * @param {object} funnel The funnel to reload
         */
        reload: function (funnel) {
            if (funnelMap.has(funnel.id)) {
                return $.ajax({
                    type: 'GET',
                    url: funnel.uri,
                    dataType: 'json'
                }).done(function (response) {
                    nf.Funnel.set(response.funnel);
                });
            }
        },
        
        /**
         * Positions the component.
         * 
         * @param {string} id   The id
         */
        position: function (id) {
            d3.select('#id-' + id).call(nf.CanvasUtils.position);
        },
        
        /**
         * Sets the specified funnel(s). If the is an array, it 
         * will set each funnel. If it is not an array, it will 
         * attempt to set the specified funnel.
         * 
         * @param {object | array} funnels
         */
        set: function (funnels) {
            var set = function (funnel) {
                if (funnelMap.has(funnel.id)) {
                    // update the current entry
                    var funnelEntry = funnelMap.get(funnel.id);
                    funnelEntry.component = funnel;

                    // update the connection in the UI
                    d3.select('#id-' + funnel.id).call(updateFunnels);
                }
            };

            // determine how to handle the specified funnel status
            if ($.isArray(funnels)) {
                $.each(funnels, function (_, funnel) {
                    set(funnel);
                });
            } else {
                set(funnels);
            }
        },
        
        /**
         * Removes the specified funnel.
         * 
         * @param {array|string} funnels      The funnel id
         */
        remove: function (funnels) {
            if ($.isArray(funnels)) {
                $.each(funnels, function (_, funnel) {
                    funnelMap.remove(funnel);
                });
            } else {
                funnelMap.remove(funnels);
            }

            // apply the selection and handle all removed funnels
            select().exit().call(removeFunnels);
        },
        
        /**
         * Removes all processors.
         */
        removeAll: function () {
            nf.Funnel.remove(funnelMap.keys());
        }
    };
}());