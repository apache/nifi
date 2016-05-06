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

nf.Label = (function () {

    var dimensions = {
        width: 150,
        height: 150
    };

    var MIN_HEIGHT = 20;
    var MIN_WIDTH = 64;

    // -----------------------------
    // labels currently on the graph
    // -----------------------------

    var labelMap;

    // --------------------
    // component containers
    // --------------------

    var labelContainer;

    // ---------------------------------
    // drag handler for the label points
    // ---------------------------------

    var labelPointDrag;

    // --------------------------
    // privately scoped functions
    // --------------------------

    /**
     * Selects the labels elements against the current label map.
     */
    var select = function () {
        return labelContainer.selectAll('g.label').data(labelMap.values(), function (d) {
            return d.id;
        });
    };

    /**
     * Renders the labels in the specified selection.
     * 
     * @param {selection} entered           The selection of labels to be rendered
     * @param {boolean} selected            Whether the label should be selected
     */
    var renderLabels = function (entered, selected) {
        if (entered.empty()) {
            return;
        }

        var label = entered.append('g')
                .attr({
                    'id': function (d) {
                        return 'id-' + d.id;
                    },
                    'class': 'label component'
                })
                .classed('selected', selected)
                .call(nf.CanvasUtils.position);

        // label border
        label.append('rect')
                .attr({
                    'class': 'border',
                    'fill': 'transparent',
                    'stroke-opacity': 0.8,
                    'stroke-width': 1
                });

        // label 
        label.append('rect')
                .attr({
                    'class': 'body',
                    'fill-opacity': 0.8,
                    'stroke-opacity': 0.8,
                    'stroke-width': 0
                });

        // label value
        label.append('text')
                .attr({
                    'xml:space': 'preserve',
                    'font-weight': 'bold',
                    'fill': 'black',
                    'class': 'label-value'
                });

        // always support selecting
        label.call(nf.Selectable.activate).call(nf.ContextMenu.activate);

        // only support dragging when appropriate
        label.filter(function (d) {
            return d.accessPolicy.canWrite && d.accessPolicy.canRead;
        }).call(nf.Draggable.activate);

        // call update to trigger some rendering
        label.call(updateLabels);
    };

    /**
     * Updates the labels in the specified selection.
     * 
     * @param {selection} updated               The labels to be updated
     */
    var updateLabels = function (updated) {
        if (updated.empty()) {
            return;
        }

        // update the border using the configured color
        updated.select('rect.border')
                .attr({
                    'stroke': function (d) {
                        var color = nf.Label.defaultColor();

                        if (d.accessPolicy.canRead) {
                            // use the specified color if appropriate
                            if (nf.Common.isDefinedAndNotNull(d.component.style['background-color'])) {
                                color = d.component.style['background-color'];
                            }
                        }

                        return color;
                    },
                    'width': function (d) {
                        return d.dimensions.width;
                    },
                    'height': function (d) {
                        return d.dimensions.height;
                    }
                });

        // update the body fill using the configured color
        updated.select('rect.body')
                .attr({
                    'fill': function (d) {
                        var color = nf.Label.defaultColor();

                        if (d.accessPolicy.canRead) {
                            // use the specified color if appropriate
                            if (nf.Common.isDefinedAndNotNull(d.component.style['background-color'])) {
                                color = d.component.style['background-color'];
                            }
                        }

                        return color;
                    },
                    'width': function (d) {
                        return d.dimensions.width;
                    },
                    'height': function (d) {
                        return d.dimensions.height;
                    }
                });

        // go through each label being updated
        updated.each(function (d) {
            var updatedLabel = d3.select(this);

            if (d.accessPolicy.canRead) {
                // update the label
                var label = updatedLabel.select('text.label-value');

                // udpate the font size
                label.attr('font-size', function () {
                    var fontSize = '12px';

                    // use the specified color if appropriate
                    if (nf.Common.isDefinedAndNotNull(d.component.style['font-size'])) {
                        fontSize = d.component.style['font-size'];
                    }

                    return fontSize;
                });

                // remove the previous label value
                label.selectAll('tspan').remove();

                // parse the lines in this label
                var lines = [];
                if (nf.Common.isDefinedAndNotNull(d.component.label)) {
                    lines = d.component.label.split('\n');
                } else {
                    lines.push('');
                }

                // add label value
                $.each(lines, function (i, line) {
                    label.append('tspan')
                            .attr('x', '0.4em')
                            .attr('dy', '1.2em')
                            .text(function () {
                                return line;
                            });
                });


                // -----------
                // labelpoints
                // -----------

                if (d.accessPolicy.canWrite) {
                    var pointData = [
                        {x: d.dimensions.width, y: d.dimensions.height}
                    ];
                    var points = updatedLabel.selectAll('rect.labelpoint').data(pointData);

                    // create a point for the end
                    points.enter().append('rect')
                            .attr({
                                'class': 'labelpoint',
                                'width': 10,
                                'height': 10
                            })
                            .call(labelPointDrag);

                    // update the midpoints
                    points.attr('transform', function (p) {
                        return 'translate(' + (p.x - 10) + ', ' + (p.y - 10) + ')';
                    });

                    // remove old items
                    points.exit().remove();
                }
            }
        });
    };

    /**
     * Removes the labels in the specified selection.
     * 
     * @param {selection} removed               The labels to be removed
     */
    var removeLabels = function (removed) {
        removed.remove();
    };

    return {
        config: {
            width: dimensions.width,
            height: dimensions.height
        },
        
        /**
         * Initializes of the Processor handler.
         */
        init: function () {
            labelMap = d3.map();

            // create the label container
            labelContainer = d3.select('#canvas').append('g')
                    .attr({
                        'pointer-events': 'all',
                        'class': 'labels'
                    });

            // handle bend point drag events
            labelPointDrag = d3.behavior.drag()
                    .on('dragstart', function () {
                        // stop further propagation
                        d3.event.sourceEvent.stopPropagation();
                    })
                    .on('drag', function () {
                        var label = d3.select(this.parentNode);
                        var labelData = label.datum();

                        // update the dimensions and ensure they are still within bounds
                        labelData.dimensions.width = Math.max(MIN_WIDTH, d3.event.x);
                        labelData.dimensions.height = Math.max(MIN_HEIGHT, d3.event.y);

                        // redraw this connection
                        updateLabels(label);
                    })
                    .on('dragend', function () {
                        var label = d3.select(this.parentNode);
                        var labelData = label.datum();

                        // determine if the width has changed
                        var different = false;
                        if (nf.Common.isDefinedAndNotNull(labelData.component.width) || labelData.dimensions.width !== labelData.component.width) {
                            different = true;
                        }

                        // determine if the height has changed
                        if (!different && nf.Common.isDefinedAndNotNull(labelData.component.height) || labelData.dimensions.height !== labelData.component.height) {
                            different = true;
                        }

                        // only save the updated bends if necessary
                        if (different) {
                            var labelEntity = {
                                'revision': nf.Client.getRevision(),
                                'component': {
                                    'id': labelData.id,
                                    'width': labelData.dimensions.width,
                                    'height': labelData.dimensions.height
                                }
                            }

                            $.ajax({
                                type: 'PUT',
                                url: labelData.component.uri,
                                data: JSON.stringify(labelEntity),
                                dataType: 'json',
                                contentType: 'application/json'
                            }).done(function (response) {
                                // update the revision
                                nf.Client.setRevision(response.revision);

                                // request was successful, update the entry
                                nf.Label.set(response);
                            }).fail(function () {
                                // determine the previous width
                                var width = dimensions.width;
                                if (nf.Common.isDefinedAndNotNull(labelData.component.width)) {
                                    width = labelData.component.width;
                                }

                                // determine the previous height
                                var height = dimensions.height;
                                if (nf.Common.isDefinedAndNotNull(labelData.component.height)) {
                                    height = labelData.component.height;
                                }

                                // restore the previous dimensions
                                labelData.dimensions = {
                                    width: width,
                                    height: height
                                };

                                // refresh the label
                                label.call(updateLabels);
                            });
                        }

                        // stop further propagation
                        d3.event.sourceEvent.stopPropagation();
                    });
        },
        
        /**
         * Populates the graph with the specified labels.
         * 
         * @argument {object | array} labelEntities                   The labels to add
         * @argument {boolean} selectAll                Whether or not to select the new contents
         */
        add: function (labelEntities, selectAll) {
            selectAll = nf.Common.isDefinedAndNotNull(selectAll) ? selectAll : false;

            var add = function (labelEntity) {
                // add the label
                labelMap.set(labelEntity.id, $.extend({
                    type: 'Label'
                }, labelEntity));
            };

            // determine how to handle the specified label status
            if ($.isArray(labelEntities)) {
                $.each(labelEntities, function (_, labelEntity) {
                    add(labelEntity);
                });
            } else {
                add(labelEntities);
            }

            // apply the selection and handle all new labels
            select().enter().call(renderLabels, selectAll);
        },
        
        /**
         * If the label id is specified it is returned. If no label id
         * specified, all labels are returned.
         * 
         * @param {string} id
         */
        get: function (id) {
            if (nf.Common.isUndefined(id)) {
                return labelMap.values();
            } else {
                return labelMap.get(id);
            }
        },
        
        /**
         * If the label id is specified it is refresh according to the current 
         * state. If not label id is specified, all labels are refreshed.
         * 
         * @param {string} id      Optional
         */
        refresh: function (id) {
            if (nf.Common.isDefinedAndNotNull(id)) {
                d3.select('#id-' + id).call(updateLabels);
            } else {
                d3.selectAll('g.label').call(updateLabels);
            }
        },
        
        /**
         * Reloads the label state from the server and refreshes the UI.
         * If the label is currently unknown, this function just returns.
         * 
         * @param {object} label The label to reload
         */
        reload: function (label) {
            if (labelMap.has(label.id)) {
                return $.ajax({
                    type: 'GET',
                    url: label.uri,
                    dataType: 'json'
                }).done(function (response) {
                    nf.Label.set(response);
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
         * Sets the specified label(s). If the is an array, it 
         * will set each label. If it is not an array, it will 
         * attempt to set the specified label.
         * 
         * @param {object | array} labelEntities
         */
        set: function (labelEntities) {
            var set = function (labelEntity) {
                if (labelMap.has(labelEntity.id)) {
                    // update the current entry
                    var labelEntry = labelMap.get(labelEntity.id);
                    $.extend(labelEntry, labelEntity);

                    // update the connection in the UI
                    d3.select('#id-' + labelEntry.id).call(updateLabels);
                }
            };

            // determine how to handle the specified label status
            if ($.isArray(labelEntities)) {
                $.each(labelEntities, function (_, label) {
                    set(label);
                });
            } else {
                set(labelEntities);
            }
        },

        /**
         * Removes the specified label.
         * 
         * @param {array|string} labels      The label id(s)
         */
        remove: function (labels) {
            if ($.isArray(labels)) {
                $.each(labels, function (_, label) {
                    labelMap.remove(label);
                });
            } else {
                labelMap.remove(labels);
            }

            // apply the selection and handle all removed labels
            select().exit().call(removeLabels);
        },
        
        /**
         * Removes all label.
         */
        removeAll: function () {
            nf.Label.remove(labelMap.keys());
        },
        
        /**
         * Returns the default color that should be used when drawing a label.
         */
        defaultColor: function () {
            return '#ffde93';
        }
    };
}());