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
                'd3',
                'nf.Common',
                'nf.Client',
                'nf.CanvasUtils'],
            function ($, d3, nfCommon, nfClient, nfCanvasUtils) {
                return (nf.Label = factory($, d3, nfCommon, nfClient, nfCanvasUtils));
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.Label =
            factory(require('jquery'),
                require('d3'),
                require('nf.Common'),
                require('nf.Client'),
                require('nf.CanvasUtils')));
    } else {
        nf.Label = factory(root.$,
            root.d3,
            root.nf.Common,
            root.nf.Client,
            root.nf.CanvasUtils);
    }
}(this, function ($, d3, nfCommon, nfClient, nfCanvasUtils) {
    'use strict';

    var nfConnectable;
    var nfDraggable;
    var nfSelectable;
    var nfQuickSelect;
    var nfContextMenu;

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

    // -----------------------------------------------------------
    // cache for components that are added/removed from the canvas
    // -----------------------------------------------------------

    var removedCache;
    var addedCache;

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
            .call(nfCanvasUtils.position);

        // label border
        label.append('rect')
            .attr({
                'class': 'border',
                'fill': 'transparent',
                'stroke': 'transparent'
            });

        // label 
        label.append('rect')
            .attr({
                'class': 'body',
                'filter': 'url(#component-drop-shadow)',
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
        label.call(nfSelectable.activate).call(nfContextMenu.activate).call(nfQuickSelect.activate);
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
                'width': function (d) {
                    return d.dimensions.width;
                },
                'height': function (d) {
                    return d.dimensions.height;
                }
            })
            .classed('unauthorized', function (d) {
                return d.permissions.canRead === false;
            });

        // update the body fill using the configured color
        updated.select('rect.body')
            .attr({
                'width': function (d) {
                    return d.dimensions.width;
                },
                'height': function (d) {
                    return d.dimensions.height;
                }
            })
            .style('fill', function (d) {
                if (!d.permissions.canRead) {
                    return null;
                }

                var color = nfLabel.defaultColor();

                // use the specified color if appropriate
                if (nfCommon.isDefinedAndNotNull(d.component.style['background-color'])) {
                    color = d.component.style['background-color'];
                }

                return color;
            })
            .classed('unauthorized', function (d) {
                return d.permissions.canRead === false;
            });

        // go through each label being updated
        updated.each(function (d) {
            var label = d3.select(this);

            // update the component behavior as appropriate
            nfCanvasUtils.editable(label, nfConnectable, nfDraggable);

            // update the label
            var labelText = label.select('text.label-value');
            var labelPoint = label.selectAll('rect.labelpoint');
            if (d.permissions.canRead) {
                // udpate the font size
                labelText.attr('font-size', function () {
                    var fontSize = '12px';

                    // use the specified color if appropriate
                    if (nfCommon.isDefinedAndNotNull(d.component.style['font-size'])) {
                        fontSize = d.component.style['font-size'];
                    }

                    return fontSize;
                });

                // remove the previous label value
                labelText.selectAll('tspan').remove();

                // parse the lines in this label
                var lines = [];
                if (nfCommon.isDefinedAndNotNull(d.component.label)) {
                    lines = d.component.label.split('\n');
                } else {
                    lines.push('');
                }

                var color = nfLabel.defaultColor();

                // use the specified color if appropriate
                if (nfCommon.isDefinedAndNotNull(d.component.style['background-color'])) {
                    color = d.component.style['background-color'];
                }

                // add label value
                $.each(lines, function (i, line) {
                    labelText.append('tspan')
                        .attr('x', '0.4em')
                        .attr('dy', '1.2em')
                        .text(function () {
                            return line;
                        })
                        .style('fill', function (d) {
                            return nfCommon.determineContrastColor(
                                nfCommon.substringAfterLast(
                                    color, '#'));
                        });
                });


                // -----------
                // labelpoints
                // -----------

                if (d.permissions.canWrite) {
                    var pointData = [
                        {x: d.dimensions.width, y: d.dimensions.height}
                    ];
                    var points = labelPoint.data(pointData);

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
            } else {
                // remove the previous label value
                labelText.selectAll('tspan').remove();

                // remove the label points
                labelPoint.remove()
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

    var nfLabel = {
        config: {
            width: dimensions.width,
            height: dimensions.height
        },

        /**
         * Initializes of the Processor handler.
         *
         * @param nfConnectableRef   The nfConnectable module.
         * @param nfDraggableRef   The nfDraggable module.
         * @param nfSelectableRef   The nfSelectable module.
         * @param nfContextMenuRef   The nfContextMenu module.
         * @param nfQuickSelectRef   The nfQuickSelect module.
         */
        init: function (nfConnectableRef, nfDraggableRef, nfSelectableRef, nfContextMenuRef, nfQuickSelectRef) {
            nfConnectable = nfConnectableRef;
            nfDraggable = nfDraggableRef;
            nfSelectable = nfSelectableRef;
            nfContextMenu = nfContextMenuRef;
            nfQuickSelect = nfQuickSelectRef;

            labelMap = d3.map();
            removedCache = d3.map();
            addedCache = d3.map();

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
                    if (nfCommon.isDefinedAndNotNull(labelData.component.width) || labelData.dimensions.width !== labelData.component.width) {
                        different = true;
                    }

                    // determine if the height has changed
                    if (!different && nfCommon.isDefinedAndNotNull(labelData.component.height) || labelData.dimensions.height !== labelData.component.height) {
                        different = true;
                    }

                    // only save the updated bends if necessary
                    if (different) {
                        var labelEntity = {
                            'revision': nfClient.getRevision(labelData),
                            'component': {
                                'id': labelData.id,
                                'width': labelData.dimensions.width,
                                'height': labelData.dimensions.height
                            }
                        }

                        $.ajax({
                            type: 'PUT',
                            url: labelData.uri,
                            data: JSON.stringify(labelEntity),
                            dataType: 'json',
                            contentType: 'application/json'
                        }).done(function (response) {
                            // request was successful, update the entry
                            nfLabel.set(response);
                        }).fail(function () {
                            // determine the previous width
                            var width = dimensions.width;
                            if (nfCommon.isDefinedAndNotNull(labelData.component.width)) {
                                width = labelData.component.width;
                            }

                            // determine the previous height
                            var height = dimensions.height;
                            if (nfCommon.isDefinedAndNotNull(labelData.component.height)) {
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
         * Adds the specified label entity.
         *
         * @param labelEntities       The label
         * @param options           Configuration options
         */
        add: function (labelEntities, options) {
            var selectAll = false;
            if (nfCommon.isDefinedAndNotNull(options)) {
                selectAll = nfCommon.isDefinedAndNotNull(options.selectAll) ? options.selectAll : selectAll;
            }

            // get the current time
            var now = new Date().getTime();

            var add = function (labelEntity) {
                addedCache.set(labelEntity.id, now);

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
            } else if (nfCommon.isDefinedAndNotNull(labelEntities)) {
                add(labelEntities);
            }

            // apply the selection and handle new labels
            var selection = select();
            selection.enter().call(renderLabels, selectAll);
            selection.call(updateLabels);
        },

        /**
         * Populates the graph with the specified labels.
         *
         * @argument {object | array} labelEntities     The labels to add
         * @argument {object} options                   Configuration options
         */
        set: function (labelEntities, options) {
            var selectAll = false;
            var transition = false;
            var overrideRevisionCheck = false;
            if (nfCommon.isDefinedAndNotNull(options)) {
                selectAll = nfCommon.isDefinedAndNotNull(options.selectAll) ? options.selectAll : selectAll;
                transition = nfCommon.isDefinedAndNotNull(options.transition) ? options.transition : transition;
                overrideRevisionCheck = nfCommon.isDefinedAndNotNull(options.overrideRevisionCheck) ? options.overrideRevisionCheck : overrideRevisionCheck;
            }

            var set = function (proposedLabelEntity) {
                var currentLabelEntity = labelMap.get(proposedLabelEntity.id);

                // set the processor if appropriate due to revision and wasn't previously removed
                if ((nfClient.isNewerRevision(currentLabelEntity, proposedLabelEntity) && !removedCache.has(proposedLabelEntity.id)) || overrideRevisionCheck === true) {
                    labelMap.set(proposedLabelEntity.id, $.extend({
                        type: 'Label'
                    }, proposedLabelEntity));
                }
            };

            if ($.isArray(labelEntities)) {
                $.each(labelMap.keys(), function (_, key) {
                    var currentLabelEntity = labelMap.get(key);
                    var isPresent = $.grep(labelEntities, function (proposedLabelEntity) {
                        return proposedLabelEntity.id === currentLabelEntity.id;
                    });

                    // if the current label is not present and was not recently added, remove it
                    if (isPresent.length === 0 && !addedCache.has(key)) {
                        labelMap.remove(key);
                    }
                });
                $.each(labelEntities, function (_, labelEntity) {
                    set(labelEntity);
                });
            } else if (nfCommon.isDefinedAndNotNull(labelEntities)) {
                set(labelEntities);
            }

            // apply the selection and handle all new labels
            var selection = select();
            selection.enter().call(renderLabels, selectAll);
            selection.call(updateLabels).call(nfCanvasUtils.position, transition);
            selection.exit().call(removeLabels);
        },

        /**
         * If the label id is specified it is returned. If no label id
         * specified, all labels are returned.
         *
         * @param {string} id
         */
        get: function (id) {
            if (nfCommon.isUndefined(id)) {
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
            if (nfCommon.isDefinedAndNotNull(id)) {
                d3.select('#id-' + id).call(updateLabels);
            } else {
                d3.selectAll('g.label').call(updateLabels);
            }
        },

        /**
         * Reloads the label state from the server and refreshes the UI.
         * If the label is currently unknown, this function just returns.
         *
         * @param {string} id The label id
         */
        reload: function (id) {
            if (labelMap.has(id)) {
                var labelEntity = labelMap.get(id);
                return $.ajax({
                    type: 'GET',
                    url: labelEntity.uri,
                    dataType: 'json'
                }).done(function (response) {
                    nfLabel.set(response);
                });
            }
        },

        /**
         * Positions the component.
         *
         * @param {string} id   The id
         */
        position: function (id) {
            d3.select('#id-' + id).call(nfCanvasUtils.position);
        },

        /**
         * Removes the specified label.
         *
         * @param {array|string} labelIds      The label id(s)
         */
        remove: function (labelIds) {
            var now = new Date().getTime();

            if ($.isArray(labelIds)) {
                $.each(labelIds, function (_, labelId) {
                    removedCache.set(labelId, now);
                    labelMap.remove(labelId);
                });
            } else {
                removedCache.set(labelIds, now);
                labelMap.remove(labelIds);
            }

            // apply the selection and handle all removed labels
            select().exit().call(removeLabels);
        },

        /**
         * Removes all label.
         */
        removeAll: function () {
            nfLabel.remove(labelMap.keys());
        },

        /**
         * Expires the caches up to the specified timestamp.
         *
         * @param timestamp
         */
        expireCaches: function (timestamp) {
            var expire = function (cache) {
                cache.forEach(function (id, entryTimestamp) {
                    if (timestamp > entryTimestamp) {
                        cache.remove(id);
                    }
                });
            };

            expire(addedCache);
            expire(removedCache);
        },

        /**
         * Returns the default color that should be used when drawing a label.
         */
        defaultColor: function () {
            return '#fff7d7';
        }
    };

    return nfLabel;
}));