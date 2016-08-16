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

nf.Processor = (function () {

    var PREVIEW_NAME_LENGTH = 25;

    // default dimensions for each type of component
    var dimensions = {
        width: 350,
        height: 130
    };

    // ---------------------------------
    // processors currently on the graph
    // ---------------------------------

    var processorMap;

    // --------------------
    // component containers
    // --------------------

    var processorContainer;

    // --------------------------
    // privately scoped functions
    // --------------------------

    /**
     * Selects the processor elements against the current processor map.
     */
    var select = function () {
        return processorContainer.selectAll('g.processor').data(processorMap.values(), function (d) {
            return d.id;
        });
    };

    // renders the processors
    var renderProcessors = function (entered, selected) {
        if (entered.empty()) {
            return;
        }

        var processor = entered.append('g')
            .attr({
                'id': function (d) {
                    return 'id-' + d.id;
                },
                'class': 'processor component'
            })
            .classed('selected', selected)
            .call(nf.CanvasUtils.position);

        // processor border
        processor.append('rect')
            .attr({
                'class': 'border',
                'width': function (d) {
                    return d.dimensions.width;
                },
                'height': function (d) {
                    return d.dimensions.height;
                },
                'fill': 'transparent',
                'stroke': 'transparent'
            });

        // processor body
        processor.append('rect')
            .attr({
                'class': 'body',
                'width': function (d) {
                    return d.dimensions.width;
                },
                'height': function (d) {
                    return d.dimensions.height;
                },
                'filter': 'url(#component-drop-shadow)',
                'stroke-width': 0
            });

        // processor name
        processor.append('text')
            .attr({
                'x': 62,
                'y': 20,
                'width': 220,
                'height': 16,
                'class': 'processor-name'
            });

        // processor icon
        processor.append('text')
            .attr({
                'x': 6,
                'y': 32,
                'class': 'processor-icon'
            })
            .text('\ue807');

        // processor stats preview
        processor.append('image')
            .call(nf.CanvasUtils.disableImageHref)
            .attr({
                'width': 294,
                'height': 58,
                'x': 8,
                'y': 35,
                'class': 'processor-stats-preview'
            });

        // make processors selectable
        processor.call(nf.Selectable.activate).call(nf.ContextMenu.activate);
    };

    /**
     * Updates the processors in the specified selection.
     *
     * @param {selection} updated       The processors to update
     */
    var updateProcessors = function (updated) {
        if (updated.empty()) {
            return;
        }

        // processor border authorization
        updated.select('rect.border')
            .classed('unauthorized', function (d) {
                return d.permissions.canRead === false;
            });

        // processor body authorization
        updated.select('rect.body')
            .classed('unauthorized', function (d) {
                return d.permissions.canRead === false;
            });

        updated.each(function (processorData) {
            var processor = d3.select(this);
            var details = processor.select('g.processor-canvas-details');

            // update the component behavior as appropriate
            nf.CanvasUtils.editable(processor);

            // if this processor is visible, render everything
            if (processor.classed('visible')) {
                if (details.empty()) {
                    details = processor.append('g').attr('class', 'processor-canvas-details');

                    // run status icon
                    details.append('text')
                        .attr({
                            'class': 'run-status-icon',
                            'x': 42,
                            'y': 20
                        });

                    // processor type
                    details.append('text')
                        .attr({
                            'class': 'processor-type',
                            'x': 62,
                            'y': 35,
                            'width': 246,
                            'height': 16
                        });

                    // -----
                    // stats
                    // -----

                    // draw the processor statistics table

                    // in
                    details.append('rect')
                        .attr({
                            'width': function () {
                                return processorData.dimensions.width;
                            },
                            'height': 19,
                            'x': 0,
                            'y': 50,
                            'fill': '#f4f6f7'
                        });

                    // border
                    details.append('rect')
                        .attr({
                            'width': function () {
                                return processorData.dimensions.width;
                            },
                            'height': 1,
                            'x': 0,
                            'y': 68,
                            'fill': '#c7d2d7'
                        });

                    // read/write
                    details.append('rect')
                        .attr({
                            'width': function () {
                                return processorData.dimensions.width;
                            },
                            'height': 19,
                            'x': 0,
                            'y': 69,
                            'fill': '#ffffff'
                        });

                    // border
                    details.append('rect')
                        .attr({
                            'width': function () {
                                return processorData.dimensions.width;
                            },
                            'height': 1,
                            'x': 0,
                            'y': 87,
                            'fill': '#c7d2d7'
                        });

                    // out
                    details.append('rect')
                        .attr({
                            'width': function () {
                                return processorData.dimensions.width;
                            },
                            'height': 20,
                            'x': 0,
                            'y': 88,
                            'fill': '#f4f6f7'
                        });

                    // border
                    details.append('rect')
                        .attr({
                            'width': function () {
                                return processorData.dimensions.width;
                            },
                            'height': 1,
                            'x': 0,
                            'y': 106,
                            'fill': '#c7d2d7'
                        });

                    // tasks/time
                    details.append('rect')
                        .attr({
                            'width': function () {
                                return processorData.dimensions.width;
                            },
                            'height': 19,
                            'x': 0,
                            'y': 107,
                            'fill': '#ffffff'
                        });

                    // stats label container
                    var processorStatsLabel = details.append('g')
                        .attr({
                            'transform': 'translate(10, 55)'
                        });

                    // in label
                    processorStatsLabel.append('text')
                        .attr({
                            'width': 73,
                            'height': 10,
                            'y': 9,
                            'class': 'stats-label'
                        })
                        .text('In');

                    // read/write label
                    processorStatsLabel.append('text')
                        .attr({
                            'width': 73,
                            'height': 10,
                            'y': 27,
                            'class': 'stats-label'
                        })
                        .text('Read/Write');

                    // out label
                    processorStatsLabel.append('text')
                        .attr({
                            'width': 73,
                            'height': 10,
                            'y': 46,
                            'class': 'stats-label'
                        })
                        .text('Out');

                    // tasks/time label
                    processorStatsLabel.append('text')
                        .attr({
                            'width': 73,
                            'height': 10,
                            'y': 65,
                            'class': 'stats-label'
                        })
                        .text('Tasks/Time');

                    // stats value container
                    var processorStatsValue = details.append('g')
                        .attr({
                            'transform': 'translate(85, 55)'
                        });

                    // in value
                    var inText = processorStatsValue.append('text')
                        .attr({
                            'width': 180,
                            'height': 9,
                            'y': 9,
                            'class': 'processor-in stats-value'
                        });

                    // in count
                    inText.append('tspan')
                        .attr({
                            'class': 'count'
                        });

                    // in size
                    inText.append('tspan')
                        .attr({
                            'class': 'size'
                        });

                    // read/write value
                    processorStatsValue.append('text')
                        .attr({
                            'width': 180,
                            'height': 10,
                            'y': 27,
                            'class': 'processor-read-write stats-value'
                        });

                    // out value
                    var outText = processorStatsValue.append('text')
                        .attr({
                            'width': 180,
                            'height': 10,
                            'y': 46,
                            'class': 'processor-out stats-value'
                        });

                    // out count
                    outText.append('tspan')
                        .attr({
                            'class': 'count'
                        });

                    // out size
                    outText.append('tspan')
                        .attr({
                            'class': 'size'
                        });

                    // tasks/time value
                    processorStatsValue.append('text')
                        .attr({
                            'width': 180,
                            'height': 10,
                            'y': 65,
                            'class': 'processor-tasks-time stats-value'
                        });

                    // stats value container
                    var processorStatsInfo = details.append('g')
                        .attr('transform', 'translate(305, 55)');

                    // in info
                    processorStatsInfo.append('text')
                        .attr({
                            'width': 25,
                            'height': 10,
                            'y': 9,
                            'class': 'stats-info'
                        })
                        .text('5 min');

                    // read/write info
                    processorStatsInfo.append('text')
                        .attr({
                            'width': 25,
                            'height': 10,
                            'y': 27,
                            'class': 'stats-info'
                        })
                        .text('5 min');

                    // out info
                    processorStatsInfo.append('text')
                        .attr({
                            'width': 25,
                            'height': 10,
                            'y': 46,
                            'class': 'stats-info'
                        })
                        .text('5 min');

                    // tasks/time info
                    processorStatsInfo.append('text')
                        .attr({
                            'width': 25,
                            'height': 10,
                            'y': 65,
                            'class': 'stats-info'
                        })
                        .text('5 min');

                    // -------------------
                    // active thread count
                    // -------------------

                    // active thread count
                    details.append('text')
                        .attr({
                            'class': 'active-thread-count-icon',
                            'y': 42
                        })
                        .text('\ue83f');

                    // active thread background
                    details.append('text')
                        .attr({
                            'class': 'active-thread-count',
                            'y': 42
                        });

                    // ---------
                    // bulletins
                    // ---------

                    // bulletin background
                    details.append('rect')
                        .attr({
                            'class': 'bulletin-background',
                            'x': function (d) {
                                return processorData.dimensions.width - 24;
                            },
                            'width': 24,
                            'height': 24
                        });

                    // bulletin icon
                    details.append('text')
                        .attr({
                            'class': 'bulletin-icon',
                            'x': function (d) {
                                return processorData.dimensions.width - 17;
                            },
                            'y': 17
                        })
                        .text('\uf24a');
                }

                if (processorData.permissions.canRead) {
                    // update the processor name
                    processor.select('text.processor-name')
                        .each(function (d) {
                            var processorName = d3.select(this);

                            // reset the processor name to handle any previous state
                            processorName.text(null).selectAll('title').remove();

                            // apply ellipsis to the processor name as necessary
                            nf.CanvasUtils.ellipsis(processorName, d.component.name);
                        }).append('title').text(function (d) {
                        return d.component.name;
                    });

                    // update the processor type
                    processor.select('text.processor-type')
                        .each(function (d) {
                            var processorType = d3.select(this);

                            // reset the processor type to handle any previous state
                            processorType.text(null).selectAll('title').remove();

                            // apply ellipsis to the processor type as necessary
                            nf.CanvasUtils.ellipsis(processorType, nf.Common.substringAfterLast(d.component.type, '.'));
                        }).append('title').text(function (d) {
                        return nf.Common.substringAfterLast(d.component.type, '.');
                    });
                } else {
                    // clear the processor name
                    processor.select('text.processor-name').text(null);

                    // clear the processor type
                    processor.select('text.processor-type').text(null);
                }

                // hide the preview
                processor.select('image.processor-stats-preview').style('display', 'none');

                // populate the stats
                processor.call(updateProcessorStatus);
            } else {
                if (processorData.permissions.canRead) {
                    // update the processor name
                    processor.select('text.processor-name')
                        .text(function (d) {
                            var name = d.component.name;
                            if (name.length > PREVIEW_NAME_LENGTH) {
                                return name.substring(0, PREVIEW_NAME_LENGTH) + String.fromCharCode(8230);
                            } else {
                                return name;
                            }
                        });
                }

                // show the preview
                processor.select('image.processor-stats-preview').style('display', 'block');

                // remove the tooltips
                processor.call(removeTooltips);

                // remove the details if necessary
                if (!details.empty()) {
                    details.remove();
                }
            }
        });
        
        // ---------------
        // processor color
        // ---------------

        // update the processor color
        updated.select('text.processor-icon')
            .style('fill', function (d) {
                
                // get the default color
                var color = nf.Processor.defaultColor();
                
                if (!d.permissions.canRead) {
                    return color;
                }

                // use the specified color if appropriate
                if (nf.Common.isDefinedAndNotNull(d.component.style['background-color'])) {
                    color = d.component.style['background-color'];
                }

                return color;
            });
    };

    /**
     * Updates the stats for the processors in the specified selection.
     *
     * @param {selection} updated           The processors to update
     */
    var updateProcessorStatus = function (updated) {
        if (updated.empty()) {
            return;
        }

        // update the run status
        updated.select('text.run-status-icon')
            .attr({
                'fill': function (d) {
                    var fill = '#728e9b';
                    if (d.status.aggregateSnapshot.runStatus === 'Invalid') {
                        fill = '#ba554a';
                    }
                    return fill;
                },
                'font-family': function (d) {
                    var family = 'FontAwesome';
                    if (d.status.aggregateSnapshot.runStatus === 'Disabled') {
                        family = 'flowfont';
                    }
                    return family;
                }
            })
            .text(function (d) {
                var img = '';
                if (d.status.aggregateSnapshot.runStatus === 'Disabled') {
                    img = '\ue802';
                } else if (d.status.aggregateSnapshot.runStatus === 'Invalid') {
                    img = '\uf071';
                } else if (d.status.aggregateSnapshot.runStatus === 'Running') {
                    img = '\uf04b';
                } else if (d.status.aggregateSnapshot.runStatus === 'Stopped') {
                    img = '\uf04d';
                }
                return img;
            })
            .each(function (d) {
                // remove the existing tip if necessary
                var tip = d3.select('#run-status-tip-' + d.id);
                if (!tip.empty()) {
                    tip.remove();
                }

                // if there are validation errors generate a tooltip
                if (d.permissions.canRead && !nf.Common.isEmpty(d.component.validationErrors)) {
                    tip = d3.select('#processor-tooltips').append('div')
                        .attr('id', function () {
                            return 'run-status-tip-' + d.id;
                        })
                        .attr('class', 'tooltip nifi-tooltip')
                        .html(function () {
                            var list = nf.Common.formatUnorderedList(d.component.validationErrors);
                            if (list === null || list.length === 0) {
                                return '';
                            } else {
                                return $('<div></div>').append(list).html();
                            }
                        });

                    // add the tooltip
                    nf.CanvasUtils.canvasTooltip(tip, d3.select(this));
                }
            });

        // in count value
        updated.select('text.processor-in tspan.count')
            .text(function (d) {
                return nf.Common.substringBeforeFirst(d.status.aggregateSnapshot.input, ' ');
            });

        // in size value
        updated.select('text.processor-in tspan.size')
            .text(function (d) {
                return ' ' + nf.Common.substringAfterFirst(d.status.aggregateSnapshot.input, ' ');
            });

        // read/write value
        updated.select('text.processor-read-write')
            .text(function (d) {
                return d.status.aggregateSnapshot.read + ' / ' + d.status.aggregateSnapshot.written;
            });

        // out count value
        updated.select('text.processor-out tspan.count')
            .text(function (d) {
                return nf.Common.substringBeforeFirst(d.status.aggregateSnapshot.output, ' ');
            });

        // out size value
        updated.select('text.processor-out tspan.size')
            .text(function (d) {
                return ' ' + nf.Common.substringAfterFirst(d.status.aggregateSnapshot.output, ' ');
            });

        // tasks/time value
        updated.select('text.processor-tasks-time')
            .text(function (d) {
                return d.status.aggregateSnapshot.tasks + ' / ' + d.status.aggregateSnapshot.tasksDuration;
            });

        updated.each(function (d) {
            var processor = d3.select(this);

            // -------------------
            // active thread count
            // -------------------

            nf.CanvasUtils.activeThreadCount(processor, d);

            // ---------
            // bulletins
            // ---------

            processor.select('rect.bulletin-background').classed('has-bulletins', function () {
                return !nf.Common.isEmpty(d.status.aggregateSnapshot.bulletins);
            });

            nf.CanvasUtils.bulletins(processor, d, function () {
                return d3.select('#processor-tooltips');
            }, 286);
        });
    };

    /**
     * Removes the processors in the specified selection.
     *
     * @param {selection} removed
     */
    var removeProcessors = function (removed) {
        if (removed.empty()) {
            return;
        }

        removed.call(removeTooltips).remove();
    };

    /**
     * Removes the tooltips for the processors in the specified selection.
     *
     * @param {selection} removed
     */
    var removeTooltips = function (removed) {
        removed.each(function (d) {
            // remove any associated tooltips
            $('#run-status-tip-' + d.id).remove();
            $('#bulletin-tip-' + d.id).remove();
        });
    };

    return {
        /**
         * Initializes of the Processor handler.
         */
        init: function () {
            processorMap = d3.map();

            // create the processor container
            processorContainer = d3.select('#canvas').append('g')
                .attr({
                    'pointer-events': 'all',
                    'class': 'processors'
                });
        },

        /**
         * Adds the specified processor entity.
         *
         * @param processorEntities       The processor
         * @param options           Configuration options
         */
        add: function (processorEntities, options) {
            var selectAll = false;
            if (nf.Common.isDefinedAndNotNull(options)) {
                selectAll = nf.Common.isDefinedAndNotNull(options.selectAll) ? options.selectAll : selectAll;
            }

            var add = function (processorEntity) {
                // add the processor
                processorMap.set(processorEntity.id, $.extend({
                    type: 'Processor',
                    dimensions: dimensions
                }, processorEntity));
            };

            // determine how to handle the specified processor
            if ($.isArray(processorEntities)) {
                $.each(processorEntities, function (_, processorEntity) {
                    add(processorEntity);
                });
            } else if (nf.Common.isDefinedAndNotNull(processorEntities)) {
                add(processorEntities);
            }

            // apply the selection and handle new processor
            var selection = select();
            selection.enter().call(renderProcessors, selectAll);
            selection.call(updateProcessors);
        },

        /**
         * Populates the graph with the specified processors.
         *
         * @argument {object | array} processorEntities                The processors to add
         * @argument {object} options                Configuration options
         */
        set: function (processorEntities, options) {
            var selectAll = false;
            var transition = false;
            if (nf.Common.isDefinedAndNotNull(options)) {
                selectAll = nf.Common.isDefinedAndNotNull(options.selectAll) ? options.selectAll : selectAll;
                transition = nf.Common.isDefinedAndNotNull(options.transition) ? options.transition : transition;
            }

            var set = function (processorEntity) {
                // add the processor
                processorMap.set(processorEntity.id, $.extend({
                    type: 'Processor',
                    dimensions: dimensions
                }, processorEntity));
            };

            // determine how to handle the specified processor
            if ($.isArray(processorEntities)) {
                $.each(processorMap.keys(), function (_, key) {
                    processorMap.remove(key);
                });
                $.each(processorEntities, function (_, processorEntity) {
                    set(processorEntity);
                });
            } else if (nf.Common.isDefinedAndNotNull(processorEntities)) {
                set(processorEntities);
            }

            // apply the selection and handle all new processors
            var selection = select();
            selection.enter().call(renderProcessors, selectAll);
            selection.call(updateProcessors).call(nf.CanvasUtils.position, transition);
            selection.exit().call(removeProcessors);
        },

        /**
         * If the processor id is specified it is returned. If no processor id
         * specified, all processors are returned.
         *
         * @param {string} id
         */
        get: function (id) {
            if (nf.Common.isUndefined(id)) {
                return processorMap.values();
            } else {
                return processorMap.get(id);
            }
        },

        /**
         * If the processor id is specified it is refresh according to the current
         * state. If not processor id is specified, all processors are refreshed.
         *
         * @param {string} id      Optional
         */
        refresh: function (id) {
            if (nf.Common.isDefinedAndNotNull(id)) {
                d3.select('#id-' + id).call(updateProcessors);
            } else {
                d3.selectAll('g.processor').call(updateProcessors);
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
         * Refreshes the components necessary after a pan event.
         */
        pan: function () {
            d3.selectAll('g.processor.entering, g.processor.leaving').call(updateProcessors);
        },

        /**
         * Reloads the processor state from the server and refreshes the UI.
         * If the processor is currently unknown, this function just returns.
         *
         * @param {string} id The processor id
         */
        reload: function (id) {
            if (processorMap.has(id)) {
                var processorEntity = processorMap.get(id);
                return $.ajax({
                    type: 'GET',
                    url: processorEntity.uri,
                    dataType: 'json'
                }).done(function (response) {
                    nf.Processor.set(response);
                });
            }
        },

        /**
         * Removes the specified processor.
         *
         * @param {array|string} processors      The processors
         */
        remove: function (processors) {
            if ($.isArray(processors)) {
                $.each(processors, function (_, processor) {
                    processorMap.remove(processor);
                });
            } else {
                processorMap.remove(processors);
            }

            // apply the selection and handle all removed processors
            select().exit().call(removeProcessors);
        },

        /**
         * Removes all processors.
         */
        removeAll: function () {
            nf.Processor.remove(processorMap.keys());
        },

        /**
         * Returns the default color that should be used when drawing a processor.
         */
        defaultColor: function () {
            return '#ad9897';
        }
    };
}());