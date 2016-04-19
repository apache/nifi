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
        width: 310,
        height: 100
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
            return d.component.id;
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
                        return 'id-' + d.component.id;
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
                    'stroke-opacity': 0.8,
                    'stroke-width': 1
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
                    'fill-opacity': 0.8,
                    'stroke-opacity': 0.8,
                    'stroke-width': 0
                });

        // processor name
        processor.append('text')
                .attr({
                    'x': 25,
                    'y': 18,
                    'width': 220,
                    'height': 16,
                    'font-size': '10pt',
                    'font-weight': 'bold',
                    'fill': 'black',
                    'class': 'processor-name'
                });

        // processor icon
        processor.append('image')
                .call(nf.CanvasUtils.disableImageHref)
                .attr({
                    'xlink:href': 'images/iconProcessor.png',
                    'width': 28,
                    'height': 26,
                    'x': 276,
                    'y': 5
                });

        // processor stats preview
        processor.append('image')
                .call(nf.CanvasUtils.disableImageHref)
                .attr({
                    'xlink:href': 'images/bgProcessorStatArea.png',
                    'width': 294,
                    'height': 58,
                    'x': 8,
                    'y': 35,
                    'class': 'processor-stats-preview'
                });

        // make processors selectable
        processor.call(nf.Selectable.activate).call(nf.ContextMenu.activate);

        // only activate dragging and connecting if appropriate
        if (nf.Common.isDFM()) {
            processor.call(nf.Draggable.activate).call(nf.Connectable.activate);
        }

        // call update to trigger some rendering
        processor.call(updateProcessors);
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

        updated.each(function () {
            var processor = d3.select(this);
            var details = processor.select('g.processor-details');

            // if this processor is visible, render everything
            if (processor.classed('visible')) {
                if (details.empty()) {
                    details = processor.append('g').attr('class', 'processor-details');

                    // run status icon
                    details.append('image')
                            .call(nf.CanvasUtils.disableImageHref)
                            .attr({
                                'class': 'run-status-icon',
                                'width': 16,
                                'height': 16,
                                'x': 5,
                                'y': 5
                            });

                    // processor type
                    details.append('text')
                            .attr({
                                'x': 25,
                                'y': 30,
                                'width': 246,
                                'height': 16,
                                'font-size': '8pt',
                                'font-weight': 'normal',
                                'fill': 'black'
                            })
                            .each(function (d) {
                                var processorType = d3.select(this);

                                // reset the processor type to handle any previous state
                                processorType.text(null).selectAll('title').remove();

                                // apply ellipsis to the processor type as necessary
                                nf.CanvasUtils.ellipsis(processorType, nf.Common.substringAfterLast(d.component.type, '.'));
                            }).append('title').text(function (d) {
                        return nf.Common.substringAfterLast(d.component.type, '.');
                    });

                    // -----
                    // stats
                    // -----

                    // draw the processor statistics table
                    details.append('rect')
                            .attr({
                                'width': 294,
                                'height': 59,
                                'x': 8,
                                'y': 35,
                                'fill': '#ffffff',
                                'stroke-width': 1,
                                'stroke': '#6f97ac',
                                'stroke-opacity': 0.8
                            });

                    details.append('rect')
                            .attr({
                                'width': 73,
                                'height': 59,
                                'x': 8,
                                'y': 35,
                                'fill': 'url(#processor-stats-background)',
                                'stroke-width': 0
                            });

                    // stats label container
                    var processorStatsLabel = details.append('g')
                            .attr({
                                'transform': 'translate(8, 45)'
                            });

                    // in label
                    processorStatsLabel.append('text')
                            .attr({
                                'width': 73,
                                'height': 10,
                                'x': 4,
                                'y': 4,
                                'class': 'processor-stats-label'
                            })
                            .text('In');

                    // read/write label
                    processorStatsLabel.append('text')
                            .attr({
                                'width': 73,
                                'height': 10,
                                'x': 4,
                                'y': 17,
                                'class': 'processor-stats-label'
                            })
                            .text('Read/Write');

                    // out label
                    processorStatsLabel.append('text')
                            .attr({
                                'width': 73,
                                'height': 10,
                                'x': 4,
                                'y': 30,
                                'class': 'processor-stats-label'
                            })
                            .text('Out');

                    // tasks/time label
                    processorStatsLabel.append('text')
                            .attr({
                                'width': 73,
                                'height': 10,
                                'x': 4,
                                'y': 43,
                                'class': 'processor-stats-label'
                            })
                            .text('Tasks/Time');

                    // stats value container
                    var processorStatsValue = details.append('g')
                            .attr({
                                'transform': 'translate(80, 45)'
                            });

                    // in value
                    processorStatsValue.append('text')
                            .attr({
                                'width': 180,
                                'height': 10,
                                'x': 4,
                                'y': 4,
                                'class': 'processor-in processor-stats-value'
                            });

                    // read/write value
                    processorStatsValue.append('text')
                            .attr({
                                'width': 180,
                                'height': 10,
                                'x': 4,
                                'y': 17,
                                'class': 'processor-read-write processor-stats-value'
                            });

                    // out value
                    processorStatsValue.append('text')
                            .attr({
                                'width': 180,
                                'height': 10,
                                'x': 4,
                                'y': 30,
                                'class': 'processor-out processor-stats-value'
                            });

                    // tasks/time value
                    processorStatsValue.append('text')
                            .attr({
                                'width': 180,
                                'height': 10,
                                'x': 4,
                                'y': 43,
                                'class': 'processor-tasks-time processor-stats-value'
                            });

                    // stats value container
                    var processorStatsInfo = details.append('g')
                            .attr('transform', 'translate(258, 45)');

                    // in info
                    processorStatsInfo.append('text')
                            .attr({
                                'width': 25,
                                'height': 10,
                                'x': 4,
                                'y': 4,
                                'class': 'processor-stats-info'
                            })
                            .text('(5 min)');

                    // read/write info
                    processorStatsInfo.append('text')
                            .attr({
                                'width': 25,
                                'height': 10,
                                'x': 4,
                                'y': 17,
                                'class': 'processor-stats-info'
                            })
                            .text('(5 min)');

                    // out info
                    processorStatsInfo.append('text')
                            .attr({
                                'width': 25,
                                'height': 10,
                                'x': 4,
                                'y': 30,
                                'class': 'processor-stats-info'
                            })
                            .text('(5 min)');

                    // tasks/time info
                    processorStatsInfo.append('text')
                            .attr({
                                'width': 25,
                                'height': 10,
                                'x': 4,
                                'y': 43,
                                'class': 'processor-stats-info'
                            })
                            .text('(5 min)');

                    // -------------------
                    // active thread count
                    // -------------------

                    // active thread count
                    details.append('rect')
                            .attr({
                                'class': 'active-thread-count-background',
                                'height': 13,
                                'y': 0,
                                'fill': '#fff',
                                'fill-opacity': '0.65',
                                'stroke': '#aaa',
                                'stroke-width': '1'
                            });

                    // active thread bacground
                    details.append('text')
                            .attr({
                                'class': 'active-thread-count',
                                'height': 13,
                                'y': 10,
                                'fill': '#000'
                            });

                    // ---------
                    // bulletins
                    // ---------

                    // bulletin icon
                    details.append('image')
                            .call(nf.CanvasUtils.disableImageHref)
                            .attr({
                                'class': 'bulletin-icon',
                                'xlink:href': 'images/iconBulletin.png',
                                'width': 12,
                                'height': 12,
                                'x': 8,
                                'y': 20
                            });
                }

                // update the run status
                details.select('image.run-status-icon')
                        .attr('xlink:href', function (d) {
                            var img = '';
                            if (d.component.state === 'DISABLED') {
                                img = 'images/iconDisable.png';
                            } else if (!nf.Common.isEmpty(d.component.validationErrors)) {
                                img = 'images/iconAlert.png';
                            } else if (d.component.state === 'RUNNING') {
                                img = 'images/iconRun.png';
                            } else if (d.component.state === 'STOPPED') {
                                img = 'images/iconStop.png';
                            }
                            return img;
                        })
                        .each(function (d) {
                            // remove the existing tip if necessary
                            var tip = d3.select('#run-status-tip-' + d.component.id);
                            if (!tip.empty()) {
                                tip.remove();
                            }

                            // if there are validation errors generate a tooltip
                            if (!nf.Common.isEmpty(d.component.validationErrors)) {
                                tip = d3.select('#processor-tooltips').append('div')
                                        .attr('id', function () {
                                            return 'run-status-tip-' + d.component.id;
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

                // hide the preview
                processor.select('image.processor-stats-preview').style('display', 'none');

                // populate the stats
                processor.call(updateProcessorStatus);
            } else {
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

        // -----------------------------------------------------------------
        // ensure there is a linear gradient defined for each possible color
        // -----------------------------------------------------------------

        // reset the colors
        var colors = d3.set();
        colors.add(nf.Common.substringAfterLast(nf.Processor.defaultColor(), '#'));

        // determine all unique colors
        processorMap.forEach(function (id, d) {
            var color = d.component.style['background-color'];
            if (nf.Common.isDefinedAndNotNull(color)) {
                colors.add(nf.Common.substringAfterLast(color, '#'));
            }
        });
        nf.Canvas.defineProcessorColors(colors.values());

        // update the processor color
        updated.select('rect.border')
                .attr('stroke', function (d) {
                    var color = nf.Processor.defaultColor();

                    // use the specified color if appropriate
                    if (nf.Common.isDefinedAndNotNull(d.component.style['background-color'])) {
                        color = d.component.style['background-color'];
                    }

                    return color;
                });

        updated.select('rect.body')
                .attr('fill', function (d) {
                    var color = nf.Processor.defaultColor();

                    // use the specified color if appropriate
                    if (nf.Common.isDefinedAndNotNull(d.component.style['background-color'])) {
                        color = d.component.style['background-color'];
                    }

                    // get just the color code part
                    color = nf.Common.substringAfterLast(color, '#');

                    return 'url(#processor-background-' + color + ')';
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

        // in value
        updated.select('text.processor-in')
                .text(function (d) {
                    if (nf.Common.isDefinedAndNotNull(d.status)) {
                        return d.status.input;
                    } else {
                        return '- / -';
                    }
                });

        // read/write value
        updated.select('text.processor-read-write')
                .text(function (d) {
                    if (nf.Common.isDefinedAndNotNull(d.status)) {
                        return d.status.read + ' / ' + d.status.written;
                    } else {
                        return '- / -';
                    }
                });

        // out value
        updated.select('text.processor-out')
                .text(function (d) {
                    if (nf.Common.isDefinedAndNotNull(d.status)) {
                        return d.status.output;
                    } else {
                        return '- / -';
                    }
                });

        // tasks/time value
        updated.select('text.processor-tasks-time')
                .text(function (d) {
                    if (nf.Common.isDefinedAndNotNull(d.status)) {
                        return d.status.tasks + ' / ' + d.status.tasksDuration;
                    } else {
                        return '- / -';
                    }
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
            $('#run-status-tip-' + d.component.id).remove();
            $('#bulletin-tip-' + d.component.id).remove();
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
         * Populates the graph with the specified processors.
         * 
         * @argument {object | array} processors                The processors to add
         * @argument {boolean} selectAll                Whether or not to select the new contents
         */
        add: function (processors, selectAll) {
            selectAll = nf.Common.isDefinedAndNotNull(selectAll) ? selectAll : false;

            var add = function (processor) {
                // add the processor
                processorMap.set(processor.id, {
                    type: 'Processor',
                    component: processor,
                    dimensions: dimensions
                });
            };

            // determine how to handle the specified processor
            if ($.isArray(processors)) {
                $.each(processors, function (_, processor) {
                    add(processor);
                });
            } else {
                add(processors);
            }

            // apply the selection and handle all new processors
            select().enter().call(renderProcessors, selectAll);
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
         * @param {object} processor The processor to reload
         */
        reload: function (processor) {
            if (processorMap.has(processor.id)) {
                return $.ajax({
                    type: 'GET',
                    url: processor.uri,
                    dataType: 'json'
                }).done(function (response) {
                    nf.Processor.set(response.processor);
                });
            }
        },
        
        /**
         * Sets the specified processor(s). If the is an array, it 
         * will set each processor. If it is not an array, it will 
         * attempt to set the specified processor.
         * 
         * @param {object | array} processors
         */
        set: function (processors) {
            var set = function (processor) {
                if (processorMap.has(processor.id)) {
                    // update the current entry
                    var processorEntry = processorMap.get(processor.id);
                    processorEntry.component = processor;

                    // update the processor in the UI
                    d3.select('#id-' + processor.id).call(updateProcessors);
                }
            };

            // determine how to handle the specified processor
            if ($.isArray(processors)) {
                $.each(processors, function (_, processor) {
                    set(processor);
                });
            } else {
                set(processors);
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
         * Sets the processor status using the specified status.
         * 
         * @param {array} processorStatus       Processor status
         */
        setStatus: function (processorStatus) {
            if (nf.Common.isEmpty(processorStatus)) {
                return;
            }

            // update the specified processor status
            $.each(processorStatus, function (_, status) {
                if (processorMap.has(status.id)) {
                    var processor = processorMap.get(status.id);
                    processor.status = status;
                }
            });

            // update the visible processor status
            d3.selectAll('g.processor.visible').call(updateProcessorStatus);
        },

        /**
         * Returns the entity key when marshalling an entity of this type.
         */
        getEntityKey: function (d) {
            return 'processor';
        },
        
        /**
         * Returns the default color that should be used when drawing a processor.
         */
        defaultColor: function () {
            return '#aaaaaa';
        }
    };
}());