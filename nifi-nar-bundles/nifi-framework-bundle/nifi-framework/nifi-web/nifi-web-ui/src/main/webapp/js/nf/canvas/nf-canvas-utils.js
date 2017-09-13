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
                'jquery',
                'nf.Common',
                'nf.ErrorHandler',
                'nf.Dialog',
                'nf.Clipboard',
                'nf.Storage'],
            function (d3, $, nfCommon, nfErrorHandler, nfDialog, nfClipboard, nfStorage) {
                return (nf.CanvasUtils = factory(d3, $, nfCommon, nfErrorHandler, nfDialog, nfClipboard, nfStorage));
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.CanvasUtils = factory(
            require('d3'),
            require('jquery'),
            require('nf.Common'),
            require('nf.ErrorHandler'),
            require('nf.Dialog'),
            require('nf.Clipboard'),
            require('nf.Storage')));
    } else {
        nf.CanvasUtils = factory(
            root.d3,
            root.$,
            root.nf.Common,
            root.nf.ErrorHandler,
            root.nf.Dialog,
            root.nf.Clipboard,
            root.nf.Storage);
    }
}(this, function (d3, $, nfCommon, nfErrorHandler, nfDialog, nfClipboard, nfStorage) {
    'use strict';

    var nfCanvas;
    var nfActions;
    var nfSnippet;
    var nfBirdseye;
    var nfGraph;

    var config = {
        storage: {
            namePrefix: 'nifi-view-'
        },
        urls: {
            controller: '../nifi-api/controller'
        }
    };

    var TWO_PI = 2 * Math.PI;

    var binarySearch = function (length, comparator) {
        var low = 0;
        var high = length - 1;
        var mid;

        var result = 0;
        while (low <= high) {
            mid = ~~((low + high) / 2);
            result = comparator(mid);
            if (result < 0) {
                high = mid - 1;
            } else if (result > 0) {
                low = mid + 1;
            } else {
                break;
            }
        }

        return mid;
    };

    var moveComponents = function (components, groupId) {
        return $.Deferred(function (deferred) {
            var parentGroupId = nfCanvasUtils.getGroupId();

            // create a snippet for the specified components
            var snippet = nfSnippet.marshal(components, parentGroupId);
            nfSnippet.create(snippet).done(function (response) {
                // move the snippet into the target
                nfSnippet.move(response.snippet.id, groupId).done(function () {
                    var componentMap = d3.map();

                    // add the id to the type's array
                    var addComponent = function (type, id) {
                        if (!componentMap.has(type)) {
                            componentMap.set(type, []);
                        }
                        componentMap.get(type).push(id);
                    };

                    // go through each component being removed
                    components.each(function (d) {
                        addComponent(d.type, d.id);
                    });

                    // refresh all component types as necessary (handle components that have been removed)
                    componentMap.forEach(function (type, ids) {
                        nfCanvasUtils.getComponentByType(type).remove(ids);
                    });

                    // refresh the birdseye
                    nfBirdseye.refresh();

                    deferred.resolve();
                }).fail(nfErrorHandler.handleAjaxError).fail(function () {
                    deferred.reject();
                });
            }).fail(nfErrorHandler.handleAjaxError).fail(function () {
                deferred.reject();
            });
        }).promise();
    };

    var nfCanvasUtils = {

        /**
         * Initialize the canvas utils.
         *
         * @param nfCanvasRef   The nfCanvas module.
         * @param nfActionsRef   The nfActions module.
         * @param nfSnippetRef   The nfSnippet module.
         * @param nfBirdseyeRef   The nfBirdseye module.
         * @param nfGraphRef   The nfGraph module.
         */
        init: function(nfCanvasRef, nfActionsRef, nfSnippetRef, nfBirdseyeRef, nfGraphRef){
            nfCanvas = nfCanvasRef;
            nfActions = nfActionsRef;
            nfSnippet = nfSnippetRef;
            nfBirdseye = nfBirdseyeRef;
            nfGraph = nfGraphRef;
        },

        config: {
            systemTooltipConfig: {
                style: {
                    classes: 'nifi-tooltip'
                },
                show: {
                    solo: true,
                    effect: false
                },
                hide: {
                    effect: false
                },
                position: {
                    at: 'bottom right',
                    my: 'top left'
                }
            }
        },

        /**
         * Gets a graph component `type`.
         *
         * @param type  The type of component.
         */
        getComponentByType: function (type) {
            return nfGraph.getComponentByType(type);
        },

        /**
         * Calculates the point on the specified bounding box that is closest to the
         * specified point.
         *
         * @param {object} p            The point
         * @param {object} bBox         The bounding box
         */
        getPerimeterPoint: function (p, bBox) {
            // calculate theta
            var theta = Math.atan2(bBox.height, bBox.width);

            // get the rectangle radius
            var xRadius = bBox.width / 2;
            var yRadius = bBox.height / 2;

            // get the center point
            var cx = bBox.x + xRadius;
            var cy = bBox.y + yRadius;

            // calculate alpha
            var dx = p.x - cx;
            var dy = p.y - cy;
            var alpha = Math.atan2(dy, dx);

            // normalize aphla into 0 <= alpha < 2 PI
            alpha = alpha % TWO_PI;
            if (alpha < 0) {
                alpha += TWO_PI;
            }

            // calculate beta
            var beta = (Math.PI / 2) - alpha;

            // detect the appropriate quadrant and return the point on the perimeter
            if ((alpha >= 0 && alpha < theta) || (alpha >= (TWO_PI - theta) && alpha < TWO_PI)) {
                // right quadrant
                return {
                    'x': bBox.x + bBox.width,
                    'y': cy + Math.tan(alpha) * xRadius
                };
            } else if (alpha >= theta && alpha < (Math.PI - theta)) {
                // bottom quadrant
                return {
                    'x': cx + Math.tan(beta) * yRadius,
                    'y': bBox.y + bBox.height
                };
            } else if (alpha >= (Math.PI - theta) && alpha < (Math.PI + theta)) {
                // left quadrant
                return {
                    'x': bBox.x,
                    'y': cy - Math.tan(alpha) * xRadius
                };
            } else {
                // top quadrant
                return {
                    'x': cx - Math.tan(beta) * yRadius,
                    'y': bBox.y
                };
            }
        },

        /**
         * Queries for bulletins for the specified components.
         *
         * @param {array} componentIds
         * @returns {deferred}
         */
        queryBulletins: function (componentIds) {
            var ids = componentIds.join('|');

            return $.ajax({
                type: 'GET',
                url: '../nifi-api/flow/bulletin-board',
                data: {
                    sourceId: ids
                },
                dataType: 'json'
            }).fail(nfErrorHandler.handleAjaxError);
        },

        /**
         * Shows the specified component in the specified group.
         *
         * @param {string} groupId       The id of the group
         * @param {string} componentId   The id of the component
         */
        showComponent: function (groupId, componentId) {
            // ensure the group id is specified
            if (nfCommon.isDefinedAndNotNull(groupId)) {
                // initiate a graph refresh
                var refreshGraph = $.Deferred(function (deferred) {
                    // load a different group if necessary
                    if (groupId !== nfCanvas.getGroupId()) {
                        // set the new group id
                        nfCanvas.setGroupId(groupId);

                        // reload
                        nfCanvas.reload().done(function () {
                            deferred.resolve();
                        }).fail(function (xhr, status, error) {
                            nfDialog.showOkDialog({
                                headerText: 'Error',
                                dialogContent: 'Unable to load the group for the specified component.'
                            });
                            deferred.reject(xhr, status, error);
                        });
                    } else {
                        deferred.resolve();
                    }
                }).promise();

                // when the refresh has completed, select the match
                refreshGraph.done(function () {
                    // attempt to locate the corresponding component
                    var component = d3.select('#id-' + componentId);
                    if (!component.empty()) {
                        nfActions.show(component);
                    } else {
                        nfDialog.showOkDialog({
                            headerText: 'Error',
                            dialogContent: 'Unable to find the specified component.'
                        });
                    }
                });

                return refreshGraph;
            }
        },

        /**
         * Displays the URL deep link on the canvas.
         *
         * @param forceCanvasLoad   Boolean enabling the update of the URL parameters.
         */
        showDeepLink: function (forceCanvasLoad) {
            // deselect components
            nfCanvasUtils.getSelection().classed('selected', false);

            // close the ok dialog if open
            if ($('#nf-ok-dialog').is(':visible') === true) {
                $('#nf-ok-dialog').modal('hide');
            }

            // Feature detection and browser support for URLSearchParams
            if ('URLSearchParams' in window) {
                // get the `urlSearchParams` from the URL
                var urlSearchParams = new URL(window.location).searchParams;
                // if the `urlSearchParams` are `undefined` then the browser does not support
                // the URL object's `.searchParams` property
                if (!nf.Common.isDefinedAndNotNull(urlSearchParams)) {
                    // attempt to get the `urlSearchParams` using the URLSearchParams constructor and
                    // the URL object's `.search` property
                    urlSearchParams = new URLSearchParams(new URL(window.location).search);
                }

                var groupId = nfCanvasUtils.getGroupId();

                // if the `urlSearchParams` are still `undefined` then the browser does not support
                // the URL object's `.search` property. In this case we cannot support deep links.
                if (nf.Common.isDefinedAndNotNull(urlSearchParams)) {
                    var componentIds = [];

                    if (urlSearchParams.get('processGroupId')) {
                        groupId = urlSearchParams.get('processGroupId');
                    }
                    if (urlSearchParams.get('componentIds')) {
                        componentIds = urlSearchParams.get('componentIds').split(',');
                    }

                    // load the graph but do not update the browser history
                    if (componentIds.length >= 1) {
                        return nfCanvasUtils.showComponents(groupId, componentIds, forceCanvasLoad);
                    } else {
                        return nfCanvasUtils.getComponentByType('ProcessGroup').enterGroup(groupId);
                    }
                } else {
                    return nfCanvasUtils.getComponentByType('ProcessGroup').enterGroup(groupId);
                }
            }
        },

        /**
         * Shows the specified components in the specified group.
         *
         * @param {string} groupId       The id of the group
         * @param {array} componentIds   The ids of the components
         * @param {bool} forceCanvasLoad   Boolean to force reload of the canvas.
         */
        showComponents: function (groupId, componentIds, forceCanvasLoad) {
            // ensure the group id is specified
            if (nfCommon.isDefinedAndNotNull(groupId)) {
                // initiate a graph refresh
                var refreshGraph = $.Deferred(function (deferred) {
                    // load a different group if necessary
                    if (groupId !== nfCanvas.getGroupId() || forceCanvasLoad) {
                        // set the new group id
                        nfCanvas.setGroupId(groupId);

                        // reload
                        nfCanvas.reload().done(function () {
                            deferred.resolve();
                        }).fail(function (xhr, status, error) {
                            nfDialog.showOkDialog({
                                headerText: 'Error',
                                dialogContent: 'Unable to enter the selected group.'
                            });

                            deferred.reject(xhr, status, error);
                        });
                    } else {
                        deferred.resolve();
                    }
                }).promise();

                // when the refresh has completed, select the match
                refreshGraph.done(function () {
                    // get the components to select
                    var components = d3.selectAll('g.component, g.connection').filter(function (d) {
                        if (componentIds.indexOf(d.id) >= 0) {
                            // remove located components from array so that only unfound components will remain
                            componentIds.splice(componentIds.indexOf(d.id), 1);
                            return d;
                        }
                    });

                    if (componentIds.length > 0) {
                        var dialogContent = $('<p></p>').text('Specified component(s) not found: ' + componentIds.join(', ') + '.').append('<br/><br/>').append($('<p>Unable to select component(s).</p>'));

                        nfDialog.showOkDialog({
                            headerText: 'Error',
                            dialogContent: dialogContent
                        });
                    }

                    nfActions.show(components);
                });

                return refreshGraph;
            }
        },

        MAX_URL_LENGTH: 2000,  // the maximum (suggested) safe string length of a URL supported by all browsers and application servers

        /**
         * Set the parameters of the URL.
         *
         * @param groupId       The process group id.
         * @param selections    The component ids.
         */
        setURLParameters: function (groupId, selections) {
            // Feature detection and browser support for URLSearchParams
            if ('URLSearchParams' in window) {
                if (!nfCommon.isDefinedAndNotNull(groupId)) {
                    groupId = nfCanvasUtils.getGroupId();
                }

                if (!nfCommon.isDefinedAndNotNull(selections)) {
                    selections = nfCanvasUtils.getSelection();
                }

                var selectedComponentIds = [];
                selections.each(function (selection) {
                    selectedComponentIds.push(selection.id);
                });

                // get all URL parameters
                var url = new URL(window.location);

                // get the `params` from the URL
                var params = new URL(window.location).searchParams;
                // if the `params` are undefined then the browser does not support
                // the URL object's `.searchParams` property
                if (!nf.Common.isDefinedAndNotNull(params)) {
                    // attempt to get the `params` using the URLSearchParams constructor and
                    // the URL object's `.search` property
                    params = new URLSearchParams(url.search);
                }

                // if the `params` are still `undefined` then the browser does not support
                // the URL object's `.search` property. In this case we cannot support deep links.
                if (nf.Common.isDefinedAndNotNull(params)) {
                    var params = new URLSearchParams(url.search);
                    params.set('processGroupId', groupId);
                    params.set('componentIds', selectedComponentIds.sort());

                    var newUrl = url.origin + url.pathname;

                    if (nfCommon.isDefinedAndNotNull(nfCanvasUtils.getParentGroupId()) || selectedComponentIds.length > 0) {
                        if (!nfCommon.isDefinedAndNotNull(nfCanvasUtils.getParentGroupId())) {
                            // we are in the root group so set processGroupId param value to 'root' alias
                            params.set('processGroupId', 'root');
                        }

                        if ((url.origin + url.pathname + '?' + params.toString()).length <= nfCanvasUtils.MAX_URL_LENGTH) {
                            newUrl = url.origin + url.pathname + '?' + params.toString();
                        } else if (nfCommon.isDefinedAndNotNull(nfCanvasUtils.getParentGroupId())) {
                            // silently remove all component ids
                            params.set('componentIds', '');
                            newUrl = url.origin + url.pathname + '?' + params.toString();
                        }
                    }

                    window.history.replaceState({'previous_url': url.href}, window.document.title, newUrl);
                }
            }
        },

        /**
         * Gets the currently selected components and connections.
         *
         * @returns {selection}     The currently selected components and connections
         */
        getSelection: function () {
            return d3.selectAll('g.component.selected, g.connection.selected');
        },

        /**
         * Centers the specified bounding box.
         *
         * @param {type} boundingBox
         */
        centerBoundingBox: function (boundingBox) {
            var scale = nfCanvas.View.scale();

            // get the canvas normalized width and height
            var canvasContainer = $('#canvas-container');
            var screenWidth = canvasContainer.width() / scale;
            var screenHeight = canvasContainer.height() / scale;

            // determine the center location for this component in canvas space
            var center = [(screenWidth / 2) - (boundingBox.width / 2), (screenHeight / 2) - (boundingBox.height / 2)];

            // calculate the difference between the center point and the position of this component and convert to screen space
            nfCanvas.View.translate([(center[0] - boundingBox.x) * scale, (center[1] - boundingBox.y) * scale]);
        },

        /**
         * Enables/disables the editable behavior for the specified selection based on their access policies.
         *
         * @param selection     selection
         * @param nfConnectableRef   The nfConnectable module.
         * @param nfDraggableRef   The nfDraggable module.
         */
        editable: function (selection, nfConnectableRef, nfDraggableRef) {
            if (nfCanvasUtils.canModify(selection)) {
                if (!selection.classed('connectable')) {
                    selection.call(nfConnectableRef.activate);
                }
                if (!selection.classed('moveable')) {
                    selection.call(nfDraggableRef.activate);
                }
            } else {
                if (selection.classed('connectable')) {
                    selection.call(nfConnectableRef.deactivate);
                }
                if (selection.classed('moveable')) {
                    selection.call(nfDraggableRef.deactivate);
                }
            }
        },

        /**
         * Conditionally apply the transition.
         *
         * @param selection     selection
         * @param transition    transition
         */
        transition: function (selection, transition) {
            if (transition && !selection.empty()) {
                return selection.transition().duration(400);
            } else {
                return selection;
            }
        },

        /**
         * Position the component accordingly.
         *
         * @param {selection} updated
         */
        position: function (updated, transition) {
            if (updated.empty()) {
                return;
            }

            return nfCanvasUtils.transition(updated, transition)
                .attr('transform', function (d) {
                    return 'translate(' + d.position.x + ', ' + d.position.y + ')';
                });
        },

        /**
         * Applies single line ellipsis to the component in the specified selection if necessary.
         *
         * @param {selection} selection
         * @param {string} text
         */
        ellipsis: function (selection, text) {
            text = text.trim();
            var width = parseInt(selection.attr('width'), 10);
            var node = selection.node();

            // set the element text
            selection.text(text);

            // see if the field is too big for the field
            if (text.length > 0 && node.getSubStringLength(0, text.length - 1) > width) {
                // make some room for the ellipsis
                width -= 5;

                // determine the appropriate index
                var i = binarySearch(text.length, function (x) {
                    var length = node.getSubStringLength(0, x);
                    if (length > width) {
                        // length is too long, try the lower half
                        return -1;
                    } else if (length < width) {
                        // length is too short, try the upper half
                        return 1;
                    }
                    return 0;
                });

                // trim at the appropriate length and add ellipsis
                selection.text(text.substring(0, i) + String.fromCharCode(8230));
            }
        },

        /**
         * Applies multiline ellipsis to the component in the specified seleciton. Text will
         * wrap for the specified number of lines. The last line will be ellipsis if necessary.
         *
         * @param {selection} selection
         * @param {integer} lineCount
         * @param {string} text
         */
        multilineEllipsis: function (selection, lineCount, text) {
            var i = 1;
            var words = text.split(/\s+/).reverse();

            // get the appropriate position
            var x = parseInt(selection.attr('x'), 10);
            var y = parseInt(selection.attr('y'), 10);
            var width = parseInt(selection.attr('width'), 10);

            var line = [];
            var tspan = selection.append('tspan')
                .attr({
                    'x': x,
                    'y': y,
                    'width': width
                });

            // go through each word
            var word = words.pop();
            while (nfCommon.isDefinedAndNotNull(word)) {
                // add the current word
                line.push(word);

                // update the label text
                tspan.text(line.join(' '));

                // if this word caused us to go too far
                if (tspan.node().getComputedTextLength() > width) {
                    // remove the current word
                    line.pop();

                    // update the label text
                    tspan.text(line.join(' '));

                    // create the tspan for the next line
                    tspan = selection.append('tspan')
                        .attr({
                            'x': x,
                            'dy': '1.2em',
                            'width': width
                        });

                    // if we've reached the last line, use single line ellipsis
                    if (++i >= lineCount) {
                        // get the remainder using the current word and
                        // reversing whats left
                        var remainder = [word].concat(words.reverse());

                        // apply ellipsis to the last line
                        nfCanvasUtils.ellipsis(tspan, remainder.join(' '));

                        // we've reached the line count
                        break;
                    } else {
                        tspan.text(word);

                        // prep the line for the next iteration
                        line = [word];
                    }
                }

                // get the next word
                word = words.pop();
            }
        },

        /**
         * Updates the active thread count on the specified selection.
         *
         * @param {selection} selection         The selection
         * @param {object} d                    The data
         * @param {function} setOffset          Optional function to handle the width of the active thread count component
         * @return
         */
        activeThreadCount: function (selection, d, setOffset) {
            // if there is active threads show the count, otherwise hide
            if (d.status.aggregateSnapshot.activeThreadCount > 0) {
                // update the active thread count
                var activeThreadCount = selection.select('text.active-thread-count')
                    .text(function () {
                        return d.status.aggregateSnapshot.activeThreadCount;
                    })
                    .style('display', 'block')
                    .each(function () {
                        var bBox = this.getBBox();
                        d3.select(this).attr('x', function () {
                            return d.dimensions.width - bBox.width - 15;
                        });
                    });

                // update the background width
                selection.select('text.active-thread-count-icon')
                    .attr('x', function () {
                        var bBox = activeThreadCount.node().getBBox();

                        // update the offset
                        if (typeof setOffset === 'function') {
                            setOffset(bBox.width + 6);
                        }

                        return d.dimensions.width - bBox.width - 20;
                    })
                    .style('display', 'block');
            } else {
                selection.selectAll('text.active-thread-count, text.active-thread-count-icon').style('display', 'none');
            }
        },

        /**
         * Disables the default browser behavior of following image href when control clicking.
         *
         * @param {selection} selection                 The image
         */
        disableImageHref: function (selection) {
            selection.on('click.disableImageHref', function () {
                if (d3.event.ctrlKey || d3.event.shiftKey) {
                    d3.event.preventDefault();
                }
            });
        },

        /**
         * Handles component bulletins.
         *
         * @param {selection} selection                    The component
         * @param {object} d                                The data
         * @param {function} getTooltipContainer            Function to get the tooltip container
         * @param {function} offset                         Optional offset
         */
        bulletins: function (selection, d, getTooltipContainer, offset) {
            offset = nfCommon.isDefinedAndNotNull(offset) ? offset : 0;

            // get the tip
            var tip = d3.select('#bulletin-tip-' + d.id);

            var hasBulletins = false;
            if (!nfCommon.isEmpty(d.bulletins)) {
                // format the bulletins
                var bulletins = nfCommon.getFormattedBulletins(d.bulletins);
                hasBulletins = bulletins.length > 0;

                if (hasBulletins) {
                    // create the unordered list based off the formatted bulletins
                    var list = nfCommon.formatUnorderedList(bulletins);
                }
            }

            // if there are bulletins show them, otherwise hide
            if (hasBulletins) {
                // update the tooltip
                selection.select('text.bulletin-icon')
                    .each(function () {
                        // create the tip if necessary
                        if (tip.empty()) {
                            tip = getTooltipContainer().append('div')
                                .attr('id', function () {
                                    return 'bulletin-tip-' + d.id;
                                })
                                .attr('class', 'tooltip nifi-tooltip');
                        }

                        // add the tooltip
                        tip.html(function () {
                            return $('<div></div>').append(list).html();
                        });

                        nfCanvasUtils.canvasTooltip(tip, d3.select(this));
                    });

                // update the tooltip background
                selection.select('text.bulletin-icon').style("visibility", "visible");
                selection.select('rect.bulletin-background').style("visibility", "visible");
            } else {
                // clean up if necessary
                if (!tip.empty()) {
                    tip.remove();
                }

                // update the tooltip background
                selection.select('text.bulletin-icon').style("visibility", "hidden");
                selection.select('rect.bulletin-background').style("visibility", "hidden");
            }
        },

        /**
         * Adds the specified tooltip to the specified target.
         *
         * @param {selection} tip           The tooltip
         * @param {selection} target        The target of the tooltip
         */
        canvasTooltip: function (tip, target) {
            target.on('mouseenter', function () {
                tip.style('top', (d3.event.pageY + 15) + 'px').style('left', (d3.event.pageX + 15) + 'px').style('display', 'block');
            })
                .on('mousemove', function () {
                    tip.style('top', (d3.event.pageY + 15) + 'px').style('left', (d3.event.pageX + 15) + 'px');
                })
                .on('mouseleave', function () {
                    tip.style('display', 'none');
                });
        },

        /**
         * Determines if the specified selection is alignable (in a single action).
         *
         * @param {selection} selection     The selection
         * @returns {boolean}
         */
        canAlign: function(selection) {
            var canAlign = true;

            // determine if the current selection is entirely connections
            var selectedConnections = selection.filter(function(d) {
                var connection = d3.select(this);
                return nfCanvasUtils.isConnection(connection);
            });

            // require multiple selections besides connections
            if (selection.size() - selectedConnections.size() < 2) {
                canAlign = false;
            }

            // require write permissions
            if (nfCanvasUtils.canModify(selection) === false) {
                canAlign = false;
            }

            return canAlign;
        },

        /**
         * Determines if the specified selection is colorable (in a single action).
         *
         * @param {selection} selection     The selection
         * @returns {boolean}
         */
        isColorable: function(selection) {
            if (selection.empty()) {
                return false;
            }

            // require read and write permissions
            if (nfCanvasUtils.canRead(selection) === false || nfCanvasUtils.canModify(selection) === false) {
                return false;
            }

            // determine if the current selection is entirely processors or labels
            var selectedProcessors = selection.filter(function(d) {
                var processor = d3.select(this);
                return nfCanvasUtils.isProcessor(processor) && nfCanvasUtils.canModify(processor);
            });
            var selectedLabels = selection.filter(function(d) {
                var label = d3.select(this);
                return nfCanvasUtils.isLabel(label) && nfCanvasUtils.canModify(label);
            });

            var allProcessors = selectedProcessors.size() === selection.size();
            var allLabels = selectedLabels.size() === selection.size();

            return allProcessors || allLabels;
        },

        /**
         * Determines if the specified selection is a connection.
         *
         * @argument {selection} selection      The selection
         */
        isConnection: function (selection) {
            return selection.classed('connection');
        },

        /**
         * Determines if the specified selection is a remote process group.
         *
         * @argument {selection} selection      The selection
         */
        isRemoteProcessGroup: function (selection) {
            return selection.classed('remote-process-group');
        },

        /**
         * Determines if the specified selection is a processor.
         *
         * @argument {selection} selection      The selection
         */
        isProcessor: function (selection) {
            return selection.classed('processor');
        },

        /**
         * Determines if the specified selection is a label.
         *
         * @argument {selection} selection      The selection
         */
        isLabel: function (selection) {
            return selection.classed('label');
        },

        /**
         * Determines if the specified selection is an input port.
         *
         * @argument {selection} selection      The selection
         */
        isInputPort: function (selection) {
            return selection.classed('input-port');
        },

        /**
         * Determines if the specified selection is an output port.
         *
         * @argument {selection} selection      The selection
         */
        isOutputPort: function (selection) {
            return selection.classed('output-port');
        },

        /**
         * Determines if the specified selection is a process group.
         *
         * @argument {selection} selection      The selection
         */
        isProcessGroup: function (selection) {
            return selection.classed('process-group');
        },

        /**
         * Determines if the specified selection is a funnel.
         *
         * @argument {selection} selection      The selection
         */
        isFunnel: function (selection) {
            return selection.classed('funnel');
        },

        /**
         * Determines if the components in the specified selection are runnable.
         *
         * @argument {selection} selection      The selection
         * @return {boolean}                    Whether the selection is runnable
         */
        areRunnable: function (selection) {
            if (selection.empty()) {
                return true;
            }

            var runnable = true;
            selection.each(function () {
                if (!nfCanvasUtils.isRunnable(d3.select(this))) {
                    runnable = false;
                    return false;
                }
            });

            return runnable;
        },

        /**
         * Determines if the component in the specified selection is runnable.
         *
         * @argument {selection} selection      The selection
         * @return {boolean}                    Whether the selection is runnable
         */
        isRunnable: function (selection) {
            if (selection.size() !== 1) {
                return false;
            }

            if (nfCanvasUtils.isProcessGroup(selection)) {
                return true;
            }

            if (nfCanvasUtils.canModify(selection) === false) {
                return false;
            }

            var runnable = false;
            var selectionData = selection.datum();
            if (nfCanvasUtils.isProcessor(selection) || nfCanvasUtils.isInputPort(selection) || nfCanvasUtils.isOutputPort(selection)) {
                runnable = nfCanvasUtils.supportsModification(selection) && selectionData.status.aggregateSnapshot.runStatus === 'Stopped';
            }

            return runnable;
        },

        /**
         * Determines if the components in the specified selection are stoppable.
         *
         * @argument {selection} selection      The selection
         * @return {boolean}                    Whether the selection is stoppable
         */
        areStoppable: function (selection) {
            if (selection.empty()) {
                return true;
            }

            var stoppable = true;
            selection.each(function () {
                if (!nfCanvasUtils.isStoppable(d3.select(this))) {
                    stoppable = false;
                    return false;
                }
            });

            return stoppable;
        },

        /**
         * Determines if the component in the specified selection is runnable.
         *
         * @argument {selection} selection      The selection
         * @return {boolean}                    Whether the selection is runnable
         */
        isStoppable: function (selection) {
            if (selection.size() !== 1) {
                return false;
            }

            if (nfCanvasUtils.isProcessGroup(selection)) {
                return true;
            }

            if (nfCanvasUtils.canModify(selection) === false) {
                return false;
            }

            var stoppable = false;
            var selectionData = selection.datum();
            if (nfCanvasUtils.isProcessor(selection) || nfCanvasUtils.isInputPort(selection) || nfCanvasUtils.isOutputPort(selection)) {
                stoppable = selectionData.status.aggregateSnapshot.runStatus === 'Running';
            }

            return stoppable;
        },

        /**
         * Filters the specified selection for any components that supports enable.
         *
         * @argument {selection} selection      The selection
         */
        filterEnable: function (selection) {
            return selection.filter(function (d) {
                var selected = d3.select(this);
                var selectedData = selected.datum();

                // ensure its a processor, input port, or output port and supports modification and is disabled (can enable)
                return ((nfCanvasUtils.isProcessor(selected) || nfCanvasUtils.isInputPort(selected) || nfCanvasUtils.isOutputPort(selected)) &&
                nfCanvasUtils.supportsModification(selected) &&
                selectedData.status.aggregateSnapshot.runStatus === 'Disabled');
            });
        },

        /**
         * Determines if the specified selection contains any components that supports enable.
         *
         * @argument {selection} selection      The selection
         */
        canEnable: function (selection) {
            if (selection.empty()) {
                return false;
            }

            if (nfCanvasUtils.canModify(selection) === false) {
                return false;
            }

            return nfCanvasUtils.filterEnable(selection).size() === selection.size();
        },

        /**
         * Filters the specified selection for any components that supports disable.
         *
         * @argument {selection} selection      The selection
         */
        filterDisable: function (selection) {
            return selection.filter(function (d) {
                var selected = d3.select(this);
                var selectedData = selected.datum();

                // ensure its a processor, input port, or output port and supports modification and is stopped (can disable)
                return ((nfCanvasUtils.isProcessor(selected) || nfCanvasUtils.isInputPort(selected) || nfCanvasUtils.isOutputPort(selected)) &&
                nfCanvasUtils.supportsModification(selected) &&
                (selectedData.status.aggregateSnapshot.runStatus === 'Stopped' ||
                selectedData.status.aggregateSnapshot.runStatus === 'Invalid'));
            });
        },

        /**
         * Determines if the specified selection contains any components that supports disable.
         *
         * @argument {selection} selection      The selection
         */
        canDisable: function (selection) {
            if (selection.empty()) {
                return false;
            }

            if (nfCanvasUtils.canModify(selection) === false) {
                return false;
            }

            return nfCanvasUtils.filterDisable(selection).size() === selection.size();
        },


        /**
         * Determines if the specified selection can all start transmitting.
         *
         * @argument {selection} selection      The selection
         * @return {boolean}                    Whether the selection can start transmitting
         */
        canAllStartTransmitting: function (selection) {
            if (selection.empty()) {
                return false;
            }

            var canStartTransmitting = true;
            selection.each(function () {
                if (!nfCanvasUtils.canStartTransmitting(d3.select(this))) {
                    canStartTransmitting = false;
                }
            });
            return canStartTransmitting;
        },

        /**
         * Determines if the specified selection supports starting transmission.
         *
         * @argument {selection} selection      The selection
         */
        canStartTransmitting: function (selection) {
            if (selection.size() !== 1) {
                return false;
            }

            if (nfCanvasUtils.canModify(selection) === false || nfCanvasUtils.canRead(selection) === false) {
                return false;
            }

            return nfCanvasUtils.isRemoteProcessGroup(selection);
        },

        /**
         * Determines if the specified selection can all stop transmitting.
         *
         * @argument {selection} selection      The selection
         * @return {boolean}                    Whether the selection can stop transmitting
         */
        canAllStopTransmitting: function (selection) {
            if (selection.empty()) {
                return false;
            }

            var canStopTransmitting = true;
            selection.each(function () {
                if (!nfCanvasUtils.canStopTransmitting(d3.select(this))) {
                    canStopTransmitting = false;
                }
            });
            return canStopTransmitting;
        },

        /**
         * Determines if the specified selection can stop transmission.
         *
         * @argument {selection} selection      The selection
         */
        canStopTransmitting: function (selection) {
            if (selection.size() !== 1) {
                return false;
            }

            if (nfCanvasUtils.canModify(selection) === false || nfCanvasUtils.canRead(selection) === false) {
                return false;
            }

            return nfCanvasUtils.isRemoteProcessGroup(selection);
        },

        /**
         * Determines whether the components in the specified selection are deletable.
         *
         * @argument {selection} selection      The selection
         * @return {boolean}            Whether the selection is deletable
         */
        areDeletable: function (selection) {
            if (selection.empty()) {
                return false;
            }

            var isDeletable = true;
            selection.each(function () {
                if (!nfCanvasUtils.isDeletable(d3.select(this))) {
                    isDeletable = false;
                }
            });
            return isDeletable;
        },

        /**
         * Determines whether the component in the specified selection is deletable.
         *
         * @argument {selection} selection      The selection
         * @return {boolean}            Whether the selection is deletable
         */
        isDeletable: function (selection) {
            if (selection.size() !== 1) {
                return false;
            }

            // ensure the user has write permissions to the current process group
            if (nfCanvas.canWrite() === false) {
                return false;
            }

            if (nfCanvasUtils.canModify(selection) === false) {
                return false;
            }

            return nfCanvasUtils.supportsModification(selection);
        },

        /**
         * Determines whether the specified selection is configurable.
         *
         * @param selection
         */
        isConfigurable: function (selection) {
            // ensure the correct number of components are selected
            if (selection.size() !== 1) {
                if (selection.empty()) {
                    return true;
                } else {
                    return false;
                }
            }

            if (nfCanvasUtils.isProcessGroup(selection)) {
                return true;
            }
            if (nfCanvasUtils.canRead(selection) === false || nfCanvasUtils.canModify(selection) === false) {
                return false;
            }
            if (nfCanvasUtils.isFunnel(selection)) {
                return false;
            }

            return nfCanvasUtils.supportsModification(selection);
        },

        /**
         * Determines whether the specified selection has details.
         *
         * @param selection
         */
        hasDetails: function (selection) {
            // ensure the correct number of components are selected
            if (selection.size() !== 1) {
                return false;
            }

            if (nfCanvasUtils.canRead(selection) === false) {
                return false;
            }
            if (nfCanvasUtils.canModify(selection)) {
                if (nfCanvasUtils.isProcessor(selection) || nfCanvasUtils.isInputPort(selection) || nfCanvasUtils.isOutputPort(selection) || nfCanvasUtils.isRemoteProcessGroup(selection) || nfCanvasUtils.isConnection(selection)) {
                    return !nfCanvasUtils.isConfigurable(selection);
                }
            } else {
                return nfCanvasUtils.isProcessor(selection) || nfCanvasUtils.isConnection(selection) || nfCanvasUtils.isInputPort(selection) || nfCanvasUtils.isOutputPort(selection) || nfCanvasUtils.isRemoteProcessGroup(selection);
            }

            return false;
        },

        /**
         * Determines whether the user can configure or open the policy management page.
         */
        canManagePolicies: function () {
            var selection = nfCanvasUtils.getSelection();

            // ensure 0 or 1 components selected
            if (selection.size() <= 1) {
                // if something is selected, ensure it's not a connection
                if (!selection.empty() && nfCanvasUtils.isConnection(selection)) {
                    return false;
                }

                // ensure access to read tenants
                return nfCommon.canAccessTenants();
            }

            return false;
        },

        /**
         * Determines whether the components in the specified selection are writable.
         *
         * @argument {selection} selection      The selection
         * @return {boolean}            Whether the selection is writable
         */
        canModify: function (selection) {
            var selectionSize = selection.size();
            var writableSize = selection.filter(function (d) {
                return d.permissions.canWrite;
            }).size();

            return selectionSize === writableSize;
        },

        /**
         * Determines whether the components in the specified selection are readable.
         *
         * @argument {selection} selection      The selection
         * @return {boolean}            Whether the selection is readable
         */
        canRead: function (selection) {
            var selectionSize = selection.size();
            var readableSize = selection.filter(function (d) {
                return d.permissions.canRead;
            }).size();

            return selectionSize === readableSize;
        },

        /**
         * Determines whether the specified selection is in a state to support modification.
         *
         * @argument {selection} selection      The selection
         */
        supportsModification: function (selection) {
            if (selection.size() !== 1) {
                return false;
            }

            // get the selection data
            var selectionData = selection.datum();

            var supportsModification = false;
            if (nfCanvasUtils.isProcessor(selection) || nfCanvasUtils.isInputPort(selection) || nfCanvasUtils.isOutputPort(selection)) {
                supportsModification = !(selectionData.status.aggregateSnapshot.runStatus === 'Running' || selectionData.status.aggregateSnapshot.activeThreadCount > 0);
            } else if (nfCanvasUtils.isRemoteProcessGroup(selection)) {
                supportsModification = !(selectionData.status.transmissionStatus === 'Transmitting' || selectionData.status.aggregateSnapshot.activeThreadCount > 0);
            } else if (nfCanvasUtils.isProcessGroup(selection)) {
                supportsModification = true;
            } else if (nfCanvasUtils.isFunnel(selection)) {
                supportsModification = true;
            } else if (nfCanvasUtils.isLabel(selection)) {
                supportsModification = true;
            } else if (nfCanvasUtils.isConnection(selection)) {
                var isSourceConfigurable = false;
                var isDestinationConfigurable = false;

                var sourceComponentId = nfCanvasUtils.getConnectionSourceComponentId(selectionData);
                var source = d3.select('#id-' + sourceComponentId);
                if (!source.empty()) {
                    if (nfCanvasUtils.isRemoteProcessGroup(source) || nfCanvasUtils.isProcessGroup(source)) {
                        isSourceConfigurable = true;
                    } else {
                        isSourceConfigurable = nfCanvasUtils.supportsModification(source);
                    }
                }

                var destinationComponentId = nfCanvasUtils.getConnectionDestinationComponentId(selectionData);
                var destination = d3.select('#id-' + destinationComponentId);
                if (!destination.empty()) {
                    if (nfCanvasUtils.isRemoteProcessGroup(destination) || nfCanvasUtils.isProcessGroup(destination)) {
                        isDestinationConfigurable = true;
                    } else {
                        isDestinationConfigurable = nfCanvasUtils.supportsModification(destination);
                    }
                }

                supportsModification = isSourceConfigurable && isDestinationConfigurable;
            }
            return supportsModification;
        },

        /**
         * Determines the connectable type for the specified source selection.
         *
         * @argument {selection} selection      The selection
         */
        getConnectableTypeForSource: function (selection) {
            var type;
            if (nfCanvasUtils.isProcessor(selection)) {
                type = 'PROCESSOR';
            } else if (nfCanvasUtils.isRemoteProcessGroup(selection)) {
                type = 'REMOTE_OUTPUT_PORT';
            } else if (nfCanvasUtils.isProcessGroup(selection)) {
                type = 'OUTPUT_PORT';
            } else if (nfCanvasUtils.isInputPort(selection)) {
                type = 'INPUT_PORT';
            } else if (nfCanvasUtils.isFunnel(selection)) {
                type = 'FUNNEL';
            }
            return type;
        },

        /**
         * Determines the connectable type for the specified destination selection.
         *
         * @argument {selection} selection      The selection
         */
        getConnectableTypeForDestination: function (selection) {
            var type;
            if (nfCanvasUtils.isProcessor(selection)) {
                type = 'PROCESSOR';
            } else if (nfCanvasUtils.isRemoteProcessGroup(selection)) {
                type = 'REMOTE_INPUT_PORT';
            } else if (nfCanvasUtils.isProcessGroup(selection)) {
                type = 'INPUT_PORT';
            } else if (nfCanvasUtils.isOutputPort(selection)) {
                type = 'OUTPUT_PORT';
            } else if (nfCanvasUtils.isFunnel(selection)) {
                type = 'FUNNEL';
            }
            return type;
        },

        /**
         * Determines if the graph is currently in a state to copy.
         *
         * @argument {selection} selection    The selection
         */
        isCopyable: function (selection) {
            // if nothing is selected return
            if (selection.empty()) {
                return false;
            }

            if (nfCanvasUtils.canRead(selection) === false) {
                return false;
            }

            // determine how many copyable components are selected
            var copyable = selection.filter(function (d) {
                var selected = d3.select(this);
                if (nfCanvasUtils.isConnection(selected)) {
                    var sourceIncluded = !selection.filter(function (source) {
                        var sourceComponentId = nfCanvasUtils.getConnectionSourceComponentId(d);
                        return sourceComponentId === source.id;
                    }).empty();
                    var destinationIncluded = !selection.filter(function (destination) {
                        var destinationComponentId = nfCanvasUtils.getConnectionDestinationComponentId(d);
                        return destinationComponentId === destination.id;
                    }).empty();
                    return sourceIncluded && destinationIncluded;
                } else {
                    return nfCanvasUtils.isProcessor(selected) || nfCanvasUtils.isFunnel(selected) || nfCanvasUtils.isLabel(selected) || nfCanvasUtils.isProcessGroup(selected) || nfCanvasUtils.isRemoteProcessGroup(selected) || nfCanvasUtils.isInputPort(selected) || nfCanvasUtils.isOutputPort(selected);
                }
            });

            // ensure everything selected is copyable
            return selection.size() === copyable.size();
        },

        /**
         * Determines if something is currently pastable.
         */
        isPastable: function () {
            return nfCanvas.canWrite() && nfClipboard.isCopied();
        },

        /**
         * Persists the current user view.
         */
        persistUserView: function () {
            var name = config.storage.namePrefix + nfCanvas.getGroupId();

            // create the item to store
            var translate = nfCanvas.View.translate();
            var item = {
                scale: nfCanvas.View.scale(),
                translateX: translate[0],
                translateY: translate[1]
            };

            // store the item
            nfStorage.setItem(name, item);
        },

        /**
         * Gets the name for this connection.
         *
         * @param {object} connection
         */
        formatConnectionName: function (connection) {
            if (!nfCommon.isBlank(connection.name)) {
                return connection.name;
            } else if (nfCommon.isDefinedAndNotNull(connection.selectedRelationships)) {
                return connection.selectedRelationships.join(', ');
            }
            return '';
        },

        /**
         * Reloads a connection's source and destination.
         *
         * @param {string} sourceComponentId          The connection source id
         * @param {string} destinationComponentId     The connection destination id
         */
        reloadConnectionSourceAndDestination: function (sourceComponentId, destinationComponentId) {
            if (nfCommon.isBlank(sourceComponentId) === false) {
                var source = d3.select('#id-' + sourceComponentId);
                if (source.empty() === false) {
                    nfGraph.reload(source);
                }
            }
            if (nfCommon.isBlank(destinationComponentId) === false) {
                var destination = d3.select('#id-' + destinationComponentId);
                if (destination.empty() === false) {
                    nfGraph.reload(destination);
                }
            }
        },

        /**
         * Returns the component id of the source of this processor. If the connection is attached
         * to a port in a [sub|remote] group, the component id will be that of the group. Otherwise
         * it is the component itself.
         *
         * @param {object} connection   The connection in question
         */
        getConnectionSourceComponentId: function (connection) {
            var sourceId = connection.sourceId;
            if (connection.sourceGroupId !== nfCanvas.getGroupId()) {
                sourceId = connection.sourceGroupId;
            }
            return sourceId;
        },

        /**
         * Returns the component id of the source of this processor. If the connection is attached
         * to a port in a [sub|remote] group, the component id will be that of the group. Otherwise
         * it is the component itself.
         *
         * @param {object} connection   The connection in question
         */
        getConnectionDestinationComponentId: function (connection) {
            var destinationId = connection.destinationId;
            if (connection.destinationGroupId !== nfCanvas.getGroupId()) {
                destinationId = connection.destinationGroupId;
            }
            return destinationId;
        },

        /**
         * Attempts to restore a persisted view. Returns a flag that indicates if the
         * view was restored.
         */
        restoreUserView: function () {
            var viewRestored = false;

            try {
                // see if we can restore the view position from storage
                var name = config.storage.namePrefix + nfCanvas.getGroupId();
                var item = nfStorage.getItem(name);

                // ensure the item is valid
                if (nfCommon.isDefinedAndNotNull(item)) {
                    if (isFinite(item.scale) && isFinite(item.translateX) && isFinite(item.translateY)) {
                        // restore previous view
                        nfCanvas.View.translate([item.translateX, item.translateY]);
                        nfCanvas.View.scale(item.scale);

                        // refresh the canvas
                        nfCanvas.View.refresh({
                            transition: true
                        });

                        // mark the view was restore
                        viewRestored = true;
                    }
                }
            } catch (e) {
                // likely could not parse item.. ignoring
            }

            return viewRestored;
        },

        /**
         * Gets the origin of the bounding box for the specified selection.
         *
         * @argument {selection} selection      The selection
         */
        getOrigin: function (selection) {
            var origin = {};

            selection.each(function (d) {
                var selected = d3.select(this);
                if (!nfCanvasUtils.isConnection(selected)) {
                    if (nfCommon.isUndefined(origin.x) || d.position.x < origin.x) {
                        origin.x = d.position.x;
                    }
                    if (nfCommon.isUndefined(origin.y) || d.position.y < origin.y) {
                        origin.y = d.position.y;
                    }
                }
            });

            return origin;
        },

        /**
         * Moves the specified components into the current parent group.
         *
         * @param {selection} components
         */
        moveComponentsToParent: function (components) {
            var groupId = nfCanvas.getParentGroupId();

            // if the group id is null, we're already in the top most group
            if (groupId === null) {
                nfDialog.showOkDialog({
                    headerText: 'Process Group',
                    dialogContent: 'Components are already in the topmost group.'
                });
            } else {
                moveComponents(components, groupId);
            }
        },

        /**
         * Moves the specified components into the specified group.
         *
         * @param {selection} components    The components to move
         * @param {selection} group         The destination group
         */
        moveComponents: function (components, group) {
            var groupData = group.datum();

            // move the components into the destination and...
            moveComponents(components, groupData.id).done(function () {
                // reload the target group
                nfCanvasUtils.getComponentByType('ProcessGroup').reload(groupData.id);
            });
        },

        /**
         * Removes any dangling edges. All components are retained as well as any
         * edges whose source and destination are also retained.
         *
         * @param {selection} selection
         * @returns {array}
         */
        trimDanglingEdges: function (selection) {
            // returns whether the source and destination of the specified connection are present in the specified selection
            var keepConnection = function (connection) {
                var sourceComponentId = nfCanvasUtils.getConnectionSourceComponentId(connection);
                var destinationComponentId = nfCanvasUtils.getConnectionDestinationComponentId(connection);

                // determine if both source and destination are selected
                var includesSource = false;
                var includesDestination = false;
                selection.each(function (d) {
                    if (d.id === sourceComponentId) {
                        includesSource = true;
                    }
                    if (d.id === destinationComponentId) {
                        includesDestination = true;
                    }
                });

                return includesSource && includesDestination;
            };

            // include all components and connections whose source/destination are also selected
            return selection.filter(function (d) {
                if (d.type === 'Connection') {
                    return keepConnection(d);
                } else {
                    return true;
                }
            });
        },

        /**
         * Determines if the component in the specified selection is a valid connection source.
         *
         * @param {selection} selection         The selection
         * @return {boolean} Whether the selection is a valid connection source
         */
        isValidConnectionSource: function (selection) {
            if (selection.size() !== 1) {
                return false;
            }

            // always allow connections from process groups
            if (nfCanvasUtils.isProcessGroup(selection)) {
                return true;
            }

            // require read and write for a connection source since we'll need to read the source to obtain valid relationships, etc
            if (nfCanvasUtils.canRead(selection) === false || nfCanvasUtils.canModify(selection) === false) {
                return false;
            }

            return nfCanvasUtils.isProcessor(selection) || nfCanvasUtils.isRemoteProcessGroup(selection) ||
                nfCanvasUtils.isInputPort(selection) || nfCanvasUtils.isFunnel(selection);
        },

        /**
         * Determines if the component in the specified selection is a valid connection destination.
         *
         * @param {selection} selection         The selection
         * @return {boolean} Whether the selection is a valid connection destination
         */
        isValidConnectionDestination: function (selection) {
            if (selection.size() !== 1) {
                return false;
            }

            if (nfCanvasUtils.isProcessGroup(selection)) {
                return true;
            }

            // require write for a connection destination
            if (nfCanvasUtils.canModify(selection) === false) {
                return false;
            }

            if (nfCanvasUtils.isRemoteProcessGroup(selection) || nfCanvasUtils.isOutputPort(selection) || nfCanvasUtils.isFunnel(selection)) {
                return true;
            }

            // if processor, ensure it supports input
            if (nfCanvasUtils.isProcessor(selection)) {
                var destinationData = selection.datum();
                return destinationData.inputRequirement !== 'INPUT_FORBIDDEN';
            }
        },

        /**
         * Returns whether the authorizer is managed.
         */
        isManagedAuthorizer: function () {
            return nfCanvas.isManagedAuthorizer();
        },

        /**
         * Returns whether the authorizer is configurable.
         */
        isConfigurableAuthorizer: function () {
            return nfCanvas.isConfigurableAuthorizer();
        },

        /**
         * Returns whether the authorizer support configurable users and groups.
         */
        isConfigurableUsersAndGroups: function () {
            return nfCanvas.isConfigurableUsersAndGroups();
        },

        /**
         * Set the group id.
         *
         * @argument {string} gi       The group id
         */
        setGroupId: function (gi) {
            return nfCanvas.setGroupId(gi);
        },

        /**
         * Get the group id.
         */
        getGroupId: function () {
            return nfCanvas.getGroupId();
        },

        /**
         * Get the group name.
         */
        getGroupName: function () {
            return nfCanvas.getGroupName();
        },

        /**
         * Get the parent group id.
         */
        getParentGroupId: function () {
            return nfCanvas.getParentGroupId();
        },

        /**
         * Reloads the status for the entire canvas (components and flow.)
         */
        reload: function () {
            return nfCanvas.reload({
                'transition': true
            });
        },

        /**
         * Whether the current user can read from this group.
         *
         * @returns {boolean}   can write
         */
        canReadFromGroup: function () {
            return nfCanvas.canRead();
        },

        /**
         * Whether the current user can write in this group.
         *
         * @returns {boolean}   can write
         */
        canWrite: function () {
            return nfCanvas.canWrite();
        },

        /**
         * Refreshes the view based on the configured translation and scale.
         *
         * @param {object} options Options for the refresh operation
         */
        refreshCanvasView: function (options) {
            return nfCanvas.View.refresh(options);
        },

        /**
         * Sets/gets the current scale.
         *
         * @param {number} scale        The new scale
         */
        scaleCanvasView: function (scale) {
            return nfCanvas.View.scale(scale);
        },

        /**
         * Sets/gets the current translation.
         *
         * @param {array} translate     [x, y]
         */
        translateCanvasView: function (translate) {
            return nfCanvas.View.translate(translate);
        },

        /**
         * Zooms to fit the entire graph on the canvas.
         */
        fitCanvasView: function () {
            return nfCanvas.View.fit();
        },

        /**
         * Zooms in a single zoom increment.
         */
        zoomCanvasViewIn: function () {
            return nfCanvas.View.zoomIn();
        },

        /**
         * Zooms out a single zoom increment.
         */
        zoomCanvasViewOut: function () {
            return nfCanvas.View.zoomOut();
        },

        /**
         * Zooms to the actual size (1 to 1).
         */
        actualSizeCanvasView: function () {
            return nfCanvas.View.actualSize();
        },

        /**
         * Whether or not a component should be rendered based solely on the current scale.
         *
         * @returns {Boolean}
         */
        shouldRenderPerScale: function () {
            return nfCanvas.View.shouldRenderPerScale();
        },

        /**
         * Gets the canvas offset.
         */
        getCanvasOffset: function () {
            return nfCanvas.CANVAS_OFFSET;
        }
    };
    return nfCanvasUtils;
}));