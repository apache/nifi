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

nf.ng.Canvas.GraphControlsCtrl = function (serviceProvider, navigateCtrl, operateCtrl) {
    'use strict';

    /**
     * Opens the specified graph control.
     *
     * @param {jQuery} graphControl
     */
    var openGraphControl = function (graphControl) {
        // undock if necessary
        if ($('div.graph-control-content').is(':visible') === false) {
            $('#graph-controls div.graph-control-docked').hide();
            $('#graph-controls div.graph-control-header-container').show();
            $('.graph-control').removeClass('docked');
        }

        // show the content of the specified graph control
        graphControl.children('div.graph-control-content').show();
        graphControl.find('div.graph-control-expansion').removeClass('fa-plus-square-o').addClass('fa-minus-square-o');

        // handle specific actions as necessary
        if (graphControl.attr('id') === 'navigation-control') {
            nf.Birdseye.updateBirdseyeVisibility(true);
        }

        // get the current visibility
        var graphControlVisibility = nf.Storage.getItem('graph-control-visibility');
        if (graphControlVisibility === null) {
            graphControlVisibility = {};
        }

        // update the visibility for this graph control
        var graphControlId = graphControl.attr('id');
        graphControlVisibility[graphControlId] = true;
        nf.Storage.setItem('graph-control-visibility', graphControlVisibility);
    };

    /**
     * Hides the specified graph control.
     *
     * @param {jQuery} graphControl
     */
    var hideGraphControl = function (graphControl) {
        // hide the content of the specified graph control
        graphControl.children('div.graph-control-content').hide();
        graphControl.find('div.graph-control-expansion').removeClass('fa-minus-square-o').addClass('fa-plus-square-o');

        // dock if necessary
        if ($('div.graph-control-content').is(':visible') === false) {
            $('#graph-controls div.graph-control-header-container').hide();
            $('#graph-controls div.graph-control-docked').show();
            $('.graph-control').addClass('docked');
        }

        // handle specific actions as necessary
        if (graphControl.attr('id') === 'navigation-control') {
            nf.Birdseye.updateBirdseyeVisibility(false);
        }

        // get the current visibility
        var graphControlVisibility = nf.Storage.getItem('graph-control-visibility');
        if (graphControlVisibility === null) {
            graphControlVisibility = {};
        }

        // update the visibility for this graph control
        var graphControlId = graphControl.attr('id');
        graphControlVisibility[graphControlId] = false;
        nf.Storage.setItem('graph-control-visibility', graphControlVisibility);
    };

    function GraphControlsCtrl(navigateCtrl, operateCtrl) {
        this.navigateCtrl = navigateCtrl;
        this.operateCtrl = operateCtrl;
    }

    GraphControlsCtrl.prototype = {
        constructor: GraphControlsCtrl,

        /**
         *  Register the header controller.
         */
        register: function () {
            if (serviceProvider.graphControlsCtrl === undefined) {
                serviceProvider.register('graphControlsCtrl', graphControlsCtrl);
            }
        },

        /**
         * Initialize the graph controls.
         */
        init: function () {
            this.operateCtrl.init();
            // initial the graph control visibility
            var graphControlVisibility = nf.Storage.getItem('graph-control-visibility');
            if (graphControlVisibility !== null) {
                $.each(graphControlVisibility, function (id, isVisible) {
                    var graphControl = $('#' + id);
                    if (graphControl) {
                        if (isVisible) {
                            openGraphControl(graphControl);
                        } else {
                            hideGraphControl(graphControl);
                        }
                    }
                });
            } else {
                openGraphControl($('#navigation-control'));
                openGraphControl($('#operation-control'));
            }
        },

        /**
         * Undock the graph control.
         * @param {jQuery} $event
         */
        undock: function ($event) {
            openGraphControl($($event.target).parent());
        },

        /**
         * Expand the graph control.
         * @param {jQuery} $event
         */
        expand: function ($event) {
            var icon = $($event.target);
            if (icon.find('.fa-plus-square-o').length > 0 || icon.hasClass('fa-plus-square-o') || icon.parent().children().find('.fa-plus-square-o').length > 0) {
                openGraphControl(icon.closest('div.graph-control'));
            } else {
                hideGraphControl(icon.closest('div.graph-control'));
            }
        },

        /**
         * Gets the icon to show for the selection context.
         */
        getContextIcon: function () {
            var selection = nf.CanvasUtils.getSelection();

            if (selection.empty()) {
                if (nf.Canvas.getParentGroupId() === null) {
                    return 'icon-drop';
                } else {
                    return 'icon-group';
                }
            } else {
                if (selection.size() === 1) {
                    if (nf.CanvasUtils.isProcessor(selection)) {
                        return 'icon-processor';
                    } else if (nf.CanvasUtils.isProcessGroup(selection)) {
                        return 'icon-group';
                    } else if (nf.CanvasUtils.isInputPort(selection)) {
                        return 'icon-port-in';
                    } else if (nf.CanvasUtils.isOutputPort(selection)) {
                        return 'icon-port-out';
                    } else if (nf.CanvasUtils.isRemoteProcessGroup(selection)) {
                        return 'icon-group-remote';
                    } else if (nf.CanvasUtils.isFunnel(selection)) {
                        return 'icon-funnel';
                    } else if (nf.CanvasUtils.isLabel(selection)) {
                        return 'icon-label';
                    } else if (nf.CanvasUtils.isConnection(selection)) {
                        return 'icon-connect';
                    }
                } else {
                    return 'icon-drop';
                }
            }
        },

        /**
         * Will hide target when appropriate.
         */
        hide: function () {
            var selection = nf.CanvasUtils.getSelection();
            if (selection.size() > 1) {
                return 'invisible'
            } else {
                return '';
            }
        },

        /**
         * Gets the name to show for the selection context.
         */
        getContextName: function () {
            var selection = nf.CanvasUtils.getSelection();
            var canRead = nf.Canvas.canRead();

            if (selection.empty()) {
                if (canRead) {
                    return nf.Canvas.getGroupName();
                } else {
                    return nf.Canvas.getGroupId();
                }
            } else {
                if (selection.size() === 1) {
                    var d = selection.datum();
                    if (d.permissions.canRead) {
                        if (nf.CanvasUtils.isLabel(selection)) {
                            if ($.trim(d.component.label) !== '') {
                                return d.component.label;
                            } else {
                                return '';
                            }
                        } else if (nf.CanvasUtils.isConnection(selection)) {
                            return nf.CanvasUtils.formatConnectionName(d.component);
                        } else {
                            return d.component.name;
                        }
                    } else {
                        return d.id;
                    }
                } else {
                    return 'Multiple components selected';
                }
            }
        },

        /**
         * Gets the type to show for the selection context.
         */
        getContextType: function () {
            var selection = nf.CanvasUtils.getSelection();

            if (selection.empty()) {
                return 'Process Group';
            } else {
                if (selection.size() === 1) {
                    if (nf.CanvasUtils.isProcessor(selection)) {
                        return 'Processor';
                    } else if (nf.CanvasUtils.isProcessGroup(selection)) {
                        return 'Process Group';
                    } else if (nf.CanvasUtils.isInputPort(selection)) {
                        return 'Input Port';
                    } else if (nf.CanvasUtils.isOutputPort(selection)) {
                        return 'Output Port';
                    } else if (nf.CanvasUtils.isRemoteProcessGroup(selection)) {
                        return 'Remote Process Group';
                    } else if (nf.CanvasUtils.isFunnel(selection)) {
                        return 'Funnel';
                    } else if (nf.CanvasUtils.isLabel(selection)) {
                        return 'Label';
                    } else if (nf.CanvasUtils.isConnection(selection)) {
                        return 'Connection';
                    }
                } else {
                    return 'Multiple selected';
                }
            }
        },

        /**
         * Gets the id to show for the selection context.
         */
        getContextId: function () {
            var selection = nf.CanvasUtils.getSelection();

            if (selection.empty()) {
                return nf.Canvas.getGroupId();
            } else {
                if (selection.size() === 1) {
                    var d = selection.datum();
                    return d.id;
                } else {
                    return 'Multiple selected';
                }     
            }
        },

        /**
         * Determines whether the user can configure or open the details dialog.
         */
        canConfigureOrOpenDetails: function () {
            var selection = nf.CanvasUtils.getSelection();

            if (selection.empty()) {
                return true;
            }

            return nf.CanvasUtils.isConfigurable(selection) || nf.CanvasUtils.hasDetails(selection);
        },

        /**
         * Opens either the configuration or details view based on the current state.
         */
        openConfigureOrDetailsView: function () {
            var selection = nf.CanvasUtils.getSelection();

            if (selection.empty()) {
                nf.ProcessGroupConfiguration.showConfiguration(nf.Canvas.getGroupId());
            }

            if (nf.CanvasUtils.isConfigurable(selection)) {
                nf.Actions.showConfiguration(selection);
            } else if (nf.CanvasUtils.hasDetails(selection)) {
                nf.Actions.showDetails(selection);
            }
        },

        /**
         * Determines whether the user can configure or open the policy management page.
         */
        canManagePolicies: function () {
            var selection = nf.CanvasUtils.getSelection();

            // ensure 0 or 1 components selected
            if (selection.size() <= 1) {
                // if something is selected, ensure it's not a connection
                if (!selection.empty() && nf.CanvasUtils.isConnection(selection)) {
                    return false;
                }

                // ensure access to read tenants
                return nf.Common.canAccessTenants();
            }

            return false;
        }
    }

    var graphControlsCtrl = new GraphControlsCtrl(navigateCtrl, operateCtrl);
    graphControlsCtrl.register();
    return graphControlsCtrl;
};