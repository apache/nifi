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
                'nf.CanvasUtils',
                'nf.ng.Bridge'],
            function ($, d3, nfCommon, nfCanvasUtils, nfNgBridge) {
                return (nf.ContextMenu = factory($, d3, nfCommon, nfCanvasUtils, nfNgBridge));
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.ContextMenu =
            factory(require('jquery'),
                require('d3'),
                require('nf.Common'),
                require('nf.CanvasUtils'),
                require('nf.ng.Bridge')));
    } else {
        nf.ContextMenu = factory(root.$,
            root.d3,
            root.nf.Common,
            root.nf.CanvasUtils,
            root.nf.ng.Bridge);
    }
}(this, function ($, d3, nfCommon, nfCanvasUtils, nfNgBridge) {
    'use strict';

    var nfActions;

    /**
     * Returns whether the current group is not the root group.
     *
     * @param {selection} selection         The selection of currently selected components
     */
    var isNotRootGroup = function (selection) {
        return nfCanvasUtils.getParentGroupId() !== null && selection.empty();
    };

    /**
     * Determines whether the component in the specified selection is configurable.
     *
     * @param {selection} selection         The selection of currently selected components
     */
    var isConfigurable = function (selection) {
        return nfCanvasUtils.isConfigurable(selection);
    };

    /**
     * Determines whether the component in the specified selection has configuration details.
     *
     * @param {selection} selection         The selection of currently selected components
     */
    var hasDetails = function (selection) {
        return nfCanvasUtils.hasDetails(selection);
    };

    /**
     * Determines whether the components in the specified selection are deletable.
     *
     * @param {selection} selection         The selection of currently selected components
     */
    var isDeletable = function (selection) {
        return nfCanvasUtils.areDeletable(selection);
    };

    /**
     * Determines whether user can create a template from the components in the specified selection.
     *
     * @param {selection} selection         The selection of currently selected components
     */
    var canCreateTemplate = function (selection) {
        return nfCanvasUtils.canWrite() && (selection.empty() && nfCanvasUtils.canRead(selection));
    };

    /**
     * Determines whether user can upload a template.
     *
     * @param {selection} selection         The selection of currently selected components
     */
    var canUploadTemplate = function (selection) {
        return nfCanvasUtils.canWrite() && selection.empty();
    };

    /**
     * Determines whether components in the specified selection are group-able.
     *
     * @param {selection} selection         The selection of currently selected components
     */
    var canGroup = function (selection) {
        return nfCanvasUtils.getComponentByType('Connection').isDisconnected(selection) && nfCanvasUtils.canModify(selection);
    };

    /**
     * Determines whether components in the specified selection are enable-able.
     *
     * @param {selection} selection         The selection of currently selected components
     */
    var canEnable = function (selection) {
        return nfCanvasUtils.canEnable(selection);
    };

    /**
     * Determines whether components in the specified selection are diable-able.
     *
     * @param {selection} selection         The selection of currently selected components
     */
    var canDisable = function (selection) {
        return nfCanvasUtils.canDisable(selection);
    };

    /**
     * Determines whether user can manage policies of the components in the specified selection.
     *
     * @param {selection} selection         The selection of currently selected components
     */
    var canManagePolicies = function (selection) {
        return nfCanvasUtils.isConfigurableAuthorizer() && nfCanvasUtils.canManagePolicies(selection);
    };

    /**
     * Determines whether the components in the specified selection are runnable.
     *
     * @param {selection} selection         The selection of currently selected components
     */
    var isRunnable = function (selection) {
        return nfCanvasUtils.areRunnable(selection);
    };

    /**
     * Determines whether the components in the specified selection are stoppable.
     *
     * @param {selection} selection         The selection of currently selected components
     */
    var isStoppable = function (selection) {
        return nfCanvasUtils.areStoppable(selection);
    };

    /**
     * Determines whether the components in the specified selection support stats.
     *
     * @param {selection} selection         The selection of currently selected components
     */
    var supportsStats = function (selection) {
        // ensure the correct number of components are selected
        if (selection.size() !== 1) {
            return false;
        }

        return nfCanvasUtils.isProcessor(selection) || nfCanvasUtils.isProcessGroup(selection) || nfCanvasUtils.isRemoteProcessGroup(selection) || nfCanvasUtils.isConnection(selection);
    };

    /**
     * Determines whether the components in the specified selection has usage documentation.
     *
     * @param {selection} selection         The selection of currently selected components
     */
    var hasUsage = function (selection) {
        // ensure the correct number of components are selected
        if (selection.size() !== 1) {
            return false;
        }
        if (nfCanvasUtils.canRead(selection) === false) {
            return false;
        }

        return nfCanvasUtils.isProcessor(selection);
    };

    /**
     * Determines whether there is one component selected.
     *
     * @param {selection} selection         The selection of currently selected components
     */
    var isNotConnection = function (selection) {
        return selection.size() === 1 && !nfCanvasUtils.isConnection(selection);
    };

    /**
     * Determines whether the components in the specified selection are copyable.
     *
     * @param {selection} selection         The selection of currently selected components
     */
    var isCopyable = function (selection) {
        return nfCanvasUtils.isCopyable(selection);
    };

    /**
     * Determines whether the components in the specified selection are pastable.
     *
     * @param {selection} selection         The selection of currently selected components
     */
    var isPastable = function (selection) {
        return nfCanvasUtils.isPastable();
    };

    /**
     * Determines whether the specified selection is empty.
     *
     * @param {selection} selection         The seleciton
     */
    var emptySelection = function (selection) {
        return selection.empty();
    };

    /**
     * Determines whether the componets in the specified selection support being moved to the front.
     *
     * @param {selection} selection         The selection
     */
    var canMoveToFront = function (selection) {
        // ensure the correct number of components are selected
        if (selection.size() !== 1) {
            return false;
        }
        if (nfCanvasUtils.canModify(selection) === false) {
            return false;
        }

        return nfCanvasUtils.isConnection(selection);
    };

    /**
     * Determines whether the components in the specified selection are alignable.
     *
     * @param {selection} selection          The selection
     */
    var canAlign = function (selection) {
        return nfCanvasUtils.canAlign(selection);
    };

    /**
     * Determines whether the components in the specified selection are colorable.
     *
     * @param {selection} selection          The selection
     */
    var isColorable = function (selection) {
        return nfCanvasUtils.isColorable(selection);
    };

    /**
     * Determines whether the component in the specified selection is a connection.
     *
     * @param {selection} selection         The selection
     */
    var isConnection = function (selection) {
        // ensure the correct number of components are selected
        if (selection.size() !== 1) {
            return false;
        }

        return nfCanvasUtils.isConnection(selection);
    };

    /**
     * Determines whether the component in the specified selection could possibly have downstream components.
     *
     * @param {selection} selection         The selection
     */
    var hasDownstream = function (selection) {
        // ensure the correct number of components are selected
        if (selection.size() !== 1) {
            return false;
        }

        return nfCanvasUtils.isFunnel(selection) || nfCanvasUtils.isProcessor(selection) || nfCanvasUtils.isProcessGroup(selection) ||
            nfCanvasUtils.isRemoteProcessGroup(selection) || nfCanvasUtils.isInputPort(selection) ||
            (nfCanvasUtils.isOutputPort(selection) && nfCanvasUtils.getParentGroupId() !== null);
    };

    /**
     * Determines whether the component in the specified selection could possibly have upstream components.
     *
     * @param {selection} selection         The selection
     */
    var hasUpstream = function (selection) {
        // ensure the correct number of components are selected
        if (selection.size() !== 1) {
            return false;
        }

        return nfCanvasUtils.isFunnel(selection) || nfCanvasUtils.isProcessor(selection) || nfCanvasUtils.isProcessGroup(selection) ||
            nfCanvasUtils.isRemoteProcessGroup(selection) || nfCanvasUtils.isOutputPort(selection) ||
            (nfCanvasUtils.isInputPort(selection) && nfCanvasUtils.getParentGroupId() !== null);
    };

    /**
     * Determines whether the current selection is a stateful processor.
     *
     * @param {selection} selection
     */
    var isStatefulProcessor = function (selection) {
        // ensure the correct number of components are selected
        if (selection.size() !== 1) {
            return false;
        }
        if (nfCanvasUtils.canRead(selection) === false || nfCanvasUtils.canModify(selection) === false) {
            return false;
        }

        if (nfCanvasUtils.isProcessor(selection)) {
            var processorData = selection.datum();
            return processorData.component.persistsState === true;
        } else {
            return false;
        }
    };

    /**
     * Determines whether the current selection is a process group.
     *
     * @param {selection} selection
     */
    var isProcessGroup = function (selection) {
        // ensure the correct number of components are selected
        if (selection.size() !== 1) {
            return false;
        }

        return nfCanvasUtils.isProcessGroup(selection);
    };

    /**
     * Determines whether the current selection could have provenance.
     *
     * @param {selection} selection
     */
    var canAccessProvenance = function (selection) {
        // ensure the correct number of components are selected
        if (selection.size() !== 1) {
            return false;
        }

        return !nfCanvasUtils.isLabel(selection) && !nfCanvasUtils.isConnection(selection) && !nfCanvasUtils.isProcessGroup(selection)
            && !nfCanvasUtils.isRemoteProcessGroup(selection) && nfCommon.canAccessProvenance();
    };

    /**
     * Determines whether the current selection is a remote process group.
     *
     * @param {selection} selection
     */
    var isRemoteProcessGroup = function (selection) {
        // ensure the correct number of components are selected
        if (selection.size() !== 1) {
            return false;
        }
        if (nfCanvasUtils.canRead(selection) === false) {
            return false;
        }

        return nfCanvasUtils.isRemoteProcessGroup(selection);
    };

    /**
     * Determines if the components in the specified selection support starting transmission.
     *
     * @param {selection} selection
     */
    var canStartTransmission = function (selection) {
        if (nfCanvasUtils.canModify(selection) === false) {
            return false;
        }

        return nfCanvasUtils.canAllStartTransmitting(selection);
    };

    /**
     * Determines if the components in the specified selection support stopping transmission.
     *
     * @param {selection} selection
     */
    var canStopTransmission = function (selection) {
        if (nfCanvasUtils.canModify(selection) === false) {
            return false;
        }

        return nfCanvasUtils.canAllStopTransmitting(selection);
    };

    /**
     * Only DFMs can empty a queue.
     *
     * @param {selection} selection
     */
    var canEmptyQueue = function (selection) {
        return isConnection(selection);
    };

    /**
     * Only DFMs can list a queue.
     *
     * @param {selection} selection
     */
    var canListQueue = function (selection) {
        return isConnection(selection);
    };

    /**
     * Determines if the components in the specified selection can be moved into a parent group.
     *
     * @param {type} selection
     */
    var canMoveToParent = function (selection) {
        if (nfCanvasUtils.canModify(selection) === false) {
            return false;
        }

        // TODO - also check can modify in parent

        return !selection.empty() && nfCanvasUtils.getComponentByType('Connection').isDisconnected(selection) && nfCanvasUtils.getParentGroupId() !== null;
    };

    /**
     * Adds a menu item to the context menu.
     *
     * {
     *      click: refresh (function),
     *      text: 'Start' (string),
     *      clazz: 'fa fa-refresh'
     * }
     *
     * @param {jQuery} contextMenu The context menu
     * @param {object} item The menu item configuration
     */
    var addMenuItem = function (contextMenu, item) {
        if (typeof item.click === 'function') {
            var menuItem = $('<div class="context-menu-item"></div>').on('click', item['click']).on('contextmenu', function (evt) {
                // invoke the desired action
                item['click'](evt);

                // stop propagation and prevent default
                evt.preventDefault();
                evt.stopPropagation();
            }).on('mouseenter', function () {
                $(this).addClass('hover');
            }).on('mouseleave', function () {
                $(this).removeClass('hover');
            }).appendTo(contextMenu);

            // create the img and conditionally add the style
            var img = $('<div class="context-menu-item-img"></div>').addClass(item['clazz']).appendTo(menuItem);
            if (nfCommon.isDefinedAndNotNull(item['imgStyle'])) {
                img.addClass(item['imgStyle']);
            }

            $('<div class="context-menu-item-text"></div>').text(item['text']).appendTo(menuItem);
            $('<div class="clear"></div>').appendTo(menuItem);
        }
    };

    /**
     * Positions and shows the context menu.
     *
     * @param {jQuery} contextMenu  The context menu
     * @param {object} options      The context menu configuration
     */
    var positionAndShow = function (contextMenu, options) {
        contextMenu.css({
            'left': options.x + 'px',
            'top': options.y + 'px'
        }).show();
    };

    /**
     * Executes the specified action with the optional selection.
     *
     * @param {string} action
     * @param {selection} selection
     * @param {mouse event} evt
     */
    var executeAction = function (action, selection, evt) {
        // execute the action
        nfActions[action](selection, evt);

        // close the context menu
        nfContextMenu.hide();
    };

    // defines the available actions and the conditions which they apply
    var actions = [
        {condition: emptySelection, menuItem: {clazz: 'fa fa-refresh', text: nf._.msg('nf-context-menu.Refresh'), action: 'reload'}},
        {condition: isNotRootGroup, menuItem: {clazz: 'fa fa-level-up', text: nf._.msg('nf-context-menu.LeaveGroup'), action: 'leaveGroup'}},
        {condition: isConfigurable, menuItem: {clazz: 'fa fa-gear', text: nf._.msg('nf-context-menu.Configure'), action: 'showConfiguration'}},
        {condition: hasDetails, menuItem: {clazz: 'fa fa-gear', text: nf._.msg('nf-context-menu.ViewConfiguration'), action: 'showDetails'}},
        {condition: isProcessGroup, menuItem: {clazz: 'fa fa-sign-in', text: nf._.msg('nf-context-menu.EnterGroup'), action: 'enterGroup'}},
        {condition: isRunnable, menuItem: {clazz: 'fa fa-play', text: nf._.msg('nf-context-menu.Start'), action: 'start'}},
        {condition: isStoppable, menuItem: {clazz: 'fa fa-stop', text: nf._.msg('nf-context-menu.Stop'), action: 'stop'}},
        {condition: canEnable, menuItem: {clazz: 'fa fa-flash', text: nf._.msg('nf-context-menu.canEnable'), action: 'enable'}},
        {condition: canDisable, menuItem: {clazz: 'icon icon-enable-false', text: nf._.msg('nf-context-menu.canDisable'), action: 'disable'}},
        {condition: canGroup, menuItem: {clazz: 'icon icon-group', text: nf._.msg('nf-context-menu.canGroup'), action: 'group'}},
        {condition: isRemoteProcessGroup, menuItem: {clazz: 'fa fa-cloud', text: nf._.msg('nf-context-menu.RemotePorts'), action: 'remotePorts'}},
        {condition: canStartTransmission, menuItem: {clazz: 'fa fa-bullseye', text: nf._.msg('nf-context-menu.EnableTransmission'), action: 'enableTransmission'}},
        {condition: canStopTransmission, menuItem: {clazz: 'icon icon-transmit-false', text: nf._.msg('nf-context-menu.DisableTransmission'), action: 'disableTransmission'}},
        {condition: supportsStats, menuItem: {clazz: 'fa fa-area-chart', text: nf._.msg('nf-context-menu.StatsHistory'), action: 'showStats'}},
        {condition: canAccessProvenance, menuItem: {clazz: 'icon icon-provenance', imgStyle: 'context-menu-provenance', text: nf._.msg('nf-context-menu.DataProvenance'), action: 'openProvenance'}},
        {condition: isStatefulProcessor, menuItem: {clazz: 'fa fa-tasks', text: nf._.msg('nf-context-menu.ViewState'), action: 'viewState'}},
        {condition: canMoveToFront, menuItem: {clazz: 'fa fa-clone', text: nf._.msg('nf-context-menu.BringFront'), action: 'toFront'}},
        {condition: isConnection, menuItem: {clazz: 'fa fa-long-arrow-left', text: nf._.msg('nf-context-menu.GoToSource'), action: 'showSource'}},
        {condition: isConnection, menuItem: {clazz: 'fa fa-long-arrow-right', text: nf._.msg('nf-context-menu.GoToDestination'), action: 'showDestination'}},
        {condition: hasUpstream, menuItem: {clazz: 'icon icon-connect', text: nf._.msg('nf-context-menu.UpstreamConnections'), action: 'showUpstream'}},
        {condition: hasDownstream, menuItem: {clazz: 'icon icon-connect', text: nf._.msg('nf-context-menu.DownstreamConnections'), action: 'showDownstream'}},
        {condition: hasUsage, menuItem: {clazz: 'fa fa-book', text: nf._.msg('nf-context-menu.Usage'), action: 'showUsage'}},
        {condition: isRemoteProcessGroup, menuItem: {clazz: 'fa fa-refresh', text: nf._.msg('nf-context-menu.Refresh'), action: 'refreshRemoteFlow'}},
        {condition: isRemoteProcessGroup, menuItem: {clazz: 'fa fa-external-link', text: nf._.msg('nf-context-menu.GoTo'), action: 'openUri'}},
        {condition: isColorable, menuItem: {clazz: 'fa fa-paint-brush', text: nf._.msg('nf-context-menu.ChangeColor'), action: 'fillColor'}},
        {condition: isNotConnection, menuItem: {clazz: 'fa fa-crosshairs', text: nf._.msg('nf-context-menu.CenterInView'), action: 'center'}},
        {condition: isCopyable, menuItem: {clazz: 'fa fa-copy', text: nf._.msg('nf-context-menu.Copy'), action: 'copy'}},
        {condition: isPastable, menuItem: {clazz: 'fa fa-paste', text: nf._.msg('nf-context-menu.Paste'), action: 'paste'}},
        {condition: canMoveToParent, menuItem: {clazz: 'fa fa-arrows', text: nf._.msg('nf-context-menu.MoveParentGroup'), action: 'moveIntoParent'}},
        {condition: canListQueue, menuItem: {clazz: 'fa fa-list', text: nf._.msg('nf-context-menu.QueueList'), action: 'listQueue'}},
        {condition: canEmptyQueue, menuItem: {clazz: 'fa fa-minus-circle', text: nf._.msg('nf-context-menu.EmptyQueue'), action: 'emptyQueue'}},
        {condition: isDeletable, menuItem: {clazz: 'fa fa-trash', text: nf._.msg('nf-context-menu.Delete'), action: 'delete'}},
        {condition: canManagePolicies, menuItem: {clazz: 'fa fa-key', text: nf._.msg('nf-context-menu.canManagePolicies'), action: 'managePolicies'}},
        {condition: canAlign, menuItem: {clazz: 'fa fa-align-center', text: nf._.msg('nf-context-menu.canAlign1'), action: 'alignVertical'}},
        {condition: canAlign, menuItem: { clazz: 'fa fa-align-center fa-rotate-90', text: nf._.msg('nf-context-menu.canAlign2'), action: 'alignHorizontal'}},
        {condition: canUploadTemplate, menuItem: {clazz: 'icon icon-template-import', text: nf._.msg('nf-context-menu.canUploadTemplate'), action: 'uploadTemplate'}},
        {condition: canCreateTemplate, menuItem: {clazz: 'icon icon-template-save', text: nf._.msg('nf-context-menu.canCreateTemplate'), action: 'template'}}
    ];

    var nfContextMenu = {

        /**
         * Initialize the context menu.
         *
         * @param nfActionsRef   The nfActions module.
         */
        init: function (nfActionsRef) {
            nfActions = nfActionsRef;

            $('#context-menu').on('contextmenu', function (evt) {
                // stop propagation and prevent default
                evt.preventDefault();
                evt.stopPropagation();
            });
        },

        /**
         * Shows the context menu.
         */
        show: function () {
            var contextMenu = $('#context-menu').empty();
            var canvasBody = $('#canvas-body').get(0);
            var bannerFooter = $('#banner-footer').get(0);
            var breadCrumb = $('#breadcrumbs').get(0);

            // get the current selection
            var selection = nfCanvasUtils.getSelection();

            // consider each component action for the current selection
            $.each(actions, function (_, action) {
                // determine if this action is application for this selection
                if (action.condition(selection, nfCanvasUtils.getComponentByType('Connection'))) {
                    var menuItem = action.menuItem;

                    addMenuItem(contextMenu, {
                        clazz: menuItem.clazz,
                        imgStyle: menuItem.imgStyle,
                        text: menuItem.text,
                        click: function (evt) {
                            executeAction(menuItem.action, selection, evt);
                        }
                    });
                }
            });

            // get the location for the context menu
            var position = d3.mouse(canvasBody);

            // nifi 1864 make sure the context menu is not hidden by the browser boundaries
            if (position[0] + contextMenu.width() > canvasBody.clientWidth) {
                position[0] = canvasBody.clientWidth - contextMenu.width() - 2;
            }
            if (position[1] + contextMenu.height() > (canvasBody.clientHeight - breadCrumb.clientHeight - bannerFooter.clientHeight)) {
                position[1] = canvasBody.clientHeight - breadCrumb.clientHeight - bannerFooter.clientHeight - contextMenu.height() - 3;
            }

            // show the context menu
            positionAndShow(contextMenu, {
                'x': position[0],
                'y': position[1]
            });

            // inform Angular app incase we've click on the canvas
            nfNgBridge.digest();
        },

        /**
         * Hides the context menu.
         */
        hide: function () {
            $('#context-menu').hide();
        },

        /**
         * Activates the context menu for the components in the specified selection.
         *
         * @param {selection} components    The components to enable the context menu for
         */
        activate: function (components) {
            components.on('contextmenu.selection', function () {
                // get the clicked component to update selection
                nfContextMenu.show();

                // stop propagation and prevent default
                d3.event.preventDefault();
                d3.event.stopPropagation();
            });
        }
    };

    return nfContextMenu;
}));