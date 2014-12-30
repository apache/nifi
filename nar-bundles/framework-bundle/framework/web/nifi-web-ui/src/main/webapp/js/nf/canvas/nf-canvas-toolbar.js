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
nf.CanvasToolbar = (function () {

    var actions;

    return {
        /**
         * Initializes the canvas toolbar.
         */
        init: function () {
            actions = {};

            var separator = $('<div/>').addClass('control-separator');
            var border = $('<div/>').addClass('control-border');

            var globalControls = $('#global-controls')[0];
            border.clone().appendTo(globalControls);
            actions['enable'] = new nf.ToolbarAction(globalControls, 'enable', 'action-enable', 'enable-all', 'enable-all-hover', 'enable-all-disable', 'Enable');
            border.clone().appendTo(globalControls);
            actions['disable'] = new nf.ToolbarAction(globalControls, 'disable', 'action-disable', 'disable-all', 'disable-all-hover', 'disable-all-disable', 'Disable');
            border.clone().appendTo(globalControls);
            separator.clone().appendTo(globalControls);
            border.clone().appendTo(globalControls);
            actions['start'] = new nf.ToolbarAction(globalControls, 'start', 'action-start', 'start-all', 'start-all-hover', 'start-all-disable', 'Start');
            border.clone().appendTo(globalControls);
            actions['stop'] = new nf.ToolbarAction(globalControls, 'stop', 'action-stop', 'stop-all', 'stop-all-hover', 'stop-all-disable', 'Stop');
            border.clone().appendTo(globalControls);
            separator.clone().appendTo(globalControls);
            border.clone().appendTo(globalControls);
            actions['template'] = new nf.ToolbarAction(globalControls, 'template', 'action-template', 'template', 'template-hover', 'template-disable', 'Create Template');
            border.clone().appendTo(globalControls);
            separator.clone().appendTo(globalControls);
            border.clone().appendTo(globalControls);
            actions['copy'] = new nf.ToolbarAction(globalControls, 'copy', 'action-copy', 'copy', 'copy-hover', 'copy-disable', 'Copy');
            border.clone().appendTo(globalControls);
            actions['paste'] = new nf.ToolbarAction(globalControls, 'paste', 'action-paste', 'paste', 'paste-hover', 'paste-disable', 'Paste');
            border.clone().appendTo(globalControls);
            separator.clone().appendTo(globalControls);
            border.clone().appendTo(globalControls);
            actions['group'] = new nf.ToolbarAction(globalControls, 'group', 'action-group', 'group', 'group-hover', 'group-disable', 'Group');
            border.appendTo(globalControls);
            separator.clone().appendTo(globalControls);
            border.clone().appendTo(globalControls);
            actions['fill'] = new nf.ToolbarAction(globalControls, 'fillColor', 'action-fill', 'fill', 'fill-hover', 'fill-disable', 'Change Color');
            border.clone().appendTo(globalControls);
            separator.clone().appendTo(globalControls);
            border.clone().appendTo(globalControls);
            actions['delete'] = new nf.ToolbarAction(globalControls, 'delete', 'action-delete', 'delete', 'delete-hover', 'delete-disable', 'Delete');
            border.appendTo(globalControls);
            separator.appendTo(globalControls);

            // set up initial states for selection-less items
            if (nf.Common.isDFM()) {
                actions['start'].enable();
                actions['stop'].enable();
                actions['template'].enable();
            } else {
                actions['start'].disable();
                actions['stop'].disable();
                actions['template'].disable();
            }

            // disable actions that require selection
            actions['enable'].disable();
            actions['disable'].disable();
            actions['copy'].disable();
            actions['paste'].disable();
            actions['fill'].disable();
            actions['delete'].disable();
            actions['group'].disable();

            // add a clipboard listener if appropriate
            if (nf.Common.isDFM()) {
                nf.Clipboard.addListener(this, function (action, data) {
                    if (nf.Clipboard.isCopied()) {
                        actions['paste'].enable();
                    } else {
                        actions['paste'].disable();
                    }
                });
            }
        },
        
        /**
         * Called when the selection changes to update the toolbar appropriately.
         */
        refresh: function () {
            // wait for the toolbar to initialize
            if (nf.Common.isUndefined(actions)) {
                return;
            }

            // only refresh the toolbar if DFM
            if (nf.Common.isDFM()) {
                var selection = nf.CanvasUtils.getSelection();

                // if all selected components are deletable enable the delete button
                if (!selection.empty()) {
                    var enableDelete = true;
                    selection.each(function (d) {
                        if (!nf.CanvasUtils.isDeletable(d3.select(this))) {
                            enableDelete = false;
                            return false;
                        }
                    });
                    if (enableDelete) {
                        actions['delete'].enable();
                    } else {
                        actions['delete'].disable();
                    }
                } else {
                    actions['delete'].disable();
                }

                // if there are any copyable components enable the button
                if (nf.CanvasUtils.isCopyable(selection)) {
                    actions['copy'].enable();
                } else {
                    actions['copy'].disable();
                }

                // determine if the selection is groupable
                if (!selection.empty() && nf.CanvasUtils.isDisconnected(selection)) {
                    actions['group'].enable();
                } else {
                    actions['group'].disable();
                }

                // determine how many colorable components are selected
                var colorableComponents = selection.filter(function (d) {
                    var selected = d3.select(this);
                    return nf.CanvasUtils.isProcessor(selected) || nf.CanvasUtils.isLabel(selected);
                });

                // if there are any colorable components enable the button
                if (colorableComponents.size() === 1 && colorableComponents.size() === selection.size()) {
                    actions['fill'].enable();
                } else {
                    actions['fill'].disable();
                }
                
                // determine if there are any selected components that support enable/disable
                var supportsEnable = selection.filter(function(d) {
                    var selected = d3.select(this);
                    return nf.CanvasUtils.isProcessor(selected) || nf.CanvasUtils.isInputPort(selected) || nf.CanvasUtils.isOutputPort(selected);
                });
                
                // ensure the entire selection supports enable/disable
                if (!supportsEnable.empty() && supportsEnable.size() === selection.size()) {
                    actions['enable'].enable();
                    actions['disable'].enable();
                } else {
                    actions['enable'].disable();
                    actions['disable'].disable();
                }
            }
        }
    };
}());