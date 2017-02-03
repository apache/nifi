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
                'nf.Common',
                'nf.CanvasUtils'],
            function ($, common, canvasUtils) {
                return (nf.PortDetails = factory($, common, canvasUtils));
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.PortDetails =
            factory(require('jquery'),
                require('nf.Common'),
                require('nf.CanvasUtils')));
    } else {
        nf.PortDetails = factory(root.$,
            root.nf.Common,
            root.nf.CanvasUtils);
    }
}(this, function ($, common, canvasUtils) {
    'use strict';

    return {
        init: function () {
            // configure the processor details dialog
            $('#port-details').modal({
                scrollableContentStyle: 'scrollable',
                headerText: 'Port Details',
                buttons: [{
                    buttonText: 'Ok',
                    color: {
                        base: '#728E9B',
                        hover: '#004849',
                        text: '#ffffff'
                    },
                    handler: {
                        click: function () {
                            // hide the dialog
                            $('#port-details').modal('hide');
                        }
                    }
                }],
                handler: {
                    close: function () {
                        // clear the processor details
                        common.clearField('read-only-port-name');
                        common.clearField('read-only-port-id');
                        common.clearField('read-only-port-comments');
                    }
                }
            });
        },

        showDetails: function (selection) {
            // if the specified component is a processor, load its properties
            if (canvasUtils.isInputPort(selection) || canvasUtils.isOutputPort(selection)) {
                var selectionData = selection.datum();

                // populate the port settings
                common.populateField('read-only-port-name', selectionData.component.name);
                common.populateField('read-only-port-id', selectionData.id);
                common.populateField('read-only-port-comments', selectionData.component.comments);

                // show the details
                $('#port-details').modal('show');
            }
        }
    };
}));