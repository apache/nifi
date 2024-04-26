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
        define(['jquery', 'nf.FlowVersion', 'nf.Settings', 'nf.Dialog', 'nf.Common'],
            function ($, nfFlowVersion, nfDialog, nfCommon) {
                return (nf.ng.RegistryImportComponent = factory($, nfFlowVersion,nfSettings, nfDialog, nfCommon));
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.ng.RegistryImportComponent =
            factory(require('jquery'), require('nf.FlowVersion', 'nf.Settings', 'nf.Dialog', 'nf.Common')));
    } else {
        nf.ng.RegistryImportComponent = factory(root.$, root.nf.FlowVersion, root.nf.Settings, root.nf.Dialog, root.nf.Common);
    }
}(this, function ($, nfFlowVersion, nfSettings, nfDialog, nfCommon) {
    'use strict';

    return function () {
        'use strict';

        function RegistryImportComponent() {
            this.icon = 'icon icon-import-from-registry';
            this.hoverIcon = 'icon icon-import-from-registry-add';
        }

        RegistryImportComponent.prototype = {
            constructor: RegistryImportComponent,

            /**
             * Gets the component.
             *
             * @returns {*|jQuery|HTMLElement}
             */
            getElement: function () {
                return $('#registry-import-component');
            },

            /**
             * Enable the component.
             */
            enabled: function () {
                this.getElement().attr('disabled', false);
            },

            /**
             * Disable the component.
             */
            disabled: function () {
                this.getElement().attr('disabled', true);
            },

            /**
             * Handler function for when component is dropped on the canvas.
             *
             * @argument {object} pt        The point that the component was dropped.
             */
            dropHandler: function (pt) {
                if (nfCommon.canVersionFlows()) {
                    nfFlowVersion.showImportFlowDialog(pt);
                } else {
                    nfDialog.showYesNoDialog({
                        headerText: 'No Registry Client available',
                        dialogContent: 'In order to import flows from a Registry a Registry Client must be configured in Controller Settings.',
                        noText: 'Cancel',
                        yesText: 'Configure',
                        yesHandler: function () {
                            // get the connection data
                            nfSettings.showSettings('Registry Clients');
                        }
                    });
                }
            },

            /**
             * The drag icon for the toolbox component.
             *
             * @param event
             * @returns {*|jQuery|HTMLElement}
             */
            dragIcon: function (event) {
                return $('<div class="icon icon-import-from-registry-add"></div>');
            }
        }

        var registryImportComponent = new RegistryImportComponent();
        return registryImportComponent;
    };
}));