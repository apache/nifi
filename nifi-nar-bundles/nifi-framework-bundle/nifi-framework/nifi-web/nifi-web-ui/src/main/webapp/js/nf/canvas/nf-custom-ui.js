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
            'nf.Shell',
            'nf.Dialog',
            'nf.Storage',
            'nf.Client'], function ($, nfCommon, nfShell, nfDialog, nfStorage, nfClient) {
            return (nf.CustomUi = factory($, nfCommon, nfShell, nfDialog, nfStorage, nfClient));
        });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.CustomUi = factory(require('jquery'),
            require('nf.Common'),
            require('nf.Shell'),
            require('nf.Dialog'),
            require('nf.Storage'),
            require('nf.Client')));
    } else {
        nf.CustomUi = factory(root.$,
            root.nf.Common,
            root.nf.Shell,
            root.nf.Dialog,
            root.nf.Storage,
            root.nf.Client);
    }
}(this, function ($, nfCommon, nfShell, nfDialog, nfStorage, nfClient) {
    'use strict';

    return {
        /**
         * Shows the custom ui.
         *
         * @argument {object} entity            The component id
         * @arugment {string} uri               The custom ui uri
         * @argument {boolean} editable         Whether the custom ui should support editing
         */
        showCustomUi: function (entity, uri, editable) {
            return $.Deferred(function (deferred) {
                nfCommon.getAccessToken('../nifi-api/access/ui-extension-token').done(function (uiExtensionToken) {
                    // record the processor id
                    $('#shell-close-button');

                    var revision = nfClient.getRevision(entity);

                    // build the customer ui params
                    var customUiParams = {
                        'id': entity.id,
                        'revision': revision.version,
                        'clientId': revision.clientId,
                        'editable': editable,
                        'disconnectedNodeAcknowledged': nfStorage.isDisconnectionAcknowledged()
                    };

                    // conditionally include the ui extension token
                    if (!nfCommon.isBlank(uiExtensionToken)) {
                        customUiParams['access_token'] = uiExtensionToken;
                    }

                    // show the shell
                    nfShell.showPage('..' + uri + '?' + $.param(customUiParams), false).done(function () {
                        deferred.resolve();
                    });
                }).fail(function () {
                    nfDialog.showOkDialog({
                        headerText: 'Advanced Configuration',
                        dialogContent: 'Unable to generate access token for accessing the advanced configuration dialog.'
                    });
                    deferred.resolve();
                });
            }).promise();
        }
    }
}));