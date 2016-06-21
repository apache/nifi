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

/* global nf */

nf.CustomUi = {
    /**
     * Shows the custom ui.
     * 
     * @argument {object} entity            The component id
     * @arugment {string} uri               The custom ui uri
     * @argument {boolean} editable         Whether the custom ui should support editing
     */
    showCustomUi: function (entity, uri, editable) {
        return $.Deferred(function (deferred) {
            nf.Common.getAccessToken('../nifi-api/access/ui-extension-token').done(function (uiExtensionToken) {
                // record the processor id
                $('#shell-close-button');

                var revision = nf.Client.getRevision(entity);

                // build the customer ui params
                var customUiParams = {
                    'id': entity.id,
                    'revision': revision.version,
                    'clientId': revision.clientId,
                    'editable': editable
                };

                // conditionally include the ui extension token
                if (!nf.Common.isBlank(uiExtensionToken)) {
                    customUiParams['access_token'] = uiExtensionToken;
                }

                // show the shell
                nf.Shell.showPage('..' + uri + '?' + $.param(customUiParams), false).done(function () {
                    deferred.resolve();
                });
            }).fail(function () {
                nf.Dialog.showOkDialog({
                    headerText: 'Configuration',
                    dialogContent: 'Unable to generate access token for accessing the advanced configuration dialog.'
                });
                deferred.resolve();
            });
        }).promise();
    }
};