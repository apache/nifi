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
        define(['nf.ErrorHandler',
                'nf.Common',
                'nf.Canvas',
                'nf.ContextMenu'],
            function (ajaxErrorHandler, nfCommon, nfCanvas, nfContextMenu) {
                return (nf.ErrorHandler = factory(ajaxErrorHandler, nfCommon, nfCanvas, nfContextMenu));
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.ErrorHandler =
            factory(require('nf.ErrorHandler'),
                require('nf.Common'),
                require('nf.Canvas'),
                require('nf.ContextMenu')));
    } else {
        nf.ErrorHandler = factory(root.nf.ErrorHandler,
            root.nf.Common,
            root.nf.Canvas,
            root.nf.ContextMenu);
    }
}(this, function (ajaxErrorHandler, nfCommon, nfCanvas, nfContextMenu) {
    'use strict';

    return {

        /**
         * Method for handling ajax errors. This also closes the canvas.
         *
         * @argument {object} xhr       The XmlHttpRequest
         * @argument {string} status    The status of the request
         * @argument {string} error     The error
         */
        handleAjaxError: function (xhr, status, error) {
            ajaxErrorHandler.handleAjaxError(xhr, status, error);
            nfCommon.showLogoutLink();

            // hide the splash screen if required
            if ($('#splash').is(':visible')) {
                nfCanvas.hideSplash();
            }

            // hide the context menu
            nfContextMenu.hide();

            // shut off the auto refresh
            nfCanvas.stopPolling();

            // allow page refresh with ctrl-r
            nfCanvas.disableRefreshHotKey();
        }
    };
}));