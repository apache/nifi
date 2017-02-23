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
                'nf.Common'],
            function ($, nfCommon) {
                return (nf.Client = factory($, nfCommon));
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.Client =
            factory(require('jquery'),
                require('nf.Common')));
    } else {
        nf.Client =
            factory(root.$,
                root.nf.Common);
    }
}(this, function ($, nfCommon) {
    'use strict';

    var clientId = null;

    return {
        /**
         * Initializes the client.
         *
         * @returns deferred
         */
        init: function () {
            return $.ajax({
                type: 'GET',
                url: '../nifi-api/flow/client-id',
            }).done(function (cid) {
                clientId = cid;
            });
        },

        /**
         * Builds the revision fof the specified component
         * @param d The component
         * @returns The revision
         */
        getRevision: function (d) {
            return {
                'clientId': clientId,
                'version': d.revision.version
            };
        },

        /**
         * Determines whether the proposedData is not older than the currentData.
         *
         * @param currentData Maybe be null, if the proposedData is new to this canvas
         * @param proposedData Maybe not be null
         * @return {boolean} whether proposedData is newer than currentData
         */
        isNewerRevision: function (currentData, proposedData) {
            if (nfCommon.isDefinedAndNotNull(currentData)) {
                var currentRevision = currentData.revision;
                var proposedRevision = proposedData.revision;

                // return whether the proposed revision is not less
                return proposedRevision.version >= currentRevision.version;
            } else {
                return true;
            }
        }
    };
}));