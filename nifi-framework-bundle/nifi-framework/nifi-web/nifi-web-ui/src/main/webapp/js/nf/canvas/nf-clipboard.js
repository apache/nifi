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

/**
 * Clipboard used for copying and pasting content.
 */
(function (root, factory) {
    if (typeof define === 'function' && define.amd) {
        define(['jquery',
                'nf.Common'],
            function ($, nfCommon) {
                return (nf.Clipboard = factory($, nfCommon));
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.Clipboard =
            factory(require('jquery'),
                require('nf.Common')));
    } else {
        nf.Clipboard = factory(root.$,
            root.nf.Common);
    }
}(this, function ($, nfCommon) {
    'use strict';

    var COPY = 'copy';
    var PASTE = 'paste';
    var data = null;
    var listeners = {};

    return {
        /**
         * Add a listener to receive copy and paste events.
         *
         * @argument {object} listener      A clipboard listener
         * @argument {function} funct       Callback when clipboard events occur
         */
        addListener: function (listener, funct) {
            listeners[listener] = funct;
        },

        /**
         * Remove the specified listener.
         *
         * @argument {object} listener      A clipboard listener
         */
        removeListener: function (listener) {
            if (nfCommon.isDefinedAndNotNull(listeners[listener])) {
                delete listeners[listener];
            }
        },

        /**
         * Copy the specified data.
         *
         * @argument {object} d      The data to copy to the clipboard
         */
        copy: function (d) {
            data = d;

            // inform the listeners
            for (var listener in listeners) {
                listeners[listener].call(listener, COPY, data);
            }
        },

        /**
         * Checks to see if any data has been copied.
         */
        isCopied: function () {
            return nfCommon.isDefinedAndNotNull(data);
        },

        /**
         * Gets the most recent data thats copied. This operation
         * will remove the corresponding data from the clipboard.
         */
        paste: function () {
            return $.Deferred(function (deferred) {
                // ensure there was data
                if (nfCommon.isDefinedAndNotNull(data)) {
                    var clipboardData = data;

                    // resolve the deferred
                    deferred.resolve(clipboardData);

                    // clear the clipboard
                    data = null;

                    // inform the listeners
                    for (var listener in listeners) {
                        listeners[listener].call(listener, PASTE, clipboardData);
                    }
                } else {
                    deferred.reject();
                }
            }).promise();
        }
    };
}));