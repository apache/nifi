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
        define([], function () {
            return (nf.AuthorizationStorage = factory());
        });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.AuthorizationStorage = factory());
    } else {
        nf.AuthorizationStorage = factory();
    }
}(this, function () {
    var TOKEN_ITEM_KEY = 'Access-Token-Expiration';

    var REQUEST_TOKEN_PATTERN = new RegExp('Request-Token=([^;]+)');

    return {
        /**
         * Get Request Token from document cookies
         *
         * @return Request Token string or null when not found
         */
        getRequestToken: function () {
            var requestToken = null;
            var requestTokenMatcher = REQUEST_TOKEN_PATTERN.exec(document.cookie);
            if (requestTokenMatcher) {
                requestToken = requestTokenMatcher[1];
            }
            return requestToken;
        },

        /**
         * Get Token from Session Storage
         *
         * @return Bearer Token string
         */
        getToken: function () {
            return sessionStorage.getItem(TOKEN_ITEM_KEY);
        },

        /**
         * Has Token returns the status of whether Session Storage contains the Token
         *
         * @return Boolean status of whether Session Storage contains the Token
         */
        hasToken: function () {
            var token = this.getToken();
            return typeof token === 'string';
        },

        /**
         * Remove Token from Session Storage
         *
         */
        removeToken: function () {
            sessionStorage.removeItem(TOKEN_ITEM_KEY);
        },

        /**
         * Set Token in Session Storage
         *
         * @param token Token String
         */
        setToken: function (token) {
            sessionStorage.setItem(TOKEN_ITEM_KEY, token);
        }
    };
}));