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
                'nf.AuthorizationStorage'],
            function ($, nfAuthorizationStorage) {
                return (nf.AjaxSetup = factory($, nfAuthorizationStorage));
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.AjaxSetup = factory(require('jquery'),
            require('nf.AuthorizationStorage')));
    } else {
        nf.AjaxSetup = factory(root.$,
            root.nf.AuthorizationStorage);
    }
}(this, function ($, nfAuthorizationStorage) {
    /**
     * Performs ajax setup for use within NiFi.
     */
    $(document).ready(function ($) {
        $.ajaxSetup({
            'beforeSend': function (xhr) {
                // Get the Request Token for CSRF mitigation on and send on all requests
                var requestToken = nfAuthorizationStorage.getRequestToken();
                if (requestToken !== null) {
                    xhr.setRequestHeader('Request-Token', requestToken);
                }
            }
        });
    });
}));