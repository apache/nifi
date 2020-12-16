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

/* global top, define, module, require, exports */

(function (root, factory) {
    if (typeof define === 'function' && define.amd) {
        define(['jquery'],
            function ($) {
                return (nf.Logout = factory($));
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.Logout =
            factory(require('jquery')));
    } else {
        nf.Logout = factory(root.$);
    }
}(this, function ($) {
    'use strict';

    $(document).ready(function () {
        $('#user-home').on('mouseenter', function () {
            $(this).addClass('link-over');
        }).on('mouseleave', function () {
            $(this).removeClass('link-over');
        }).on('click', function () {
            window.location = '../nifi';
        });
    });
}));