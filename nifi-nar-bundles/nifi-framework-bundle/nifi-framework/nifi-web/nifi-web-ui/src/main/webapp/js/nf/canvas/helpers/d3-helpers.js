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
        define([],
            function () {
                return (nf.ng.D3Helpers = factory());
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.ng.D3Helpers =
            factory());
    } else {
        nf.ng.D3Helpers = factory();
    }
}(this, function () {
    'use strict';

    return {
        multiAttr: function(selection, attrs) {
            Object.keys(attrs).forEach(function (key) {
                selection.attr(key, attrs[key]);
            });
            return selection;
        }
    };
}));