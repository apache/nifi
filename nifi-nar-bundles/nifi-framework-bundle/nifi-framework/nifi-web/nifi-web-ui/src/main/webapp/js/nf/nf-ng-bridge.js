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

/* global nf, d3 */

nf.ng.Bridge = (function () {

    function AngularBridge() {
        this.rootScope;
    };
    AngularBridge.prototype = {
        constructor: AngularBridge,

        /**
         * Sets the root scope for the angular application being bridged.
         *
         * @param {object} scope    An object that refers to the application model.
         */
        setRootScope: function (scope) {
            this.rootScope = scope;
        },

        /**
         * Inspects the root scope of the bridged angular application to look up
         * objects (to be provided as the `this` context) and invoke methods.
         *
         * @param {string} thisArg          The value of `this` provided for the call to `fun`.
         *                                  Note that this may not be the actual value seen
         *                                  by the method: if the method is a function in
         *                                  non-strict mode code, null and undefined will be
         *                                  replaced with the global object, and primitive
         *                                  values will be boxed.
         * @param {string} fun              The function to call.
         * @param [, arg1[, arg2[, ...]]]   Arguments for the `fun`
         * @returns {*}                     The value of the `fun` if `fun` returns a value.
         *
         */
        call: function (thisArg, fun) {
            var objArray = thisArg.split(".");
            var obj = this.rootScope;
            angular.forEach(objArray, function (value) {
                obj = obj[value];
            });
            var funArray = fun.split(".");
            fun = this.rootScope;
            angular.forEach(funArray, function (value) {
                fun = fun[value];
            });
            var args = Array.prototype.slice.call(arguments, 2);
            var result = fun.apply(obj, args);
            this.rootScope.$apply();
            if (result) {
                return result;
            }
        },

        /**
         * Inspects the root scope of the bridged angular application to look up
         * and return object.
         *
         * @param {string} name     The name of the object to lookup.
         * @returns {Object|*}
         */
        get: function (name) {
            var objArray = name.split(".");
            var obj = this.rootScope;
            angular.forEach(objArray, function (value) {
                obj = obj[value];
            });
            return obj;
        },

        digest: function () {
            this.rootScope.$digest();
        }
    };
    var angularBridge = new AngularBridge();

    return angularBridge;
}());