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

nf.ng.ServiceProvider = (function () {

    function ServiceProvider() {
        function ServiceProvider() {
        };
        ServiceProvider.prototype = {
            constructor: ServiceProvider,

            /**
             * Registers the given `object` by `name`.
             *
             * @param {string} name     The lookup name of the object being registered
             * @param {object} object   The object to register
             */
            register: function (name, object) {
                serviceProvider[name] = object;
            },

            /**
             * Removes the given object from the registry.
             *
             * @param {string objectName    The lookup name of the object to remove from the registry
             */
            remove: function (objectName) {
                delete serviceProvider[objectName];
            }
        };
        var serviceProvider = new ServiceProvider();
        return serviceProvider;
    }

    ServiceProvider.$inject = [];

    return ServiceProvider;
}());