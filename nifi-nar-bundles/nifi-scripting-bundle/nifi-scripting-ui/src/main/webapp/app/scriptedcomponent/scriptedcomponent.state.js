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
'use strict';

/**
 * Defines a scripted component route.
 *
 * @argument {object} $stateProvider       The router state manager
 */
var ScriptedComponentState = function ($stateProvider) {

    $stateProvider
        .state('scriptedcomponent', {
            url: "/scriptedcomponent?id&revision&clientId&editable&entityType",
            templateUrl: "app/scriptedcomponent/scriptedcomponent.view.html",
            controller: 'ScriptedComponentController',
            resolve: {
                details: ['ScriptedComponentFactory', '$stateParams',
                    function (ScriptedComponentFactory, $stateParams) {
                        // get the component details
                        return ScriptedComponentFactory.getDetails($stateParams.id);
                    }
                ]
            },
            data: {
                // NiFi script engine name to MIME
                nameToMime: {
                    'clojure': 'text/x-clojure',
                    'ecmascript': 'application/ecmascript',
                    'groovy': 'text/x-groovy',
                    'lua': 'text/x-lua',
                    'python': 'text/x-python',
                    'ruby': 'text/x-ruby'
                }
            }
        })

};

ScriptedComponentState.$inject = ['$stateProvider'];

angular.module('standardUI').config(ScriptedComponentState);