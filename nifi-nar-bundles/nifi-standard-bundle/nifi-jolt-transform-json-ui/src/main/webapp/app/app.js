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

var AppRun =  function($rootScope,$state,$http){

    if (nf.Storage.hasItem('jwt')) {
        var token = nf.Storage.getItem('jwt');
        $http.defaults.headers.common.Authorization = 'Bearer ' + token;
    }

    $rootScope.$on('$stateChangeError', function(event, toState, toParams, fromState, fromParams, error){
        event.preventDefault();
        $state.go('error');
    });

};

var AppConfig = function ($urlRouterProvider) {

    $urlRouterProvider.otherwise(function($injector,$location){
        var urlComponents = $location.absUrl().split("?");
        return '/main?' + urlComponents[1];
    });

};

AppRun.$inject = ['$rootScope','$state','$http'];

AppConfig.$inject = ['$urlRouterProvider'];

angular.module('standardUI', ['ui.codemirror','ui.router'])
    .run(AppRun)
    .config(AppConfig);