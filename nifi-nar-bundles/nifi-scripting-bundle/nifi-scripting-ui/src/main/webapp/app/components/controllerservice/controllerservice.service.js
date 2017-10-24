/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

'use strict';

var ControllerServiceService = function ControllerServiceService($http) {

    return {
        'setProperties': setProperties,
        'getType': getType,
        'getDetails': getDetails
    };

    function setProperties(controllerServiceId, revisionId, clientId, properties) {
        var urlParams = 'controllerServiceId=' + controllerServiceId + '&revisionId=' + revisionId + '&clientId=' + clientId;
        return $http({
            url: "api/standard/controller-service/properties?" + urlParams,
            method: 'PUT',
            data: properties
        });
    }

    function getType(id) {
        return $http({
            url: "api/standard/controller-service/details?controllerServiceId=" + id,
            method: 'GET',
            transformResponse: [function (data) {
                var obj = JSON.parse(data)
                var type = obj['type'];
                return type;
            }]
        });
    }

    function getDetails(id) {
        return $http({
            url: "api/standard/controller-service/details?controllerServiceId=" + id,
            method: 'GET'
        });
    }
}

ControllerServiceService.$inject = ['$http'];

angular.module('standardUI').service('ControllerServiceService', ControllerServiceService);