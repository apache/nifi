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

nf.ng.Canvas.AppCtrl = (function () {

    function AppCtrl($scope, serviceProvider) {
        function AppCtrl(serviceProvider) {
            //global nf namespace for reference throughout angular app
            this.nf = nf;
            //any registered angular service is available through the serviceProvider
            this.serviceProvider = serviceProvider;
        };
        AppCtrl.prototype = {
            constructor: AppCtrl
        };
        var appCtrl = new AppCtrl(serviceProvider);
        $scope.appCtrl = appCtrl;

        //For production angular applications .scope() is unavailable so we set
        //the root scope of the bootstrapped app on the bridge
        nf.ng.Bridge.setRootScope($scope);
    }

    AppCtrl.$inject = ['$scope', 'serviceProvider', 'headerCtrl', 'graphControlsCtrl'];

    return AppCtrl;
}());