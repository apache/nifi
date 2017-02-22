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
        define(['nf.ng.Bridge',
                'nf.Common',
                'nf.CanvasUtils',
                'nf.ClusterSummary',
                'nf.Actions'],
            function (nfNgBridge, nfCanvasUtils, nfCommon, nfClusterSummary, nfActions) {
                return (nf.ng.AppCtrl = factory(nfNgBridge, nfCanvasUtils, nfCommon, nfClusterSummary, nfActions));
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.ng.AppCtrl =
            factory(require('nf.ng.Bridge'),
                require('nf.CanvasUtils'),
                require('nf.Common'),
                require('nf.ClusterSummary'),
                require('nf.Actions')));
    } else {
        nf.ng.AppCtrl = factory(root.nf.ng.Bridge,
            root.nf.CanvasUtils,
            root.nf.Common,
            root.nf.ClusterSummary,
            root.nf.Actions);
    }
}(this, function (nfNgBridge, nfCanvasUtils, nfCommon, nfClusterSummary, nfActions) {
    'use strict';

    return function ($scope, serviceProvider) {
        'use strict';

        function AppCtrl(serviceProvider) {
            //add essential modules to the scope for availability throughout the angular container
            this.nf = {
                "Common": nfCommon,
                "ClusterSummary": nfClusterSummary,
                "Actions": nfActions,
                "CanvasUtils": nfCanvasUtils,
            };

            //any registered angular service is available through the serviceProvider
            this.serviceProvider = serviceProvider;
        }

        AppCtrl.prototype = {
            constructor: AppCtrl
        }

        var appCtrl = new AppCtrl(serviceProvider);
        $scope.appCtrl = appCtrl;

        //For production angular applications .scope() is unavailable so we set
        //the root scope of the bootstrapped app on the bridge
        nfNgBridge.rootScope = $scope;
    }
}));