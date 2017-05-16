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
        define(['jquery'], function ($) {
            return (nf.ClusterSummary = factory($));
        });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.ClusterSummary =
            factory(require('jquery')));
    } else {
        nf.ClusterSummary = factory(root.$);
    }
}(this, function ($) {
    var clustered = false;
    var connectedToCluster = false;
    var connectedStateChanged = false;

    var config = {
        urls: {
            clusterSummary: '../nifi-api/flow/cluster/summary'
        }
    };

    return {

        /**
         * Loads the flow configuration and updated the cluster state.
         *
         * @returns xhr
         */
        loadClusterSummary: function () {
            return $.ajax({
                type: 'GET',
                url: config.urls.clusterSummary,
                dataType: 'json'
            }).done(function (clusterSummaryResult) {
                var clusterSummaryResponse = clusterSummaryResult;
                var clusterSummary = clusterSummaryResponse.clusterSummary;

                // see if the connected state changes
                if (connectedToCluster !== clusterSummary.connectedToCluster) {
                    connectedStateChanged = true;
                }

                // establish the initial cluster state
                clustered = clusterSummary.clustered;
                connectedToCluster = clusterSummary.connectedToCluster;
            });
        },

        /**
         * Return whether this instance of NiFi is clustered.
         *
         * @returns {Boolean}
         */
        isClustered: function () {
            return clustered === true;
        },

        /**
         * Return whether this instance is connected to a cluster.
         *
         * @returns {boolean}
         */
        isConnectedToCluster: function () {
            return connectedToCluster === true;
        },

        /**
         * Returns whether the connected state has changed since the last time
         * didConnectedStateChange was invoked.
         */
        didConnectedStateChange: function () {
            var stateChanged = connectedStateChanged;
            connectedStateChanged = false;
            return stateChanged;
        }
    };
}));