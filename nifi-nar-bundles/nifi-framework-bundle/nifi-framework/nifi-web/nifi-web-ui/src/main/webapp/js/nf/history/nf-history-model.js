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
        define(['jquery',
                'Slick',
                'nf.Common',
                'nf.ErrorHandler'],
            function ($, Slick, nfCommon, nfErrorHandler) {
                return (nf.HistoryModel = factory($, Slick, nfCommon, nfErrorHandler));
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.HistoryModel =
            factory(require('jquery'),
                require('Slick'),
                require('nf.Common'),
                require('nf.ErrorHandler')));
    } else {
        nf.HistoryModel = factory(root.$,
            root.Slick,
            root.nf.Common,
            root.nf.ErrorHandler);
    }
}(this, function ($, Slick, nfCommon, nfErrorHandler) {
    'use strict';

    // private
    var PAGESIZE = 50;

    var data = {
        length: 0
    };

    var filter = {};
    var sortcol = null;
    var sortdir = 1;

    var h_request = null;
    var xhr = null; // ajax request

    // events
    var onDataLoading = new Slick.Event();
    var onDataLoaded = new Slick.Event();

    var isDataLoaded = function (from, to) {
        for (var i = from; i <= to; i++) {
            if (data[i] === undefined || data[i] === null) {
                return false;
            }
        }

        return true;
    };

    var clear = function () {
        for (var key in data) {
            delete data[key];
        }
        data.length = 0;
    };

    var ensureData = function (from, to) {
        if (xhr) {
            xhr.abort();
            for (var i = xhr.fromPage; i <= xhr.toPage; i++) {
                data[i * PAGESIZE] = undefined;
            }
        }

        if (from < 0) {
            from = 0;
        }

        var fromPage = Math.floor(from / PAGESIZE);
        var toPage = Math.floor(to / PAGESIZE);

        while (data[fromPage * PAGESIZE] !== undefined && fromPage < toPage) {
            fromPage++;
        }

        while (data[toPage * PAGESIZE] !== undefined && fromPage < toPage) {
            toPage--;
        }

        if (fromPage > toPage || ((fromPage === toPage) && data[fromPage * PAGESIZE] !== undefined)) {
            // TODO:  look-ahead
            return;
        }

        var query = {};

        // add the start and end date to the query params
        query = $.extend({
            count: ((toPage - fromPage) * PAGESIZE) + PAGESIZE,
            offset: fromPage * PAGESIZE
        }, query);

        // conditionally add the sort details
        if (sortcol !== null) {
            query['sortColumn'] = sortcol;
            query['sortOrder'] = (sortdir > 0) ? "asc" : "desc";
        }

        // add the filter
        query = $.extend(query, filter);

        // if there is an request currently scheduled, cancel it
        if (h_request !== null) {
            clearTimeout(h_request);
        }

        // schedule the request for data
        h_request = setTimeout(function () {
            for (var i = fromPage; i <= toPage; i++) {
                data[i * PAGESIZE] = null; // null indicates a 'requested but not available yet'
            }

            // notify that loading is about to occur
            onDataLoading.notify({
                from: from,
                to: to
            });

            // perform query...
            var xhr = $.ajax({
                type: 'GET',
                url: '../nifi-api/flow/history',
                data: query,
                dataType: 'json'
            }).done(function (response) {
                var history = response.history;

                // calculate the indices
                var from = fromPage * PAGESIZE;
                var to = from + history.actions.length;

                // update the data length
                data.length = history.total;

                // populate the history actions
                for (var i = 0; i < history.actions.length; i++) {
                    data[from + i] = history.actions[i];
                    data[from + i].index = from + i;
                }

                // update the stats last refreshed timestamp
                $('#history-last-refreshed').text(history.lastRefreshed);

                // set the timezone for the start and end time
                $('.timezone').text(nfCommon.substringAfterLast(history.lastRefreshed, ' '));

                // show the filter message if applicable
                if (query['sourceId'] || query['userIdentity'] || query['startDate'] || query['endDate']) {
                    $('#history-filter-overview').css('visibility', 'visible');
                } else {
                    $('#history-filter-overview').css('visibility', 'hidden');
                }

                // clear the current request
                xhr = null;

                // notify data loaded
                onDataLoaded.notify({
                    from: from,
                    to: to
                });
            }).fail(nfErrorHandler.handleAjaxError);
            xhr.fromPage = fromPage;
            xhr.toPage = toPage;

        }, 50);
    };

    var reloadData = function (from, to) {
        for (var i = from; i <= to; i++)
            delete data[i];

        ensureData(from, to);
    };

    var setSort = function (column, dir) {
        sortcol = column;
        sortdir = dir;
        clear();
    };

    var setFilterArgs = function (newFilter) {
        filter = newFilter;
        clear();
    };

    var getItem = function (i) {
        return data[i];
    };

    var getLength = function () {
        return data.length;
    };

    function HistoryModel() {
    }

    HistoryModel.prototype = {
        constructor: HistoryModel,
        // properties
        data: data,
        // methods
        clear: clear,
        isDataLoaded: isDataLoaded,
        ensureData: ensureData,
        reloadData: reloadData,
        setSort: setSort,
        setFilterArgs: setFilterArgs,
        getItem: getItem,
        getLength: getLength,
        // events
        onDataLoading: onDataLoading,
        onDataLoaded: onDataLoaded
    }

    return HistoryModel;
}));