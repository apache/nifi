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

/* global nf, top, Slick */

nf.ProvenanceTable = (function () {

    /**
     * Configuration object used to hold a number of configuration items.
     */
    var config = {
        maxResults: 1000,
        defaultStartTime: '00:00:00',
        defaultEndTime: '23:59:59',
        filterText: 'Filter',
        styles: {
            filterList: 'provenance-filter-list',
            hidden: 'hidden'
        },
        urls: {
            searchOptions: '../nifi-api/controller/provenance/search-options',
            replays: '../nifi-api/controller/provenance/replays',
            provenance: '../nifi-api/controller/provenance',
            cluster: '../nifi-api/cluster',
            d3Script: 'js/d3/d3.min.js',
            lineageScript: 'js/nf/provenance/nf-provenance-lineage.js'
        }
    };

    /**
     * The last search performed
     */
    var cachedQuery = {};

    /**
     * Loads the lineage capabilities when the current browser supports SVG.
     */
    var loadLineageCapabilities = function () {
        return $.Deferred(function (deferred) {
            if (nf.Common.SUPPORTS_SVG) {
                nf.Common.cachedScript(config.urls.d3Script).done(function () {
                    nf.Common.cachedScript(config.urls.lineageScript).done(function () {
                        // initialize the lineage graph
                        nf.ProvenanceLineage.init();
                        deferred.resolve();
                    }).fail(function () {
                        deferred.reject();
                    });
                }).fail(function () {
                    deferred.reject();
                });
            } else {
                deferred.resolve();
            }
        }).promise();
    };

    /**
     * Downloads the content for the provenance event that is currently loaded in the specified direction.
     * 
     * @param {string} direction
     */
    var downloadContent = function (direction) {
        var eventId = $('#provenance-event-id').text();

        // build the url
        var url = config.urls.provenance + '/events/' + encodeURIComponent(eventId) + '/content/' + encodeURIComponent(direction);

        // conditionally include the cluster node id
        var clusterNodeId = $('#provenance-event-cluster-node-id').text();
        if (!nf.Common.isBlank(clusterNodeId)) {
            window.open(url + '?' + $.param({
                'clusterNodeId': clusterNodeId
            }));
        } else {
            window.open(url);
        }
    };

    /**
     * Views the content for the provenance event that is currently loaded in the specified direction.
     * 
     * @param {string} direction
     */
    var viewContent = function (direction) {
        var controllerUri = $('#nifi-controller-uri').text();
        var eventId = $('#provenance-event-id').text();

        // build the uri to the data
        var dataUri = controllerUri + '/provenance/events/' + encodeURIComponent(eventId) + '/content/' + encodeURIComponent(direction);

        // conditionally include the cluster node id
        var clusterNodeId = $('#provenance-event-cluster-node-id').text();
        if (!nf.Common.isBlank(clusterNodeId)) {
            var parameters = {
                'clusterNodeId': clusterNodeId
            };

            dataUri = dataUri + '?' + $.param(parameters);
        }

        // open the content viewer
        var contentViewerUrl = $('#nifi-content-viewer-url').text();

        // if there's already a query string don't add another ?... this assumes valid
        // input meaning that if the url has already included a ? it also contains at
        // least one query parameter 
        if (contentViewerUrl.indexOf('?') === -1) {
            contentViewerUrl += '?';
        } else {
            contentViewerUrl += '&';
        }

        // open the content viewer
        window.open(contentViewerUrl + $.param({
            'ref': dataUri
        }));
    };

    /**
     * Initializes the details dialog.
     */
    var initDetailsDialog = function () {
        // initialize the properties tabs
        $('#event-details-tabs').tabbs({
            tabStyle: 'tab',
            selectedTabStyle: 'selected-tab',
            tabs: [{
                    name: 'Details',
                    tabContentId: 'event-details-tab-content'
                }, {
                    name: 'Attributes',
                    tabContentId: 'attributes-tab-content'
                }, {
                    name: 'Content',
                    tabContentId: 'content-tab-content'
                }]
        });

        $('#event-details-dialog').modal({
            headerText: 'Provenance Event',
            overlayBackground: false,
            buttons: [{
                    buttonText: 'Ok',
                    handler: {
                        click: function () {
                            $('#event-details-dialog').modal('hide');
                        }
                    }
                }],
            handler: {
                close: function () {
                    // clear the details
                    $('#additional-provenance-details').empty();
                    $('#attributes-container').empty();
                    $('#parent-flowfiles-container').empty();
                    $('#child-flowfiles-container').empty();
                    $('#provenance-event-cluster-node-id').text('');
                    $('#modified-attribute-toggle').removeClass('checkbox-checked').addClass('checkbox-unchecked');
                }
            }
        });

        // toggle which attributes are visible
        $('#modified-attribute-toggle').on('click', function () {
            var unmodifiedAttributes = $('#attributes-container div.attribute-unmodified');
            if (unmodifiedAttributes.is(':visible')) {
                $('#attributes-container div.attribute-unmodified').hide();
            } else {
                $('#attributes-container div.attribute-unmodified').show();
            }
        });

        // input download
        $('#input-content-download').on('click', function () {
            downloadContent('input');
        });

        // output download
        $('#output-content-download').on('click', function () {
            downloadContent('output');
        });

        // if a content viewer url is specified, use it
        if (nf.Common.isContentViewConfigured()) {
            // input view
            $('#input-content-view').on('click', function () {
                viewContent('input');
            });

            // output view
            $('#output-content-view').on('click', function () {
                viewContent('output');
            });
        }

        // handle the replay and downloading
        if (nf.Common.isDFM()) {
            // replay
            $('#replay-content').on('click', function () {
                var parameters = {
                    eventId: $('#provenance-event-id').text()
                };

                // conditionally include the cluster node id
                var clusterNodeId = $('#provenance-event-cluster-node-id').text();
                if (!nf.Common.isBlank(clusterNodeId)) {
                    parameters['clusterNodeId'] = clusterNodeId;
                }

                $.ajax({
                    type: 'POST',
                    url: config.urls.replays,
                    data: parameters,
                    dataType: 'json'
                }).done(function (response) {
                    nf.Dialog.showOkDialog({
                        dialogContent: 'Successfully submitted replay request.',
                        overlayBackground: false
                    });
                }).fail(nf.Common.handleAjaxError);

                $('#event-details-dialog').modal('hide');
            });

            // show the replay panel
            $('#replay-details').show();
        }
    };

    /**
     * Initializes the search dialog.
     * 
     * @param {boolean} isClustered     Whether or not this NiFi clustered
     */
    var initSearchDialog = function (isClustered) {
        // configure the start and end date picker
        $('#provenance-search-start-date, #provenance-search-end-date').datepicker({
            showAnim: '',
            showOtherMonths: true,
            selectOtherMonths: true
        });

        // initialize the default start date/time
        $('#provenance-search-start-date').datepicker('setDate', '+0d');
        $('#provenance-search-end-date').datepicker('setDate', '+0d');
        $('#provenance-search-start-time').val('00:00:00');
        $('#provenance-search-end-time').val('23:59:59');

        // initialize the default file sizes
        $('#provenance-search-minimum-file-size').val('');
        $('#provenance-search-maximum-file-size').val('');

        // allow users to be able to search a specific node
        if (isClustered) {
            // make the dialog larger to support the select location
            $('#provenance-search-dialog').height(575);

            // get the nodes in the cluster
            $.ajax({
                type: 'GET',
                url: config.urls.cluster,
                dataType: 'json'
            }).done(function (response) {
                var cluster = response.cluster;
                var nodes = cluster.nodes;

                // create the searchable options
                var searchableOptions = [{
                        text: 'cluster',
                        value: null
                    }];

                // sort the nodes
                nodes.sort(function (a, b) {
                    var compA = (a.address + ':' + a.apiPort).toUpperCase();
                    var compB = (b.address + ':' + b.apiPort).toUpperCase();
                    return (compA < compB) ? -1 : (compA > compB) ? 1 : 0;
                });

                // add each node
                $.each(nodes, function (_, node) {
                    searchableOptions.push({
                        text: node.address + ':' + node.apiPort,
                        value: node.nodeId
                    });
                });

                // populate the combo
                $('#provenance-search-location').combo({
                    options: searchableOptions
                });
            }).fail(nf.Common.handleAjaxError);

            // show the node search combo
            $('#provenance-search-location-container').show();
        }

        // configure the search dialog
        $('#provenance-search-dialog').modal({
            headerText: 'Search Events',
            overlayBackground: false,
            buttons: [{
                    buttonText: 'Search',
                    handler: {
                        click: function () {
                            $('#provenance-search-dialog').modal('hide');

                            var search = {};

                            // extract the start date time
                            var startDate = $.trim($('#provenance-search-start-date').val());
                            var startTime = $.trim($('#provenance-search-start-time').val());
                            if (startDate !== '') {
                                if (startTime === '') {
                                    startTime = config.defaultStartTime;
                                    $('#provenance-search-start-time').val(startTime);
                                }
                                search['startDate'] = startDate + ' ' + startTime;
                            }

                            // extract the end date time
                            var endDate = $.trim($('#provenance-search-end-date').val());
                            var endTime = $.trim($('#provenance-search-end-time').val());
                            if (endDate !== '') {
                                if (endTime === '') {
                                    endTime = config.defaultEndTime;
                                    $('#provenance-search-end-time').val(endTime);
                                }
                                search['endDate'] = endDate + ' ' + endTime;
                            }

                            // extract the min/max file size
                            var minFileSize = $.trim($('#provenance-search-minimum-file-size').val());
                            if (minFileSize !== '') {
                                search['minimumFileSize'] = minFileSize;
                            }

                            var maxFileSize = $.trim($('#provenance-search-maximum-file-size').val());
                            if (maxFileSize !== '') {
                                search['maximumFileSize'] = maxFileSize;
                            }

                            // limit search to a specific node
                            if (isClustered) {
                                var searchLocation = $('#provenance-search-location').combo('getSelectedOption');
                                if (searchLocation.value !== null) {
                                    search['clusterNodeId'] = searchLocation.value;
                                }
                            }

                            // add the search criteria
                            search = $.extend(getSearchCriteria(), search);

                            // reload the table
                            nf.ProvenanceTable.loadProvenanceTable(search);
                        }
                    }
                }, {
                    buttonText: 'Cancel',
                    handler: {
                        click: function () {
                            $('#provenance-search-dialog').modal('hide');
                        }
                    }
                }]
        });
        
        return $.ajax({
            type: 'GET',
            url: config.urls.searchOptions,
            dataType: 'json'
        }).done(function (response) {
            var provenanceOptions = response.provenanceOptions;

            // load all searchable fields
            $.each(provenanceOptions.searchableFields, function (_, field) {
                appendSearchableField(field);
            });
        });
    };

    /**
     * Initializes the provenance query dialog.
     */
    var initProvenanceQueryDialog = function () {
        // initialize the progress bar
        $('#provenance-percent-complete').progressbar();

        // initialize the dialog
        $('#provenance-query-dialog').modal({
            headerText: 'Searching provenance events...',
            overlayBackground: false,
            handler: {
                close: function () {
                    // reset the progress bar
                    var provenanceProgressBar = $('#provenance-percent-complete');
                    provenanceProgressBar.find('div.progress-label').remove();

                    // update the progress bar
                    var label = $('<div class="progress-label"></div>').text('0%');
                    provenanceProgressBar.progressbar('value', 0).append(label);
                }
            }
        });
    };

    /**
     * Appends the specified searchable field to the search dialog.
     * 
     * @param {type} field      The searchable field
     */
    var appendSearchableField = function (field) {
        var searchableField = $('<div class="searchable-field"></div>').appendTo('#searchable-fields-container');
        $('<span class="searchable-field-id hidden"></span>').text(field.id).appendTo(searchableField);
        $('<div class="searchable-field-name"></div>').text(field.label).appendTo(searchableField);
        $('<div class="searchable-field-value"><input type="text" class="searchable-field-input"/></div>').appendTo(searchableField);
        $('<div class="clear"></div>').appendTo(searchableField);

        // make the component id accessible for populating
        if (field.id === 'ProcessorID') {
            searchableField.find('input').addClass('searchable-component-id');
        }

        // ensure the no searchable fields message is hidden
        $('#no-searchable-fields').hide();
    };

    /**
     * Gets the search criteria that the user has specified.
     */
    var getSearchCriteria = function () {
        var searchCriteria = {};
        $('#searchable-fields-container').children('div.searchable-field').each(function () {
            var searchableField = $(this);
            var fieldId = searchableField.children('span.searchable-field-id').text();
            var searchValue = $.trim(searchableField.find('input.searchable-field-input').val());

            // if the field isn't blank include it in the search
            if (!nf.Common.isBlank(searchValue)) {
                searchCriteria['search[' + fieldId + ']'] = searchValue;
            }
        });
        return searchCriteria;
    };

    /**
     * Initializes the provenance table.
     * 
     * @param {boolean} isClustered     Whether or not this instance is clustered
     */
    var initProvenanceTable = function (isClustered) {
        // define the function for filtering the list
        $('#provenance-filter').keyup(function () {
            applyFilter();
        }).focus(function () {
            if ($(this).hasClass(config.styles.filterList)) {
                $(this).removeClass(config.styles.filterList).val('');
            }
        }).blur(function () {
            if ($(this).val() === '') {
                $(this).addClass(config.styles.filterList).val(config.filterText);
            }
        }).addClass(config.styles.filterList).val(config.filterText);

        // filter options
        var filterOptions = [{
                text: 'by component name',
                value: 'componentName'
            }, {
                text: 'by component type',
                value: 'componentType'
            }, {
                text: 'by type',
                value: 'eventType'
            }];

        // if clustered, allowing filtering by node id
        if (isClustered) {
            filterOptions.push({
                text: 'by node',
                value: 'clusterNodeAddress'
            });
        }

        // initialize the filter combo
        $('#provenance-filter-type').combo({
            options: filterOptions,
            select: function (option) {
                applyFilter();
            }
        });

        // listen for browser resize events to update the page size
        $(window).resize(function () {
            nf.ProvenanceTable.resetTableSize();
        });

        // clear the current search
        $('#clear-provenance-search').click(function () {
            // clear each searchable field
            $('#searchable-fields-container').find('input.searchable-field-input').each(function () {
                $(this).val('');
            });

            // reset the default start date/time
            $('#provenance-search-start-date').datepicker('setDate', '+0d');
            $('#provenance-search-end-date').datepicker('setDate', '+0d');
            $('#provenance-search-start-time').val('00:00:00');
            $('#provenance-search-end-time').val('23:59:59');

            // reset the minimum and maximum file size
            $('#provenance-search-minimum-file-size').val('');
            $('#provenance-search-maximum-file-size').val('');

            // if we are clustered reset the selected option
            if (isClustered) {
                $('#provenance-search-location').combo('setSelectedOption', {
                    text: 'cluster'
                });
            }

            // reset the stored query
            cachedQuery = {};

            // reload the table
            nf.ProvenanceTable.loadProvenanceTable();
        });

        // add hover effect and click handler for opening the dialog
        nf.Common.addHoverEffect('#provenance-search-button', 'button-normal', 'button-over').click(function () {
            $('#provenance-search-dialog').modal('show');

            // adjust the field width for a potential scrollbar
            var searchFieldContainer = $('#searchable-fields-container');
            if (searchFieldContainer.get(0).scrollHeight > searchFieldContainer.innerHeight()) {
                $('input.searchable-field-input').width(245);
            } else {
                $('input.searchable-field-input').width(260);
            }
        });

        // define a custom formatter for the more details column
        var moreDetailsFormatter = function (row, cell, value, columnDef, dataContext) {
            return '<img src="images/iconDetails.png" title="View Details" class="pointer show-event-details" style="margin-top: 4px;"/>';
        };

        // define how general values are formatted
        var valueFormatter = function (row, cell, value, columnDef, dataContext) {
            return nf.Common.formatValue(value);
        };

        // determine if the this page is in the shell
        var isInShell = (top !== window);

        // define how the column is formatted
        var showLineageFormatter = function (row, cell, value, columnDef, dataContext) {
            var markup = '';

            // conditionally include the cluster node id
            if (nf.Common.SUPPORTS_SVG) {
                markup += '<img src="images/iconLineage.png" title="Show Lineage" class="pointer show-lineage" style="margin-top: 2px;"/>';
            }

            // conditionally support going to the component
            if (isInShell && nf.Common.isDefinedAndNotNull(dataContext.groupId)) {
                markup += '&nbsp;<img src="images/iconGoTo.png" title="Go To" class="pointer go-to" style="margin-top: 2px;"/>';
            }

            return markup;
        };

        // initialize the provenance table
        var provenanceColumns = [
            {id: 'moreDetails', name: '&nbsp;', sortable: false, resizable: false, formatter: moreDetailsFormatter, width: 50, maxWidth: 50},
            {id: 'eventTime', name: 'Date/Time', field: 'eventTime', sortable: true, defaultSortAsc: false, resizable: true},
            {id: 'eventType', name: 'Type', field: 'eventType', sortable: true, resizable: true},
            {id: 'flowFileUuid', name: 'FlowFile Uuid', field: 'flowFileUuid', sortable: true, resizable: true},
            {id: 'fileSize', name: 'Size', field: 'fileSize', sortable: true, defaultSortAsc: false, resizable: true},
            {id: 'componentName', name: 'Component Name', field: 'componentName', sortable: true, resizable: true, formatter: valueFormatter},
            {id: 'componentType', name: 'Component Type', field: 'componentType', sortable: true, resizable: true}
        ];

        // conditionally show the cluster node identifier
        if (isClustered) {
            provenanceColumns.push({id: 'clusterNodeAddress', name: 'Node', field: 'clusterNodeAddress', sortable: true, resizable: true});
        }

        // conditionally show the action column
        if (nf.Common.SUPPORTS_SVG || isInShell) {
            provenanceColumns.push({id: 'actions', name: '&nbsp;', formatter: showLineageFormatter, resizable: false, sortable: false, width: 50, maxWidth: 50});
        }

        var provenanceOptions = {
            forceFitColumns: true,
            enableTextSelectionOnCells: true,
            enableCellNavigation: true,
            enableColumnReorder: false,
            autoEdit: false,
            multiSelect: false
        };

        // create the remote model
        var provenanceData = new Slick.Data.DataView({
            inlineFilters: false
        });
        provenanceData.setItems([]);
        provenanceData.setFilterArgs({
            searchString: '',
            property: 'name'
        });
        provenanceData.setFilter(filter);

        // initialize the sort
        sort({
            columnId: 'eventTime',
            sortAsc: false
        }, provenanceData);

        // initialize the grid
        var provenanceGrid = new Slick.Grid('#provenance-table', provenanceData, provenanceColumns, provenanceOptions);
        provenanceGrid.setSelectionModel(new Slick.RowSelectionModel());
        provenanceGrid.registerPlugin(new Slick.AutoTooltips());

        // initialize the grid sorting
        provenanceGrid.setSortColumn('eventTime', false);
        provenanceGrid.onSort.subscribe(function (e, args) {
            sort({
                columnId: args.sortCol.field,
                sortAsc: args.sortAsc
            }, provenanceData);
        });
        
        // configure a click listener
        provenanceGrid.onClick.subscribe(function (e, args) {
            var target = $(e.target);

            // get the node at this row
            var item = provenanceData.getItem(args.row);

            // determine the desired action
            if (provenanceGrid.getColumns()[args.cell].id === 'actions') {
                if (target.hasClass('show-lineage')) {
                    nf.ProvenanceLineage.showLineage(item.flowFileUuid, item.eventId.toString(), item.clusterNodeId);
                } else if (target.hasClass('go-to')) {
                    goTo(item);
                }
            } else if (provenanceGrid.getColumns()[args.cell].id === 'moreDetails') {
                if (target.hasClass('show-event-details')) {
                    nf.ProvenanceTable.showEventDetails(item);
                }
            }
        });

        // wire up the dataview to the grid
        provenanceData.onRowCountChanged.subscribe(function (e, args) {
            provenanceGrid.updateRowCount();
            provenanceGrid.render();

            // update the total number of displayed events if necessary
            $('#displayed-events').text(nf.Common.formatInteger(args.current));
        });
        provenanceData.onRowsChanged.subscribe(function (e, args) {
            provenanceGrid.invalidateRows(args.rows);
            provenanceGrid.render();
        });

        // hold onto an instance of the grid
        $('#provenance-table').data('gridInstance', provenanceGrid);

        // initialize the number of displayed items
        $('#displayed-events').text('0');
        $('#total-events').text('0');
    };

    /**
     * Applies the filter found in the filter expression text field.
     */
    var applyFilter = function () {
        // get the dataview
        var provenanceGrid = $('#provenance-table').data('gridInstance');

        // ensure the grid has been initialized
        if (nf.Common.isDefinedAndNotNull(provenanceGrid)) {
            var provenanceData = provenanceGrid.getData();

            // update the search criteria
            provenanceData.setFilterArgs({
                searchString: getFilterText(),
                property: $('#provenance-filter-type').combo('getSelectedOption').value
            });
            provenanceData.refresh();
        }
    };

    /**
     * Get the text out of the filter field. If the filter field doesn't
     * have any text it will contain the text 'filter list' so this method
     * accounts for that.
     */
    var getFilterText = function () {
        var filterText = '';
        var filterField = $('#provenance-filter');
        if (!filterField.hasClass(config.styles.filterList)) {
            filterText = filterField.val();
        }
        return filterText;
    };

    /**
     * Performs the provenance filtering.
     * 
     * @param {object} item     The item subject to filtering
     * @param {object} args     Filter arguments
     * @returns {Boolean}       Whether or not to include the item
     */
    var filter = function (item, args) {
        if (args.searchString === '') {
            return true;
        }

        try {
            // perform the row filtering
            var filterExp = new RegExp(args.searchString, 'i');
        } catch (e) {
            // invalid regex
            return false;
        }

        return item[args.property].search(filterExp) >= 0;
    };

    /**
     * Sorts the data according to the sort details.
     * 
     * @param {type} sortDetails
     * @param {type} data
     */
    var sort = function (sortDetails, data) {
        // defines a function for sorting
        var comparer = function (a, b) {
            if (sortDetails.columnId === 'eventTime') {
                var aTime = nf.Common.parseDateTime(a[sortDetails.columnId]).getTime();
                var bTime = nf.Common.parseDateTime(b[sortDetails.columnId]).getTime();
                if (aTime === bTime) {
                    return a['id'] - b['id'];
                } else {
                    return aTime - bTime;
                }
            } else if (sortDetails.columnId === 'fileSize') {
                var aSize = nf.Common.parseSize(a[sortDetails.columnId]);
                var bSize = nf.Common.parseSize(b[sortDetails.columnId]);
                if (aSize === bSize) {
                    return a['id'] - b['id'];
                } else {
                    return aSize - bSize;
                }
            } else {
                var aString = nf.Common.isDefinedAndNotNull(a[sortDetails.columnId]) ? a[sortDetails.columnId] : '';
                var bString = nf.Common.isDefinedAndNotNull(b[sortDetails.columnId]) ? b[sortDetails.columnId] : '';
                if (aString === bString) {
                    return a['id'] - b['id'];
                } else {
                    return aString === bString ? 0 : aString > bString ? 1 : -1;
                }
            }
        };

        // perform the sort
        data.sort(comparer, sortDetails.sortAsc);
    };

    /**
     * Submits a new provenance query.
     * 
     * @argument {object} provenance The provenance query
     * @returns {deferred}
     */
    var submitProvenance = function (provenance) {
        return $.ajax({
            type: 'POST',
            url: config.urls.provenance,
            data: $.extend({
                maxResults: config.maxResults
            }, provenance),
            dataType: 'json'
        }).fail(nf.Common.handleAjaxError);
    };

    /**
     * Gets the results from the provenance query for the specified id.
     * 
     * @param {object} provenance
     * @returns {deferred}
     */
    var getProvenance = function (provenance) {
        var url = provenance.uri;
        if (nf.Common.isDefinedAndNotNull(provenance.clusterNodeId)) {
            url += '?' + $.param({
                clusterNodeId: provenance.clusterNodeId
            });
        }

        return $.ajax({
            type: 'GET',
            url: url,
            dataType: 'json'
        }).fail(nf.Common.handleAjaxError);
    };

    /**
     * Cancels the specified provenance query.
     * 
     * @param {object} provenance
     * @return {deferred}
     */
    var cancelProvenance = function (provenance) {
        var url = provenance.uri;
        if (nf.Common.isDefinedAndNotNull(provenance.clusterNodeId)) {
            url += '?' + $.param({
                clusterNodeId: provenance.clusterNodeId
            });
        }

        return $.ajax({
            type: 'DELETE',
            url: url,
            dataType: 'json'
        }).fail(nf.Common.handleAjaxError);
    };

    /**
     * Checks the results of the specified provenance.
     * 
     * @param {object} provenance
     */
    var loadProvenanceResults = function (provenance) {
        var provenanceRequest = provenance.request;
        var provenanceResults = provenance.results;

        // ensure there are groups specified
        if (nf.Common.isDefinedAndNotNull(provenanceResults.provenanceEvents)) {
            var provenanceTable = $('#provenance-table').data('gridInstance');
            var provenanceData = provenanceTable.getData();

            // set the items
            provenanceData.setItems(provenanceResults.provenanceEvents);
            provenanceData.reSort();
            provenanceTable.invalidate();

            // update the stats last refreshed timestamp
            $('#provenance-last-refreshed').text(provenanceResults.generated);

            // update the oldest event available
            $('#oldest-event').html(nf.Common.formatValue(provenanceResults.oldestEvent));
            $('#oldest-event-message').show();

            // set the timezone for the start and end time
            $('.timezone').text(nf.Common.substringAfterLast(provenanceResults.generated, ' '));

            // record the server offset
            nf.ProvenanceTable.serverTimeOffset = provenanceResults.timeOffset;

            // determines if the specified query is blank (no search terms, start or end date)
            var isBlankQuery = function (query) {
                return nf.Common.isUndefinedOrNull(query.startDate) && nf.Common.isUndefinedOrNull(query.endDate) && $.isEmptyObject(query.searchTerms);
            };

            // update the filter message based on the request
            if (isBlankQuery(provenanceRequest)) {
                var message = 'Showing the most recent ';
                if (provenanceResults.totalCount > config.maxResults) {
                    message += (nf.Common.formatInteger(config.maxResults) + ' of ' + provenanceResults.total + ' events, please refine the search.');
                } else {
                    message += ('events.');
                }
                $('#provenance-query-message').text(message);
                $('#clear-provenance-search').hide();
            } else {
                var message = 'Showing ';
                if (provenanceResults.totalCount > config.maxResults) {
                    message += (nf.Common.formatInteger(config.maxResults) + ' of ' + provenanceResults.total + ' events that match the specified query, please refine the search.');
                } else {
                    message += ('the events that match the specified query.');
                }
                $('#provenance-query-message').text(message);
                $('#clear-provenance-search').show();
            }

            // update the total number of events
            $('#total-events').text(nf.Common.formatInteger(provenanceResults.provenanceEvents.length));
        } else {
            $('#total-events').text('0');
        }
    };

    /**
     * Goes to the specified component if possible.
     * 
     * @argument {object} item       The event it
     */
    var goTo = function (item) {
        // ensure the component is still present in the flow
        if (nf.Common.isDefinedAndNotNull(item.groupId)) {
            // only attempt this if we're within a frame
            if (top !== window) {
                // and our parent has canvas utils and shell defined
                if (nf.Common.isDefinedAndNotNull(parent.nf) && nf.Common.isDefinedAndNotNull(parent.nf.CanvasUtils) && nf.Common.isDefinedAndNotNull(parent.nf.Shell)) {
                    parent.nf.CanvasUtils.showComponent(item.groupId, item.componentId);
                    parent.$('#shell-close-button').click();
                }
            }
        }
    };

    return {
        /**
         * The max delay between requests.
         */
        MAX_DELAY: 4,
        
        /**
         * The server time offset
         */
        serverTimeOffset: null,
        
        /**
         * Initializes the provenance table. Returns a deferred that will indicate when/if the table has initialized successfully.
         * 
         * @param {boolean} isClustered     Whether or not this instance is clustered
         */
        init: function (isClustered) {
            return $.Deferred(function (deferred) {
                // handles init failure
                var failure = function (xhr, status, error) {
                    deferred.reject();
                    nf.Common.handleAjaxError(xhr, status, error);
                };
                
                // load the lineage capabilities
                loadLineageCapabilities().done(function () {
                    initDetailsDialog();
                    initProvenanceQueryDialog();
                    initProvenanceTable(isClustered);
                    initSearchDialog(isClustered).done(function () {
                        deferred.resolve();
                    }).fail(failure);
                }).fail(failure);
            }).promise();
        },
        
        /**
         * Update the size of the grid based on its container's current size.
         */
        resetTableSize: function () {
            var provenanceGrid = $('#provenance-table').data('gridInstance');
            if (nf.Common.isDefinedAndNotNull(provenanceGrid)) {
                provenanceGrid.resizeCanvas();
            }
        },
        
        /**
         * Updates the value of the specified progress bar.
         * 
         * @param {jQuery}  progressBar
         * @param {integer} value
         * @returns {undefined}
         */
        updateProgress: function (progressBar, value) {
            // remove existing labels
            progressBar.find('div.progress-label').remove();

            // update the progress bar
            var label = $('<div class="progress-label"></div>').text(value + '%');
            if (value > 0) {
                label.css('margin-top', '-19px');
            }
            progressBar.progressbar('value', value).append(label);
        },
        
        /**
         * Loads the provenance table with events according to the specified optional 
         * query. If not query is specified or it is empty, the most recent entries will
         * be returned.
         * 
         * @param {type} query
         */
        loadProvenanceTable: function (query) {
            var provenanceProgress = $('#provenance-percent-complete');

            // add support to cancel outstanding requests - when the button is pressed we 
            // could be in one of two stages, 1) waiting to GET the status or 2)
            // in the process of GETting the status. Handle both cases by cancelling 
            // the setTimeout (1) and by setting a flag to indicate that a request has
            // been request so we can ignore the results (2).

            var cancelled = false;
            var provenance = null;
            var provenanceTimer = null;

            // update the progress bar value
            nf.ProvenanceTable.updateProgress(provenanceProgress, 0);

            // show the 'searching...' dialog
            $('#provenance-query-dialog').modal('setButtonModel', [{
                    buttonText: 'Cancel',
                    handler: {
                        click: function () {
                            cancelled = true;

                            // we are waiting for the next poll attempt
                            if (provenanceTimer !== null) {
                                // cancel it
                                clearTimeout(provenanceTimer);

                                // cancel the provenance
                                closeDialog();
                            }
                        }
                    }
                }]).modal('show');

            // -----------------------------
            // determine the provenance query
            // -----------------------------

            // handle the specified query appropriately
            if (nf.Common.isDefinedAndNotNull(query)) {
                // store the last query performed
                cachedQuery = query;
            } else if (!$.isEmptyObject(cachedQuery)) {
                // use the last query performed
                query = cachedQuery;
            } else {
                // don't use a query
                query = {};
            }

            // closes the searching dialog and cancels the query on the server
            var closeDialog = function () {
                // cancel the provenance results since we've successfully processed the results
                if (nf.Common.isDefinedAndNotNull(provenance)) {
                    cancelProvenance(provenance);
                }

                // close the dialog
                $('#provenance-query-dialog').modal('hide');
            };

            // polls the server for the status of the provenance, if the provenance is not
            // done wait nextDelay seconds before trying again
            var pollProvenance = function (nextDelay) {
                getProvenance(provenance).done(function (response) {
                    // update the provenance
                    provenance = response.provenance;

                    // process the provenance
                    processProvenanceResponse(nextDelay);
                }).fail(closeDialog);
            };

            // processes the provenance, if the provenance is not done wait delay 
            // before polling again
            var processProvenanceResponse = function (delay) {
                // if the request was cancelled just ignore the current response
                if (cancelled === true) {
                    closeDialog();
                    return;
                }

                // update the percent complete
                nf.ProvenanceTable.updateProgress(provenanceProgress, provenance.percentCompleted);

                // process the results if they are finished
                if (provenance.finished === true) {
                    // show any errors when the query finishes
                    if (!nf.Common.isEmpty(provenance.results.errors)) {
                        var errors = provenance.results.errors;
                        nf.Dialog.showOkDialog({
                            dialogContent: nf.Common.formatUnorderedList(errors),
                            overlayBackground: false
                        });
                    }

                    // process the results
                    loadProvenanceResults(provenance);

                    // hide the dialog
                    closeDialog();
                } else {
                    // start the wait to poll again
                    provenanceTimer = setTimeout(function () {
                        // clear the timer since we've been invoked
                        provenanceTimer = null;

                        // calculate the next delay (back off)
                        var backoff = delay * 2;
                        var nextDelay = backoff > nf.ProvenanceTable.MAX_DELAY ? nf.ProvenanceTable.MAX_DELAY : backoff;

                        // poll provenance
                        pollProvenance(nextDelay);
                    }, delay * 1000);
                }
            };

            // once the query is submitted wait until its finished
            submitProvenance(query).done(function (response) {
                // update the provenance
                provenance = response.provenance;

                // process the results, if they are not done wait 1 second before trying again
                processProvenanceResponse(1);
            }).fail(closeDialog);
        },
        
        /**
         * Shows the details for the specified action.
         * 
         * @param {object} event
         */
        showEventDetails: function (event) {
            // update the event details
            $('#provenance-event-id').text(event.eventId);
            $('#provenance-event-time').html(nf.Common.formatValue(event.eventTime)).ellipsis();
            $('#provenance-event-type').html(nf.Common.formatValue(event.eventType)).ellipsis();
            $('#provenance-event-flowfile-uuid').html(nf.Common.formatValue(event.flowFileUuid)).ellipsis();
            $('#provenance-event-component-id').html(nf.Common.formatValue(event.componentId)).ellipsis();
            $('#provenance-event-component-name').html(nf.Common.formatValue(event.componentName)).ellipsis();
            $('#provenance-event-component-type').html(nf.Common.formatValue(event.componentType)).ellipsis();
            $('#provenance-event-details').html(nf.Common.formatValue(event.details)).ellipsis();

            // over the default tooltip with the actual byte count
            var fileSize = $('#provenance-event-file-size').html(nf.Common.formatValue(event.fileSize)).ellipsis();
            fileSize.attr('title', nf.Common.formatInteger(event.fileSizeBytes) + ' bytes');

            // sets an duration
            var setDuration = function (field, value) {
                if (nf.Common.isDefinedAndNotNull(value)) {
                    if (value === 0) {
                        field.text('< 1ms');
                    } else {
                        field.text(nf.Common.formatDuration(value));
                    }
                } else {
                    field.html('<span class="unset">No value set</span>');
                }
            };

            // handle durations
            setDuration($('#provenance-event-duration'), event.eventDuration);
            setDuration($('#provenance-lineage-duration'), event.lineageDuration);

            // formats an event detail
            var formatEventDetail = function (label, value) {
                $('<div class="event-detail"></div>').append(
                        $('<div class="detail-name"></div>').text(label)).append(
                        $('<div class="detail-value">' + nf.Common.formatValue(value) + '</div>').ellipsis()).append(
                        $('<div class="clear"></div>')).appendTo('#additional-provenance-details');
            };

            // conditionally show RECEIVE details
            if (event.eventType === 'RECEIVE') {
                formatEventDetail('Source FlowFile Id', event.sourceSystemFlowFileId);
                formatEventDetail('Transit Uri', event.transitUri);
            }

            // conditionally show SEND details
            if (event.eventType === 'SEND') {
                formatEventDetail('Transit Uri', event.transitUri);
            }

            // conditionally show ADDINFO details
            if (event.eventType === 'ADDINFO') {
                formatEventDetail('Alternate Identifier Uri', event.alternateIdentifierUri);
            }

            // conditionally show ROUTE details
            if (event.eventType === 'ROUTE') {
                formatEventDetail('Relationship', event.relationship);
            }

            // conditionally show FETCH details
            if (event.eventType === 'FETCH') {
                formatEventDetail('Transit Uri', event.transitUri);
            }

            // conditionally show the cluster node identifier
            if (nf.Common.isDefinedAndNotNull(event.clusterNodeId)) {
                // save the cluster node id
                $('#provenance-event-cluster-node-id').text(event.clusterNodeId);

                // render the cluster node address
                formatEventDetail('Node Address', event.clusterNodeAddress);
            }

            // populate the parent/child flowfile uuids
            var parentUuids = $('#parent-flowfiles-container');
            var childUuids = $('#child-flowfiles-container');

            // handle parent flowfiles
            if (nf.Common.isEmpty(event.parentUuids)) {
                $('#parent-flowfile-count').text(0);
                parentUuids.append('<span class="unset">No parents</span>');
            } else {
                $('#parent-flowfile-count').text(event.parentUuids.length);
                $.each(event.parentUuids, function (_, uuid) {
                    $('<div></div>').text(uuid).appendTo(parentUuids);
                });
            }

            // handle child flowfiles
            if (nf.Common.isEmpty(event.childUuids)) {
                $('#child-flowfile-count').text(0);
                childUuids.append('<span class="unset">No children</span>');
            } else {
                $('#child-flowfile-count').text(event.childUuids.length);
                $.each(event.childUuids, function (_, uuid) {
                    $('<div></div>').text(uuid).appendTo(childUuids);
                });
            }

            // get the attributes container
            var attributesContainer = $('#attributes-container');

            // get any action details
            $.each(event.attributes, function (_, attribute) {
                // create the attribute record
                var attributeRecord = $('<div class="attribute-detail"></div>')
                        .append($('<div class="attribute-name">' + nf.Common.formatValue(attribute.name) + '</div>').ellipsis())
                        .appendTo(attributesContainer);

                // add the current value
                attributeRecord
                        .append($('<div class="attribute-value">' + nf.Common.formatValue(attribute.value) + '</div>').ellipsis())
                        .append('<div class="clear"></div>');

                // show the previous value if the property has changed
                if (attribute.value !== attribute.previousValue) {
                    attributeRecord
                            .append('<div class="modified-attribute-label">previous</div>')
                            .append($('<div class="modified-attribute-value">' + nf.Common.formatValue(attribute.previousValue) + '</div>').ellipsis())
                            .append('<div class="clear"></div>');
                } else {
                    // mark this attribute as not modified
                    attributeRecord.addClass('attribute-unmodified');
                }
            });

            var formatContentValue = function (element, value) {
                if (nf.Common.isDefinedAndNotNull(value)) {
                    element.removeClass('unset').text(value);
                } else {
                    element.addClass('unset').text('No value set');
                }
            };

            // content
            $('#input-content-header').text('Input Claim');
            formatContentValue($('#input-content-container'), event.inputContentClaimContainer);
            formatContentValue($('#input-content-section'), event.inputContentClaimSection);
            formatContentValue($('#input-content-identifier'), event.inputContentClaimIdentifier);
            formatContentValue($('#input-content-offset'), event.inputContentClaimOffset);
            formatContentValue($('#input-content-bytes'), event.inputContentClaimFileSizeBytes);

            // input content file size
            var inputContentSize = $('#input-content-size');
            formatContentValue(inputContentSize, event.inputContentClaimFileSize);
            if (nf.Common.isDefinedAndNotNull(event.inputContentClaimFileSize)) {
                // over the default tooltip with the actual byte count
                inputContentSize.attr('title', nf.Common.formatInteger(event.inputContentClaimFileSizeBytes) + ' bytes');
            }

            formatContentValue($('#output-content-container'), event.outputContentClaimContainer);
            formatContentValue($('#output-content-section'), event.outputContentClaimSection);
            formatContentValue($('#output-content-identifier'), event.outputContentClaimIdentifier);
            formatContentValue($('#output-content-offset'), event.outputContentClaimOffset);
            formatContentValue($('#output-content-bytes'), event.outputContentClaimFileSizeBytes);

            // output content file size
            var outputContentSize = $('#output-content-size');
            formatContentValue(outputContentSize, event.outputContentClaimFileSize);
            if (nf.Common.isDefinedAndNotNull(event.outputContentClaimFileSize)) {
                // over the default tooltip with the actual byte count
                outputContentSize.attr('title', nf.Common.formatInteger(event.outputContentClaimFileSizeBytes) + ' bytes');
            }

            if (event.inputContentAvailable === true) {
                $('#input-content-download').show();

                if (nf.Common.isContentViewConfigured()) {
                    $('#input-content-view').show();
                } else {
                    $('#input-content-view').hide();
                }
            } else {
                $('#input-content-download').hide();
                $('#input-content-view').hide();
            }

            if (event.outputContentAvailable === true) {
                $('#output-content-download').show();

                if (nf.Common.isContentViewConfigured()) {
                    $('#output-content-view').show();
                } else {
                    $('#output-content-view').hide();
                }
            } else {
                $('#output-content-download').hide();
                $('#output-content-view').hide();
            }

            if (nf.Common.isDFM()) {
                if (event.replayAvailable === true) {
                    $('#replay-content, #replay-content-connection').show();
                    formatContentValue($('#replay-connection-id'), event.sourceConnectionIdentifier);
                    $('#replay-content-message').hide();
                } else {
                    $('#replay-content, #replay-content-connection').hide();
                    $('#replay-content-message').text(event.replayExplanation).show();
                }
            }

            // show the dialog
            $('#event-details-dialog').modal('show');
        }
    };
}());