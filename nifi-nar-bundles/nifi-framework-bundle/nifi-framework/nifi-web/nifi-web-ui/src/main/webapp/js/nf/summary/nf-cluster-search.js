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
nf.ClusterSearch = (function () {
    /**
     * Configuration object used to hold a number of configuration items.
     */
    var config = {
        search: 'Search nodes',
        urls: {
            clusterSearch: '../nifi-api/cluster/search-results'
        }
    };

    return {
        /**
         * Initialize the header by register all required listeners.
         */
        init: function () {
            // configure the view single node dialog
            $('#view-single-node-dialog').modal({
                headerText: 'Select node',
                overlayBackground: false,
                buttons: [{
                        buttonText: 'Ok',
                        handler: {
                            click: function () {
                                var clusterSearchTerm = $('#cluster-search-field').val();

                                // create the search request
                                $.ajax({
                                    type: 'GET',
                                    data: {
                                        q: clusterSearchTerm
                                    },
                                    dataType: 'json',
                                    url: config.urls.clusterSearch
                                }).done(function (response) {
                                    var searchResults = response.nodeResults;

                                    // ensure the search found some results
                                    if (!$.isArray(searchResults) || searchResults.length === 0) {
                                        nf.Dialog.showOkDialog({
                                            dialogContent: 'No nodes match \'' + nf.Common.escapeHtml(clusterSearchTerm) + '\'.',
                                            overlayBackground: false
                                        });
                                    } else if (searchResults.length > 1) {
                                        nf.Dialog.showOkDialog({
                                            dialogContent: 'More than one node matches \'' + nf.Common.escapeHtml(clusterSearchTerm) + '\'.',
                                            overlayBackground: false
                                        });
                                    } else if (searchResults.length === 1) {
                                        var node = searchResults[0];

                                        // update the urls to point to this specific node of the cluster
                                        nf.SummaryTable.setClusterNodeId(node.id);

                                        // load the summary for the selected node
                                        nf.SummaryTable.loadSummaryTable();

                                        // update the header
                                        $('#summary-header-text').text(node.address + ' Summary');

                                        // close the dialog
                                        $('#view-single-node-dialog').modal('hide');
                                    }
                                });
                            }
                        }
                    }, {
                        buttonText: 'Cancel',
                        handler: {
                            click: function () {
                                // close the dialog
                                this.modal('hide');
                            }
                        }
                    }],
                handler: {
                    close: function () {
                        // reset the search field
                        $('#cluster-search-field').val(config.search).addClass('search-nodes');
                    }
                }
            });

            // configure the cluster auto complete
            $.widget('nf.clusterSearchAutocomplete', $.ui.autocomplete, {
                _normalize: function(searchResults) {
                    var items = [];
                    items.push(searchResults);
                    return items;
                },
                _renderMenu: function(ul, items) {
                    // results are normalized into a single element array
                    var searchResults = items[0];
                    
                    var self = this;
                    $.each(searchResults.nodeResults, function(_, node) {
                        self._renderItemData(ul, {
                            label: node.address,
                            value: node.address
                        });
                    });
                    
                    // ensure there were some results
                    if (ul.children().length === 0) {
                        ul.append('<li class="unset search-no-matches">No nodes matched the search terms</li>');
                    }
                },
                _resizeMenu: function () {
                    var ul = this.menu.element;
                    ul.width(299);
                }
            });

            // configure the autocomplete field
            $('#cluster-search-field').clusterSearchAutocomplete({
                minLength: 0,
                appendTo: '#search-cluster-results',
                position: {
                    my: 'left top',
                    at: 'left bottom',
                    offset: '0 1'
                },
                source: function (request, response) {
                    // create the search request
                    $.ajax({
                        type: 'GET',
                        data: {
                            q: request.term
                        },
                        dataType: 'json',
                        url: config.urls.clusterSearch
                    }).done(function (searchResponse) {
                        response(searchResponse);
                    });
                }
            }).focus(function () {
                // conditionally clear the text for the user to type
                if ($(this).val() === config.search) {
                    $(this).val('').removeClass('search-nodes');
                }
            }).val(config.search).addClass('search-nodes');

            // handle the view single node click event
            $('#view-single-node-link').click(function () {
                // hold the search nodes dialog
                $('#view-single-node-dialog').modal('show');
            });

            // handle the view cluster click event
            $('#view-cluster-link').click(function () {
                // reset the urls and refresh the table
                nf.SummaryTable.setClusterNodeId(null);
                nf.SummaryTable.loadSummaryTable();

                // update the header
                $('#summary-header-text').text('NiFi Summary');
            });

            // show the view links
            $('#view-options-container').show();
        }
    };
}());