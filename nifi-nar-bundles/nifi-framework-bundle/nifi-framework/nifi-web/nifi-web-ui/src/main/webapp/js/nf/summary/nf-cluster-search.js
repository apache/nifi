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
                'nf.Common',
                'nf.Dialog',
                'nf.SummaryTable'],
            function ($, nfCommon, nfDialog, nfSummaryTable) {
                return (nf.ClusterSearch = factory($, nfCommon, nfDialog, nfSummaryTable));
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.ClusterSearch =
            factory(require('jquery'),
                require('nf.Common'),
                require('nf.Dialog'),
                require('nf.SummaryTable')));
    } else {
        nf.ClusterSearch = factory(root.$,
            root.nf.Common,
            root.nf.Dialog,
            root.nf.SummaryTable);
    }
}(this, function ($, nfCommon, nfDialog, nfSummaryTable) {
    'use strict';

    /**
     * Configuration object used to hold a number of configuration items.
     */
    var config = {
        search: 'Search nodes',
        urls: {
            clusterSearch: '../nifi-api/flow/cluster/search-results'
        }
    };

    return {
        /**
         * Initialize the header by register all required listeners.
         */
        init: function () {
            // configure the view single node dialog
            $('#view-single-node-dialog').modal({
                scrollableContentStyle: 'scrollable',
                headerText: 'Select node',
                buttons: [{
                    buttonText: 'Ok',
                    color: {
                        base: '#728E9B',
                        hover: '#004849',
                        text: '#ffffff'
                    },
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

                                // selects the specified node
                                var selectNode = function (node) {
                                    // update the urls to point to this specific node of the cluster
                                    nfSummaryTable.setClusterNodeId(node.id);

                                    // load the summary for the selected node
                                    nfSummaryTable.loadSummaryTable();

                                    // update the header
                                    $('#summary-header-text').text(node.address + ' Summary');
                                };

                                // ensure the search found some results
                                if (!$.isArray(searchResults) || searchResults.length === 0) {
                                    nfDialog.showOkDialog({
                                        headerText: 'Cluster Search',
                                        dialogContent: 'No nodes match \'' + nfCommon.escapeHtml(clusterSearchTerm) + '\'.'
                                    });
                                } else if (searchResults.length > 1) {
                                    var exactMatch = false;

                                    // look for an exact match
                                    $.each(searchResults, function (_, result) {
                                        if (result.address === clusterSearchTerm) {
                                            selectNode(result);
                                            exactMatch = true;
                                            return false;
                                        }
                                    });

                                    // if there is an exact match, use it
                                    if (exactMatch) {
                                        // close the dialog
                                        $('#view-single-node-dialog').modal('hide');
                                    } else {
                                        nfDialog.showOkDialog({
                                            headerText: 'Cluster Search',
                                            dialogContent: 'More than one node matches \'' + nfCommon.escapeHtml(clusterSearchTerm) + '\'.'
                                        });
                                    }
                                } else if (searchResults.length === 1) {
                                    selectNode(searchResults[0]);

                                    // close the dialog
                                    $('#view-single-node-dialog').modal('hide');
                                }
                            });
                        }
                    }
                },
                    {
                        buttonText: 'Cancel',
                        color: {
                            base: '#E3E8EB',
                            hover: '#C7D2D7',
                            text: '#004849'
                        },
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
                        $('#cluster-search-field').val(config.search).addClass('search-nodes').clusterSearchAutocomplete('reset');
                    }
                }
            });

            // configure the cluster auto complete
            $.widget('nf.clusterSearchAutocomplete', $.ui.autocomplete, {
                reset: function () {
                    this.term = null;
                },
                _create: function() {
                    this._super();
                    this.widget().menu('option', 'items', '> :not(.search-no-matches)' );
                },
                _normalize: function (searchResults) {
                    var items = [];
                    items.push(searchResults);
                    return items;
                },
                _renderMenu: function (ul, items) {
                    // results are normalized into a single element array
                    var searchResults = items[0];

                    var nfClusterSearchAutocomplete = this;
                    $.each(searchResults.nodeResults, function (_, node) {
                        nfClusterSearchAutocomplete._renderItem(ul, {
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
                    ul.width($('#cluster-search-field').outerWidth() - 2);
                },
                _renderItem: function (ul, match) {
                    var itemContent = $('<a></a>').text(match.label);
                    return $('<li></li>').data('ui-autocomplete-item', match).append(itemContent).appendTo(ul);
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
                $('#cluster-search-field').focus();
            });

            // handle the view cluster click event
            $('#view-cluster-link').click(function () {
                // reset the urls and refresh the table
                nfSummaryTable.setClusterNodeId(null);
                nfSummaryTable.loadSummaryTable();

                // update the header
                $('#summary-header-text').text('NiFi Summary');
            });

            // show the view links
            $('#view-options-container').show();
        }
    };
}));