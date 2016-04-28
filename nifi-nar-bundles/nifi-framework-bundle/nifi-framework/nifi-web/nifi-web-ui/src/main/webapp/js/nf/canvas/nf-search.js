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
nf.Search = (function () {

    var config = {
        search: 'Search',
        urls: {
            search: '../nifi-api/flow/search-results'
        }
    };

    return {
        /**
         * Initialize the header by register all required listeners.
         */
        init: function () {
            $.widget('nf.searchAutocomplete', $.ui.autocomplete, {
                reset: function () {
                    this.term = null;
                },
                _resizeMenu: function () {
                    var ul = this.menu.element;
                    ul.width(399);
                },
                _normalize: function(searchResults) {
                    var items = [];
                    items.push(searchResults);
                    return items;
                },
                _renderMenu: function (ul, items) {
                    var self = this;
                    
                    // the object that holds the search results is normalized into a single element array
                    var searchResults = items[0];

                    // show all processors
                    if (!nf.Common.isEmpty(searchResults.processorResults)) {
                        ul.append('<li class="search-header"><div class="search-result-icon processor-small-icon"></div>Processors</li>');
                        $.each(searchResults.processorResults, function (i, processorMatch) {
                            self._renderItem(ul, processorMatch);
                        });
                    }

                    // show all process groups
                    if (!nf.Common.isEmpty(searchResults.processGroupResults)) {
                        ul.append('<li class="search-header"><div class="search-result-icon process-group-small-icon"></div>Process Groups</li>');
                        $.each(searchResults.processGroupResults, function (i, processGroupMatch) {
                            self._renderItem(ul, processGroupMatch);
                        });
                    }

                    // show all remote process groups
                    if (!nf.Common.isEmpty(searchResults.remoteProcessGroupResults)) {
                        ul.append('<li class="search-header"><div class="search-result-icon remote-process-group-small-icon"></div>Remote Process Groups</li>');
                        $.each(searchResults.remoteProcessGroupResults, function (i, remoteProcessGroupMatch) {
                            self._renderItem(ul, remoteProcessGroupMatch);
                        });
                    }

                    // show all connections
                    if (!nf.Common.isEmpty(searchResults.connectionResults)) {
                        ul.append('<li class="search-header"><div class="search-result-icon connection-small-icon"></div>Connections</li>');
                        $.each(searchResults.connectionResults, function (i, connectionMatch) {
                            self._renderItem(ul, connectionMatch);
                        });
                    }

                    // show all input ports
                    if (!nf.Common.isEmpty(searchResults.inputPortResults)) {
                        ul.append('<li class="search-header"><div class="search-result-icon input-port-small-icon"></div>Input Ports</li>');
                        $.each(searchResults.inputPortResults, function (i, inputPortMatch) {
                            self._renderItem(ul, inputPortMatch);
                        });
                    }

                    // show all output ports
                    if (!nf.Common.isEmpty(searchResults.outputPortResults)) {
                        ul.append('<li class="search-header"><div class="search-result-icon output-port-small-icon"></div>Output Ports</li>');
                        $.each(searchResults.outputPortResults, function (i, outputPortMatch) {
                            self._renderItem(ul, outputPortMatch);
                        });
                    }

                    // show all funnels
                    if (!nf.Common.isEmpty(searchResults.funnelResults)) {
                        ul.append('<li class="search-header"><div class="search-result-icon funnel-small-icon"></div>Funnels</li>');
                        $.each(searchResults.funnelResults, function (i, funnelMatch) {
                            self._renderItem(ul, funnelMatch);
                        });
                    }

                    // ensure there were some results
                    if (ul.children().length === 0) {
                        ul.append('<li class="unset search-no-matches">No results matched the search terms</li>');
                    }
                },
                _renderItem: function (ul, match) {
                    var itemContent = $('<a></a>').append($('<div class="search-match-header"></div>').text(match.name));
                    $.each(match.matches, function (i, match) {
                        itemContent.append($('<div class="search-match"></div>').text(match));
                    });
                    return $('<li></li>').data('ui-autocomplete-item', match).append(itemContent).appendTo(ul);
                }
            });

            // configure the search field
            $('#search-field').zIndex(1250).searchAutocomplete({
                appendTo: '#search-flow-results',
                position: {
                    my: 'right top',
                    at: 'right bottom',
                    offset: '1 1'
                },
                source: function (request, response) {
                    // create the search request
                    $.ajax({
                        type: 'GET',
                        data: {
                            q: request.term
                        },
                        dataType: 'json',
                        url: config.urls.search
                    }).done(function (searchResponse) {
                        response(searchResponse.searchResultsDTO);
                    });
                },
                select: function (event, ui) {
                    var item = ui.item;

                    // show the selected component
                    nf.CanvasUtils.showComponent(item.groupId, item.id);

                    // blur the search field
                    $(this).blur();

                    // stop event propagation
                    return false;
                },
                open: function (event, ui) {
                    // show the glass pane
                    var searchField = $(this);
                    $('<div class="search-glass-pane"></div>').one('click', function () {
                        // blur the field
                        searchField.blur();
                    }).appendTo('body');
                },
                close: function (event, ui) {
                    // set the text to 'Search' and reset the cached term
                    $(this).searchAutocomplete('reset');

                    // remove the glass pane
                    $('div.search-glass-pane').remove();
                }
            }).focus(function () {
                // hide the context menu if necessary
                nf.ContextMenu.hide();
                
                // clear the text for the user to type
                $(this).val('').removeClass('search-flow');
            }).blur(function () {
                $(this).val(config.search).addClass('search-flow');
            }).val(config.search).addClass('search-flow');
        }
    };
}());