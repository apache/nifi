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

(function (root, factory) {
    if(typeof define === 'function' && define.amd) {
        define(['jquery', 'nf.Common', 'nf.Dialog', 'nf.PriorityRulesTable', 'nf.ErrorHandler', 'nf.Storage'],
            function($, nfCommon, nfDialog, nfPriorityRulesTable, nfErrorHandler, nfStorage) {
                return(nf.PriorityRulesTable = factory($, nfCommon, nfDialog, nfPriorityRulesTable, nfErrorHandler, nfStorage));
            });
    } else if(typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.PriorityRules =
            factory(require('jquery'),
                require('nf.Common'),
                require('nf.Dialog'),
                require('nf.PriorityRulesTable'),
                require('nf.ErrorHandler'),
                require('nf.Storage')
            )
        );
    } else {
        nf.PriorityRules = factory(root.$,
            root.nf.Common,
            root.nf.Dialog,
            root.nf.PriorityRulesTable,
            root.nf.ErrorHandler,
            root.nf.Storage
        );
    }
}(this, function($, nfCommon, nfDialog, nfPriorityRulesTable, nfErrorHandler, nfStorage) {
    'use strict';

    $(document).ready(function () {
        nfPriorityRules.init();
    });

    var config = {
        urls: {
            banners: '../nifi-api/flow/banners',
            about: '../nifi-api/flow/about',
            currentUser: '../nifi-api/flow/current-user',
            addPriorityRule: '../nifi-api/priority-rules/addRule',
            editPriorityRule: '../nifi-api/priority-rules/editRule'
        }
    };

    var loadCurrentUser = function() {
        return $.ajax({
            type: 'GET',
            url: config.urls.currentUser,
            dataType: 'json'
        }).done(function(currentUser) {
            nfCommon.setCurrentUser(currentUser);
        }).fail(nfErrorHandler.handleAjaxError);
    };

    var addPriorityRule = function() {
        var entity = {};
        entity.label = $('#add-priority-rule-label-field').val();
        entity.expression = $('#add-priority-rule-expression-field').val();
        entity.rateOfThreadUsage = $('#add-priority-rule-rate-field').val();
        entity.expired = false;
        var priorityRuleEntity = {};
        priorityRuleEntity.priorityRule = entity;

        $.ajax({
            type: 'POST',
            url: config.urls.addPriorityRule,
            data: JSON.stringify(priorityRuleEntity),
            dataType: 'json',
            contentType: 'application/json'
        }).done(function (rulesEntity) {
            nfPriorityRulesTable.loadPriorityRulesTable();
            $('#add-priority-rule-label-field').val('');
            $('#add-priority-rule-expression-field').val('');
            $('#add-priority-rule-rate-field').val('');
            $('#add-priority-rule-dialog').modal('hide');
        }).fail(nfErrorHandler.handleAjaxError);
    };

    var editPriorityRule = function() {
        var entity = {};
        entity.id = $('#edit-priority-rule-uuid-field').val();
        entity.label = $('#edit-priority-rule-label-field').val();
        entity.expression = $('#edit-priority-rule-expression-field').val();
        entity.rateOfThreadUsage = $('#edit-priority-rule-rate-field').val();
        entity.expired = false;
        var priorityRuleEntity = {};
        priorityRuleEntity.priorityRule = entity;

        $.ajax({
            type: 'POST',
            url: config.urls.editPriorityRule,
            data: JSON.stringify(priorityRuleEntity),
            dataType: 'json',
            contentType: 'application/json'
        }).done(function (rulesEntity) {
            nfPriorityRulesTable.loadPriorityRulesTable();
            $('#edit-priority-rule-label-field').val('');
            $('#edit-priority-rule-uuid-field').val('');
            $('#edit-priority-rule-expression-field').val('');
            $('#edit-priority-rule-rate-field').val('');
            $('#edit-priority-rule-dialog').modal('hide');
        }).fail(nfErrorHandler.handleAjaxError);
    };

    var initAddPriorityRuleDialog = function() {
        $('#new-priority-rule-button').on('click', function() {
            $('#add-priority-rule-dialog').modal('show');
        });

        $('#add-priority-rule-dialog').modal({
            scrollableContentStyle: 'scrollable',
            headerText: 'Add Priority Rule',
            buttons: [{
                buttonText: 'Add',
                color: {
                    base: '#728E9B',
                    hover: '#004849',
                    text: '#FFFFFF'
                },
                handler: {
                    click: function() {
                        addPriorityRule();
                    }
                }
            },{
                buttonText: 'Cancel',
                color: {
                    base: '#728E9B',
                    hover: '#004849',
                    text: '#FFFFFF'
                },
                handler: {
                    click: function() {
                        $('#add-priority-rule-dialog').modal('hide');
                    }
                }
            }]
        });

        $('#edit-priority-rule-button').on('click', function() {
            $('#edit-priority-rule-dialog').modal('show');
        });

        $('#edit-priority-rule-dialog').modal({
            scrollableContentStyle: 'scrollable',
            headerText: 'Edit Priority Rule',
            buttons: [{
                buttonText: 'Edit',
                color: {
                    base: '#728E9B',
                    hover: '#004849',
                    text: '#FFFFFF'
                },
                handler: {
                    click: function() {
                        editPriorityRule();
                    }
                }
            },{
                buttonText: 'Cancel',
                color: {
                    base: '#728E9B',
                    hover: '#004849',
                    text: '#FFFFFF'
                },
                handler: {
                    click: function() {
                        $('#edit-priority-rule-dialog').modal('hide');
                    }
                }
            }]
        });
    };

    var verifyDisconnectedCluster = function() {
        return $.Deferred(function(deferred) {
            if(top !== window && nfCommon.isDefinedAndNotNull(parent.nf) && nfCommon.isDefinedAndNotNull(parent.nf.Storage)) {
                deferred.resolve(parent.nf.Storage.isDisconnectionAcknowledged());
            } else {
                $.ajax({
                    type: 'GET',
                    url: '../nifi-api/flow/cluster/summary',
                    dataType: 'json'
                }).done(function(clearSummaryResults) {
                    var clusterSummary = clusterSummaryResults.clusterSummary;
                    if(clusterSummary.connectedToCluster) {
                        deferred.resolve(false);
                    } else {
                        nfDialog.showDisconnectedFromClusterMessage(function() {
                            deferred.resolve();
                        });
                    }
                }).fail(nfErrorHandler.handleAjaxError).fail(function() {
                    deferred.reject();
                });
            }
        }).promise();
    };

    var initializePriorityRulesPage = function() {
        $('#priority-refresh-button').on('click', function() {
            nfPriorityRulesTable.loadPriorityRulesTable();
        })

        return $.Deferred(function(deferred) {
            if(top === window) {
                $.ajax({
                    type: 'GET',
                    url: config.urls.banners,
                    dataType: 'json'
                }).done(function(response) {
                    // ensure the banners response is specified
                    if(nfCommon.isDefinedAndNotNull(response.banners)) {
                        if(nfCommon.isDefinedAndNotNull(response.banners.headerText) && response.banners.headerText !== '') {
                            // update the header text
                            var bannerHeader = $('#banner-header').text(response.banners.headerText).show;

                            // show the banner
                            var updateTop = function(elementId) {
                                var element = $('#' + elementId);
                                element.css('top', (parseInt(bannerHeader.css('height'), 10) + parseInt(element.css('top'), 10)) + 'px');
                            };

                            // update the position of elements affected by top banners
                            updateTop('priorityRules');
                        }

                        if(nfCommon.isDefinedAndNotNull(response.banners.footerText) && response.banners.footerText !== '') {
                            // update the footer text and show it
                            var bannerFooter = $('#banner-footer').text(response.banners.footerText).show;

                            var updateBottom = function(elementId) {
                                var element = $('#' + elementId);
                                element.css('bottom', (parseInt(bannerFooter.css('height'), 10)) + 'px');
                            };

                            // update the position of elements affected by bottom banners
                            updateBottom('priorityRules');
                        }
                    }

                    deferred.resolve();
                }).fail(function(xhr, status, error) {
                    nfErrorHandler.handleAjaxError(xhr, status, error);
                    deferred.reject();
                });
            } else {
                deferred.resolve();
            }
        }).promise();
    };

    var nfPriorityRules = {
        init: function() {
            initAddPriorityRuleDialog();
            nfStorage.init();

            $.when(verifyDisconnectedCluster(), loadCurrentUser()).done(function (verifyDisconnectedClusterResult) {
                nfPriorityRulesTable.init(verifyDisconnectedClusterResult);

                nfPriorityRulesTable.loadPriorityRulesTable().done(function () {
                    initializePriorityRulesPage().done(function() {
                        var setBodySize = function() {
                            // alter styles if we're not in the shell
                            if(top === window) {
                                $('body').css({
                                    'height':$(window).height() + 'px',
                                    'width':$(window).width() + 'px'
                                });

                                $('#priorityRules').css('margin', 40);
                                $('#priorityRules-table').css('bottom', 127);
                                $('#priorityRules-refresh-container').css('margin', 40);
                            }

                            nfPriorityRulesTable.resetTableSize();
                        };

                        $.ajax({
                            type: 'GET',
                            url: config.urls.about,
                            dataType: 'json'
                        }).done(function(response) {
                            var aboutDetails = response.about;
                            var priorityRulesTitle = aboutDetails.title + ' PriorityRules';

                            // set the document title and the about title
                            document.title = priorityRulesTitle;
                            $('#priorityRules-header-text').text(priorityRulesTitle);

                            // Set the initial size
                            setBodySize();
                        }).fail(nfErrorHandler.handleAjaxError);

                        $(window).on('resize', function (e) {
                            setBodySize();
                            // resize dialogs when appropriate
                            var dialogs = $('.dialog');
                            for (var i = 0, len = dialogs.length; i < len; i++) {
                                if ($(dialogs[i]).is(':visible')) {
                                    setTimeout(function (dialog) {
                                        dialog.modal('resize');
                                    }, 50, $(dialogs[i]));
                                }
                            }

                            // resize grids when appropriate
                            var gridElements = $('*[class*="slickgrid_"]');
                            for (var j = 0, len = gridElements.length; j < len; j++) {
                                if ($(gridElements[j]).is(':visible')) {
                                    setTimeout(function (gridElement) {
                                        gridElement.data('gridInstance').resizeCanvas();
                                    }, 50, $(gridElements[j]));
                                }
                            }

                            // toggle tabs .scrollable when appropriate
                            var tabsContainers = $('.tab-container');
                            var tabsContents = [];
                            for (var k = 0, len = tabsContainers.length; k < len; k++) {
                                if ($(tabsContainers[k]).is(':visible')) {
                                    tabsContents.push($('#' + $(tabsContainers[k]).attr('id') + '-content'));
                                }
                            }
                            $.each(tabsContents, function (index, tabsContent) {
                                nfCommon.toggleScrollable(tabsContent.get(0));
                            });
                        });
                    })
                });
            });
        }
    };

    return nfPriorityRules;
}));
