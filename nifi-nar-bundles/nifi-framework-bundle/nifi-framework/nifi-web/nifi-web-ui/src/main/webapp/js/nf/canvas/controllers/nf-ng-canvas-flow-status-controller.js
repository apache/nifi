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
                'nf.CanvasUtils',
                'nf.ContextMenu',
                'nf.ClusterSummary',
                'nf.ErrorHandler',
                'nf.Settings',
                'nf.ParameterContexts',
                'nf.ProcessGroup',
                'nf.ProcessGroupConfiguration',
                'nf.Shell'],
            function ($, nfCommon, nfDialog, nfCanvasUtils, nfContextMenu, nfClusterSummary, nfErrorHandler, nfSettings, nfParameterContexts, nfProcessGroup, nfProcessGroupConfiguration, nfShell) {
                return (nf.ng.Canvas.FlowStatusCtrl = factory($, nfCommon, nfDialog, nfCanvasUtils, nfContextMenu, nfClusterSummary, nfErrorHandler, nfSettings, nfParameterContexts, nfProcessGroup, nfProcessGroupConfiguration, nfShell));
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.ng.Canvas.FlowStatusCtrl =
            factory(require('jquery'),
                require('nf.Common'),
                require('nf.Dialog'),
                require('nf.CanvasUtils'),
                require('nf.ContextMenu'),
                require('nf.ClusterSummary'),
                require('nf.ErrorHandler'),
                require('nf.Settings'),
                require('nf.ParameterContexts'),
                require('nf.ProcessGroup'),
                require('nf.ProcessGroupConfiguration'),
                require('nf.Shell')));
    } else {
        nf.ng.Canvas.FlowStatusCtrl = factory(root.$,
            root.nf.Common,
            root.nf.Dialog,
            root.nf.CanvasUtils,
            root.nf.ContextMenu,
            root.nf.ClusterSummary,
            root.nf.ErrorHandler,
            root.nf.Settings,
            root.nf.ParameterContexts,
            root.nf.ProcessGroup,
            root.nf.ProcessGroupConfiguration,
            root.nf.Shell);
    }
}(this, function ($, nfCommon, nfDialog, nfCanvasUtils, nfContextMenu, nfClusterSummary, nfErrorHandler, nfSettings, nfParameterContexts, nfProcessGroup, nfProcessGroupConfiguration, nfShell) {
    'use strict';

    return function (serviceProvider) {
        'use strict';

        var config = {
            search: 'Search',
            urls: {
                search: '../nifi-api/flow/search-results',
                status: '../nifi-api/flow/status',
                flowAnalysis: '../nifi-api/controller/analyze-flow'
            }
        };

        var previousRulesResponse = {};

        function FlowStatusCtrl() {
            this.connectedNodesCount = "-";
            this.clusterConnectionWarning = false;
            this.activeThreadCount = "-";
            this.terminatedThreadCount = "-";
            this.threadCounts = "-";
            this.totalQueued = "-";
            this.controllerTransmittingCount = "-";
            this.controllerNotTransmittingCount = "-";
            this.controllerRunningCount = "-";
            this.controllerStoppedCount = "-";
            this.controllerInvalidCount = "-";
            this.controllerDisabledCount = "-";
            this.controllerUpToDateCount = "-";
            this.controllerLocallyModifiedCount = "-";
            this.controllerStaleCount = "-";
            this.controllerLocallyModifiedAndStaleCount = "-";
            this.controllerSyncFailureCount = "-";
            this.statsLastRefreshed = "-";

            /**
             * The search controller.
             */
            this.search = {

                /**
                 * Get the search input element.
                 */
                getInputElement: function () {
                    return $('#search-field');
                },

                /**
                 * Get the search button element.
                 */
                getButtonElement: function () {
                    return $('#search-button');
                },

                /**
                 * Get the search container element.
                 */
                getSearchContainerElement: function () {
                    return $('#search-container');
                },

                /**
                 * Initialize the search controller.
                 */
                init: function () {

                    var searchCtrl = this;

                    // Create new jQuery UI widget
                    $.widget('nf.searchAutocomplete', $.ui.autocomplete, {
                        reset: function () {
                            this.term = null;
                        },
                        _create: function () {
                            this._super();
                            this.widget().menu('option', 'items', '> :not(.search-header, .search-no-matches)');
                        },
                        _resizeMenu: function () {
                            var ul = this.menu.element;
                            ul.width(400);
                        },
                        _normalize: function (searchResults) {
                            var items = [];
                            items.push(searchResults);
                            return items;
                        },
                        _renderMenu: function (ul, items) {
                            var nfSearchAutocomplete = this;

                            // the object that holds the search results is normalized into a single element array
                            var searchResults = items[0];

                            // show all processors
                            if (!nfCommon.isEmpty(searchResults.processorResults)) {
                                ul.append('<li class="search-header"><div class="search-result-icon icon icon-processor"></div>Processors</li>');
                                $.each(searchResults.processorResults, function (i, processorMatch) {
                                    nfSearchAutocomplete._renderItem(ul, $.extend({}, processorMatch, { type: 'processor' }));
                                });
                            }

                            // show all process groups
                            if (!nfCommon.isEmpty(searchResults.processGroupResults)) {
                                ul.append('<li class="search-header"><div class="search-result-icon icon icon-group"></div>Process Groups</li>');
                                $.each(searchResults.processGroupResults, function (i, processGroupMatch) {
                                    nfSearchAutocomplete._renderItem(ul, $.extend({}, processGroupMatch, { type: 'process group' }));
                                });
                            }

                            // show all remote process groups
                            if (!nfCommon.isEmpty(searchResults.remoteProcessGroupResults)) {
                                ul.append('<li class="search-header"><div class="search-result-icon icon icon-group-remote"></div>Remote Process Groups</li>');
                                $.each(searchResults.remoteProcessGroupResults, function (i, remoteProcessGroupMatch) {
                                    nfSearchAutocomplete._renderItem(ul, $.extend({}, remoteProcessGroupMatch, { type: 'remote process group' }));
                                });
                            }

                            // show all connections
                            if (!nfCommon.isEmpty(searchResults.connectionResults)) {
                                ul.append('<li class="search-header"><div class="search-result-icon icon icon-connect"></div>Connections</li>');
                                $.each(searchResults.connectionResults, function (i, connectionMatch) {
                                    nfSearchAutocomplete._renderItem(ul, $.extend({}, connectionMatch, { type: 'connection' }));
                                });
                            }

                            // show all input ports
                            if (!nfCommon.isEmpty(searchResults.inputPortResults)) {
                                ul.append('<li class="search-header"><div class="search-result-icon icon icon-port-in"></div>Input Ports</li>');
                                $.each(searchResults.inputPortResults, function (i, inputPortMatch) {
                                    nfSearchAutocomplete._renderItem(ul, $.extend({}, inputPortMatch, { type: 'input port' }));
                                });
                            }

                            // show all output ports
                            if (!nfCommon.isEmpty(searchResults.outputPortResults)) {
                                ul.append('<li class="search-header"><div class="search-result-icon icon icon-port-out"></div>Output Ports</li>');
                                $.each(searchResults.outputPortResults, function (i, outputPortMatch) {
                                    nfSearchAutocomplete._renderItem(ul, $.extend({}, outputPortMatch, { type: 'output port' }));
                                });
                            }

                            // show all funnels
                            if (!nfCommon.isEmpty(searchResults.funnelResults)) {
                                ul.append('<li class="search-header"><div class="search-result-icon icon icon-funnel"></div>Funnels</li>');
                                $.each(searchResults.funnelResults, function (i, funnelMatch) {
                                    nfSearchAutocomplete._renderItem(ul, $.extend({}, funnelMatch, { type: 'funnel' }));
                                });
                            }

                            // show all labels
                            if (!nfCommon.isEmpty(searchResults.labelResults)) {
                                ul.append('<li class="search-header"><div class="search-result-icon icon icon-label"></div>Labels</li>');
                                $.each(searchResults.labelResults, function (i, labelMatch) {
                                    nfSearchAutocomplete._renderItem(ul, $.extend({}, labelMatch, { type: 'label' }));
                                });
                            }

                            // show all controller services
                            if (!nfCommon.isEmpty(searchResults.controllerServiceNodeResults)) {
                                ul.append('<li class="search-header"><div class="search-result-icon icon"></div>Controller Services</li>');
                                $.each(searchResults.controllerServiceNodeResults, function (i, controllerServiceMatch) {
                                    nfSearchAutocomplete._renderItem(ul, $.extend({}, controllerServiceMatch, { type: 'controller service' }));
                                });
                            }

                            // show all parameter providers
                            if (!nfCommon.isEmpty(searchResults.parameterProviderNodeResults)) {
                                ul.append('<li class="search-header"><div class="search-result-icon icon"></div>Parameter Providers</li>');
                                $.each(searchResults.parameterProviderNodeResults, function (i, parameterProviderMatch) {
                                    nfSearchAutocomplete._renderItem(ul, $.extend({}, parameterProviderMatch, { type: 'parameter provider' }));
                                });
                            }

                            // show all parameter contexts and parameters
                            if (!nfCommon.isEmpty(searchResults.parameterContextResults)) {
                                ul.append('<li class="search-header"><div class="search-result-icon icon"></div>Parameter Contexts</li>');
                                $.each(searchResults.parameterContextResults, function (i, parameterContextMatch) {
                                    nfSearchAutocomplete._renderItem(ul, $.extend({}, parameterContextMatch, { type: 'parameter context' }));
                                });
                            }

                            // show all parameters
                            if (!nfCommon.isEmpty(searchResults.parameterResults)) {
                                ul.append('<li class="search-header"><div class="search-result-icon icon"></div>Parameters</li>');
                                $.each(searchResults.parameterResults, function (i, parameterMatch) {
                                    nfSearchAutocomplete._renderItem(ul, $.extend({}, parameterMatch, { type: 'parameter' }));
                                });
                            }

                            // ensure there were some results
                            if (ul.children().length === 0) {
                                ul.append('<li class="unset search-no-matches">No results matched the search terms</li>');
                            }
                        },
                        _renderItem: function (ul, match) {
                            var itemHeader = $('<div class="search-match-header"></div>').text(match.name);
                            var itemContent = $('<a></a>').append(itemHeader);

                            if (match.type !== 'parameter context' && match.type !== 'parameter') {
                                var parentGroupHeader = $('<div class="search-match-header"></div>').append(document.createTextNode('Parent: '));
                                var parentGroup = '-';
                                if (nfCommon.isDefinedAndNotNull(match.parentGroup)) {
                                    parentGroup = match.parentGroup.name ? match.parentGroup.name : match.parentGroup.id;
                                }
                                parentGroupHeader = parentGroupHeader.append($('<span></span>').text(parentGroup));

                                var versionedGroupHeader = $('<div class="search-match-header"></div>').append(document.createTextNode('Versioned: '));
                                var versionedGroup = '-';

                                if (nfCommon.isDefinedAndNotNull(match.versionedGroup)) {
                                    versionedGroup = match.versionedGroup.name ? match.versionedGroup.name : match.versionedGroup.id;
                                }

                                versionedGroupHeader = versionedGroupHeader.append($('<span></span>').text(versionedGroup));
                                // create a search item wrapper
                                itemContent.append(parentGroupHeader).append(versionedGroupHeader);
                            } else if (match.type === 'parameter') {
                                var paramContextHeader = $('<div class="search-match-header"></div>').append(document.createTextNode('Parameter Context: '));
                                var paramContext = '-';
                                if (nfCommon.isDefinedAndNotNull(match.parentGroup)) {
                                    paramContext = match.parentGroup.name ? match.parentGroup.name : match.parentGroup.id;
                                }
                                paramContextHeader = paramContextHeader.append($('<span></span>').text(paramContext));
                                itemContent.append(paramContextHeader);
                            }

                            // append all matches
                            $.each(match.matches, function (i, match) {
                                itemContent.append($('<div class="search-match"></div>').text(match));
                            });
                            return $('<li></li>').data('ui-autocomplete-item', match).append(itemContent).appendTo(ul);
                        }
                    });

                    // configure the new searchAutocomplete jQuery UI widget
                    this.getInputElement().searchAutocomplete({
                        delay : 1000,
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
                                    q: request.term,
                                    a: nfCanvasUtils.getGroupId()
                                },
                                dataType: 'json',
                                url: config.urls.search
                            }).done(function (searchResponse) {
                                response(searchResponse.searchResultsDTO);
                            });
                        },
                        select: function (event, ui) {
                            var item = ui.item;

                            switch (item.type) {
                                case 'parameter context':
                                    nfParameterContexts.showParameterContexts(item.id);
                                    break;
                                case 'parameter':
                                    var paramContext = item.parentGroup;
                                    nfParameterContexts.showParameterContext(paramContext.id, null, item.name);
                                    break;
                                case 'controller service':
                                    var group = item.parentGroup;
                                    nfProcessGroup.enterGroup(group.id).done(function () {
                                        nfProcessGroupConfiguration.showConfiguration(group.id).done(function () {
                                            nfProcessGroupConfiguration.selectControllerService(item.id);
                                        });
                                    });
                                    break;
                                default:
                                    var group = item.parentGroup;

                                    // show the selected component
                                    nfCanvasUtils.showComponent(group.id, item.id);
                                    break;
                            }

                            searchCtrl.getInputElement().val('').blur();

                            // stop event propagation
                            return false;
                        },
                        open: function (event, ui) {
                            // show the glass pane
                            var searchField = $(this);
                            $('<div class="search-glass-pane"></div>').one('click', function () {
                            }).appendTo('body');
                        },
                        close: function (event, ui) {
                            // set the input text to '' and reset the cached term
                            $(this).searchAutocomplete('reset');
                            searchCtrl.getInputElement().val('');

                            // remove the glass pane
                            $('div.search-glass-pane').remove();
                        }
                    });

                    // hide the search input
                    searchCtrl.toggleSearchField();
                },

                /**
                 * Toggle/Slide the search field open/closed.
                 */
                toggleSearchField: function () {
                    var searchCtrl = this;

                    // hide the context menu if necessary
                    nfContextMenu.hide();

                    var isVisible = searchCtrl.getInputElement().is(':visible');
                    var display = 'none';
                    var class1 = 'search-container-opened';
                    var class2 = 'search-container-closed';
                    if (!isVisible) {
                        searchCtrl.getButtonElement().css('background-color', '#FFFFFF');
                        display = 'inline-block';
                        class1 = 'search-container-closed';
                        class2 = 'search-container-opened';
                    } else {
                        searchCtrl.getInputElement().css('display', display);
                    }

                    this.getSearchContainerElement().switchClass(class1, class2, 500, function () {
                        searchCtrl.getInputElement().css('display', display);
                        if (!isVisible) {
                            searchCtrl.getButtonElement().css('background-color', '#FFFFFF');
                            searchCtrl.getInputElement().focus();
                        } else {
                            searchCtrl.getButtonElement().css('background-color', '#E3E8EB');
                        }
                    });
                }
            }

            /**
             * The flow analysis controller.
             */

            this.flowAnalysis = {

                /**
                 * Create the list of rule violations
                 */
                buildRuleViolationsList: function(rules, violationsAndRecs) {
                    var ruleViolationCountEl = $('#rule-violation-count');
                    var ruleViolationListEl = $('#rule-violations-list');
                    var ruleWarningCountEl = $('#rule-warning-count');
                    var ruleWarningListEl = $('#rule-warnings-list');
                    var violations = violationsAndRecs.filter(function (violation) {
                        return violation.enforcementPolicy === 'ENFORCE'
                    });
                    var warnings = violationsAndRecs.filter(function (violation) {
                        return violation.enforcementPolicy === 'WARN'
                    });
                    ruleViolationCountEl.empty().text('(' + violations.length + ')');
                    ruleWarningCountEl.empty().text('(' + warnings.length + ')');
                    ruleViolationListEl.empty();
                    ruleWarningListEl.empty();
                    violations.forEach(function(violation) {
                        var rule = rules.find(function(rule) {
                            return rule.id === violation.ruleId;
                        });
                        // create DOM elements
                        var violationListItemEl = $('<li></li>');
                        var violationEl = $('<div class="violation-list-item"></div>');
                        var violationListItemWrapperEl = $('<div class="violation-list-item-wrapper"></div>');
                        var violationRuleEl = $('<div class="rule-violations-list-item-name"></div>');
                        var violationListItemNameEl = $('<div class="violation-list-item-name"></div>');
                        var violationListItemIdEl = $('<span class="violation-list-item-id"></span>');
                        var violationInfoButtonEl = $('<button class="violation-menu-btn"><i class="fa fa-ellipsis-v rules-list-item-menu-target" aria-hidden="true"></i></button>');

                        // add text content and button data
                        $(violationRuleEl).text(rule.name);
                        violation.subjectPermissionDto.canRead ? $(violationListItemNameEl).text(violation.subjectDisplayName) : $(violationListItemNameEl).text('Unauthorized').addClass('unauthorized');
                        $(violationListItemIdEl).text(violation.subjectId);
                        $(violationListItemEl).append(violationRuleEl).append(violationListItemWrapperEl);
                        $(violationInfoButtonEl).data('violationInfo', violation);

                        // build list DOM structure
                        violationListItemWrapperEl.append(violationListItemNameEl).append(violationListItemIdEl);
                        violationEl.append(violationListItemWrapperEl).append(violationInfoButtonEl);
                        violationListItemEl.append(violationRuleEl).append(violationEl)
                        ruleViolationListEl.append(violationListItemEl);
                    });

                    warnings.forEach(function(warning) {
                        var rule = rules.find(function(rule) {
                            return rule.id === warning.ruleId;
                        });
                        // create DOM elements
                        var warningListItemEl = $('<li></li>');
                        var warningEl = $('<div class="warning-list-item"></div>');
                        var warningListItemWrapperEl = $('<div class="warning-list-item-wrapper"></div>');
                        var warningRuleEl = $('<div class="rule-warnings-list-item-name"></div>');
                        var warningListItemNameEl = $('<div class="warning-list-item-name"></div>');
                        var warningListItemIdEl = $('<span class="warning-list-item-id"></span>');
                        var warningInfoButtonEl = $('<button class="violation-menu-btn"><i class="fa fa-ellipsis-v rules-list-item-menu-target" aria-hidden="true"></i></button>');

                        // add text content and button data
                        $(warningRuleEl).text(rule.name);
                        warning.subjectPermissionDto.canRead ? $(warningListItemNameEl).text(warning.subjectDisplayName) : $(warningListItemNameEl).text('Unauthorized').addClass('unauthorized');
                        $(warningListItemIdEl).text(warning.subjectId);
                        $(warningListItemEl).append(warningRuleEl).append(warningListItemWrapperEl);
                        $(warningInfoButtonEl).data('violationInfo', warning);

                        // build list DOM structure
                        warningListItemWrapperEl.append(warningListItemNameEl).append(warningListItemIdEl);
                        warningEl.append(warningListItemWrapperEl).append(warningInfoButtonEl);
                        warningListItemEl.append(warningRuleEl).append(warningEl)
                        ruleWarningListEl.append(warningListItemEl);
                    });
                },

                /**
                 * 
                 * Render a new list when it differs from the previous violations response
                 */
                buildRuleViolations: function(rules, violations) {
                    if (Object.keys(previousRulesResponse).length !== 0) {
                        var previousRulesResponseSorted = _.sortBy(previousRulesResponse.ruleViolations, 'subjectId');
                        var violationsSorted = _.sortBy(violations, 'subjectId');
                        if (!_.isEqual(previousRulesResponseSorted, violationsSorted)) {
                            this.buildRuleViolationsList(rules, violations);
                        }
                    } else {
                        this.buildRuleViolationsList(rules, violations);
                    }
                },

                /**
                 * Create the list of flow policy rules
                 */
                buildRuleList: function(ruleType, violationsMap, rec) {
                    var requiredRulesListEl = $('#required-rules-list');
                    var recommendedRulesListEl = $('#recommended-rules-list');
                    var rule = $('<li class="rules-list-item"></li>').append($(rec.requirement).append(rec.requirementInfoButton))
                    var violationsListEl = '';
                    var violationCountEl = '';
                    
                    var violations = violationsMap.get(rec.id);
                    if (!!violations) {
                        if (violations.length === 1) {
                            violationCountEl = '<div class="rule-' + ruleType + 's-count">' + violations.length + ' ' + ruleType + '</div>';
                        } else {
                            violationCountEl = '<div class="rule-' + ruleType + 's-count">' + violations.length + ' ' + ruleType + 's</div>';
                        }
                        violationsListEl = $('<ul class="rule-' + ruleType + 's-list"></ul>');
                        violations.forEach(function(violation) {
                            // create DOM elements
                            var violationListItemEl = $('<li class="' + ruleType + '-list-item"></li>');
                            var violationWrapperEl = $('<div class="' + ruleType + '-list-item-wrapper"></div>');
                            var violationNameEl = $('<div class="' + ruleType + '-list-item-name"></div>');
                            var violationIdEl = $('<span class="' + ruleType + '-list-item-id"></span>');
                            var violationInfoButtonEl = $('<button class="violation-menu-btn"><i class="fa fa-ellipsis-v rules-list-item-menu-target" aria-hidden="true"></i></button>');

                            // add text content and button data
                            violation.subjectPermissionDto.canRead ? violationNameEl.text(violation.subjectDisplayName) : violationNameEl.text('Unauthorized');
                            violationIdEl.text(violation.subjectId);

                            // build list DOM structure
                            violationListItemEl.append(violationWrapperEl);
                            violationWrapperEl.append(violationNameEl).append(violationIdEl)
                            violationInfoButtonEl.data('violationInfo', violation);
                            (violationsListEl).append(violationListItemEl.append(violationInfoButtonEl));
                        });
                        rule.append(violationCountEl).append(violationsListEl);
                    }
                    ruleType === 'violation' ? requiredRulesListEl.append(rule) : recommendedRulesListEl.append(rule);
                },

                /**
                 * Loads the current status of the flow.
                 */
                loadFlowPolicies: function () {
                    var flowAnalysisCtrl = this;
                    var requiredRulesListEl = $('#required-rules-list');
                    var recommendedRulesListEl = $('#recommended-rules-list');
                    var requiredRuleCountEl = $('#required-rule-count');
                    var recommendedRuleCountEl = $('#recommended-rule-count');
                    var flowAnalysisLoader = $('#flow-analysis-loading-container');
                    var flowAnalysisLoadMessage = $('#flow-analysis-loading-message');

                    var groupId = nfCanvasUtils.getGroupId();
                    if (groupId !== 'root') {
                        $.ajax({
                            type: 'GET',
                            url: '../nifi-api/flow/flow-analysis/results/' + groupId,
                            dataType: 'json',
                            context: this
                        }).done(function (response) {
                            var recommendations = [];
                            var requirements = [];
                            var requirementsTotal = 0;
                            var recommendationsTotal = 0;

                            if (!_.isEqual(previousRulesResponse, response)) {
                                // clear previous accordion content
                                requiredRulesListEl.empty();
                                recommendedRulesListEl.empty();
                                flowAnalysisCtrl.buildRuleViolations(response.rules, response.ruleViolations);

                                if (response.flowAnalysisPending) {
                                    flowAnalysisLoader.addClass('ajax-loading');
                                    flowAnalysisLoadMessage.show();
                                } else {
                                    flowAnalysisLoader.removeClass('ajax-loading');
                                    flowAnalysisLoadMessage.hide();
                                }

                                // For each ruleViolations: 
                                // * group violations by ruleId
                                // * build DOM elements
                                // * get the ruleId and find the matching rule id
                                // * append violation list to matching rule list item
                                var violationsMap = new Map();
                                response.ruleViolations.forEach(function(violation) {
                                    if (violationsMap.has(violation.ruleId)){
                                        violationsMap.get(violation.ruleId).push(violation);
                                     } else {
                                        violationsMap.set(violation.ruleId, [violation]);
                                     }
                                });
    
                                // build list of recommendations
                                response.rules.forEach(function(rule) {
                                    if (rule.enforcementPolicy === 'WARN') {
                                        var requirement = '<div class="rules-list-rule-info"></div>';
                                        var requirementName = $('<div></div>').text(rule.name);
                                        var requirementInfoButton = '<button class="rule-menu-btn"><i class="fa fa-ellipsis-v rules-list-item-menu-target" aria-hidden="true"></i></button>';
                                        recommendations.push(
                                            {
                                                'requirement': $(requirement).append(requirementName),
                                                'requirementInfoButton': $(requirementInfoButton).data('ruleInfo', rule),
                                                'id': rule.id
                                            }
                                        )
                                        recommendationsTotal++;
                                    }
                                });

                                // add class to notification icon for recommended rules
                                var hasRecommendations = response.ruleViolations.findIndex(function(violation) {
                                    return violation.enforcementPolicy === 'WARN';
                                });
                                if (hasRecommendations !== -1) {
                                    $('#flow-analysis .flow-analysis-notification-icon ').addClass('recommendations');
                                } else {
                                    $('#flow-analysis .flow-analysis-notification-icon ').removeClass('recommendations');
                                }
    
                                // build list of requirements
                                recommendedRuleCountEl.empty().append('(' + recommendationsTotal + ')');
                                recommendations.forEach(function(rec) {
                                    flowAnalysisCtrl.buildRuleList('recommendation', violationsMap, rec);
                                });

                                response.rules.forEach(function(rule) {
                                    if (rule.enforcementPolicy === 'ENFORCE') {
                                        var requirement = '<div class="rules-list-rule-info"></div>';
                                        var requirementName = $('<div></div>').text(rule.name);
                                        var requirementInfoButton = '<button class="rule-menu-btn"><i class="fa fa-ellipsis-v rules-list-item-menu-target" aria-hidden="true"></i></button>';
                                        requirements.push(
                                            {
                                                'requirement': $(requirement).append(requirementName),
                                                'requirementInfoButton': $(requirementInfoButton).data('ruleInfo', rule),
                                                'id': rule.id
                                            }
                                        )
                                        requirementsTotal++;
                                    }
                                });

                                // add class to notification icon for required rules
                                var hasViolations = response.ruleViolations.findIndex(function(violation) {
                                    return violation.enforcementPolicy === 'ENFORCE';
                                })
                                if (hasViolations !== -1) {
                                    $('#flow-analysis .flow-analysis-notification-icon ').addClass('violations');
                                } else {
                                    $('#flow-analysis .flow-analysis-notification-icon ').removeClass('violations');
                                }
    
                                requiredRuleCountEl.empty().append('(' + requirementsTotal + ')');
                                
                                // build violations
                                requirements.forEach(function(rec) {
                                    flowAnalysisCtrl.buildRuleList('violation', violationsMap, rec);                              
                                });

                                $('#required-rules').accordion('refresh');
                                $('#recommended-rules').accordion('refresh');
                                // report the updated status
                                previousRulesResponse = response;
    
                                // setup rule menu handling
                                flowAnalysisCtrl.setRuleMenuHandling();

                                // setup violation menu handling
                                flowAnalysisCtrl.setViolationMenuHandling(response.rules);
                            }
                        }).fail(nfErrorHandler.handleAjaxError);
                    }
                },

                /**
                 * Set event bindings for rule menus
                 */
                setRuleMenuHandling: function() {
                    $('.rule-menu-btn').click(function(event) {
                        // stop event from immediately bubbling up to document and triggering closeRuleWindow
                        event.stopPropagation();
                        // unbind previously bound rule data that may still exist
                        unbindRuleMenuHandling();

                        var ruleInfo = $(this).data('ruleInfo');
                        $('#violation-menu').hide();
                        $('#rule-menu').show();
                        $('#rule-menu').position({
                            my: "left top",
                            at: "left top",
                            of: event
                        });

                        // rule menu bindings
                        if (nfCommon.canAccessController()) {
                            $('#rule-menu-edit-rule').removeClass('disabled');
                            $('#rule-menu-edit-rule .rule-menu-option-icon').removeClass('disabled');
                            $('#rule-menu-edit-rule').on('click', openRuleDetailsDialog);
                        } else {
                            $('#rule-menu-edit-rule').addClass('disabled');
                            $('#rule-menu-edit-rule .rule-menu-option-icon').addClass('disabled');
                        }
                        $('#rule-menu-view-documentation').on('click', viewRuleDocumentation);
                        $(document).on('click', closeRuleWindow);

                        function viewRuleDocumentation(e) {
                            nfShell.showPage('../nifi-docs/documentation?' + $.param({
                                select: ruleInfo.type,
                                group: ruleInfo.bundle.group,
                                artifact: ruleInfo.bundle.artifact,
                                version: ruleInfo.bundle.version
                            })).done(function () {});
                            $("#rule-menu").hide();
                            unbindRuleMenuHandling();
                        }

                        function closeRuleWindow(e) {
                            if ($(e.target).parents("#rule-menu").length === 0) {
                                $("#rule-menu").hide();
                                unbindRuleMenuHandling();
                            }
                        }

                        function openRuleDetailsDialog() {
                            $('#rule-menu').hide();
                            nfSettings.showSettings().done(function() {
                                nfSettings.selectFlowAnalysisRule(ruleInfo.id);
                            });
                            unbindRuleMenuHandling();
                        }

                        function unbindRuleMenuHandling() {
                            $('#rule-menu-edit-rule').off("click");
                            $('#rule-menu-view-documentation').off("click");
                            $(document).unbind('click', closeRuleWindow);
                        }

                    });
                },

                /**
                 * Set event bindings for violation menus
                 */
                setViolationMenuHandling: function(rules) {
                    $('.violation-menu-btn').click(function(event) {
                        // stop event from immediately bubbling up to document and triggering closeViolationWindow
                        event.stopPropagation();
                        var violationInfo = $(this).data('violationInfo');
                        $('#rule-menu').hide();
                        $('#violation-menu').show();
                        $('#violation-menu').position({
                            my: "left top",
                            at: "left top",
                            of: event
                        });

                        // violation menu bindings
                        if (violationInfo.subjectPermissionDto.canRead) {
                            $('#violation-menu-more-info').removeClass('disabled');
                            $('#violation-menu-more-info .violation-menu-option-icon').removeClass('disabled');
                            $('#violation-menu-more-info').on( "click", openRuleViolationMoreInfoDialog);
                        } else {
                            $('#violation-menu-more-info').addClass('disabled');
                            $('#violation-menu-more-info .violation-menu-option-icon').addClass('disabled');
                        }
                        
                        if (violationInfo.subjectComponentType === 'PROCESSOR' && violationInfo.subjectPermissionDto.canRead) {
                            $('#violation-menu-go-to').removeClass('hidden');
                            $('#violation-menu-go-to').on('click', goToComponent);
                        } else {
                            $('#violation-menu-go-to').addClass('hidden');
                        }
                        $(document).on('click', closeViolationWindow);

                        function closeViolationWindow(e) {
                            if ($(e.target).parents("#violation-menu").length === 0) {
                                $("#violation-menu").hide();
                                unbindViolationMenuHandling();
                            }
                        }

                        function openRuleViolationMoreInfoDialog() {
                            var rule = rules.find(function(rule){ 
                                return rule.id === violationInfo.ruleId;
                            });
                            $('#violation-menu').hide();
                            $('#violation-type-pill').empty()
                                                    .removeClass()
                                                    .addClass(violationInfo.enforcementPolicy.toLowerCase() + ' violation-type-pill')
                                                    .append(violationInfo.enforcementPolicy);
                            $('#violation-description').empty().append(violationInfo.violationMessage);
                            $('#violation-menu-more-info-dialog').modal( "show" );
                            $('.violation-docs-link').click(function () {
                                // open the documentation for this flow analysis rule
                                nfShell.showPage('../nifi-docs/documentation?' + $.param({
                                    select: rule.type,
                                    group: rule.bundle.group,
                                    artifact: rule.bundle.artifact,
                                    version: rule.bundle.version
                                })).done(function () {});
                            });
                            unbindViolationMenuHandling();
                        }

                        function goToComponent() {
                            $('#violation-menu').hide();
                            nfCanvasUtils.showComponent(violationInfo.groupId, violationInfo.subjectId);
                            unbindViolationMenuHandling();
                        }

                        function unbindViolationMenuHandling() {
                            $('#violation-menu-more-info').off("click");
                            $('#violation-menu-go-to').off("click");
                            $(document).unbind('click', closeViolationWindow);
                        }
                    });
                },

                /**
                 * Initialize the flow analysis controller.
                 */
                init: function () {
                    var flowAnalysisCtrl = this;
                    var drawer = $('#flow-analysis-drawer');
                    var requiredRulesEl = $('#required-rules');
                    var recommendedRulesEl = $('#recommended-rules');
                    var flowAnalysisRefreshIntervalSeconds = nfCommon.getAutoRefreshInterval();

                    $('#flow-analysis').click(function () {
                        $(this).toggleClass('opened');
                        drawer.toggleClass('opened');
                    });
                    requiredRulesEl.accordion({
                        collapsible: true,
                        active: false,
                        icons: {
                            "header": "fa fa-chevron-down",
                            "activeHeader": "fa fa-chevron-up"
                        }
                    });

                    recommendedRulesEl.accordion({
                        collapsible: true,
                        active: false,
                        icons: {
                            "header": "fa fa-chevron-down",
                            "activeHeader": "fa fa-chevron-up"
                        }
                    });
                    $('#rule-menu').hide();
                    $('#violation-menu').hide();
                    $('#rule-menu-more-info-dialog').modal({
                        scrollableContentStyle: 'scrollable',
                        headerText: 'Rule Information',
                        buttons: [{
                            buttonText: 'OK',
                                color: {
                                    base: '#728E9B',
                                    hover: '#004849',
                                    text: '#ffffff'
                                },
                            handler: {
                                click: function () {
                                    $(this).modal('hide');
                                }
                            }
                        }],
                        handler: {
                            close: function () {}
                        }
                    });
                    $('#violation-menu-more-info-dialog').modal({
                        scrollableContentStyle: 'scrollable',
                        headerText: 'Violation Information',
                        buttons: [{
                            buttonText: 'OK',
                                color: {
                                    base: '#728E9B',
                                    hover: '#004849',
                                    text: '#ffffff'
                                },
                            handler: {
                                click: function () {
                                    $(this).modal('hide');
                                }
                            }
                        }],
                        handler: {
                            close: function () {}
                        }
                    });

                    this.loadFlowPolicies();
                    setInterval(this.loadFlowPolicies.bind(this), flowAnalysisRefreshIntervalSeconds * 1000);
                    
                    this.toggleOnlyViolations(false);
                    this.toggleOnlyWarnings(false);
                    // handle show only violations checkbox
                    $('#show-only-violations').on('change', function(event) {
                        var isChecked = $(this).hasClass('checkbox-checked');
                        flowAnalysisCtrl.toggleOnlyViolations(isChecked);
                    });

                    $('#show-only-warnings').on('change', function(event) {
                        var isChecked = $(this).hasClass('checkbox-checked');
                        flowAnalysisCtrl.toggleOnlyWarnings(isChecked);
                    });
                },

                /**
                 * Show/hide violations menu
                 */
                toggleOnlyViolations: function(isViolationsChecked) {
                    var requiredRulesEl = $('#required-rules');
                    var recommendedRulesEl = $('#recommended-rules');
                    var ruleViolationsEl = $('#rule-violations');

                    var isWarningsChecked = $('#show-only-warnings').hasClass(
                      'checkbox-checked'
                    );

                    isViolationsChecked
                      ? ruleViolationsEl.show()
                      : ruleViolationsEl.hide();
                    if (isViolationsChecked || isWarningsChecked) {
                      requiredRulesEl.hide();
                      recommendedRulesEl.hide();
                    } else {
                      requiredRulesEl.show();
                      recommendedRulesEl.show();
                    }
                    this.loadFlowPolicies();
                },

                /**
                 * Show/hide warnings menu
                 */
                toggleOnlyWarnings: function (isWarningsChecked) {
                    var requiredRulesEl = $('#required-rules');
                    var recommendedRulesEl = $('#recommended-rules');
                    var ruleWarningsEl = $('#rule-warnings');
                    var isViolationsChecked = $('#show-only-violations').hasClass(
                      'checkbox-checked'
                    );
    
                    isWarningsChecked
                      ? ruleWarningsEl.show()
                      : ruleWarningsEl.hide();
                    if (isWarningsChecked || isViolationsChecked) {
                      requiredRulesEl.hide();
                      recommendedRulesEl.hide();
                    } else {
                      requiredRulesEl.show();
                      recommendedRulesEl.show();
                    }
                    this.loadFlowPolicies();
                  },
            }

            /**
             * The bulletins controller.
             */
            this.bulletins = {

                /**
                 * Update the bulletins.
                 *
                 * @param response  The controller bulletins returned from the `../nifi-api/controller/bulletins` endpoint.
                 */
                update: function (response) {

                    // icon for system bulletins
                    var bulletinIcon = $('#bulletin-button');
                    var currentBulletins = bulletinIcon.data('bulletins');

                    // update the bulletins if necessary
                    if (nfCommon.doBulletinsDiffer(currentBulletins, response.bulletins)) {
                        bulletinIcon.data('bulletins', response.bulletins);

                        // get the formatted the bulletins
                        var bulletins = nfCommon.getFormattedBulletins(response.bulletins);

                        // bulletins for this processor are now gone
                        if (bulletins.length === 0) {
                            if (bulletinIcon.data('qtip')) {
                                bulletinIcon.removeClass('has-bulletins').qtip('api').destroy(true);
                            }
                        } else {
                            var newBulletins = nfCommon.formatUnorderedList(bulletins);

                            // different bulletins, refresh
                            if (bulletinIcon.data('qtip')) {
                                bulletinIcon.qtip('option', 'content.text', newBulletins);
                            } else {
                                // no bulletins before, show icon and tips
                                bulletinIcon.addClass('has-bulletins').qtip($.extend({},
                                    nfCanvasUtils.config.systemTooltipConfig,
                                    {
                                        content: newBulletins,
                                        position: {
                                            at: 'bottom left',
                                            my: 'top right',
                                            adjust: {
                                                x: 4
                                            }
                                        }
                                    }
                                ));
                            }
                        }
                    }

                    // update controller service and reporting task bulletins
                    nfSettings.setBulletins(response.controllerServiceBulletins, response.reportingTaskBulletins);
                }

            }
        }

        FlowStatusCtrl.prototype = {
            constructor: FlowStatusCtrl,

            /**
             * Initialize the flow status controller.
             */
            init: function () {
                this.search.init();
                this.flowAnalysis.init();
            },

            /**
             * Reloads the current status of the flow.
             */
            reloadFlowStatus: function () {
                var flowStatusCtrl = this;

                return $.ajax({
                    type: 'GET',
                    url: config.urls.status,
                    dataType: 'json'
                }).done(function (response) {
                    // report the updated status
                    if (nfCommon.isDefinedAndNotNull(response.controllerStatus)) {
                        flowStatusCtrl.update(response.controllerStatus);
                    }
                }).fail(nfErrorHandler.handleAjaxError);
            },

            /**
             * Updates the cluster summary.
             *
             * @param summary
             */
            updateClusterSummary: function (summary) {
                // update the connection state
                if (summary.connectedToCluster) {
                    var connectedNodes = summary.connectedNodes.split(' / ');
                    if (connectedNodes.length === 2 && connectedNodes[0] !== connectedNodes[1]) {
                        this.clusterConnectionWarning = true;
                    } else {
                        this.clusterConnectionWarning = false;
                    }
                    this.connectedNodesCount = summary.connectedNodes;
                } else {
                    this.connectedNodesCount = 'Disconnected';
                }
            },

            /**
             * Returns whether there are any terminated threads.
             *
             * @returns {boolean} whether there are any terminated threads
             */
            hasTerminatedThreads: function () {
                if (Number.isInteger(this.terminatedThreadCount)) {
                    return this.terminatedThreadCount > 0;
                } else {
                    return false;
                }
            },

            /**
             * Returns any additional styles to apply to the thread counts.
             *
             * @returns {string}
             */
            getExtraThreadStyles: function () {
                if (Number.isInteger(this.terminatedThreadCount) && this.terminatedThreadCount > 0) {
                    return 'warning';
                } else if (this.activeThreadCount === 0) {
                    return 'zero';
                }

                return '';
            },

            /**
             * Returns any additional styles to apply to the cluster label.
             *
             * @returns {string}
             */
            getExtraClusterStyles: function () {
                if (this.connectedNodesCount === 'Disconnected' || this.clusterConnectionWarning === true) {
                    return 'warning';
                }

                return '';
            },

            /**
             * Update the flow status counts.
             *
             * @param status  The controller status returned from the `../nifi-api/flow/status` endpoint.
             */
            update: function (status) {
                // update the report values
                this.activeThreadCount = status.activeThreadCount;
                this.terminatedThreadCount = status.terminatedThreadCount;

                if (this.hasTerminatedThreads()) {
                    this.threadCounts = this.activeThreadCount + ' (' + this.terminatedThreadCount + ')';
                } else {
                    this.threadCounts = this.activeThreadCount;
                }

                this.totalQueued = status.queued;

                if (this.totalQueued.indexOf('0 / 0') >= 0) {
                    $('#flow-status-container').find('.fa-list').addClass('zero');
                } else {
                    $('#flow-status-container').find('.fa-list').removeClass('zero');
                }

                // update the component counts
                this.controllerTransmittingCount =
                    nfCommon.isDefinedAndNotNull(status.activeRemotePortCount) ?
                        status.activeRemotePortCount : '-';

                if (this.controllerTransmittingCount > 0) {
                    $('#flow-status-container').find('.fa-bullseye').removeClass('zero').addClass('transmitting');
                } else {
                    $('#flow-status-container').find('.fa-bullseye').removeClass('transmitting').addClass('zero');
                }

                this.controllerNotTransmittingCount =
                    nfCommon.isDefinedAndNotNull(status.inactiveRemotePortCount) ?
                        status.inactiveRemotePortCount : '-';

                if (this.controllerNotTransmittingCount > 0) {
                    $('#flow-status-container').find('.icon-transmit-false').removeClass('zero').addClass('not-transmitting');
                } else {
                    $('#flow-status-container').find('.icon-transmit-false').removeClass('not-transmitting').addClass('zero');
                }

                this.controllerRunningCount =
                    nfCommon.isDefinedAndNotNull(status.runningCount) ? status.runningCount : '-';

                if (this.controllerRunningCount > 0) {
                    $('#flow-status-container').find('.fa-play').removeClass('zero').addClass('running');
                } else {
                    $('#flow-status-container').find('.fa-play').removeClass('running').addClass('zero');
                }

                this.controllerStoppedCount =
                    nfCommon.isDefinedAndNotNull(status.stoppedCount) ? status.stoppedCount : '-';

                if (this.controllerStoppedCount > 0) {
                    $('#flow-status-container').find('.fa-stop').removeClass('zero').addClass('stopped');
                } else {
                    $('#flow-status-container').find('.fa-stop').removeClass('stopped').addClass('zero');
                }

                this.controllerInvalidCount =
                    nfCommon.isDefinedAndNotNull(status.invalidCount) ? status.invalidCount : '-';

                if (this.controllerInvalidCount > 0) {
                    $('#flow-status-container').find('.fa-warning').removeClass('zero').addClass('invalid');
                } else {
                    $('#flow-status-container').find('.fa-warning').removeClass('invalid').addClass('zero');
                }

                this.controllerDisabledCount =
                    nfCommon.isDefinedAndNotNull(status.disabledCount) ? status.disabledCount : '-';

                if (this.controllerDisabledCount > 0) {
                    $('#flow-status-container').find('.icon-enable-false').removeClass('zero').addClass('disabled');
                } else {
                    $('#flow-status-container').find('.icon-enable-false').removeClass('disabled').addClass('zero');
                }

                this.controllerUpToDateCount =
                    nfCommon.isDefinedAndNotNull(status.upToDateCount) ? status.upToDateCount : '-';

                if (this.controllerUpToDateCount > 0) {
                    $('#flow-status-container').find('.fa-check').removeClass('zero').addClass('up-to-date');
                } else {
                    $('#flow-status-container').find('.fa-check').removeClass('up-to-date').addClass('zero');
                }

                this.controllerLocallyModifiedCount =
                    nfCommon.isDefinedAndNotNull(status.locallyModifiedCount) ? status.locallyModifiedCount : '-';

                if (this.controllerLocallyModifiedCount > 0) {
                    $('#flow-status-container').find('.fa-asterisk').removeClass('zero').addClass('locally-modified');
                } else {
                    $('#flow-status-container').find('.fa-asterisk').removeClass('locally-modified').addClass('zero');
                }

                this.controllerStaleCount =
                    nfCommon.isDefinedAndNotNull(status.staleCount) ? status.staleCount : '-';

                if (this.controllerStaleCount > 0) {
                    $('#flow-status-container').find('.fa-arrow-circle-up').removeClass('zero').addClass('stale');
                } else {
                    $('#flow-status-container').find('.fa-arrow-circle-up').removeClass('stale').addClass('zero');
                }

                this.controllerLocallyModifiedAndStaleCount =
                    nfCommon.isDefinedAndNotNull(status.locallyModifiedAndStaleCount) ? status.locallyModifiedAndStaleCount : '-';

                if (this.controllerLocallyModifiedAndStaleCount > 0) {
                    $('#flow-status-container').find('.fa-exclamation-circle').removeClass('zero').addClass('locally-modified-and-stale');
                } else {
                    $('#flow-status-container').find('.fa-exclamation-circle').removeClass('locally-modified-and-stale').addClass('zero');
                }

                this.controllerSyncFailureCount =
                    nfCommon.isDefinedAndNotNull(status.syncFailureCount) ? status.syncFailureCount : '-';

                if (this.controllerSyncFailureCount > 0) {
                    $('#flow-status-container').find('.fa-question').removeClass('zero').addClass('sync-failure');
                } else {
                    $('#flow-status-container').find('.fa-question').removeClass('sync-failure').addClass('zero');
                }

            },

            /**
             * Updates the controller level bulletins
             *
             * @param response
             */
            updateBulletins: function (response) {
                this.bulletins.update(response);
            },

            /**
             * Reloads flow analysis rules
             *
             */
            reloadFlowPolicies: function () {
                this.flowAnalysis.loadFlowPolicies();
            }
        }

        var flowStatusCtrl = new FlowStatusCtrl();
        return flowStatusCtrl;
    };
}));