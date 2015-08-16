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

/* global Slick */

$(document).ready(function () {
    ua.editable = $('#attribute-updater-editable').text() === 'true';
    ua.init();
});

var ua = {
    searchRules: 'Search rule name',
    newRuleIndex: 0,
    editable: false,
    
    /**
     * Initializes this web application.
     * 
     * @returns {undefined}
     */
    init: function () {

        // configure the dialogs
        ua.initNewRuleDialog();
        ua.initNewConditionDialog();
        ua.initNewActionDialog();
        ua.initOkDialog();
        ua.initYesNoDialog();

        // configure the grids
        var conditionsGrid = ua.initConditionsGrid();
        var actionsGrid = ua.initActionsGrid();

        // enable grid resizing
        $(window).resize(function (e) {
            if (e.target === window) {
                conditionsGrid.resizeCanvas();
                actionsGrid.resizeCanvas();
                ua.resizeSelectedRuleNameField();
            }
        });

        // initialize the rule list
        ua.initRuleList();

        // button click for new rules
        $('#new-rule').on('click', function () {
            $('#new-rule-dialog').modal('show');
            $('#new-rule-name').focus();
        });

        // button click for new conditions/actions
        $('#new-condition').on('click', function () {
            var ruleId = $('#selected-rule-id').text();

            if (ruleId === '') {
                $('#ok-dialog-content').text('No rule is selected.');
                $('#ok-dialog').modal('setHeaderText', 'Configuration Error').modal('show');
            } else {
                // clear the current content
                $('#new-condition-expression').nfeditor('setValue', '');

                // show the dialog
                $('#new-condition-dialog').center().show();
                $('#new-condition-expression').nfeditor('refresh').nfeditor('focus');
            }
        });
        $('#new-action').on('click', function () {
            var ruleId = $('#selected-rule-id').text();

            if (ruleId === '') {
                $('#ok-dialog-content').text('No rule is selected.');
                $('#ok-dialog').modal('setHeaderText', 'Configuration Error').modal('show');
            } else {
                // clear the current content
                $('#new-action-attribute').val('');
                $('#new-action-value').nfeditor('setValue', '');

                // show the dialog
                $('#new-action-dialog').center().show();
                $('#new-action-attribute').focus();
                $('#new-action-value').nfeditor('refresh');
            }
        });

        // handle rule name changes
        $('#selected-rule-name').on('blur', function () {
            var ruleName = $(this).val();
            var ruleItem = $('#rule-list').children('li.selected');
            var rule = ruleItem.data('rule');
            if (rule.name !== ruleName) {
                ruleItem.addClass('unsaved');
            }
        }).on('focus', function () {
            if (ua.editable) {
                // commit any condition edits
                var conditionsEditController = conditionsGrid.getEditController();
                conditionsEditController.commitCurrentEdit();

                // commit any action edits
                var actionsEditController = actionsGrid.getEditController();
                actionsEditController.commitCurrentEdit();
            } else {
                ua.removeAllDetailDialogs();
            }
        });

        // add the handler for saving the rules
        $('#selected-rule-save').on('click', function () {
            ua.saveSelectedRule();
        });

        // define the function for filtering the list
        $('#rule-filter').keyup(function () {
            ua.applyRuleFilter();
        }).focus(function () {
            if ($(this).hasClass('rule-filter-list')) {
                $(this).removeClass('rule-filter-list').val('');
            }
        }).blur(function () {
            if ($(this).val() === '') {
                $(this).addClass('rule-filter-list').val('Filter');
            }
        }).addClass('rule-filter-list').val('Filter');

        // filter type
        $('#rule-filter-type').combo({
            options: [{
                    text: 'by name',
                    value: 'name'
                }, {
                    text: 'by condition',
                    value: 'condition'
                }, {
                    text: 'by action',
                    value: 'action'
                }],
            select: function (option) {
                ua.applyRuleFilter();
            }
        });

        // show the save button if appropriate
        if (ua.editable) {
            $('#selected-rule-save').show();
            $('#new-rule').show();
            $('#new-condition').show();
            $('#new-action').show();

            // make the combo for the flow file policy
            $('#flowfile-policy').combo({
                options: [{
                        text: 'use clone',
                        value: 'USE_CLONE',
                        description: 'Matching rules are executed with a copy of the original flowfile.'
                    }, {
                        text: 'use original',
                        value: 'USE_ORIGINAL',
                        description: 'Matching rules are executed with the original flowfile in the order specified below.'
                    }],
                select: function (selectedOption) {
                    var selectedFlowFilePolicy = $('#selected-flowfile-policy').text();

                    // only consider saving the new flowfile policy when appropriate
                    if (selectedFlowFilePolicy !== '' && selectedFlowFilePolicy !== selectedOption.value) {
                        var entity = {
                            processorId: ua.getProcessorId(),
                            revision: ua.getRevision(),
                            clientId: ua.getClientId(),
                            flowFilePolicy: selectedOption.value
                        };

                        $.ajax({
                            type: 'PUT',
                            url: 'api/criteria/evaluation-context',
                            data: JSON.stringify(entity),
                            processData: false,
                            contentType: 'application/json'
                        }).then(function (evaluationContext) {
                            ua.showMessage('FlowFile Policy saved as "' + selectedOption.text + '".');

                            // record the newly selected value
                            $('#selected-flowfile-policy').text(evaluationContext.flowFilePolicy);
                        }, function (xhr, status, error) {
                            // show an error message
                            $('#ok-dialog-content').text('Unable to save new FlowFile Policy due to:' + xhr.responseText);
                            $('#ok-dialog').modal('setHeaderText', 'Error').modal('show');
                        });
                    }
                }
            });
        } else {
            // make the rule name read only when not editable
            $('#selected-rule-name').attr('readonly', 'readonly');

            // make the combo for the flow file policy
            $('#flowfile-policy').combo({
                options: [{
                        text: 'use clone',
                        value: 'USE_CLONE',
                        description: 'Matching rules are executed with a copy of the original flowfile.',
                        disabled: true
                    }, {
                        text: 'use original',
                        value: 'USE_ORIGINAL',
                        description: 'Matching rules are executed with the original flowfile in the order specified below.',
                        disabled: true
                    }]
            });
        }

        // add styles for buttons
        ua.addHoverEffect('div.button', 'button-normal', 'button-over');

        // load the rules
        ua.loadRuleList();

        // potentionally resize the name field
        ua.resizeSelectedRuleNameField();

        // initialize the tooltips
        $('img.icon-info').qtip({
            style: {
                classes: 'ui-tooltip-tipped ui-tooltip-shadow'
            },
            show: {
                solo: true,
                effect: false
            },
            hide: {
                effect: false
            },
            position: {
                at: 'bottom right',
                my: 'top left'
            }
        });
    },
    
    /**
     * Initializes the new rule dialog.
     * 
     * @returns {undefined}
     */
    initNewRuleDialog: function () {
        // new rule dialog configuration
        $('#new-rule-dialog').modal({
            headerText: 'New Rule',
            overlayBackground: false,
            buttons: [{
                    buttonText: 'Add',
                    handler: {
                        click: function () {
                            var ruleName = $('#new-rule-name').val();
                            var copyFromRuleField = $('#copy-from-rule-name');
                            var copyFromRuleAutoComplete = copyFromRuleField.data('copy-from-rule');
                            var copyFromRuleName = copyFromRuleField.val();

                            $.Deferred(function (deferred) {
                                // rule name must be specified
                                if (ruleName === '') {
                                    deferred.rejectWith(this, ['The rule name must be specified.']);
                                    return;
                                }

                                // use the rule from autocomplete if present
                                if (typeof copyFromRuleAutoComplete !== 'undefined' && copyFromRuleAutoComplete !== null) {
                                    deferred.resolveWith(this, [copyFromRuleAutoComplete]);
                                    return;
                                }

                                // use no existing rule
                                if (copyFromRuleField.hasClass('search') || copyFromRuleName === '') {
                                    deferred.resolve();
                                    return;
                                }

                                // query to get the details on the specified rule
                                $.ajax({
                                    type: 'GET',
                                    data: {
                                        processorId: ua.getProcessorId(),
                                        q: copyFromRuleName
                                    },
                                    dataType: 'json',
                                    url: 'api/criteria/rules/search-results'
                                }).then(function (response) {
                                    var rules = response.rules;

                                    if ($.isArray(rules)) {
                                        if (rules.length > 1) {
                                            deferred.rejectWith(this, ['Unable to copy existing rule. Multiple rules match the name "' + copyFromRuleName + '".']);
                                        } else if (rules.length === 0 || rules[0].name !== copyFromRuleName) {
                                            deferred.rejectWith(this, ['Unable to copy existing rule. Specified rule "' + copyFromRuleName + '" does not exist.']);
                                        } else {
                                            deferred.resolveWith(this, [rules[0]]);
                                        }
                                    }

                                }, function (xhr, status, error) {
                                    deferred.rejectWith(this, [xhr.responseText]);
                                });
                            }).then(function (copyFromRule) {
                                // add the new rule
                                var ruleElement = ua.createNewRuleItem({
                                    id: 'unsaved-rule-' + (ua.newRuleIndex++),
                                    name: ruleName
                                });

                                // select the new rule
                                ruleElement.click();

                                // if we are copying from another rule load the details
                                if (typeof copyFromRule !== 'undefined' && copyFromRule !== null) {
                                    var conditionsGrid = $('#selected-rule-conditions').data('gridInstance');
                                    var conditionsData = conditionsGrid.getData();
                                    conditionsData.setItems(copyFromRule.conditions);

                                    var actionsGrid = $('#selected-rule-actions').data('gridInstance');
                                    var actionsData = actionsGrid.getData();
                                    actionsData.setItems(copyFromRule.actions);
                                }

                                // mark the rule as modified
                                $('#rule-list').children('li.selected').addClass('unsaved');

                                // ensure the rule list is visible
                                if ($('#no-rules').is(':visible')) {
                                    ua.showRuleList();
                                } else {
                                    // re-apply the rule filter
                                    ua.applyRuleFilter();
                                }
                            }, function (error) {
                                $('#ok-dialog-content').text(error);
                                $('#ok-dialog').modal('setHeaderText', 'Configuration Error').modal('show');
                            });

                            // close the dialog
                            $('#new-rule-dialog').modal('hide');
                        }
                    }
                }, {
                    buttonText: 'Cancel',
                    handler: {
                        click: function () {
                            // close the dialog
                            $('#new-rule-dialog').modal('hide');
                        }
                    }
                }],
            handler: {
                close: function () {
                    // reset the content
                    $('#new-rule-name').val('');
                    $('#copy-from-rule-name').removeData('copy-from-rule').addClass('search').val(ua.searchRules);
                }
            }
        });

        // initialize the access control auto complete
        $.widget('nf.copyFromRuleAutocomplete', $.ui.autocomplete, {
            _normalize: function(searchResults) {
                var items = [];
                items.push(searchResults);
                return items;
            },
            _resizeMenu: function () {
                var ul = this.menu.element;
                ul.width(374);
            },
            _renderMenu: function (ul, items) {
                var self = this;

                // the object that holds the search results is normalized into a single element array
                var searchResults = items[0];

                if ($.isArray(searchResults.rules) && searchResults.rules.length > 0) {
                    // go through each matching rule
                    $.each(searchResults.rules, function (i, rule) {
                        // add the user match
                        self._renderRule(ul, rule);
                    });
                }

                // ensure there were some results
                if (ul.children().length === 0) {
                    ul.append('<li class="unset" style="padding: 0.2em 0.4em;"><div>No rules match</div></li>');
                }
            },
            _renderRule: function (ul, ruleMatch) {
                var ruleContent = $('<a></a>').append($('<div></div>').text(ruleMatch.name));
                return $('<li style="height: 20px;"></li>').data('ui-autocomplete-item', ruleMatch).append(ruleContent).appendTo(ul);
            }
        });

        // configure the autocomplete field
        $('#copy-from-rule-name').copyFromRuleAutocomplete({
            minLength: 0,
            appendTo: '#update-attributes-content',
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
                        processorId: ua.getProcessorId(),
                        q: request.term
                    },
                    dataType: 'json',
                    url: 'api/criteria/rules/search-results'
                }).done(function (searchResponse) {
                    response(searchResponse);
                });
            },
            select: function (event, ui) {
                var rule = ui.item;

                // store the selected rule
                $(this).data('copy-from-rule', rule).val(rule.name);

                // stop event propagation
                return false;
            }
        }).focus(function () {
            // conditionally clear the text for the user to type
            if ($(this).val() === ua.searchRules) {
                $(this).val('').removeClass('search');
            }
        }).val(ua.searchRules).addClass('search');
    },
    
    /**
     * Initializes the new condition dialog.
     */
    initNewConditionDialog: function () {
        var languageId = 'nfel';
        var editorClass = languageId + '-editor';

        var add = function () {
            var conditionExpression = $('#new-condition-expression').nfeditor('getValue');

            // ensure the appropriate details have been specified
            if (conditionExpression === '') {
                $('#ok-dialog-content').text('Expression is required.');
                $('#ok-dialog').modal('setHeaderText', 'Configuration Error').modal('show');
            } else {
                var condition = {
                    expression: conditionExpression
                };

                var entity = {
                    processorId: ua.getProcessorId(),
                    revision: ua.getRevision(),
                    clientId: ua.getClientId(),
                    condition: condition
                };

                // create the condition
                $.ajax({
                    type: 'POST',
                    url: 'api/criteria/rules/conditions',
                    data: JSON.stringify(entity),
                    processData: false,
                    contentType: 'application/json'
                }).then(function (response) {
                    var conditionsGrid = $('#selected-rule-conditions').data('gridInstance');
                    var conditionsData = conditionsGrid.getData();

                    // add the new condition
                    conditionsData.addItem(response.condition);

                    // mark the selected rule as unsaved
                    $('#rule-list').children('li.selected').addClass('unsaved');

                    // only hide the the dialog once the condition has been validated
                    $('#new-condition-dialog').hide();
                }, function (xhr, status, error) {
                    $('#ok-dialog-content').text(xhr.responseText);
                    $('#ok-dialog').modal('setHeaderText', 'Configuration Error').modal('show');
                });
            }
        };

        var cancel = function () {
            // close the dialog
            $('#new-condition-dialog').hide();
        };

        // create the editor
        $('#new-condition-expression').addClass(editorClass).nfeditor({
            languageId: languageId,
            width: 374,
            minWidth: 374,
            height: 135,
            minHeight: 135,
            resizable: true,
            escape: cancel,
            enter: add
        });

        // new condition dialog configuration
        $('#new-condition-dialog').draggable({
            cancel: 'input, textarea, pre, .button, .' + editorClass,
            containment: 'parent'
        }).on('click', '#new-condition-add', add).on('click', '#new-condition-cancel', cancel);
    },
    
    /**
     * Initializes the new action dialog.
     */
    initNewActionDialog: function () {
        var languageId = 'nfel';
        var editorClass = languageId + '-editor';

        var add = function () {
            var actionAttribute = $('#new-action-attribute').val();
            var actionValue = $('#new-action-value').nfeditor('getValue');

            // ensure the appropriate details have been specified
            if (actionAttribute === '') {
                $('#ok-dialog-content').text('Attribute name is required.');
                $('#ok-dialog').modal('setHeaderText', 'Configuration Error').modal('show');
            } else {
                var action = {
                    attribute: actionAttribute,
                    value: actionValue
                };

                var entity = {
                    processorId: ua.getProcessorId(),
                    revision: ua.getRevision(),
                    clientId: ua.getClientId(),
                    action: action
                };

                // create the condition
                $.ajax({
                    type: 'POST',
                    url: 'api/criteria/rules/actions',
                    data: JSON.stringify(entity),
                    processData: false,
                    contentType: 'application/json'
                }).then(function (response) {
                    var actionsGrid = $('#selected-rule-actions').data('gridInstance');
                    var actionsData = actionsGrid.getData();

                    // add the new condition
                    actionsData.addItem(response.action);

                    // mark the selected rule as unsaved
                    $('#rule-list').children('li.selected').addClass('unsaved');

                    // only close the dialog once the action has been validated
                    $('#new-action-dialog').hide();
                }, function (xhr, status, error) {
                    $('#ok-dialog-content').text(xhr.responseText);
                    $('#ok-dialog').modal('setHeaderText', 'Configuration Error').modal('show');
                });
            }
        };

        var cancel = function () {
            // close the dialog
            $('#new-action-dialog').hide();
        };

        // create the editor
        $('#new-action-value').addClass(editorClass).nfeditor({
            languageId: languageId,
            width: 374,
            minWidth: 374,
            height: 135,
            minHeight: 135,
            resizable: true,
            escape: cancel,
            enter: add
        });

        // configuration the dialog
        $('#new-action-dialog').draggable({
            cancel: 'input, textarea, pre, .button, .' + editorClass,
            containment: 'parent'
        }).on('click', '#new-action-add', add).on('click', '#new-action-cancel', cancel);

        // enable tabs in the property value
        $('#new-action-attribute').on('keydown', function (e) {
            if (e.which === $.ui.keyCode.ENTER && !e.shiftKey) {
                add();
            } else if (e.which === $.ui.keyCode.ESCAPE) {
                e.preventDefault();
                cancel();
            }
        });
    },
    
    /**
     * Configure the ok dialog.
     * 
     * @returns {undefined}
     */
    initOkDialog: function () {
        $('#ok-dialog').modal({
            overlayBackground: false,
            buttons: [{
                    buttonText: 'Ok',
                    handler: {
                        click: function () {
                            // close the dialog
                            $('#ok-dialog').modal('hide');
                        }
                    }
                }],
            handler: {
                close: function () {
                    // clear the content
                    $('#ok-dialog-content').empty();
                }
            }
        });
    },
    
    /**
     * Configure the yes no dialog.
     * 
     * @returns {undefined}
     */
    initYesNoDialog: function () {
        $('#yes-no-dialog').modal({
            overlayBackground: false
        });
    },
    
    /**
     * Initializes the conditions grid.
     * 
     * @returns {undefined}
     */
    initConditionsGrid: function () {
        // custom formatter for the actions column
        var conditionsActionFormatter = function (row, cell, value, columnDef, dataContext) {
            return '<img src="images/iconDelete.png" title="Delete" class="pointer" style="margin-top: 2px" onclick="javascript:ua.deleteRow(\'#selected-rule-conditions\', \'' + row + '\');"/>';
        };

        // initialize the conditions grid
        var conditionsColumns = [
            {id: "expression", name: "Expression", field: "expression", sortable: true, cssClass: 'pointer', editor: ua.getNfelEditor, validator: ua.requiredFieldValidator}
        ];

        var conditionsOptions = {
            forceFitColumns: true,
            enableCellNavigation: true,
            enableColumnReorder: false,
            enableAddRow: false,
            autoEdit: false
        };

        if (ua.editable) {
            // add the delete column
            conditionsColumns.push({id: "actions", name: "&nbsp;", formatter: conditionsActionFormatter, resizable: false, sortable: true, width: 50, maxWidth: 50});

            // make the table editable
            conditionsOptions = $.extend({
                editable: true
            }, conditionsOptions);
        }

        // initialize the dataview
        var conditionsData = new Slick.Data.DataView({inlineFilters: false});
        conditionsData.setItems([]);

        // initialize the grid
        var conditionsGrid = new Slick.Grid('#selected-rule-conditions', conditionsData, conditionsColumns, conditionsOptions);
        conditionsGrid.setSelectionModel(new Slick.RowSelectionModel());
        conditionsGrid.onBeforeEditCell.subscribe(function (e, args) {
            return !ua.preventEdit(e, args);
        });
        conditionsGrid.onCellChange.subscribe(function (e, args) {
            $('#rule-list').children('li.selected').addClass('unsaved');
        });
        conditionsGrid.onSort.subscribe(function (e, args) {
            var comparer = function (a, b) {
                return a[args.sortCol.field] > b[args.sortCol.field];
            };
            conditionsData.sort(comparer, args.sortAsc);
        });
        conditionsGrid.onClick.subscribe(function (e, args) {
            if (ua.editable) {
                // edits the clicked cell
                conditionsGrid.gotoCell(args.row, args.cell, true);
            } else {
                ua.showValue(conditionsGrid, args.row, args.cell);
            }

            // prevents standard edit logic
            e.stopImmediatePropagation();
        });

        // wire up the dataview to the grid
        conditionsData.onRowCountChanged.subscribe(function (e, args) {
            conditionsGrid.updateRowCount();
            conditionsGrid.render();
        });
        conditionsData.onRowsChanged.subscribe(function (e, args) {
            conditionsGrid.invalidateRows(args.rows);
            conditionsGrid.render();
        });

        // hold onto an instance of the grid
        $('#selected-rule-conditions').data('gridInstance', conditionsGrid);
        return conditionsGrid;
    },
    
    /**
     * Initializes the actions grid.
     */
    initActionsGrid: function () {
        // custom formatter for the actions column
        var actionsActionFormatter = function (row, cell, value, columnDef, dataContext) {
            return '<img src="images/iconDelete.png" title="Delete" class="pointer" style="margin-top: 2px" onclick="javascript:ua.deleteRow(\'#selected-rule-actions\', \'' + row + '\');"/>';
        };

        // initialize the actions grid
        var actionsColumns = [
            {id: "attribute", name: "Attribute", field: "attribute", sortable: true, cssClass: 'pointer', editor: ua.getCustomLongTextEditor, validator: ua.requiredFieldValidator},
            {id: "value", name: "Value", field: "value", sortable: true, cssClass: 'pointer', editor: ua.getNfelEditor, validator: ua.requiredFieldValidator}
        ];
        var actionsOptions = {
            forceFitColumns: true,
            enableCellNavigation: true,
            enableColumnReorder: false,
            enableAddRow: false,
            autoEdit: false
        };

        if (ua.editable) {
            // add the delete column
            actionsColumns.push({id: "actions", name: "&nbsp;", formatter: actionsActionFormatter, resizable: false, sortable: true, width: 50, maxWidth: 50});

            // make the table editable
            actionsOptions = $.extend({
                editable: true
            }, actionsOptions);
        }

        // initialize the dataview
        var actionsData = new Slick.Data.DataView({inlineFilters: false});
        actionsData.setItems([]);

        // initialize the grid
        var actionsGrid = new Slick.Grid('#selected-rule-actions', actionsData, actionsColumns, actionsOptions);
        actionsGrid.setSelectionModel(new Slick.RowSelectionModel());
        actionsGrid.onBeforeEditCell.subscribe(function (e, args) {
            return !ua.preventEdit(e, args);
        });
        actionsGrid.onCellChange.subscribe(function (e, args) {
            $('#rule-list').children('li.selected').addClass('unsaved');
        });
        actionsGrid.onSort.subscribe(function (e, args) {
            var comparer = function (a, b) {
                return a[args.sortCol.field] > b[args.sortCol.field];
            };
            actionsData.sort(comparer, args.sortAsc);
        });
        actionsGrid.onClick.subscribe(function (e, args) {
            if (ua.editable) {
                // edits the clicked cell
                actionsGrid.gotoCell(args.row, args.cell, true);
            } else {
                ua.showValue(actionsGrid, args.row, args.cell);
            }

            // prevents standard edit logic
            e.stopImmediatePropagation();
        });

        // wire up the dataview to the grid
        actionsData.onRowCountChanged.subscribe(function (e, args) {
            actionsGrid.updateRowCount();
            actionsGrid.render();
        });
        actionsData.onRowsChanged.subscribe(function (e, args) {
            actionsGrid.invalidateRows(args.rows);
            actionsGrid.render();
        });

        // hold onto an instance of the grid
        $('#selected-rule-actions').data('gridInstance', actionsGrid);
        return actionsGrid;
    },
    
    /**
     * Initializes the rule list.
     * 
     * @returns {undefined}
     */
    initRuleList: function () {
        var conditionsGrid = $('#selected-rule-conditions').data('gridInstance');
        var actionsGrid = $('#selected-rule-actions').data('gridInstance');

        // handle rule clicks
        var ruleList = $('#rule-list');

        // conditionally support reordering
        if (ua.editable) {
            // make the list sortable
            ruleList.sortable({
                helper: 'clone',
                cancel: 'li.unsaved',
                update: function (event, ui) {
                    // attempt save the rule order
                    ua.saveRuleOrder().fail(function () {
                        // and if it fails, revert
                        ruleList.sortable('cancel');
                    });
                }
            });
        }

        ruleList.on('click', 'li', function () {
            var li = $(this);
            var rule = li.data('rule');

            // get the currently selected rule to see if a change is necessary
            var currentlySelectedRuleItem = ruleList.children('li.selected');

            // dont do anything if this is already selected
            if (li.attr('id') === currentlySelectedRuleItem.attr('id')) {
                return;
            }

            // handle saving if necessary
            var saveModifiedRule = $.Deferred(function (deferred) {
                if (currentlySelectedRuleItem.length) {
                    // if we're editable, ensure any active edits are committed before continuing
                    if (ua.editable) {
                        // commit any condition edits
                        var conditionsEditController = conditionsGrid.getEditController();
                        conditionsEditController.commitCurrentEdit();

                        // commit any action edits
                        var actionsEditController = actionsGrid.getEditController();
                        actionsEditController.commitCurrentEdit();
                    }

                    // get the currently selected rule
                    var currentlySelectedRule = currentlySelectedRuleItem.data('rule');

                    // determine if the currently selected rule has been modified
                    if (currentlySelectedRuleItem.hasClass('unsaved')) {
                        $('#yes-no-dialog-content').text('Rule \'' + currentlySelectedRule.name + '\' has unsaved changes. Do you want to save?');
                        $('#yes-no-dialog').modal('setHeaderText', 'Save Changes').modal('setButtonModel', [{
                                buttonText: 'Yes',
                                handler: {
                                    click: function () {
                                        // close the dialog
                                        $('#yes-no-dialog').modal('hide');

                                        // save the previous rule
                                        ua.saveSelectedRule().then(function () {
                                            deferred.resolve();
                                        }, function () {
                                            deferred.reject();
                                        });
                                    }
                                }
                            }, {
                                buttonText: 'No',
                                handler: {
                                    click: function () {
                                        // close the dialog
                                        $('#yes-no-dialog').modal('hide');

                                        // since changes are being discarded, remove the modified indicator when the rule has been previously saved
                                        if (currentlySelectedRuleItem.attr('id').indexOf('unsaved-rule') === -1) {
                                            currentlySelectedRuleItem.removeClass('unsaved');
                                        }

                                        // no save selected... resolve
                                        deferred.resolve();
                                    }
                                }
                            }]).modal('show');
                    } else {
                        deferred.resolve();
                    }
                } else {
                    deferred.resolve();
                }
            }).promise();

            // ensure any modified rule is saved before continuing
            saveModifiedRule.done(function () {
                // select the specified rule
                ua.selectRule(rule).done(function (rule) {
                    // update the selection
                    li.data('rule', rule).addClass('selected').siblings().removeClass('selected');
                });
            });
        }).on('click', 'div.remove-rule', function (e) {
            var li = $(this).closest('li');
            var rule = li.data('rule');

            // remove the rule
            ua.deleteRule(rule).done(function () {
                li.remove();

                // attempt to get the first visible rule
                var firstVisibleRule = ruleList.children('li:visible:first');
                if (firstVisibleRule.length === 1) {
                    firstVisibleRule.click();
                } else {
                    // only hide the rule list when we know there are no rules (could be filtered out)
                    if (ruleList.is(':empty')) {
                        // update the rule list visibility
                        ua.hideRuleList();
                    }

                    // clear the rule details
                    ua.clearRuleDetails();
                }

                // re-apply the rule filter
                ua.applyRuleFilter();
            });

            // stop event propagation;
            e.stopPropagation();
        }).children(':first').click();
    },
    
    /**
     * Saves the current rule order
     */
    saveRuleOrder: function () {
        var ruleList = $('#rule-list');

        // get the proper order
        var ruleOrder = ruleList.sortable('toArray');

        // only include existing rules
        var existingRuleOrder = $.grep(ruleOrder, function (ruleId) {
            return ruleId.indexOf('unsaved-rule-') === -1;
        });

        var entity = {
            processorId: ua.getProcessorId(),
            revision: ua.getRevision(),
            clientId: ua.getClientId(),
            ruleOrder: existingRuleOrder
        };

        return $.ajax({
            type: 'PUT',
            url: 'api/criteria/evaluation-context',
            data: JSON.stringify(entity),
            processData: false,
            contentType: 'application/json'
        }).then(function () {
            ua.showMessage('New rule ordering saved.');
        }, function (xhr, status, error) {
            // show an error message
            $('#ok-dialog-content').text('Unable to reorder the rules due to:' + xhr.responseText);
            $('#ok-dialog').modal('setHeaderText', 'Error').modal('show');
        });
    },
    
    /**
     * Clears the rule details.
     */
    clearRuleDetails: function () {
        var conditionsGrid = $('#selected-rule-conditions').data('gridInstance');
        var actionsGrid = $('#selected-rule-actions').data('gridInstance');

        if (ua.editable) {
            // cancel any condition edits
            var conditionsEditController = conditionsGrid.getEditController();
            conditionsEditController.cancelCurrentEdit();

            // cancel any action edits
            var actionsEditController = actionsGrid.getEditController();
            actionsEditController.cancelCurrentEdit();
        } else {
            // ensure all detail dialogs are closed
            ua.removeAllDetailDialogs();
        }

        // update the selected rule name
        $('#selected-rule-name').val('').hide();
        $('#no-rule-selected-label').show();

        // clear the grids
        var conditionsData = conditionsGrid.getData();
        conditionsData.setItems([]);

        var actionsData = actionsGrid.getData();
        actionsData.setItems([]);
    },
    
    /**
     * Loads the rule list.
     * 
     * @returns {undefined}
     */
    loadRuleList: function () {
        var ruleList = $.ajax({
            type: 'GET',
            url: 'api/criteria/rules?' + $.param({
                processorId: ua.getProcessorId(),
                verbose: true
            })
        }).done(function (response) {
            var rules = response.rules;

            // populate the rules
            if (rules && rules.length > 0) {
                // add each rules
                $.each(rules, function (_, rule) {
                    ua.createNewRuleItem(rule);
                });

                // show the listing
                ua.showRuleList();

                // select the first rule
                $('#rule-list').children('li:visible:first').click();
            } else {
                ua.hideRuleList();
            }
        });

        var evaluationContext = $.ajax({
            type: 'GET',
            url: 'api/criteria/evaluation-context?' + $.param({
                processorId: ua.getProcessorId()
            })
        }).done(function (evaluationContext) {
            // record the currently selected value
            $('#selected-flowfile-policy').text(evaluationContext.flowFilePolicy);

            // populate the control
            $('#flowfile-policy').combo('setSelectedOption', {
                value: evaluationContext.flowFilePolicy
            });
        });

        // allow the rule list and evaluation context to load
        return $.Deferred(function (deferred) {
            $.when(ruleList, evaluationContext).then(function () {
                deferred.resolve();
            }, function () {
                $('#ok-dialog-content').text('Unable to load the rule list and evalaution criteria.');
                $('#ok-dialog').modal('setHeaderText', 'Error').modal('show');

                deferred.reject();
            });
        }).promise();
    },
    
    /**
     * Selects the specified rule and populates its details.
     * 
     * @param {object} rule
     * @returns 
     */
    selectRule: function (rule) {
        var ruleId = rule.id;

        var conditionsGrid = $('#selected-rule-conditions').data('gridInstance');
        var conditionsData = conditionsGrid.getData();

        var actionsGrid = $('#selected-rule-actions').data('gridInstance');
        var actionsData = actionsGrid.getData();

        return $.Deferred(function (deferred) {
            // if this is an existing rule, load it
            if (ruleId.indexOf('unsaved-rule-') === -1) {
                $.ajax({
                    type: 'GET',
                    url: 'api/criteria/rules/' + encodeURIComponent(ruleId) + '?' + $.param({
                        processorId: ua.getProcessorId(),
                        verbose: true
                    })
                }).then(function (response) {
                    deferred.resolveWith(this, [response.rule]);
                }, function () {
                    // show an error message
                    $('#ok-dialog-content').text('Unable to load details for rule \'' + rule.name + '\'.');
                    $('#ok-dialog').modal('setHeaderText', 'Error').modal('show');

                    // reject the deferred
                    deferred.reject();
                });
            } else {
                deferred.resolveWith(this, [$.extend({
                        conditions: [],
                        actions: []
                    }, rule)]);
            }
        }).done(function (selectedRule) {
            if (!ua.editable) {
                // if we are in read only mode, ensure there are no lingering pop ups
                ua.removeAllDetailDialogs();
            }

            // populate the rule details
            $('#selected-rule-id').text(selectedRule.id);
            $('#selected-rule-name').val(selectedRule.name).show();
            $('#no-rule-selected-label').hide();

            // populate the rule conditions
            conditionsGrid.setSortColumn('expression', true);
            conditionsData.setItems(selectedRule.conditions);
            conditionsGrid.invalidate();

            // populate the rule actions
            actionsGrid.setSortColumn('attribute', true);
            actionsData.setItems(selectedRule.actions);
            actionsGrid.invalidate();
        }).promise();
    },
    
    /**
     * Deletes the specified rule.
     * 
     * @param {type} rule
     * @returns 
     */
    deleteRule: function (rule) {
        var ruleId = rule.id;

        return $.Deferred(function (deferred) {

            // confirm the rule deletion
            $('#yes-no-dialog-content').text('Delete rule \'' + rule.name + '\'?');
            $('#yes-no-dialog').modal('setHeaderText', 'Delete Confirmation').modal('setButtonModel', [{
                    buttonText: 'Yes',
                    handler: {
                        click: function () {
                            // close the dialog
                            $('#yes-no-dialog').modal('hide');

                            // if this is an existing rule, delete it
                            if (ruleId.indexOf('unsaved-rule-') === -1) {
                                $.ajax({
                                    type: 'DELETE',
                                    url: 'api/criteria/rules/' + encodeURIComponent(ruleId) + '?' + $.param({
                                        processorId: ua.getProcessorId(),
                                        revision: ua.getRevision(),
                                        clientId: ua.getClientId(),
                                        verbose: true
                                    })
                                }).then(function () {
                                    ua.showMessage('Rule \'' + rule.name + '\' was deleted successfully.');
                                    deferred.resolve();
                                }, function (xhr, status, error) {
                                    $('#ok-dialog-content').text('Unable to delete the rule "' + rule.name + '" because ' + xhr.responseText);
                                    $('#ok-dialog').modal('setHeaderText', 'Error').modal('show');
                                    deferred.rejectWith(this, arguments);
                                });
                            } else {
                                deferred.resolve();
                            }
                        }
                    }
                }, {
                    buttonText: 'No',
                    handler: {
                        click: function () {
                            // close the dialog
                            $('#yes-no-dialog').modal('hide');

                            // no save selected... resolve
                            deferred.reject();
                        }
                    }
                }]).modal('show');

        }).promise();
    },
    
    /**
     * Saves the currently selected rule.
     * 
     * @returns {unresolved}
     */
    saveSelectedRule: function () {
        var ruleId = $('#selected-rule-id').text();

        var conditionsGrid = $('#selected-rule-conditions').data('gridInstance');
        var conditionsData = conditionsGrid.getData();

        var actionsGrid = $('#selected-rule-actions').data('gridInstance');
        var actionsData = actionsGrid.getData();

        // ensure a rule was populated
        if (ruleId === '') {
            $('#ok-dialog-content').text('No rule is selected.');
            $('#ok-dialog').modal('setHeaderText', 'Configuration Error').modal('show');
            return;
        }

        // commit any condition edits
        var conditionsEditController = conditionsGrid.getEditController();
        conditionsEditController.commitCurrentEdit();

        // commit any action edits
        var actionsEditController = actionsGrid.getEditController();
        actionsEditController.commitCurrentEdit();

        // marshal the rule
        var rule = {
            name: $('#selected-rule-name').val(),
            conditions: conditionsData.getItems(),
            actions: actionsData.getItems()
        };

        // marshal the entity
        var entity = {
            processorId: ua.getProcessorId(),
            clientId: ua.getClientId(),
            revision: ua.getRevision(),
            rule: rule
        };

        // determine the type of request to make
        var url = 'api/criteria/rules';
        var httpMethod = 'POST';
        if (ruleId.indexOf('unsaved-rule-') === -1) {
            rule['id'] = ruleId;
            httpMethod = 'PUT';
            url = url + '/' + encodeURIComponent(ruleId);
        }

        // create/update the rule
        return $.ajax({
            type: httpMethod,
            url: url,
            data: JSON.stringify(entity),
            processData: false,
            contentType: 'application/json'
        }).then(function (response) {
            var rule = response.rule;

            // update the id of the rule
            $('#selected-rule-id').text(rule.id);

            // update the rule item
            var selectedRuleItem = $('#rule-list').children('li.selected');
            selectedRuleItem.data('rule', rule).attr('id', rule.id).removeClass('unsaved').children('div.rule-label').text(rule.name);

            // indicate that that message was saved
            ua.showMessage('Rule \'' + rule.name + '\' was saved successfully.');
        }, function (xhr, status, error) {
            $('#ok-dialog-content').text(xhr.responseText);
            $('#ok-dialog').modal('setHeaderText', 'Error').modal('show');
        });
    },
    
    /**
     * Deletes the specified row from the specified grid.
     * 
     * @param {type} gridSelector
     * @param {type} row
     * @returns {undefined}
     */
    deleteRow: function (gridSelector, row) {
        var grid = $(gridSelector).data('gridInstance');
        var data = grid.getData();
        var item = data.getItem(row);
        data.deleteItem(item.id);

        // mark the rule as modified
        $('#rule-list').children('li.selected').addClass('unsaved');
    },
    
    /**
     * Creates a new rule and adds it to the rule list.
     * 
     * @param {type} rule
     */
    createNewRuleItem: function (rule) {
        var ruleList = $('#rule-list');

        // create the rule item
        var ruleLabel = $('<div></div>').addClass('rule-label ellipsis').text(rule.name).ellipsis();
        var ruleItem = $('<li></li>').attr('id', rule.id).data('rule', rule).append(ruleLabel).appendTo(ruleList);

        if (ua.editable) {
            $('<div></div>').addClass('remove-rule').appendTo(ruleItem);
        } else {
            // remove the pointer cursor when not editable
            ruleItem.css('cursor', 'default');
        }

        // apply the ellipsis
        ruleLabel.attr('title', rule.name).ellipsis();

        return ruleItem;
    },
    
    /**
     * Hides the rule list.
     * 
     * @returns {undefined}
     */
    hideRuleList: function () {
        $('#rule-list-container').hide();
        $('#no-rules').show();
        $('#rule-filter-controls').hide();
    },
    
    /**
     * Shows the rule list.
     * 
     * @returns {undefined}
     */
    showRuleList: function () {
        $('#rule-list-container').show();
        $('#no-rules').hide();
        $('#rule-filter-controls').show();

        // apply the filter
        ua.applyRuleFilter();
    },
    
    // Rule filter functions. 

    /**
     * Get the filter text.
     * 
     * @returns {unresolved}
     */
    getFilterText: function () {
        var filter = '';
        var ruleFilter = $('#rule-filter');
        if (!ruleFilter.hasClass('rule-filter-list')) {
            filter = ruleFilter.val();
        }
        return filter;
    },
    
    /**
     * Get the text for the rule to be filtered.
     * 
     * @param {type} li
     * @returns {Array}
     */
    getRuleText: function (li) {
        var rule = li.data('rule');
        var filterType = $('#rule-filter-type').combo('getSelectedOption');

        // determine the filter type (name, condition, action)
        if (filterType.value === 'name') {
            return [rule.name];
        } else if (filterType.value === 'condition') {
            var conditions = [];
            $.each(rule.conditions, function (_, condition) {
                conditions.push(condition.expression);
            });
            return conditions;
        } else {
            var actions = [];
            $.each(rule.actions, function (_, action) {
                actions.push(action.attribute);
                actions.push(action.value);
            });
            return actions;
        }
    },
    
    /**
     * Apply the rule filter.
     * 
     * @returns {undefined}
     */
    applyRuleFilter: function () {
        var ruleList = $('#rule-list');
        var ruleItems = ruleList.children();
        var filter = ua.getFilterText();

        var matchingRules;
        if (filter !== '') {
            matchingRules = 0;

            // determines if the specified str matches the filter
            var matchRuleText = function (ruleText) {
                try {
                    var filterExp = new RegExp(filter, 'i');
                    return ruleText.search(filterExp) >= 0;
                } catch (e) {
                    // the regex is invalid
                    return false;
                }
            };

            // update the displayed rule count
            $.each(ruleItems, function (_, ruleItem) {
                var li = $(ruleItem);

                // get the rule text for matching
                var ruleTextList = ua.getRuleText(li);

                // see if any of the text from this rule matches
                var ruleMatches = false;
                $.each(ruleTextList, function (_, ruleText) {
                    // update the count and item visibility as appropriate
                    if (matchRuleText(ruleText)) {
                        ruleMatches = true;

                        // stop iteration
                        return false;
                    }
                });

                // handle whether the rule matches
                if (ruleMatches || li.hasClass('unsaved')) {
                    li.show();
                    matchingRules++;
                } else {
                    // if we are hiding the currently selected rule, clear it
                    if (li.hasClass('selected')) {
                        ua.clearRuleDetails();
                    }

                    // hide the rule
                    li.removeClass('selected').hide();
                }
            });
        } else {
            // ensure every rule is visible
            ruleItems.show();

            // set the number of displayed rules
            matchingRules = ruleItems.length;
        }

        // update the rule count
        $('#displayed-rules').text(matchingRules);
        $('#total-rules').text(ruleItems.length);
    },
    
    /**
     * Adds a hover effects to the specified selector.
     * 
     * @param {type} selector
     * @param {type} normalStyle
     * @param {type} overStyle
     */
    addHoverEffect: function (selector, normalStyle, overStyle) {
        $(document).on('mouseenter', selector, function () {
            $(this).removeClass(normalStyle).addClass(overStyle);
        }).on('mouseleave', selector, function () {
            $(this).removeClass(overStyle).addClass(normalStyle);
        });
        return $(selector).addClass(normalStyle);
    },
    
    /**
     * Shows the specified text and clears it after 10 seconds.
     * 
     * @param {type} text
     * @returns {undefined}
     */
    showMessage: function (text) {
        $('#message').text(text);
        setTimeout(function () {
            $('#message').text('');
        }, 10000);
    },
    
    /**
     * Custom validator for required fields.
     * 
     * @param {type} value
     */
    requiredFieldValidator: function (value) {
        if (value === null || value === undefined || !value.length) {
            return {valid: false, msg: "This is a required field"};
        } else {
            return {valid: true, msg: null};
        }
    },
    
    /**
     * Function for prevent cell editing before a rule is selected.
     * 
     * @param {type} e
     * @param {type} args
     */
    preventEdit: function (e, args) {
        var ruleId = $('#selected-rule-id').text();

        if (ruleId === '') {
            $('#ok-dialog-content').text('No rule is selected.');
            $('#ok-dialog').modal('setHeaderText', 'Configuration Error').modal('show');
            return true;
        } else {
            return false;
        }
    },
    
    /**
     * Shows the property value for the specified row and cell.
     * 
     * @param {slickgrid} grid
     * @param {integer} row
     * @param {integer} cell
     */
    showValue: function (grid, row, cell) {
        // remove any currently open detail dialogs
        ua.removeAllDetailDialogs();

        var container = $('#update-attributes-content');

        // get the property in question
        var data = grid.getData();
        var item = data.getItem(row);

        // get the column in question
        var columns = grid.getColumns();
        var columnDefinition = columns[cell];
        var value = item[columnDefinition.field];

        // get details about the location of the cell
        var cellNode = $(grid.getCellNode(row, cell));
        var offset = cellNode.offset();

        // create the wrapper
        var wrapper = $('<div class="update-attribute-detail"></div>').css({
            'z-index': 100000,
            'position': 'absolute',
            'background': 'white',
            'padding': '5px',
            'overflow': 'hidden',
            'border': '3px solid #365C6A',
            'box-shadow': '4px 4px 6px rgba(0, 0, 0, 0.9)',
            'cursor': 'move',
            'top': offset.top - 5,
            'left': offset.left - 5
        }).appendTo(container);

        // the attribute column does not get the nfel editor
        if (columnDefinition.id === 'attribute') {
            // make it draggable
            wrapper.draggable({
                containment: 'parent'
            });

            // create the input field
            $('<textarea hidefocus rows="5" readonly="readonly"/>').css({
                'background': 'white',
                'width': (cellNode.width() - 5) + 'px',
                'height': '80px',
                'border-width': '0',
                'outline': '0',
                'overflow-y': 'auto',
                'resize': 'both',
                'margin-bottom': '28px'
            }).text(value).appendTo(wrapper);
        } else {
            var languageId = 'nfel';
            var editorClass = languageId + '-editor';

            // prevent dragging over the nf editor
            wrapper.draggable({
                cancel: 'input, textarea, pre, .button, .' + editorClass,
                containment: 'parent'
            });

            // create the editor
            $('<div></div>').addClass(editorClass).appendTo(wrapper).nfeditor({
                languageId: languageId,
                width: (cellNode.width() - 5) + 'px',
                content: value,
                minWidth: 175,
                minHeight: 100,
                readOnly: true,
                resizable: true
            });
        }

        // add an ok button that will remove the entire pop up
        var ok = $('<div class="button button-normal">Ok</div>').on('click', function () {
            wrapper.hide().remove();
        });
        $('<div></div>').css({
            'position': 'absolute',
            'bottom': '0',
            'left': '0',
            'right': '0',
            'padding': '0 3px 5px'
        }).append(ok).append('<div class="clear"></div>').appendTo(wrapper);
    },
    
    /**
     * Removes all currently open process property detail dialogs.
     */
    removeAllDetailDialogs: function () {
        $('body').children('div.update-attribute-detail').hide().remove();
    },
    
    /**
     * Gets a custom editor for editing long values.
     * 
     * @param {type} args
     */
    getCustomLongTextEditor: function (args) {
        var scope = this;
        var defaultValue = '';
        var wrapper;
        var input;

        this.init = function () {
            var container = $('#update-attributes-content');

            // create the wrapper
            wrapper = $('<div></div>').css({
                'z-index': 100000,
                'position': 'absolute',
                'background': 'white',
                'padding': '5px',
                'overflow': 'hidden',
                'border': '3px solid #365C6A',
                'box-shadow': '4px 4px 6px rgba(0, 0, 0, 0.9)',
                'cursor': 'move'
            }).draggable({
                containment: 'parent'
            }).appendTo(container);

            // create the input field
            input = $('<textarea hidefocus rows="5"/>').css({
                'background': 'white',
                'width': args.position.width + 'px',
                'height': '80px',
                'border-width': '0',
                'outline': '0',
                'resize': 'both',
                'margin-bottom': '28px'
            }).on('keydown', scope.handleKeyDown).appendTo(wrapper);

            // create the button panel
            var ok = $('<div class="button button-normal">Ok</div>').on('click', scope.save);
            var cancel = $('<div class="button button-normal">Cancel</div>').on('click', scope.cancel);
            $('<div></div>').css({
                'position': 'absolute',
                'bottom': '0',
                'left': '0',
                'right': '0',
                'padding': '0 3px 5px'
            }).append(ok).append(cancel).append('<div class="clear"></div>').appendTo(wrapper);

            // position and focus
            scope.position(args.position);
            input.focus().select();
        };

        this.handleKeyDown = function (e) {
            if (e.which === $.ui.keyCode.ENTER && e.ctrlKey) {
                scope.save();
            } else if (e.which === $.ui.keyCode.ESCAPE) {
                e.preventDefault();
                scope.cancel();
            } else if (e.which === $.ui.keyCode.TAB && e.shiftKey) {
                e.preventDefault();
                args.grid.navigatePrev();
            } else if (e.which === $.ui.keyCode.TAB) {
                e.preventDefault();
                args.grid.navigateNext();
            }
        };

        this.save = function () {
            args.commitChanges();
        };

        this.cancel = function () {
            input.val(defaultValue);
            args.cancelChanges();
        };

        this.hide = function () {
            wrapper.hide();
        };

        this.show = function () {
            wrapper.show();
        };

        this.position = function (position) {
            wrapper.css({
                'top': position.top - 5,
                'left': position.left - 5
            });
        };

        this.destroy = function () {
            wrapper.remove();
        };

        this.focus = function () {
            input.focus();
        };

        this.loadValue = function (item) {
            input.val(defaultValue = item[args.column.field]);
            input.select();
        };

        this.serializeValue = function () {
            return input.val();
        };

        this.applyValue = function (item, state) {
            item[args.column.field] = state;
        };

        this.isValueChanged = function () {
            return (!(input.val() === "" && defaultValue === null)) && (input.val() !== defaultValue);
        };

        this.validate = function () {
            return {
                valid: true,
                msg: null
            };
        };

        // initialize the custom long text editor
        this.init();
    },
    
    /**
     * Gets a custom editor for editing long values.
     * 
     * @param {type} args
     */
    getNfelEditor: function (args) {
        var scope = this;
        var defaultValue = '';
        var wrapper;
        var editor;

        this.init = function () {
            var container = $('#update-attributes-content');

            var languageId = 'nfel';
            var editorClass = languageId + '-editor';

            // create the wrapper
            wrapper = $('<div></div>').addClass('slickgrid-nfel-editor').css({
                'z-index': 100000,
                'position': 'absolute',
                'background': 'white',
                'padding': '5px',
                'overflow': 'hidden',
                'border': '3px solid #365C6A',
                'box-shadow': '4px 4px 6px rgba(0, 0, 0, 0.9)',
                'cursor': 'move'
            }).draggable({
                cancel: 'input, textarea, pre, .button, div.' + editorClass,
                containment: 'parent'
            }).appendTo(container);

            // create the editor
            editor = $('<div></div>').addClass(editorClass).appendTo(wrapper).nfeditor({
                languageId: languageId,
                width: args.position.width,
                minWidth: 175,
                minHeight: 80,
                resizable: true,
                escape: function () {
                    scope.cancel();
                },
                enter: function () {
                    scope.save();
                }
            });

            // create the button panel
            var ok = $('<div class="button button-normal">Ok</div>').on('click', scope.save);
            var cancel = $('<div class="button button-normal">Cancel</div>').on('click', scope.cancel);
            $('<div></div>').css({
                'position': 'absolute',
                'bottom': '0',
                'left': '0',
                'right': '0',
                'padding': '0 3px 5px'
            }).append(ok).append(cancel).append('<div class="clear"></div>').appendTo(wrapper);

            // position and focus
            scope.position(args.position);
            editor.nfeditor('focus').nfeditor('selectAll');
        };

        this.save = function () {
            args.commitChanges();
        };

        this.cancel = function () {
            editor.nfeditor('setValue', defaultValue);
            args.cancelChanges();
        };

        this.hide = function () {
            wrapper.hide();
        };

        this.show = function () {
            wrapper.show();
        };

        this.position = function (position) {
            wrapper.css({
                'top': position.top - 5,
                'left': position.left - 5
            });
        };

        this.destroy = function () {
            wrapper.remove();
        };

        this.focus = function () {
            editor.nfeditor('focus');
        };

        this.loadValue = function (item) {
            defaultValue = item[args.column.field];
            editor.nfeditor('setValue', defaultValue).nfeditor('selectAll');
        };

        this.serializeValue = function () {
            return editor.nfeditor('getValue');
        };

        this.applyValue = function (item, state) {
            item[args.column.field] = state;
        };

        this.isValueChanged = function () {
            var value = scope.serializeValue();
            return (!(value === "" && defaultValue === null)) && (value !== defaultValue);
        };

        this.validate = function () {
            return {
                valid: true,
                msg: null
            };
        };

        // initialize the custom long text editor
        this.init();
    },
    
    /**
     * Adjust the size of the selected rule name field.
     */
    resizeSelectedRuleNameField: function () {
        var ruleDetailsPanel = $('#rule-details-panel');
        var ruleNameField = $('#selected-rule-name');
        ruleNameField.width(Math.min(500, ruleDetailsPanel.width() - 10));
    },
    
    /**
     * Gets the client id.
     * 
     * @returns 
     */
    getClientId: function () {
        return $('#attribute-updater-client-id').text();
    },
    
    /**
     * Gets the revision.
     * 
     * @returns 
     */
    getRevision: function () {
        return $('#attribute-updater-revision').text();
    },
    
    /**
     * Gets the processor id.
     * 
     * @returns
     */
    getProcessorId: function () {
        return $('#attribute-updater-processor-id').text();
    }
};