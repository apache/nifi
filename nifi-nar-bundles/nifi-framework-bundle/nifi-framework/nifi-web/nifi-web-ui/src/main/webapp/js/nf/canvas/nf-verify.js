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
                'nf.ng.Bridge',
                'nf.ErrorHandler'],
            function ($, nfCommon, nfDialog, nfNgBridge, nfErrorHandler) {
                return (nf.Verify = factory($, nfCommon, nfDialog, nfNgBridge, nfErrorHandler));
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.Verify =
            factory(require('jquery'),
                require('nf.Common'),
                require('nf.Dialog'),
                require('nf.ng.Bridge'),
                require('nf.ErrorHandler')));
    } else {
        nf.Verify = factory(root.$,
            root.nf.Common,
            root.nf.Dialog,
            root.nf.ng.Bridge,
            root.nf.ErrorHandler);
    }
}(this, function ($, nfCommon, nfDialog, nfNgBridge, nfErrorHandler) {
    'use strict';

    var gridOptions = {
        autosizeColsMode: Slick.GridAutosizeColsMode.LegacyForceFit,
        enableTextSelectionOnCells: true,
        enableCellNavigation: true,
        enableColumnReorder: false,
        editable: true,
        enableAddRow: false,
        autoEdit: false,
        multiSelect: false,
        rowHeight: 24
    };

    // text editor
    var textEditor = function (args) {
        var scope = this;
        var initialValue = '';
        var previousValue;
        var wrapper;
        var isEmpty;
        var input;

        this.init = function () {
            var container = $('body');

            // record the previous value
            previousValue = args.item[args.column.field];

            // create the wrapper
            wrapper = $('<div></div>').addClass('slickgrid-editor').css({
                'z-index': 100000,
                'position': 'absolute',
                'border-radius': '2px',
                'box-shadow': 'rgba(0, 0, 0, 0.247059) 0px 2px 5px',
                'background-color': 'rgb(255, 255, 255)',
                'overflow': 'hidden',
                'padding': '10px 20px',
                'cursor': 'move',
                'transform': 'translate3d(0px, 0px, 0px)'
            }).appendTo(container);

            // create the input field
            input = $('<textarea hidefocus rows="5"/>').css({
                'height': '80px',
                'width': args.position.width + 'px',
                'min-width': '212px',
                'margin-bottom': '5px',
                'margin-top': '10px',
                'white-space': 'pre'
            }).tab().on('keydown', scope.handleKeyDown).appendTo(wrapper);

            wrapper.draggable({
                cancel: '.button, textarea, .nf-checkbox',
                containment: 'parent'
            });

            // create the button panel
            var stringCheckPanel = $('<div class="string-check-container" />');
            stringCheckPanel.appendTo(wrapper);

            // build the custom checkbox
            isEmpty = $('<div class="nf-checkbox string-check" />')
                .on('change', function (event, args) {
                    // if we are setting as an empty string, disable the editor
                    if (args.isChecked) {
                        input.prop('disabled', true).val('');
                    } else {
                        input.prop('disabled', false).val(previousValue);
                    }
                }).appendTo(stringCheckPanel);
            $('<span class="string-check-label nf-checkbox-label">&nbsp;Set empty string</span>').appendTo(stringCheckPanel);

            var ok = $('<div class="button">Ok</div>').css({
                'color': '#fff',
                'background': '#728E9B'
            }).hover(
                function () {
                    $(this).css('background', '#004849');
                }, function () {
                    $(this).css('background', '#728E9B');
                }).on('click', scope.save);
            var cancel = $('<div class="secondary-button">Cancel</div>').css({
                'color': '#004849',
                'background': '#E3E8EB'
            }).hover(
                function () {
                    $(this).css('background', '#C7D2D7');
                }, function () {
                    $(this).css('background', '#E3E8EB');
                }).on('click', scope.cancel);
            $('<div></div>').css({
                'position': 'relative',
                'top': '10px',
                'left': '20px',
                'width': '212px',
                'clear': 'both',
                'float': 'right'
            }).append(ok).append(cancel).append('<div class="clear"></div>').appendTo(wrapper);

            // position and focus
            scope.position(args.position);
            input.focus().select();
        };

        this.handleKeyDown = function (e) {
            if (e.which === $.ui.keyCode.ENTER && !e.shiftKey) {
                scope.save();
            } else if (e.which === $.ui.keyCode.ESCAPE) {
                scope.cancel();

                // prevent further propagation or escape press and prevent default behavior
                e.stopImmediatePropagation();
                e.preventDefault();
            }
        };

        this.save = function () {
            args.commitChanges();
        };

        this.cancel = function () {
            input.val(initialValue);
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
                'top': position.top - 27,
                'left': position.left - 20
            });
        };

        this.destroy = function () {
            wrapper.remove();
        };

        this.focus = function () {
            input.focus();
        };

        this.loadValue = function (item) {
            var isEmptyChecked = false;

            // determine the value to use when populating the text field
            if (nfCommon.isDefinedAndNotNull(item[args.column.field])) {
                initialValue = item[args.column.field];
                isEmptyChecked = initialValue === '';
            }

            // determine if its an empty string
            var checkboxStyle = isEmptyChecked ? 'checkbox-checked' : 'checkbox-unchecked';
            isEmpty.addClass(checkboxStyle);

            input.val(initialValue);
            input.select();
        };

        this.serializeValue = function () {
            // if the field has been cleared, set the value accordingly
            if (input.val() === '') {
                // if the user has checked the empty string checkbox, use emtpy string
                if (isEmpty.hasClass('checkbox-checked')) {
                    return '';
                } else {
                    return null;
                }
            } else {
                // if there is text specified, use that value
                return input.val();
            }
        };

        this.applyValue = function (item, state) {
            item[args.column.field] = state;
        };

        this.isValueChanged = function () {
            return scope.serializeValue() !== previousValue;
        };

        this.validate = function () {
            return {
                valid: true,
                msg: null
            };
        };

        // initialize the custom long text editor
        this.init();
    };

    /**
     * Reset the dialog.
     */
    var resetDialog = function () {
        var referencedAttributesGrid = $('#referenced-attributes-table').data('gridInstance');
        var referencedAttributesData = referencedAttributesGrid.getData();
        referencedAttributesGrid.setSelectedRows([]);
        referencedAttributesData.setItems([]);
    };

    /**
     * Initialized the referenced attributes dialog.
     */
    var initializeReferencedAttributesDialog = function () {
        $('#referenced-attributes-dialog').modal({
            scrollableContentStyle: 'scrollable',
            headerText: 'Referenced Attributes',
            handler: {
                close: function () {
                    resetDialog();
                },
                open: function () {
                    var referencedAttributesGrid = $('#referenced-attributes-table').data('gridInstance');
                    if (nfCommon.isDefinedAndNotNull(referencedAttributesGrid)) {
                        referencedAttributesGrid.resizeCanvas();
                    }
                }
            }
        });
    };

    var verify = function (componentId, componentUri, proposedProperties, referencedAttributeMap, handleVerificationResults, verificationResultsContainer) {
        // submit verification
        performVerification(componentId, componentUri, proposedProperties, referencedAttributeMap).done(function (verificationResults) {
            // empty the previous listing
            verificationResultsContainer.empty();

            // render the verification results
            $.each(verificationResults, function (i, result) {
                var verificationResultContainer = $('<div class="verification-result"></div>').appendTo(verificationResultsContainer);

                // determine the icon for this result
                var outcomeClass;
                switch (result.outcome) {
                    case 'SUCCESSFUL':
                        outcomeClass = 'fa-check';
                        break;
                    case 'FAILED':
                        outcomeClass = 'fa-times';
                        break;
                    case 'SKIPPED':
                        outcomeClass = 'fa-exclamation';
                        break;
                }

                // build the header
                var verificationHeader = $('<div class="verification-result-header"></div>').appendTo(verificationResultContainer);
                $('<div class="verification-result-outcome fa ' + outcomeClass + '"></div>').appendTo(verificationHeader);
                $('<div class="verification-result-step-name"></div>').text(result.verificationStepName).appendTo(verificationHeader);
                $('<div class="clear"></div>').appendTo(verificationHeader);

                // build the explanation
                $('<div class="verification-result-explanation"></div>').text(result.explanation).appendTo(verificationResultContainer);
            });

            // invoke the verification callback if specified
            if (typeof handleVerificationResults === 'function') {
                handleVerificationResults(verificationResults, referencedAttributeMap);
            }
        });
    }

    /**
     * Updates the button model for the verification of this component and proposed properties.
     *
     * @param componentId the component id
     * @param componentUri the component uri
     * @param proposedProperties the proposed properties
     * @param handleVerificationResults verification results callback
     * @param verificationResultsContainer container where verification results should be rendered
     */
    var updateReferencedAttributesButtonModel = function (componentId, componentUri, proposedProperties, handleVerificationResults, verificationResultsContainer) {
        $('#referenced-attributes-dialog').modal('setButtonModel', [{
            buttonText: 'Verify',
            color: {
                base: '#728E9B',
                hover: '#004849',
                text: '#ffffff'
            },
            handler: {
                click: function () {
                    // get the referenced attributes name/values
                    var referencedAttributesGrid = $('#referenced-attributes-table').data('gridInstance');
                    var referencedAttributesData = referencedAttributesGrid.getData();
                    var referencedAttributes = referencedAttributesData.getItems();

                    // map the referenced attributes
                    var referencedAttributeMap = referencedAttributes.reduce(function(map, referencedAttribute) {
                        map[referencedAttribute.name] = referencedAttribute.value;
                        return map;
                    }, {});

                    // hide the referenced attributes dialog prior to performing the verification
                    $('#referenced-attributes-dialog').modal('hide');

                    // verify
                    verify(componentId, componentUri, proposedProperties, referencedAttributeMap, handleVerificationResults, verificationResultsContainer);
                }
            }
        }, {
            buttonText: 'Cancel',
            color: {
                base: '#E3E8EB',
                hover: '#C7D2D7',
                text: '#004849'
            },
            handler: {
                click: function () {
                    $('#referenced-attributes-dialog').modal('hide');
                }
            }
        }]);
    };

    /**
     * Initializes the verification request status dialog.
     */
    var initializeVerificationRequestStatusDialog = function () {
        // configure the verification request status dialog
        $('#verification-request-status-dialog').modal({
            scrollableContentStyle: 'scrollable',
            headerText: 'Verifying Properties',
            handler: {
                close: function () {
                    // clear the current button model
                    $('#verification-request-status-dialog').modal('setButtonModel', []);
                }
            }
        });
    };

    /**
     * Initialized the new referenced attribute dialog.
     */
    var initializeNewAttributeDialog = function () {
        $('#new-referenced-attribute-dialog').modal({
            headerText: 'New Attribute',
            buttons: [{
                buttonText: 'Ok',
                color: {
                    base: '#728E9B',
                    hover: '#004849',
                    text: '#ffffff'
                },
                handler: {
                    click: function () {
                        addNewReferencedAttribute();
                    }
                }
            }, {
                buttonText: 'Cancel',
                color: {
                    base: '#E3E8EB',
                    hover: '#C7D2D7',
                    text: '#004849'
                },
                handler: {
                    click: function () {
                        $('#new-referenced-attribute-dialog').modal('hide');
                    }
                }
            }],
            handler: {
                close: function () {
                    $('#new-referenced-attribute-name').val('');
                },
                open: function () {
                    $('#new-referenced-attribute-name').focus();
                }
            }
        });

        $('#new-referenced-attribute-name').on('keydown', function (e) {
            var code = e.keyCode ? e.keyCode : e.which;
            if (code === $.ui.keyCode.ENTER) {
                addNewReferencedAttribute();

                // prevents the enter from propagating into the field for editing the new property value
                e.stopImmediatePropagation();
                e.preventDefault();
            }
        });

        $('#add-referenced-attribute').on('click', function () {
            $('#new-referenced-attribute-dialog').modal('show');
        });
    }

    /**
     * Adds a new referenced attribute.
     */
    var addNewReferencedAttribute = function () {
        var attributeName = $.trim($('#new-referenced-attribute-name').val());

        // ensure the property name is specified
        if (attributeName !== '') {
            var referencedAttributeGrid = $('#referenced-attributes-table').data('gridInstance');
            var referencedAttributeData = referencedAttributeGrid.getData();

            // ensure the property name is unique
            var matchingAttribute = null;
            $.each(referencedAttributeData.getItems(), function (_, item) {
                if (attributeName === item.name) {
                    matchingAttribute = item;
                }
            });

            if (matchingAttribute === null) {
                referencedAttributeData.beginUpdate();

                // add a row for the new attribute
                referencedAttributeData.addItem({
                    id: attributeName,
                    name: attributeName,
                    value: null,
                });

                referencedAttributeData.endUpdate();

                // sort the data
                referencedAttributeData.reSort();

                // select the new variable row
                var row = referencedAttributeData.getRowById(attributeName);
                referencedAttributeGrid.setActiveCell(row, referencedAttributeGrid.getColumnIndex('value'));
                referencedAttributeGrid.editActiveCell();
            } else {
                // if this row is currently hidden, clear the value and show it
                nfDialog.showOkDialog({
                    headerText: 'Attribute Exists',
                    dialogContent: 'An attribute with this name already exists.'
                });

                // select the existing properties row
                var matchingRow = referencedAttributeData.getRowById(matchingAttribute.id);
                referencedAttributeGrid.setSelectedRows([matchingRow]);
                referencedAttributeGrid.scrollRowIntoView(matchingRow);
            }

            // close the new variable dialog
            $('#new-referenced-attribute-dialog').modal('hide');
        } else {
            nfDialog.showOkDialog({
                headerText: 'Attribute Error',
                dialogContent: 'The name of the attribute must be specified.'
            });
        }
    };

    /**
     * Sorts the specified data using the specified sort details.
     *
     * @param {object} sortDetails
     * @param {object} data
     */
    var sortReferencedAttributes = function (sortDetails, data) {
        // defines a function for sorting
        var comparer = function (a, b) {
            var aString = nfCommon.isDefinedAndNotNull(a[sortDetails.columnId]) ? a[sortDetails.columnId] : '';
            var bString = nfCommon.isDefinedAndNotNull(b[sortDetails.columnId]) ? b[sortDetails.columnId] : '';
            return aString === bString ? 0 : aString > bString ? 1 : -1;
        };

        // perform the sort
        data.sort(comparer, sortDetails.sortAsc);
    };

    /**
     * Initializes the referenced attributes table
     */
    var initializeReferencedAttributesTable = function () {
        var referencedAttributesGridElement = $('#referenced-attributes-table');

        var nameFormatter = function (row, cell, value, columnDef, dataContext) {
            return nfCommon.escapeHtml(value);
        };

        var valueFormatter = function (row, cell, value, columnDef, dataContext) {
            if (value === '') {
                return '<span class="table-cell blank">Empty string set</span>';
            } else if (value === null) {
                return '<span class="unset">No value set</span>';
            } else {
                return nfCommon.escapeHtml(value);
            }
        };

        var actionFormatter = function (row, cell, value, columnDef, dataContext) {
            return '<div title="Delete" class="delete-attribute pointer fa fa-trash"></div>';
        };

        // define the column model for the referenced attributes table
        var referencedAttributesColumns = [
            {
                id: 'name',
                name: 'Name',
                field: 'name',
                formatter: nameFormatter,
                sortable: true,
                resizable: true
            },
            {
                id: 'value',
                name: 'Value',
                field: 'value',
                formatter: valueFormatter,
                sortable: true,
                resizable: true,
                cssClass: 'pointer'
            },
            {
                id: "actions",
                name: "&nbsp;",
                minWidth: 10,
                width: 10,
                formatter: actionFormatter
            }
        ];

        // initialize the dataview
        var referencedAttributesData = new Slick.Data.DataView({
            inlineFilters: false
        });
        referencedAttributesData.getItemMetadata = function (index) {
            return {
                columns: {
                    value: {
                        editor: textEditor
                    }
                }
            };
        };

        // initialize the sort
        sortReferencedAttributes({
            columnId: 'name',
            sortAsc: true
        }, referencedAttributesData);

        // initialize the grid
        var referencedAttributesGrid = new Slick.Grid(referencedAttributesGridElement, referencedAttributesData, referencedAttributesColumns, gridOptions);
        referencedAttributesGrid.setSelectionModel(new Slick.RowSelectionModel());
        referencedAttributesGrid.registerPlugin(new Slick.AutoTooltips());
        referencedAttributesGrid.setSortColumn('name', true);
        referencedAttributesGrid.onSort.subscribe(function (e, args) {
            sortReferencedAttributes({
                columnId: args.sortCol.id,
                sortAsc: args.sortAsc
            }, referencedAttributesData);
        });
        referencedAttributesGrid.onClick.subscribe(function (e, args) {
            if (referencedAttributesGrid.getColumns()[args.cell].id === 'value') {
                referencedAttributesGrid.gotoCell(args.row, args.cell, true);

                // prevents standard edit logic
                e.stopImmediatePropagation();
            } else if (referencedAttributesGrid.getColumns()[args.cell].id === 'actions') {
                var attribute = referencedAttributesData.getItem(args.row);

                var target = $(e.target);
                if (target.hasClass('delete-attribute')) {
                    // remove the attribute from the table
                    referencedAttributesData.deleteItem(attribute.id);
                }
            }
        });
        referencedAttributesGrid.onBeforeCellEditorDestroy.subscribe(function (e, args) {
            setTimeout(function() {
                referencedAttributesGrid.resizeCanvas();
            }, 50);
        });

        // wire up the dataview to the grid
        referencedAttributesData.onRowCountChanged.subscribe(function (e, args) {
            referencedAttributesGrid.updateRowCount();
            referencedAttributesGrid.render();
        });
        referencedAttributesData.onRowsChanged.subscribe(function (e, args) {
            referencedAttributesGrid.invalidateRows(args.rows);
            referencedAttributesGrid.render();
        });
        referencedAttributesData.syncGridSelection(referencedAttributesGrid, true);

        // hold onto an instance of the grid
        referencedAttributesGridElement.data('gridInstance', referencedAttributesGrid);
    };

    /**
     * Performs verification for the specific configuration and referenced attributes.
     *
     * @param id the component id
     * @param componentUrl the component url
     * @param proposedProperties the proposed properties
     * @param attributes the attributes names/values
     */
    var performVerification = function (id, componentUrl, proposedProperties, attributes) {
        var MAX_DELAY = 4;
        var cancelled = false;
        var verificationRequest = null;
        var verificationRequestTimer = null;

        return $.Deferred(function (deferred) {
            // updates the progress bar
            var updateProgress = function (percentComplete) {
                // remove existing labels
                var progressBar = $('#verification-request-percent-complete');
                progressBar.find('div.progress-label').remove();
                progressBar.find('md-progress-linear').remove();

                // update the progress
                var label = $('<div class="progress-label"></div>').text(percentComplete + '%');
                (nfNgBridge.injector.get('$compile')($('<md-progress-linear ng-cloak ng-value="' + percentComplete + '" class="md-hue-2" md-mode="determinate" aria-label="Searching Queue"></md-progress-linear>'))(nfNgBridge.rootScope)).appendTo(progressBar);
                progressBar.append(label);
            };

            // update the button model of the drop request status dialog
            $('#verification-request-status-dialog').modal('setButtonModel', [{
                headerText: 'Verifying Properties',
                buttonText: 'Stop',
                color: {
                    base: '#728E9B',
                    hover: '#004849',
                    text: '#ffffff'
                },
                handler: {
                    click: function () {
                        cancelled = true;

                        // we are waiting for the next poll attempt
                        if (verificationRequestTimer !== null) {
                            // cancel it
                            clearTimeout(verificationRequestTimer);

                            // cancel the verification request
                            completeVerificationRequest();
                        }
                    }
                }
            }]);

            // completes the verification request by removing it
            var completeVerificationRequest = function () {
                $('#verification-request-status-dialog').modal('hide');

                var reject = cancelled;

                // ensure the verification requests are present
                if (nfCommon.isDefinedAndNotNull(verificationRequest)) {
                    $.ajax({
                        type: 'DELETE',
                        url: verificationRequest.uri,
                        dataType: 'json'
                    });

                    // use the verification request from when the verification completed
                    if (nfCommon.isEmpty(verificationRequest.results)) {
                        if (cancelled === false) {
                            reject = true;

                            // show the dialog
                            nfDialog.showOkDialog({
                                headerText: 'Verifying Properties',
                                dialogContent: 'There are no results.'
                            });
                        }
                    }
                } else {
                    reject = true;
                }

                if (reject) {
                    deferred.reject();
                } else {
                    deferred.resolve(verificationRequest.results);
                }
            };

            // process the verification request
            var processVerificationRequest = function (delay) {
                // update the percent complete
                updateProgress(verificationRequest.percentCompleted);

                // update the status of the verification request
                $('#verification-request-status-message').text(verificationRequest.state);

                // close the dialog if the
                if (verificationRequest.complete === true || cancelled === true) {
                    completeVerificationRequest();
                } else {
                    // wait delay to poll again
                    verificationRequestTimer = setTimeout(function () {
                        // clear the verification request timer
                        verificationRequestTimer = null;

                        // schedule to poll the status again in nextDelay
                        pollVerificationRequest(Math.min(MAX_DELAY, delay * 2));
                    }, delay * 1000);
                }
            };

            // schedule for the next poll iteration
            var pollVerificationRequest = function (nextDelay) {
                $.ajax({
                    type: 'GET',
                    url: verificationRequest.uri,
                    dataType: 'json'
                }).done(function (response) {
                    verificationRequest = response.request;
                    processVerificationRequest(nextDelay);
                }).fail(completeVerificationRequest).fail(nfErrorHandler.handleAjaxError);
            };

            // initialize the progress bar value
            updateProgress(0);

            // show the progress dialog
            $('#verification-request-status-dialog').modal('show');

            // issue the request to verify the properties
            $.ajax({
                type: 'POST',
                url: componentUrl + '/config/verification-requests',
                data: JSON.stringify({
                    request: {
                        componentId: id,
                        properties: proposedProperties,
                        attributes: attributes
                    }
                }),
                dataType: 'json',
                contentType: 'application/json'
            }).done(function (response) {
                // process the verification request
                verificationRequest = response.request;
                processVerificationRequest(1);
            }).fail(completeVerificationRequest).fail(nfErrorHandler.handleAjaxError);
        }).promise();
    };

    return {

        /**
         * Initialize the verification component.
         */
        init: function () {
            initializeVerificationRequestStatusDialog();
            initializeReferencedAttributesDialog();
            initializeReferencedAttributesTable();
            initializeNewAttributeDialog();
        },

        /**
         * Peforms verification for a given component.
         *
         * @param id the component id
         * @param componentUrl the component url
         * @param proposedProperties the proposed attributes
         * @param referencedAttributes the most recently submitted referenced attributes, null if not submitted previously
         * @param handleVerificationResults callback for handling verification results
         * @param verificationResultsContainer container where verification results should be rendered
         */
        verify: function (id, componentUrl, proposedProperties, referencedAttributes, handleVerificationResults, verificationResultsContainer) {
            // get the referenced attributes
            $.ajax({
                type: 'POST',
                url: componentUrl + '/config/analysis',
                data: JSON.stringify({
                    configurationAnalysis: {
                        componentId: id,
                        properties: proposedProperties
                    }
                }),
                dataType: 'json',
                contentType: 'application/json'
            }).done(function(response) {
                var configurationAnalysis = response.configurationAnalysis;

                // if the component does not support additional verification there is no need to prompt for attribute values
                if (configurationAnalysis.supportsVerification === false) {
                    verify(id, componentUrl, proposedProperties, {}, handleVerificationResults, verificationResultsContainer);
                } else {
                    // combine the previously entered referenced attributes with the updated attributes from the configuration analysis
                    var combinedReferencedAttributes = $.extend({}, configurationAnalysis.referencedAttributes, referencedAttributes);

                    var referencedAttributesGrid = $('#referenced-attributes-table').data('gridInstance');
                    var referencedAttributesData = referencedAttributesGrid.getData();

                    // begin the update
                    referencedAttributesData.beginUpdate();

                    $.each(combinedReferencedAttributes, function (name, value) {
                        referencedAttributesData.addItem({
                            id: name,
                            name: name,
                            value: value
                        });
                    });

                    // complete the update
                    referencedAttributesData.endUpdate();
                    referencedAttributesData.reSort();

                    // update the button model for this verification
                    updateReferencedAttributesButtonModel(id, componentUrl, proposedProperties, handleVerificationResults, verificationResultsContainer);

                    // show the dialog
                    $('#referenced-attributes-dialog').modal('show');
                }
            }).fail(nfErrorHandler.handleAjaxError);
        }
    };
}));
