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
nf.ProcessorDetails = (function () {

    /**
     * Shows the property value for the specified row and cell.
     * 
     * @param {type} row
     * @param {type} cell
     */
    var showPropertyValue = function (row, cell) {
        // remove any currently open detail dialogs
        removeAllPropertyDetailDialogs();

        // get the property in question
        var propertyGrid = $('#read-only-processor-properties').data('gridInstance');
        var propertyData = propertyGrid.getData();
        var property = propertyData.getItem(row);

        // ensure there is a value
        if (nf.Common.isDefinedAndNotNull(property.value)) {

            // get the processor details to insert the description tooltip
            var details = $('#processor-details').data('processorDetails');
            var propertyDescriptor = details.config.descriptors[property.property];

            // ensure we're not dealing with a sensitive property
            if (!isSensitiveProperty(propertyDescriptor)) {

                // get details about the location of the cell
                var cellNode = $(propertyGrid.getCellNode(row, cell));
                var offset = cellNode.offset();

                // create the wrapper
                var wrapper = $('<div class="processor-property-detail"></div>').css({
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
                }).appendTo('body');

                // so the nfel editor is appropriate
                if (supportsEl(propertyDescriptor)) {
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
                        width: cellNode.width(),
                        content: property.value,
                        minWidth: 175,
                        minHeight: 100,
                        readOnly: true,
                        resizable: true
                    });
                } else {
                    // prevent dragging over standard components
                    wrapper.draggable({
                        containment: 'parent'
                    });

                    // create the input field
                    $('<textarea hidefocus rows="5" readonly="readonly"/>').css({
                        'background': 'white',
                        'width': cellNode.width() + 'px',
                        'height': '80px',
                        'border-width': '0',
                        'outline': '0',
                        'overflow-y': 'auto',
                        'resize': 'both',
                        'margin-bottom': '28px'
                    }).text(property.value).appendTo(wrapper);
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
            }
        }
    };

    /**
     * Removes all currently open process property detail dialogs.
     */
    var removeAllPropertyDetailDialogs = function () {
        $('body').children('div.processor-property-detail').hide().remove();
    };

    /**
     * Determines if the specified property is sensitive.
     * 
     * @argument {object} propertyDescriptor        The property descriptor
     */
    var isSensitiveProperty = function (propertyDescriptor) {
        if (nf.Common.isDefinedAndNotNull(propertyDescriptor)) {
            return propertyDescriptor.sensitive === true;
        } else {
            return false;
        }
    };

    /**
     * Returns whether the specified property supports EL.
     * 
     * @param {object} propertyDescriptor           The property descriptor
     */
    var supportsEl = function (propertyDescriptor) {
        if (nf.Common.isDefinedAndNotNull(propertyDescriptor)) {
            return propertyDescriptor.supportsEl === true;
        } else {
            return false;
        }
    };

    /**
     * Creates an option for the specified relationship name.
     * 
     * @argument {object} relationship      The relationship
     */
    var createRelationshipOption = function (relationship) {
        var relationshipLabel = $('<div class="relationship-name ellipsis"></div>').text(relationship.name);

        // build the relationship checkbox element
        if (relationship.autoTerminate === true) {
            relationshipLabel.css('font-weight', 'bold');
        }

        // build the relationship container element
        var relationshipContainerElement = $('<div class="processor-relationship-container"></div>').append(relationshipLabel).appendTo('#read-only-auto-terminate-relationship-names');
        if (!nf.Common.isBlank(relationship.description)) {
            var relationshipDescription = $('<div class="relationship-description"></div>').text(relationship.description);
            relationshipContainerElement.append(relationshipDescription);
        }

        return relationshipContainerElement;
    };

    return {
        /**
         * Initializes the processor details dialog.
         * 
         * @param {boolean} overlayBackground       Whether to overlay the background
         */
        init: function (overlayBackground) {
            overlayBackground = nf.Common.isDefinedAndNotNull(overlayBackground) ? overlayBackground : true;

            // initialize the properties tabs
            $('#processor-details-tabs').tabbs({
                tabStyle: 'tab',
                selectedTabStyle: 'selected-tab',
                tabs: [{
                        name: 'Settings',
                        tabContentId: 'details-standard-settings-tab-content'
                    }, {
                        name: 'Scheduling',
                        tabContentId: 'details-scheduling-tab-content'
                    }, {
                        name: 'Properties',
                        tabContentId: 'details-processor-properties-tab-content'
                    }, {
                        name: 'Comments',
                        tabContentId: 'details-processor-comments-tab-content'
                    }],
                select: function () {
                    // resize the property grid in case this is the first time its rendered
                    if ($(this).text() === 'Properties') {
                        var propertyGrid = $('#read-only-processor-properties').data('gridInstance');
                        if (nf.Common.isDefinedAndNotNull(propertyGrid)) {
                            propertyGrid.resizeCanvas();
                        }
                    }

                    // show the border if processor relationship names if necessary
                    var processorRelationships = $('#read-only-auto-terminate-relationship-names');
                    if (processorRelationships.is(':visible') && processorRelationships.get(0).scrollHeight > processorRelationships.innerHeight()) {
                        processorRelationships.css('border-width', '1px');
                    }
                }
            });

            // configure the processor details dialog
            $('#processor-details').modal({
                headerText: 'Processor Details',
                overlayBackground: overlayBackground,
                handler: {
                    close: function () {
                        // empty the relationship list
                        $('#read-only-auto-terminate-relationship-names').css('border-width', '0').empty();

                        // clear the grid
                        var propertyGrid = $('#read-only-processor-properties').data('gridInstance');
                        var propertyData = propertyGrid.getData();
                        propertyData.setItems([]);

                        // clear the processor details
                        nf.Common.clearField('read-only-processor-id');
                        nf.Common.clearField('read-only-processor-type');
                        nf.Common.clearField('read-only-processor-name');
                        nf.Common.clearField('read-only-concurrently-schedulable-tasks');
                        nf.Common.clearField('read-only-scheduling-period');
                        nf.Common.clearField('read-only-penalty-duration');
                        nf.Common.clearField('read-only-yield-duration');
                        nf.Common.clearField('read-only-run-duration');
                        nf.Common.clearField('read-only-bulletin-level');
                        nf.Common.clearField('read-only-execution-status');
                        nf.Common.clearField('read-only-processor-comments');

                        // removed the cached processor details
                        $('#processor-details').removeData('processorDetails');
                        $('#processor-details').removeData('processorHistory');

                        // remove any currently open detail dialogs
                        removeAllPropertyDetailDialogs();
                    }
                }
            });

            // function for formatting the property name
            var nameFormatter = function (row, cell, value, columnDef, dataContext) {
                var nameWidthOffset = 10;
                var cellContent = $('<div></div>');

                // format the contents
                var formattedValue = $('<span/>').addClass('table-cell').text(value).appendTo(cellContent);
                if (dataContext.type === 'required') {
                    formattedValue.addClass('required');
                }

                // get the processor details to insert the description tooltip
                var details = $('#processor-details').data('processorDetails');
                var propertyDescriptor = details.config.descriptors[dataContext.property];

                // show the property description if applicable
                if (nf.Common.isDefinedAndNotNull(propertyDescriptor)) {
                    if (!nf.Common.isBlank(propertyDescriptor.description) || !nf.Common.isBlank(propertyDescriptor.defaultValue) || !nf.Common.isBlank(propertyDescriptor.supportsEl)) {
                        $('<img class="icon-info" src="images/iconInfo.png" alt="Info" title="" style="float: right; margin-right: 6px; margin-top: 4px;" />').appendTo(cellContent);
                        nameWidthOffset = 26; // 10 + icon width (10) + icon margin (6)
                    }
                }

                // adjust the width accordingly
                formattedValue.width(columnDef.width - nameWidthOffset).ellipsis();

                // return the cell content
                return cellContent.html();
            };

            // function for formatting the property value
            var valueFormatter = function (row, cell, value, columnDef, dataContext) {
                var valueMarkup;
                if (nf.Common.isDefinedAndNotNull(value)) {
                    // identify the property descriptor
                    var processorDetails = $('#processor-details').data('processorDetails');
                    var propertyDescriptor = processorDetails.config.descriptors[dataContext.property];

                    // determine if the property is sensitive
                    if (isSensitiveProperty(propertyDescriptor)) {
                        valueMarkup = '<span class="table-cell sensitive">Sensitive value set</span>';
                    } else {
                        if (value === '') {
                            valueMarkup = '<span class="table-cell blank">Empty string set</span>';
                        } else {
                            valueMarkup = '<div class="table-cell value"><pre class="ellipsis">' + nf.Common.escapeHtml(value) + '</pre></div>';
                        }
                    }
                } else {
                    valueMarkup = '<span class="unset">No value set</span>';
                }

                // format the contents
                var content = $(valueMarkup);
                if (dataContext.type === 'required') {
                    content.addClass('required');
                }
                content.find('.ellipsis').width(columnDef.width - 10).ellipsis();

                // return the appropriate markup
                return $('<div/>').append(content).html();
            };

            // initialize the processor type table
            var processorDetailsColumns = [
                {id: 'property', field: 'property', name: 'Property', sortable: false, resizable: true, rerenderOnResize: true, formatter: nameFormatter},
                {id: 'value', field: 'value', name: 'Value', sortable: false, resizable: true, cssClass: 'pointer', rerenderOnResize: true, formatter: valueFormatter}
            ];
            var processorDetailsOptions = {
                forceFitColumns: true,
                enableTextSelectionOnCells: true,
                enableCellNavigation: false,
                enableColumnReorder: false,
                autoEdit: false
            };

            // initialize the dataview
            var processorDetailsData = new Slick.Data.DataView({
                inlineFilters: false
            });
            processorDetailsData.setItems([]);

            // initialize the grid
            var processorDetailsGrid = new Slick.Grid('#read-only-processor-properties', processorDetailsData, processorDetailsColumns, processorDetailsOptions);
            processorDetailsGrid.setSelectionModel(new Slick.RowSelectionModel());
            processorDetailsGrid.onClick.subscribe(function (e, args) {
                // only consider property values
                if (args.cell === 1) {
                    // show the property value in a resizable dialog
                    showPropertyValue(args.row, args.cell);

                    // prevents standard edit logic
                    e.stopImmediatePropagation();
                }
            });

            // wire up the dataview to the grid
            processorDetailsData.onRowCountChanged.subscribe(function (e, args) {
                processorDetailsGrid.updateRowCount();
                processorDetailsGrid.render();
            });
            processorDetailsData.onRowsChanged.subscribe(function (e, args) {
                processorDetailsGrid.invalidateRows(args.rows);
                processorDetailsGrid.render();
            });

            // hold onto an instance of the grid and listen for mouse events to add tooltips where appropriate
            $('#read-only-processor-properties').data('gridInstance', processorDetailsGrid).on('mouseenter', 'div.slick-cell', function (e) {
                var infoIcon = $(this).find('img.icon-info');
                if (infoIcon.length && !infoIcon.data('qtip')) {
                    var property = $(this).find('span.table-cell').text();

                    // get the processor details to insert the description tooltip
                    var details = $('#processor-details').data('processorDetails');
                    var propertyDescriptor = details.config.descriptors[property];

                    // get the processor history
                    var processorHistory = $('#processor-details').data('processorHistory');
                    var propertyHistory = processorHistory.propertyHistory[property];

                    // format the tooltip
                    var tooltip = nf.Common.formatPropertyTooltip(propertyDescriptor, propertyHistory);

                    if (nf.Common.isDefinedAndNotNull(tooltip)) {
                        infoIcon.qtip($.extend(nf.Common.config.tooltipConfig, {
                            content: tooltip
                        }));
                    }
                }
            });
        },
        /**
         * Shows the details for the specified processor.
         * 
         * @argument {string} groupId       The group id
         * @argument {string} processorId   The processor id
         */
        showDetails: function (groupId, processorId) {
            // load the properties for the specified processor
            var getProcessor = $.ajax({
                type: 'GET',
                url: '../nifi-api/controller/process-groups/' + encodeURIComponent(groupId) + '/processors/' + encodeURIComponent(processorId),
                dataType: 'json'
            }).done(function (response) {
                if (nf.Common.isDefinedAndNotNull(response.processor)) {
                    // get the processor details
                    var details = response.processor;

                    // record the processor details
                    $('#processor-details').data('processorDetails', details);

                    // populate the processor settings
                    nf.Common.populateField('read-only-processor-id', details['id']);
                    nf.Common.populateField('read-only-processor-type', nf.Common.substringAfterLast(details['type'], '.'));
                    nf.Common.populateField('read-only-processor-name', details['name']);
                    nf.Common.populateField('read-only-concurrently-schedulable-tasks', details.config['concurrentlySchedulableTaskCount']);
                    nf.Common.populateField('read-only-scheduling-period', details.config['schedulingPeriod']);
                    nf.Common.populateField('read-only-penalty-duration', details.config['penaltyDuration']);
                    nf.Common.populateField('read-only-yield-duration', details.config['yieldDuration']);
                    nf.Common.populateField('read-only-run-duration', nf.Common.formatDuration(details.config['runDurationMillis']));
                    nf.Common.populateField('read-only-bulletin-level', details.config['bulletinLevel']);
                    nf.Common.populateField('read-only-processor-comments', details.config['comments']);

                    var showRunSchedule = true;

                    // make the scheduling strategy human readable
                    var schedulingStrategy = details.config['schedulingStrategy'];
                    if (schedulingStrategy === 'EVENT_DRIVEN') {
                        showRunSchedule = false;
                        schedulingStrategy = 'Event driven';
                    } else if (schedulingStrategy === 'CRON_DRIVEN') {
                        schedulingStrategy = 'CRON driven';
                    } else if (schedulingStrategy === 'TIMER_DRIVEN') {
                        schedulingStrategy = "Timer driven";
                    } else {
                        schedulingStrategy = "On primary node";
                    }
                    nf.Common.populateField('read-only-scheduling-strategy', schedulingStrategy);

                    // only show the run schedule when applicable
                    if (showRunSchedule === true) {
                        $('#read-only-run-schedule').show();
                    } else {
                        $('#read-only-run-schedule').hide();
                    }

                    // load the relationship list
                    if (!nf.Common.isEmpty(details.relationships)) {
                        $.each(details.relationships, function (i, relationship) {
                            createRelationshipOption(relationship);
                        });
                    } else {
                        $('#read-only-auto-terminate-relationship-names').append('<div class="unset">This processor has no relationships.</div>');
                    }

                    // get the property grid
                    var propertyGrid = $('#read-only-processor-properties').data('gridInstance');
                    var propertyData = propertyGrid.getData();
                    var properties = details.config.properties;
                    var descriptors = details.config.descriptors;

                    // generate the processor properties
                    if (nf.Common.isDefinedAndNotNull(properties)) {
                        propertyData.beginUpdate();

                        var i = 0;
                        $.each(properties, function (name, value) {
                            // get the property descriptor
                            var descriptor = descriptors[name];

                            // determine the property type
                            var type = 'userDefined';
                            var displayName = name;
                            if (nf.Common.isDefinedAndNotNull(descriptor)) {
                                if (descriptor.required === true) {
                                    type = 'required';
                                } else if (descriptor.dynamic === true) {
                                    type = 'userDefined';
                                } else {
                                    type = 'optional';
                                }
                                
                                // use the display name if possible
                                displayName = descriptor.displayName;
                                
                                // determine the value
                                if (nf.Common.isUndefined(value) || nf.Common.isNull(value)) {
                                    value = descriptor.defaultValue;
                                }
                            }

                            // add the row
                            propertyData.addItem({
                                id: i++,
                                property: displayName,
                                value: value,
                                type: type
                            });
                        });

                        propertyData.endUpdate();
                    }
                }
            });

            // get the processor history
            var getProcessorHistory = $.ajax({
                type: 'GET',
                url: '../nifi-api/controller/history/processors/' + encodeURIComponent(processorId),
                dataType: 'json'
            }).done(function (response) {
                var processorHistory = response.processorHistory;

                // record the processor history
                $('#processor-details').data('processorHistory', processorHistory);
            });

            // show the dialog once we have the processor and its history
            $.when(getProcessor, getProcessorHistory).then(function (response) {
                var processorResponse = response[0];
                var processor = processorResponse.processor;

                var buttons = [{
                        buttonText: 'Ok',
                        handler: {
                            click: function () {
                                // hide the dialog
                                $('#processor-details').modal('hide');
                            }
                        }
                    }];

                // determine if we should show the advanced button
                if (nf.Common.isDefinedAndNotNull(nf.CustomProcessorUi) && nf.Common.isDefinedAndNotNull(processor.config.customUiUrl) && processor.config.customUiUrl !== '') {
                    buttons.push({
                        buttonText: 'Advanced',
                        handler: {
                            click: function () {
                                // reset state and close the dialog manually to avoid hiding the faded background
                                $('#processor-details').modal('hide');

                                // show the custom ui
                                nf.CustomProcessorUi.showCustomUi(processor.id, processor.config.customUiUrl, false);
                            }
                        }
                    });
                }

                // show the dialog
                $('#processor-details').modal('setButtonModel', buttons).modal('show');

                // add ellipsis if necessary
                $('#processor-details div.relationship-name').ellipsis();

                // show the border if necessary
                var processorRelationships = $('#read-only-auto-terminate-relationship-names');
                if (processorRelationships.is(':visible') && processorRelationships.get(0).scrollHeight > processorRelationships.innerHeight()) {
                    processorRelationships.css('border-width', '1px');
                }
            }, function (xhr, status, error) {
                if (xhr.status === 400 || xhr.status === 404 || xhr.status === 409) {
                    nf.Dialog.showOkDialog({
                        dialogContent: nf.Common.escapeHtml(xhr.responseText),
                        overlayBackground: false
                    });
                } else {
                    nf.Common.handleAjaxError(xhr, status, error);
                }
            });
        }
    };
}());