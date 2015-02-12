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
nf.CanvasToolbox = (function () {

    var config = {
        /**
         * Tag properties configuration.
         */
        maxTags: 25,
        maxTagFontSize: 2,
        minTagFontSize: 1,
        minWidth: 20,
        filterText: 'Filter',
        type: {
            processor: 'Processor',
            inputPort: 'Input Port',
            outputPort: 'Output Port',
            processGroup: 'Process Group',
            remoteProcessGroup: 'Remote Process Group',
            connection: 'Connection',
            funnel: 'Funnel',
            template: 'Template',
            label: 'Label'
        },
        styles: {
            filterList: 'filter-list'
        },
        urls: {
            controller: '../nifi-api/controller',
            processorTypes: '../nifi-api/controller/processor-types',
            templates: '../nifi-api/controller/templates'
        }
    };

    /**
     * Creates a toolbox icon for dragging onto the graph.
     * 
     * @argument {string} type              The type of component
     * @argument {jQuery} toolbox           The toolbox to add the icon to
     * @argument {string} cls               The css class to apply to the icon
     * @argument {string} hoverCls          The css class to apply when hovering
     * @argument {string} dragCls           The css class to apply when dragging
     * @argument {string} dropHandler       Callback to handle the drop event
     */
    var addToolboxIcon = function (type, toolbox, cls, hoverCls, dragCls, dropHandler) {
        // generate the img id
        var imgId = type + '-icon';

        // create the image which is used as the toolbox icon (drag source)
        $('<div/>').attr('id', imgId).attr('title', type).addClass(cls).addClass('pointer').addClass('toolbox-icon').hover(function () {
            $(this).removeClass(cls).addClass(hoverCls);
        }, function () {
            $(this).removeClass(hoverCls).addClass(cls);
        }).draggable({
            'zIndex': 1011,
            'helper': function () {
                return $('<div class="toolbox-icon"></div>').addClass(dragCls).appendTo('body');
            },
            'containment': 'body',
            'start': function(e, ui) {
                // hide the context menu if necessary
                nf.ContextMenu.hide();
            },
            'stop': function (e, ui) {
                var translate = nf.Canvas.View.translate();
                var scale = nf.Canvas.View.scale();

                var mouseX = e.originalEvent.pageX;
                var mouseY = e.originalEvent.pageY - nf.Canvas.CANVAS_OFFSET;

                // invoke the drop handler if we're over the canvas
                if (mouseX >= 0 && mouseY >= 0) {
                    // adjust the x and y coordinates accordingly
                    var x = (mouseX / scale) - (translate[0] / scale);
                    var y = (mouseY / scale) - (translate[1] / scale);

                    dropHandler({
                        x: x,
                        y: y
                    });
                } 
            }
        }).appendTo(toolbox);
    };

    /**
     * Adds the specified tag filter.
     * 
     * @argument {string} tag       The tag to add
     */
    var addTagFilter = function (tag) {
        // ensure this tag hasn't already been added
        var tagFilter = $('#tag-filter');
        var tagFilterExists = false;
        tagFilter.find('li div.selected-tag-text').each(function () {
            if (tag === $(this).text()) {
                tagFilterExists = true;
                return false;
            }
        });

        // add this tag filter if applicable
        if (!tagFilterExists) {
            // create the list item content
            var tagText = $('<div class="selected-tag-text"></div>').text(tag);
            var removeTagIcon = $('<img src="images/iconDelete.png" class="remove-selected-tag pointer"></img>').click(function () {
                // remove this tag
                $(this).closest('li').remove();

                // re-apply the filter
                applyFilter();
            });
            var selectedTagItem = $('<div></div>').append(tagText).append(removeTagIcon);

            // create the list item and update the tag filter list
            $('<li></li>').append(selectedTagItem).appendTo(tagFilter);

            // re-apply the filter
            applyFilter();
        }
    };

    /**
     * Filters the processor type table.
     */
    var applyFilter = function () {
        // get the dataview
        var processorTypesGrid = $('#processor-types-table').data('gridInstance');

        // ensure the grid has been initialized
        if (nf.Common.isDefinedAndNotNull(processorTypesGrid)) {
            var processorTypesData = processorTypesGrid.getData();

            // update the search criteria
            processorTypesData.setFilterArgs({
                searchString: getFilterText(),
                property: $('#processor-type-filter-options').combo('getSelectedOption').value
            });
            processorTypesData.refresh();
        }
    };

    /**
     * Determines if the item matches the filter.
     * 
     * @param {object} item     The item to filter
     * @param {object} args     The filter criteria
     * @returns {boolean}       Whether the item matches the filter
     */
    var matchesRegex = function (item, args) {
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

        // determine if the item matches the filter
        return item[args.property].search(filterExp) >= 0;
    };

    /**
     * Performs the filtering.
     * 
     * @param {object} item     The item subject to filtering
     * @param {object} args     Filter arguments
     * @returns {Boolean}       Whether or not to include the item
     */
    var filter = function (item, args) {
        // determine if the item matches the filter
        var matchesFilter = matchesRegex(item, args);

        // determine if the row matches the selected tags
        var matchesTags = true;
        if (matchesFilter) {
            var tagFilters = $('#tag-filter li');
            var hasSelectedTags = tagFilters.length > 0;
            if (hasSelectedTags) {
                matchesTags = matchesSelectedTags(tagFilters, item['tags']);
            }
        }

        // determine if this row should be visible
        var matches = matchesFilter && matchesTags;

        // if this row is currently selected and its being filtered
        if (matches === false && $('#selected-processor-type').text() === item['type']) {
            // clear the selected row
            $('#processor-type-description').text('');
            $('#processor-type-name').text('');
            $('#selected-processor-name').text('');
            $('#selected-processor-type').text('');

            // clear the active cell the it can be reselected when its included
            var processTypesGrid = $('#processor-types-table').data('gridInstance');
            processTypesGrid.resetActiveCell();
        }

        return matches;
    };

    /**
     * Determines if the specified tags match all the tags selected by the user.
     * 
     * @argument {jQuery} tagFilters    The tag filters
     * @argument {string} tags          The tags to test
     */
    var matchesSelectedTags = function (tagFilters, tags) {
        var selectedTags = [];
        tagFilters.each(function () {
            selectedTags.push($(this).text());
        });

        // normalize the tags
        var normalizedTags = tags.toLowerCase();

        var matches = true;
        $.each(selectedTags, function (i, selectedTag) {
            if (normalizedTags.indexOf(selectedTag) === -1) {
                matches = false;
                return false;
            }
        });

        return matches;
    };

    /**
     * Sorts the specified data using the specified sort details.
     * 
     * @param {object} sortDetails
     * @param {object} data
     */
    var sort = function (sortDetails, data) {
        // defines a function for sorting
        var comparer = function (a, b) {
            var aString = nf.Common.isDefinedAndNotNull(a[sortDetails.columnId]) ? a[sortDetails.columnId] : '';
            var bString = nf.Common.isDefinedAndNotNull(b[sortDetails.columnId]) ? b[sortDetails.columnId] : '';
            return aString === bString ? 0 : aString > bString ? 1 : -1;
        };

        // perform the sort
        data.sort(comparer, sortDetails.sortAsc);
    };

    /**
     * Get the text out of the filter field. If the filter field doesn't
     * have any text it will contain the text 'filter list' so this method
     * accounts for that.
     */
    var getFilterText = function () {
        var filterText = '';
        var filterField = $('#processor-type-filter');
        if (!filterField.hasClass(config.styles.filterList)) {
            filterText = filterField.val();
        }
        return filterText;
    };

    /**
     * Resets the filtered processor types.
     */
    var resetProcessorDialog = function () {
        // clear and selected tags
        var tagFilter = $('#tag-filter');
        tagFilter.empty();

        // clear any filter strings
        $('#processor-type-filter').addClass(config.styles.filterList).val(config.filterText);

        // reset the filter before closing
        applyFilter();

        // clear the selected row
        $('#processor-type-description').text('');
        $('#processor-type-name').text('');
        $('#selected-processor-name').text('');
        $('#selected-processor-type').text('');

        // unselect any current selection
        var processTypesGrid = $('#processor-types-table').data('gridInstance');
        processTypesGrid.setSelectedRows([]);
        processTypesGrid.resetActiveCell();
    };

    /**
     * Prompts the user to select the type of new processor to create.
     * 
     * @argument {object} pt        The point that the processor was dropped
     */
    var promptForProcessorType = function (pt) {
        // handles adding the selected processor at the specified point
        var addProcessor = function () {
            // get the type of processor currently selected
            var name = $('#selected-processor-name').text();
            var processorType = $('#selected-processor-type').text();

            // ensure something was selected
            if (name === '' || processorType === '') {
                nf.Dialog.showOkDialog({
                    dialogContent: 'The type of processor to create must be selected.',
                    overlayBackground: false
                });
            } else {
                // create the new processor
                createProcessor(name, processorType, pt);
            }

            // hide the dialog
            $('#new-processor-dialog').modal('hide');
        };

        // get the grid reference
        var grid = $('#processor-types-table').data('gridInstance');

        // add the processor when its double clicked in the table
        var gridDoubleClick = function (e, args) {
            var processorType = grid.getDataItem(args.row);

            $('#selected-processor-name').text(processorType.label);
            $('#selected-processor-type').text(processorType.type);

            addProcessor();
        };

        // register a handler for double click events
        grid.onDblClick.subscribe(gridDoubleClick);

        // update the button model
        $('#new-processor-dialog').modal('setButtonModel', [{
                buttonText: 'Add',
                handler: {
                    click: addProcessor
                }
            }, {
                buttonText: 'Cancel',
                handler: {
                    click: function () {
                        $('#new-processor-dialog').modal('hide');
                    }
                }
            }]);

        // set a new handler for closing the the dialog
        $('#new-processor-dialog').modal('setHandler', {
            close: function () {
                // remove the handler
                grid.onDblClick.unsubscribe(gridDoubleClick);

                // clear the current filters
                resetProcessorDialog();
            }
        });

        // show the dialog
        $('#new-processor-dialog').modal('show');
        
        // set the focus in the filter field
        $('#processor-type-filter').focus();

        // adjust the grid canvas now that its been rendered
        grid.resizeCanvas();
    };

    /**
     * Create the processor and add to the graph.
     * 
     * @argument {string} name              The processor name
     * @argument {string} processorType     The processor type
     * @argument {object} pt                The point that the processor was dropped
     */
    var createProcessor = function (name, processorType, pt) {
        var revision = nf.Client.getRevision();

        // create a new processor of the defined type
        $.ajax({
            type: 'POST',
            url: config.urls.controller + '/process-groups/' + encodeURIComponent(nf.Canvas.getGroupId()) + '/processors',
            data: {
                version: revision.version,
                clientId: revision.clientId,
                name: name,
                type: processorType,
                x: pt.x,
                y: pt.y
            },
            dataType: 'json'
        }).done(function (response) {
            if (nf.Common.isDefinedAndNotNull(response.processor)) {
                // update the revision
                nf.Client.setRevision(response.revision);

                // add the processor to the graph
                nf.Graph.add({
                    'processors': [response.processor]
                }, true);

                // update component visibility
                nf.Canvas.View.updateVisibility();

                // update the birdseye
                nf.Birdseye.refresh();
            }
        }).fail(nf.Common.handleAjaxError);
    };

    /**
     * Prompts the user to enter the name for the input port.
     * 
     * @argument {object} pt        The point that the input port was dropped
     */
    var promptForInputPortName = function (pt) {
        var addInputPort = function () {
            // get the name of the input port and clear the textfield
            var portName = $('#new-port-name').val();
            
            // hide the dialog
            $('#new-port-dialog').modal('hide');

            // create the input port
            createInputPort(portName, pt);
        };

        $('#new-port-dialog').modal('setButtonModel', [{
                buttonText: 'Add',
                handler: {
                    click: addInputPort
                }
            }, {
                buttonText: 'Cancel',
                handler: {
                    click: function () {
                        $('#new-port-dialog').modal('hide');
                    }
                }
            }]);

        // update the port type
        $('#new-port-type').text('Input');

        // show the dialog
        $('#new-port-dialog').modal('show');

        // set up the focus and key handlers
        $('#new-port-name').focus().off('keyup').on('keyup', function (e) {
            var code = e.keyCode ? e.keyCode : e.which;
            if (code === $.ui.keyCode.ENTER) {
                addInputPort();
            }
        });
    };

    /**
     * Create the input port and add to the graph.
     * 
     * @argument {string} portName          The input port name
     * @argument {object} pt                The point that the input port was dropped
     */
    var createInputPort = function (portName, pt) {
        var revision = nf.Client.getRevision();

        // create a new processor of the defined type
        $.ajax({
            type: 'POST',
            url: config.urls.controller + '/process-groups/' + encodeURIComponent(nf.Canvas.getGroupId()) + '/input-ports',
            data: {
                version: revision.version,
                clientId: revision.clientId,
                name: portName,
                x: pt.x,
                y: pt.y
            },
            dataType: 'json'
        }).done(function (response) {
            if (nf.Common.isDefinedAndNotNull(response.inputPort)) {
                // update the revision
                nf.Client.setRevision(response.revision);

                // add the port to the graph
                nf.Graph.add({
                    'inputPorts': [response.inputPort]
                }, true);

                // update component visibility
                nf.Canvas.View.updateVisibility();

                // update the birdseye
                nf.Birdseye.refresh();
            }
        }).fail(nf.Common.handleAjaxError);
    };

    /**
     * Prompts the user to enter the name for the output port.
     * 
     * @argument {object} pt        The point that the output port was dropped
     */
    var promptForOutputPortName = function (pt) {
        var addOutputPort = function () {
            // get the name of the output port and clear the textfield
            var portName = $('#new-port-name').val();
            
            // hide the dialog
            $('#new-port-dialog').modal('hide');

            // create the output port
            createOutputPort(portName, pt);
        };

        $('#new-port-dialog').modal('setButtonModel', [{
                buttonText: 'Add',
                handler: {
                    click: addOutputPort
                }
            }, {
                buttonText: 'Cancel',
                handler: {
                    click: function () {
                        $('#new-port-dialog').modal('hide');
                    }
                }
            }]);

        // update the port type
        $('#new-port-type').text('Output');

        // set the focus and show the dialog
        $('#new-port-dialog').modal('show');

        // set up the focus and key handlers
        $('#new-port-name').focus().off('keyup').on('keyup', function (e) {
            var code = e.keyCode ? e.keyCode : e.which;
            if (code === $.ui.keyCode.ENTER) {
                addOutputPort();
            }
        });
    };

    /**
     * Create the input port and add to the graph.
     * 
     * @argument {string} portName          The output port name
     * @argument {object} pt                The point that the output port was dropped
     */
    var createOutputPort = function (portName, pt) {
        var revision = nf.Client.getRevision();

        // create a new processor of the defined type
        $.ajax({
            type: 'POST',
            url: config.urls.controller + '/process-groups/' + encodeURIComponent(nf.Canvas.getGroupId()) + '/output-ports',
            data: {
                version: revision.version,
                clientId: revision.clientId,
                name: portName,
                x: pt.x,
                y: pt.y
            },
            dataType: 'json'
        }).done(function (response) {
            if (nf.Common.isDefinedAndNotNull(response.outputPort)) {
                // update the revision
                nf.Client.setRevision(response.revision);

                // add the port to the graph
                nf.Graph.add({
                    'outputPorts': [response.outputPort]
                }, true);

                // update component visibility
                nf.Canvas.View.updateVisibility();

                // update the birdseye
                nf.Birdseye.refresh();
            }
        }).fail(nf.Common.handleAjaxError);
    };

    /**
     * Create the group and add to the graph.
     * 
     * @argument {string} groupName The name of the group
     * @argument {object} pt        The point that the group was dropped
     */
    var createGroup = function (groupName, pt) {
        var revision = nf.Client.getRevision();

        // create a new processor of the defined type
        return $.ajax({
            type: 'POST',
            url: config.urls.controller + '/process-groups/' + encodeURIComponent(nf.Canvas.getGroupId()) + '/process-group-references',
            data: {
                version: revision.version,
                clientId: revision.clientId,
                name: groupName,
                x: pt.x,
                y: pt.y
            },
            dataType: 'json'
        }).done(function (response) {
            if (nf.Common.isDefinedAndNotNull(response.processGroup)) {
                // update the revision
                nf.Client.setRevision(response.revision);

                // add the processor to the graph
                nf.Graph.add({
                    'processGroups': [response.processGroup]
                }, true);

                // update component visibility
                nf.Canvas.View.updateVisibility();

                // update the birdseye
                nf.Birdseye.refresh();
            }
        }).fail(nf.Common.handleAjaxError);
    };

    /**
     * Prompts the user to enter the URI for the remote process group.
     * 
     * @argument {object} pt        The point that the remote group was dropped
     */
    var promptForRemoteProcessGroupUri = function (pt) {
        var addRemoteProcessGroup = function () {
            // get the uri of the controller and clear the textfield
            var remoteProcessGroupUri = $('#new-remote-process-group-uri').val();
            
            // hide the dialog
            $('#new-remote-process-group-dialog').modal('hide');

            // create the remote process group
            createRemoteProcessGroup(remoteProcessGroupUri, pt);
        };

        $('#new-remote-process-group-dialog').modal('setButtonModel', [{
                buttonText: 'Add',
                handler: {
                    click: addRemoteProcessGroup
                }
            }, {
                buttonText: 'Cancel',
                handler: {
                    click: function () {
                        $('#new-remote-process-group-dialog').modal('hide');
                    }
                }
            }]);

        // show the dialog
        $('#new-remote-process-group-dialog').modal('show');

        // set the focus and key handlers
        $('#new-remote-process-group-uri').focus().off('keyup').on('keyup', function (e) {
            var code = e.keyCode ? e.keyCode : e.which;
            if (code === $.ui.keyCode.ENTER) {
                addRemoteProcessGroup();
            }
        });
    };

    /**
     * Create the controller and add to the graph.
     * 
     * @argument {string} remoteProcessGroupUri         The remote group uri
     * @argument {object} pt                            The point that the remote group was dropped
     */
    var createRemoteProcessGroup = function (remoteProcessGroupUri, pt) {
        var revision = nf.Client.getRevision();

        // create a new processor of the defined type
        $.ajax({
            type: 'POST',
            url: config.urls.controller + '/process-groups/' + encodeURIComponent(nf.Canvas.getGroupId()) + '/remote-process-groups',
            data: {
                version: revision.version,
                clientId: revision.clientId,
                uri: remoteProcessGroupUri,
                x: pt.x,
                y: pt.y
            },
            dataType: 'json'
        }).done(function (response) {
            if (nf.Common.isDefinedAndNotNull(response.remoteProcessGroup)) {
                // update the revision
                nf.Client.setRevision(response.revision);

                // add the processor to the graph
                nf.Graph.add({
                    'remoteProcessGroups': [response.remoteProcessGroup]
                }, true);

                // update component visibility
                nf.Canvas.View.updateVisibility();

                // update the birdseye
                nf.Birdseye.refresh();
            }
        }).fail(nf.Common.handleAjaxError);
    };

    /**
     * Creates a new funnel at the specified point.
     * 
     * @argument {object} pt        The point that the funnel was dropped
     */
    var createFunnel = function (pt) {
        var revision = nf.Client.getRevision();

        // create a new funnel
        $.ajax({
            type: 'POST',
            url: config.urls.controller + '/process-groups/' + encodeURIComponent(nf.Canvas.getGroupId()) + '/funnels',
            data: {
                version: revision.version,
                clientId: revision.clientId,
                x: pt.x,
                y: pt.y
            },
            dataType: 'json'
        }).done(function (response) {
            if (nf.Common.isDefinedAndNotNull(response.funnel)) {
                // update the revision
                nf.Client.setRevision(response.revision);

                // add the funnel to the graph
                nf.Graph.add({
                    'funnels': [response.funnel]
                }, true);

                // update the birdseye
                nf.Birdseye.refresh();
            }
        }).fail(nf.Common.handleAjaxError);
    };

    /**
     * Prompts the user to select a template.
     * 
     * @argument {object} pt        The point that the template was dropped
     */
    var promptForTemplate = function (pt) {
        $.ajax({
            type: 'GET',
            url: config.urls.templates,
            dataType: 'json'
        }).done(function (response) {
            var templates = response.templates;
            if (nf.Common.isDefinedAndNotNull(templates) && templates.length > 0) {
                var options = [];
                $.each(templates, function (_, template) {
                    options.push({
                        text: template.name,
                        value: template.id,
                        description: nf.Common.escapeHtml(template.description)
                    });
                });

                // configure the templates combo
                $('#available-templates').combo({
                    maxHeight: 300,
                    options: options
                });

                // update the button model
                $('#instantiate-template-dialog').modal('setButtonModel', [{
                        buttonText: 'Add',
                        handler: {
                            click: function () {
                                // get the type of processor currently selected
                                var selectedOption = $('#available-templates').combo('getSelectedOption');
                                var templateId = selectedOption.value;

                                // hide the dialog
                                $('#instantiate-template-dialog').modal('hide');

                                // instantiate the specified template
                                createTemplate(templateId, pt);
                            }
                        }
                    }, {
                        buttonText: 'Cancel',
                        handler: {
                            click: function () {
                                $('#instantiate-template-dialog').modal('hide');
                            }
                        }
                    }]);

                // show the dialog
                $('#instantiate-template-dialog').modal('show');
            } else {
                nf.Dialog.showOkDialog({
                    headerText: 'Instantiate Template',
                    dialogContent: 'No templates have been loaded into this NiFi.',
                    overlayBackground: false
                });
            }

        }).fail(nf.Common.handleAjaxError);
    };

    /**
     * Instantiates the specified template and 
     * 
     * @argument {string} templateId        The template id
     * @argument {object} pt                The point that the template was dropped
     */
    var createTemplate = function (templateId, pt) {
        var revision = nf.Client.getRevision();

        // create a new instance of the new template
        $.ajax({
            type: 'POST',
            url: config.urls.controller + '/process-groups/' + encodeURIComponent(nf.Canvas.getGroupId()) + '/template-instance',
            data: {
                version: revision.version,
                clientId: revision.clientId,
                templateId: templateId,
                originX: pt.x,
                originY: pt.y
            },
            dataType: 'json'
        }).done(function (response) {
            // update the revision
            nf.Client.setRevision(response.revision);

            // populate the graph accordingly
            nf.Graph.add(response.contents, true);

            // update component visibility
            nf.Canvas.View.updateVisibility();

            // update the birdseye
            nf.Birdseye.refresh();
        }).fail(nf.Common.handleAjaxError);
    };

    /**
     * Create the label and add to the graph.
     * 
     * @argument {object} pt        The point that the label was dropped
     */
    var createLabel = function (pt) {
        var revision = nf.Client.getRevision();

        // create a new label
        $.ajax({
            type: 'POST',
            url: config.urls.controller + '/process-groups/' + encodeURIComponent(nf.Canvas.getGroupId()) + '/labels',
            data: {
                version: revision.version,
                clientId: revision.clientId,
                x: pt.x,
                y: pt.y,
                width: nf.Label.config.width,
                height: nf.Label.config.height
            },
            dataType: 'json'
        }).done(function (response) {
            if (nf.Common.isDefinedAndNotNull(response.label)) {
                // update the revision
                nf.Client.setRevision(response.revision);

                // add the label to the graph
                nf.Graph.add({
                    'labels': [response.label]
                }, true);

                // update the birdseye
                nf.Birdseye.refresh();
            }
        }).fail(nf.Common.handleAjaxError);
    };

    return {
        /**
         * Initialize the canvas toolbox.
         */
        init: function () {
            var toolbox = $('#toolbox');

            // ensure the user can create graph components
            if (nf.Common.isDFM()) {

                // create the draggable icons
                addToolboxIcon(config.type.processor, toolbox, 'processor-icon', 'processor-icon-hover', 'processor-icon-drag', promptForProcessorType);
                addToolboxIcon(config.type.inputPort, toolbox, 'input-port-icon', 'input-port-icon-hover', 'input-port-icon-drag', promptForInputPortName);
                addToolboxIcon(config.type.outputPort, toolbox, 'output-port-icon', 'output-port-icon-hover', 'output-port-icon-drag', promptForOutputPortName);
                addToolboxIcon(config.type.processGroup, toolbox, 'process-group-icon', 'process-group-icon-hover', 'process-group-icon-drag', nf.CanvasToolbox.promptForGroupName);
                addToolboxIcon(config.type.remoteProcessGroup, toolbox, 'remote-process-group-icon', 'remote-process-group-icon-hover', 'remote-process-group-icon-drag', promptForRemoteProcessGroupUri);
                addToolboxIcon(config.type.funnel, toolbox, 'funnel-icon', 'funnel-icon-hover', 'funnel-icon-drag', createFunnel);
                addToolboxIcon(config.type.template, toolbox, 'template-icon', 'template-icon-hover', 'template-icon-drag', promptForTemplate);
                addToolboxIcon(config.type.label, toolbox, 'label-icon', 'label-icon-hover', 'label-icon-drag', createLabel);

                // specify the combo options
                $('#processor-type-filter-options').combo({
                    options: [{
                            text: 'by type',
                            value: 'label'
                        }, {
                            text: 'by tag',
                            value: 'tags'
                        }],
                    select: function (option) {
                        applyFilter();
                    }
                });

                // initialize the processor type table
                var processorTypesColumns = [
                    {id: 'type', name: 'Type', field: 'label', sortable: true, resizable: true},
                    {id: 'tags', name: 'Tags', field: 'tags', sortable: true, resizable: true}
                ];
                var processorTypesOptions = {
                    forceFitColumns: true,
                    enableTextSelectionOnCells: true,
                    enableCellNavigation: true,
                    enableColumnReorder: false,
                    autoEdit: false,
                    multiSelect: false
                };

                // initialize the dataview
                var processorTypesData = new Slick.Data.DataView({
                    inlineFilters: false
                });
                processorTypesData.setItems([]);
                processorTypesData.setFilterArgs({
                    searchString: getFilterText(),
                    property: $('#processor-type-filter-options').combo('getSelectedOption').value
                });
                processorTypesData.setFilter(filter);

                // initialize the sort
                sort({
                    columnId: 'type',
                    sortAsc: true
                }, processorTypesData);

                // initialize the grid
                var processorTypesGrid = new Slick.Grid('#processor-types-table', processorTypesData, processorTypesColumns, processorTypesOptions);
                processorTypesGrid.setSelectionModel(new Slick.RowSelectionModel());
                processorTypesGrid.registerPlugin(new Slick.AutoTooltips());
                processorTypesGrid.setSortColumn('type', true);
                processorTypesGrid.onSort.subscribe(function (e, args) {
                    sort({
                        columnId: args.sortCol.field,
                        sortAsc: args.sortAsc
                    }, processorTypesData);
                });
                processorTypesGrid.onSelectedRowsChanged.subscribe(function (e, args) {
                    if ($.isArray(args.rows) && args.rows.length === 1) {
                        var processorTypeIndex = args.rows[0];
                        var processorType = processorTypesGrid.getDataItem(processorTypeIndex);

                        // set the processor type description
                        if (nf.Common.isBlank(processorType.description)) {
                            $('#processor-type-description').attr('title', '').html('<span class="unset">No description specified</span>');
                        } else {
                            $('#processor-type-description').html(processorType.description).ellipsis();
                        }

                        // populate the dom
                        $('#processor-type-name').text(processorType.label).ellipsis();
                        $('#selected-processor-name').text(processorType.label);
                        $('#selected-processor-type').text(processorType.type);
                    }
                });

                // hold onto an instance of the grid
                $('#processor-types-table').data('gridInstance', processorTypesGrid);

                // load the available processor types, this select is shown in the
                // new processor dialog when a processor is dragged onto the screen
                $.ajax({
                    type: 'GET',
                    url: config.urls.processorTypes,
                    dataType: 'json'
                }).done(function (response) {
                    var tagCloud = {};
                    var tags = [];

                    // begin the update
                    processorTypesData.beginUpdate();

                    // go through each processor type
                    $.each(response.processorTypes, function (i, documentedType) {
                        var type = documentedType.type;

                        // create the row for the processor type
                        processorTypesData.addItem({
                            id: i,
                            label: nf.Common.substringAfterLast(type, '.'),
                            type: type,
                            description: nf.Common.escapeHtml(documentedType.description),
                            tags: documentedType.tags.join(', ')
                        });


                        // count the frequency of each tag for this type
                        $.each(documentedType.tags, function (i, tag) {
                            var normalizedTagName = tag.toLowerCase();

                            if (nf.Common.isDefinedAndNotNull(tagCloud[normalizedTagName])) {
                                tagCloud[normalizedTagName].count = tagCloud[normalizedTagName].count + 1;
                            } else {
                                var tagCloudEntry = {
                                    term: normalizedTagName,
                                    count: 1
                                };
                                tags.push(tagCloudEntry);
                                tagCloud[normalizedTagName] = tagCloudEntry;
                            }
                        });
                    });

                    // end the udpate
                    processorTypesData.endUpdate();

                    // set the total number of processors
                    $('#total-processor-types, #displayed-processor-types').text(response.processorTypes.length);

                    // handle the case when no tags are present
                    if (tags.length > 0) {
                        // sort the tags by frequency to limit the less frequent tags
                        tags.sort(function (a, b) {
                            return b.count - a.count;
                        });

                        // limit to the most frequest tags
                        if (tags.length > config.maxTags) {
                            tags = tags.slice(0, config.maxTags);
                        }

                        // determine the max frequency
                        var maxFrequency = tags[0].count;

                        // sort the tags alphabetically
                        tags.sort(function (a, b) {
                            var compA = a.term.toUpperCase();
                            var compB = b.term.toUpperCase();
                            return (compA < compB) ? -1 : (compA > compB) ? 1 : 0;
                        });

                        // set the tag content
                        $.each(tags, function (i, tag) {
                            // determine the appropriate font size
                            var fontSize = Math.log(tag.count) / Math.log(maxFrequency) * (config.maxTagFontSize - config.minTagFontSize) + config.minTagFontSize;
                            var minWidth = config.minWidth * fontSize;

                            // create the tag cloud entry
                            $('<li></li>').append($('<span class="link"></span>').text(tag.term).css({
                                'font-size': fontSize + 'em'
                            })).css({
                                'min-width': minWidth + 'px'
                            }).click(function () {
                                // ensure we don't exceed 5 selected
                                if ($('#tag-filter').children('li').length < 5) {
                                    var tagText = $(this).children('span').text();
                                    addTagFilter(tagText);
                                }
                            }).appendTo('#tag-cloud').ellipsis();
                        });
                    } else {
                        // indicate when no tags are found
                        $('<li><span class="unset">No tags specified</span></li>').appendTo('#tag-cloud');
                    }

                    // wire up the dataview to the grid
                    processorTypesData.onRowCountChanged.subscribe(function (e, args) {
                        processorTypesGrid.updateRowCount();
                        processorTypesGrid.render();

                        // update the total number of displayed processors
                        $('#displayed-processor-types').text(args.current);
                    });
                    processorTypesData.onRowsChanged.subscribe(function (e, args) {
                        processorTypesGrid.invalidateRows(args.rows);
                        processorTypesGrid.render();
                    });
                    processorTypesData.syncGridSelection(processorTypesGrid, false);
                }).fail(nf.Common.handleAjaxError);

                // define the function for filtering the list
                $('#processor-type-filter').keyup(function () {
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

                // configure the new processor dialog
                $('#new-processor-dialog').modal({
                    headerText: 'Add Processor',
                    overlayBackground: false
                });

                // configure the new port dialog
                $('#new-port-dialog').modal({
                    headerText: 'Add Port',
                    overlayBackground: false,
                    handler: {
                        close: function () {
                            $('#new-port-name').val('');
                        }
                    }
                });

                // configure the new process group dialog
                $('#new-process-group-dialog').modal({
                    headerText: 'Add Process Group',
                    overlayBackground: false,
                    handler: {
                        close: function () {
                            $('#new-process-group-name').val('');
                        }
                    }
                });

                // configure the new remote process group dialog
                $('#new-remote-process-group-dialog').modal({
                    headerText: 'Add Remote Process Group',
                    overlayBackground: false,
                    handler: {
                        close: function () {
                            $('#new-remote-process-group-uri').val('');
                        }
                    }
                });

                // configure the instantiate template dialog
                $('#instantiate-template-dialog').modal({
                    headerText: 'Instantiate Template',
                    overlayBackgroud: false
                });
            } else {
                // add disabled icons
                $('<div/>').attr('title', config.type.processor).addClass('processor-icon-disable').addClass('toolbox-icon').appendTo(toolbox);
                $('<div/>').attr('title', config.type.inputPort).addClass('input-port-icon-disable').addClass('toolbox-icon').appendTo(toolbox);
                $('<div/>').attr('title', config.type.outputPort).addClass('output-port-icon-disable').addClass('toolbox-icon').appendTo(toolbox);
                $('<div/>').attr('title', config.type.processGroup).addClass('process-group-icon-disable').addClass('toolbox-icon').appendTo(toolbox);
                $('<div/>').attr('title', config.type.remoteProcessGroup).addClass('remote-process-group-icon-disable').addClass('toolbox-icon').appendTo(toolbox);
                $('<div/>').attr('title', config.type.funnel).addClass('funnel-icon-disable').addClass('toolbox-icon').appendTo(toolbox);
                $('<div/>').attr('title', config.type.template).addClass('template-icon-disable').addClass('toolbox-icon').appendTo(toolbox);
                $('<div/>').attr('title', config.type.label).addClass('label-icon-disable').addClass('toolbox-icon').appendTo(toolbox);
            }
        },
        
        /**
         * Prompts the user to enter the name for the group.
         * 
         * @argument {object} pt        The point that the group was dropped
         */
        promptForGroupName: function (pt) {
            return $.Deferred(function (deferred) {
                var addGroup = function () {
                    // get the name of the group and clear the textfield
                    var groupName = $('#new-process-group-name').val();

                    // hide the dialog
                    $('#new-process-group-dialog').modal('hide');

                    // create the group and resolve the deferred accordingly
                    createGroup(groupName, pt).done(function (response) {
                        deferred.resolve(response.processGroup);
                    }).fail(function () {
                        deferred.reject();
                    });
                };

                $('#new-process-group-dialog').modal('setButtonModel', [{
                        buttonText: 'Add',
                        handler: {
                            click: addGroup
                        }
                    }, {
                        buttonText: 'Cancel',
                        handler: {
                            click: function () {
                                // reject the deferred
                                deferred.reject();

                                // close the dialog
                                $('#new-process-group-dialog').modal('hide');
                            }
                        }
                    }]);

                // show the dialog
                $('#new-process-group-dialog').modal('show');

                // set up the focus and key handlers
                $('#new-process-group-name').focus().off('keyup').on('keyup', function (e) {
                    var code = e.keyCode ? e.keyCode : e.which;
                    if (code === $.ui.keyCode.ENTER) {
                        addGroup();
                    }
                });
            }).promise();
        }
    };
}());
