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
nf.ConnectionDetails = (function () {

    /**
     * Initialize the details for the source of the connection.
     * 
     * @argument {string} groupId               The id of the current group
     * @argument {string} groupName             The name of the current group
     * @argument {object} source        The source of the connection
     */
    var initializeConnectionSource = function (groupId, groupName, source) {
        if (source.type === 'PROCESSOR') {
            return initializeSourceProcessor(groupId, groupName, source);
        } else if (source.type === 'FUNNEL') {
            return initializeSourceFunnel(groupId, groupName, source);
        } else if (source.type === 'REMOTE_OUTPUT_PORT') {
            return initializeRemoteSourcePort(groupId, groupName, source);
        } else {
            return initializeLocalSourcePort(groupId, groupName, source);
        }
    };

    /**
     * Initialize the details for the source processor.
     * 
     * @argument {string} groupId               The id of the current group
     * @argument {string} groupName             The name of the current group
     * @argument {object} source            The source of the connection
     */
    var initializeSourceProcessor = function (groupId, groupName, source) {
        return $.ajax({
            type: 'GET',
            url: '../nifi-api/controller/process-groups/' + encodeURIComponent(groupId) + '/processors/' + encodeURIComponent(source.id),
            dataType: 'json'
        }).done(function (response) {
            var processor = response.processor;
            var processorName = $('<div class="label"></div>').text(processor.name);
            var processorType = $('<div></div>').text(nf.Common.substringAfterLast(processor.type, '.'));

            // populate source processor details
            $('#read-only-connection-source-label').text('From processor');
            $('#read-only-connection-source').append(processorName).append(processorType);
            $('#read-only-connection-source-group-name').text(groupName);
        });
    };

    /**
     * Initialize the details for the source funnel.
     * 
     * @argument {string} groupId               The id of the current group
     * @argument {string} groupName             The name of the current group
     * @argument {object} source            The source of the connection
     */
    var initializeSourceFunnel = function (groupId, groupName, source) {
        return $.Deferred(function (deferred) {
            $('#read-only-connection-source-label').text('From funnel');
            $('#read-only-connection-source').append('funnel');
            $('#read-only-connection-source-group-name').text(groupName);
            deferred.resolve();
        }).promise();
    };

    /**
     * Initialize the details for the remote source port.
     * 
     * @argument {string} groupId               The id of the current group
     * @argument {string} groupName             The name of the current group
     * @argument {object} source            The source of the connection
     */
    var initializeRemoteSourcePort = function (groupId, groupName, source) {
        return $.ajax({
            type: 'GET',
            url: '../nifi-api/controller/process-groups/' + encodeURIComponent(groupId) + '/remote-process-groups/' + encodeURIComponent(source.groupId),
            data: {
                verbose: true
            },
            dataType: 'json'
        }).done(function (response) {
            var remoteProcessGroup = response.remoteProcessGroup;

            // populate source port details
            $('#read-only-connection-source-label').text('From output');
            $('#read-only-connection-source').text(source.name);
            $('#read-only-connection-source-group-name').text(remoteProcessGroup.name);
        });
    };

    /**
     * Initialize the details for the source port.
     * 
     * @argument {string} groupId               The id of the current group
     * @argument {string} groupName             The name of the current group
     * @argument {object} source            The source of the connection
     */
    var initializeLocalSourcePort = function (groupId, groupName, source) {
        return $.Deferred(function (deferred) {
            if (groupId === source.groupId) {
                // populate source port details
                $('#read-only-connection-source-label').text('From input');
                $('#read-only-connection-source').text(source.name);
                $('#read-only-connection-source-group-name').text(groupName);

                deferred.resolve();
            } else {
                $.ajax({
                    type: 'GET',
                    url: '../nifi-api/controller/process-groups/' + encodeURIComponent(source.groupId),
                    data: {
                        verbose: true
                    },
                    dataType: 'json'
                }).done(function (response) {
                    var processGroup = response.processGroup;

                    // populate source port details
                    $('#read-only-connection-source-label').text('From output');
                    $('#read-only-connection-source').text(source.name);
                    $('#read-only-connection-source-group-name').text(processGroup.name);

                    deferred.resolve();
                }).fail(function () {
                    deferred.reject();
                });
            }
        }).promise();
    };

    /**
     * Initialize the details for the destination of the connection.
     * 
     * @argument {string} groupId               The id of the current group
     * @argument {string} groupName             The name of the current group
     * @argument {object} destination            The destination of the connection
     */
    var initializeConnectionDestination = function (groupId, groupName, destination) {
        if (destination.type === 'PROCESSOR') {
            return initializeDestinationProcessor(groupId, groupName, destination);
        } else if (destination.type === 'FUNNEL') {
            return initializeDestinationFunnel(groupId, groupName, destination);
        } else if (destination.type === 'REMOTE_INPUT_PORT') {
            return initializeDestinationRemotePort(groupId, groupName, destination);
        } else {
            return initializeDestinationLocalPort(groupId, groupName, destination);
        }
    };

    /**
     * Initialize the details for the destination processor.
     * 
     * @argument {string} groupId               The id of the current group
     * @argument {string} groupName             The name of the current group
     * @argument {object} destination            The destination of the connection
     */
    var initializeDestinationProcessor = function (groupId, groupName, destination) {
        return $.Deferred(function (deferred) {
            $.ajax({
                type: 'GET',
                url: '../nifi-api/controller/process-groups/' + encodeURIComponent(groupId) + '/processors/' + encodeURIComponent(destination.id),
                dataType: 'json'
            }).done(function (response) {
                var processor = response.processor;
                var processorName = $('<div class="label"></div>').text(processor.name);
                var processorType = $('<div></div>').text(nf.Common.substringAfterLast(processor.type, '.'));

                // populate destination processor details
                $('#read-only-connection-target-label').text('To processor');
                $('#read-only-connection-target').append(processorName).append(processorType);
                $('#read-only-connection-target-group-name').text(groupName);

                deferred.resolve();
            }).fail(function () {
                deferred.reject();
            });
        }).promise();
    };

    /**
     * Initialize the details for the source funnel.
     * 
     * @argument {string} groupId               The id of the current group
     * @argument {string} groupName             The name of the current group
     * @argument {object} destination            The destination of the connection
     */
    var initializeDestinationFunnel = function (groupId, groupName, destination) {
        return $.Deferred(function (deferred) {
            $('#read-only-connection-target-label').text('To funnel');
            $('#read-only-connection-target').append('funnel');
            $('#read-only-connection-target-group-name').text(groupName);
            deferred.resolve();
        }).promise();
    };

    /**
     * Initialize the details for the remote source port.
     * 
     * @argument {string} groupId               The id of the current group
     * @argument {string} groupName             The name of the current group
     * @argument {object} destination            The destination of the connection
     */
    var initializeDestinationRemotePort = function (groupId, groupName, destination) {
        return $.ajax({
            type: 'GET',
            url: '../nifi-api/controller/process-groups/' + encodeURIComponent(groupId) + '/remote-process-groups/' + encodeURIComponent(destination.groupId),
            data: {
                verbose: true
            },
            dataType: 'json'
        }).done(function (response) {
            var remoteProcessGroup = response.remoteProcessGroup;

            // populate source port details
            $('#read-only-connection-target-label').text('To input');
            $('#read-only-connection-target').text(destination.name);
            $('#read-only-connection-target-group-name').text(remoteProcessGroup.name);
        });
    };

    /**
     * Initialize the details for the destination port.
     * 
     * @argument {string} groupId               The id of the current group
     * @argument {string} groupName             The name of the current group
     * @argument {object} destination            The destination of the connection
     */
    var initializeDestinationLocalPort = function (groupId, groupName, destination) {
        return $.Deferred(function (deferred) {
            if (groupId === destination.groupId) {
                // populate destination port details
                $('#read-only-connection-target-label').text('To output');
                $('#read-only-connection-target').text(destination.name);
                $('#read-only-connection-target-group-name').text(groupName);

                deferred.resolve();
            } else {
                $.ajax({
                    type: 'GET',
                    url: '../nifi-api/controller/process-groups/' + encodeURIComponent(destination.groupId),
                    data: {
                        verbose: true
                    },
                    dataType: 'json'
                }).done(function (response) {
                    var processGroup = response.processGroup;

                    // populate destination port details
                    $('#read-only-connection-target-label').text('To input');
                    $('#read-only-connection-target').text(destination.name);
                    $('#read-only-connection-target-group-name').text(processGroup.name);

                    deferred.resolve();
                }).fail(function () {
                    deferred.reject();
                });
            }
        }).promise();
    };


    /**
     * Creates the relationship option for the specified relationship.
     * 
     * @argument {string} name      The relationship name
     */
    var createRelationshipOption = function (name) {
        $('<div class="available-relationship-container"></div>').append(
                $('<div class="relationship-name"></div>').text(name)).appendTo('#read-only-relationship-names');
    };

    return {
        /**
         * Initializes the connection details dialog.
         * 
         * @param {boolean} overlayBackground       Whether to overlay the background
         */
        init: function (overlayBackground) {
            overlayBackground = nf.Common.isDefinedAndNotNull(overlayBackground) ? overlayBackground : true;

            // initialize the details tabs
            $('#connection-details-tabs').tabbs({
                tabStyle: 'tab',
                selectedTabStyle: 'selected-tab',
                tabs: [{
                        name: 'Details',
                        tabContentId: 'read-only-connection-details-tab-content'
                    }, {
                        name: 'Settings',
                        tabContentId: 'read-only-connection-settings-tab-content'
                    }]
            });

            // configure the connection details dialog
            $('#connection-details').modal({
                headerText: 'Connection Details',
                overlayBackground: overlayBackground,
                buttons: [{
                        buttonText: 'Ok',
                        handler: {
                            click: function () {
                                $('#connection-details').modal('hide');
                            }
                        }
                    }],
                handler: {
                    close: function () {
                        // clear the relationship names
                        $('#read-only-relationship-names').empty();

                        // clear the connection details
                        nf.Common.clearField('read-only-connection-name');
                        nf.Common.clearField('read-only-connection-id');

                        // clear the connection source details
                        $('#read-only-connection-source-label').text('');
                        $('#read-only-connection-source').empty();
                        $('#read-only-connection-source-group-name').text('');

                        // clear the connection target details
                        $('#read-only-connection-target-label').text('');
                        $('#read-only-connection-target').empty();
                        $('#read-only-connection-target-group-name').text('');

                        // clear the relationship details
                        $('#read-only-relationship-names').css('border-width', '0').empty();

                        // clear the connection settings
                        $('#read-only-flow-file-expiration').text('');
                        $('#read-only-back-pressure-object-threshold').text('');
                        $('#read-only-back-pressure-data-size-threshold').text('');
                        $('#read-only-prioritizers').empty();
                    }
                }
            });
        },
        
        /**
         * Shows the details for the specified edge.
         * 
         * @argument {string} groupId           The group id
         * @argument {string} connectionId      The connection id
         */
        showDetails: function (groupId, connectionId) {
            // get the group details
            var groupXhr = $.ajax({
                type: 'GET',
                url: '../nifi-api/controller/process-groups/' + encodeURIComponent(groupId),
                dataType: 'json'
            });

            // get the connection details
            var connectionXhr = $.ajax({
                type: 'GET',
                url: '../nifi-api/controller/process-groups/' + encodeURIComponent(groupId) + '/connections/' + encodeURIComponent(connectionId),
                dataType: 'json'
            });

            // populate the dialog once get have all necessary details
            $.when(groupXhr, connectionXhr).done(function (groupResult, connectionResult) {
                var groupResponse = groupResult[0];
                var connectionResponse = connectionResult[0];

                if (nf.Common.isDefinedAndNotNull(groupResponse.processGroup) && nf.Common.isDefinedAndNotNull(connectionResponse.connection)) {
                    var processGroup = groupResponse.processGroup;
                    var connection = connectionResponse.connection;

                    // process the source
                    var connectionSource = initializeConnectionSource(processGroup.id, processGroup.name, connection.source);

                    // process the destination
                    var connectionDestination = initializeConnectionDestination(processGroup.id, processGroup.name, connection.destination);

                    // finish populating the dialog once the source and destination have been loaded
                    $.when(connectionSource, connectionDestination).done(function () {
                        // process the relationships
                        var availableRelationships = connection.availableRelationships;
                        var selectedRelationships = connection.selectedRelationships;

                        // show the available relationship if applicable
                        if (nf.Common.isDefinedAndNotNull(availableRelationships) || nf.Common.isDefinedAndNotNull(selectedRelationships)) {
                            // populate the available connections
                            $.each(availableRelationships, function (i, name) {
                                createRelationshipOption(name);
                            });

                            // ensure all selected relationships are present
                            // (may be undefined) and selected
                            $.each(selectedRelationships, function (i, name) {
                                // mark undefined relationships accordingly
                                if ($.inArray(name, availableRelationships) === -1) {
                                    var option = createRelationshipOption(name);
                                    $(option).children('div.relationship-name').addClass('undefined');
                                }

                                // ensure all selected relationships are bold
                                var relationships = $('#read-only-relationship-names').children('div');
                                $.each(relationships.children('div.relationship-name'), function (i, relationshipNameElement) {
                                    var relationshipName = $(relationshipNameElement);
                                    if (relationshipName.text() === name) {
                                        relationshipName.css('font-weight', 'bold');
                                    }
                                });
                            });

                            $('#selected-relationship-text').show();
                            $('#read-only-relationship-names-container').show();
                        } else {
                            $('#selected-relationship-text').hide();
                            $('#read-only-relationship-names-container').hide();
                        }

                        // set the connection details
                        nf.Common.populateField('read-only-connection-name', connection.name);
                        nf.Common.populateField('read-only-connection-id', connection.id);
                        nf.Common.populateField('read-only-flow-file-expiration', connection.flowFileExpiration);
                        nf.Common.populateField('read-only-back-pressure-object-threshold', connection.backPressureObjectThreshold);
                        nf.Common.populateField('read-only-back-pressure-data-size-threshold', connection.backPressureDataSizeThreshold);

                        // prioritizers
                        if (nf.Common.isDefinedAndNotNull(connection.prioritizers) && connection.prioritizers.length > 0) {
                            var prioritizerList = $('<ol></ol>').css('list-style', 'decimal inside none');
                            $.each(connection.prioritizers, function (i, type) {
                                prioritizerList.append($('<li></li>').text(nf.Common.substringAfterLast(type, '.')));
                            });
                            $('#read-only-prioritizers').append(prioritizerList);
                        } else {
                            var noValueSet = $('<span class="unset">No value set</span>');
                            $('#read-only-prioritizers').append(noValueSet);
                        }

                        // select the first tab
                        $('#connection-details-tabs').find('li:first').click();

                        // show the dialog
                        $('#connection-details').modal('show');

                        // show the border if necessary
                        var relationshipNames = $('#read-only-relationship-names');
                        if (relationshipNames.is(':visible') && relationshipNames.get(0).scrollHeight > relationshipNames.innerHeight()) {
                            relationshipNames.css('border-width', '1px');
                        }
                    });
                }
            }).fail(nf.Common.handleAjaxError);
        }
    };
}());