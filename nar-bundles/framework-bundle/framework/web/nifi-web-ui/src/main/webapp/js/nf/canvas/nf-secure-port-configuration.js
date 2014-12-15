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
nf.SecurePortConfiguration = (function () {

    var portUri = '';

    var config = {
        search: 'User DNs, groups, etc'
    };

    /**
     * Initializes the port dialog.
     */
    var initPortConfigurationDialog = function () {
        // initialize the properties tabs
        $('#secure-port-configuration-tabs').tabbs({
            tabStyle: 'tab',
            selectedTabStyle: 'selected-tab',
            tabs: [{
                    name: 'Settings',
                    tabContentId: 'secure-port-settings-tab-content'
                }, {
                    name: 'Access Control',
                    tabContentId: 'secure-port-access-control-tab-content'
                }]
        });

        // initialize the dialog
        $('#secure-port-configuration').modal({
            headerText: 'Configure Secure Port',
            overlayBackground: true,
            buttons: [{
                    buttonText: 'Apply',
                    handler: {
                        click: function () {
                            var portId = $('#secure-port-id').text();
                            var portType = $('#secure-port-type').text();

                            var portDto = {};
                            portDto['id'] = portId;
                            portDto['name'] = $('#secure-port-name').val();
                            portDto['comments'] = $('#secure-port-comments').val();
                            portDto['groupAccessControl'] = getAllowedGroups();
                            portDto['userAccessControl'] = getAllowedUsers();

                            // include the concurrent tasks if appropriate
                            if ($('#secure-port-concurrent-task-container').is(':visible')) {
                                portDto['concurrentlySchedulableTaskCount'] = $('#secure-port-concurrent-tasks').val();
                            }

                            // mark the processor disabled if appropriate
                            if ($('#secure-port-enabled').hasClass('checkbox-unchecked')) {
                                portDto['state'] = 'DISABLED';
                            } else if ($('#secure-port-enabled').hasClass('checkbox-checked')) {
                                portDto['state'] = 'STOPPED';
                            }

                            var portEntity = {};
                            portEntity['revision'] = nf.Client.getRevision();
                            portEntity[portType] = portDto;

                            // update the selected component
                            $.ajax({
                                type: 'PUT',
                                data: JSON.stringify(portEntity),
                                contentType: 'application/json',
                                url: portUri,
                                dataType: 'json'
                            }).then(function (response) {
                                // update the revision
                                nf.Client.setRevision(response.revision);

                                var port;
                                if (nf.Common.isDefinedAndNotNull(response.inputPort)) {
                                    port = response.inputPort;
                                } else {
                                    port = response.outputPort;
                                }

                                // refresh the port component
                                nf.Port.set(port);

                                // close the details panel
                                $('#secure-port-configuration').modal('hide');
                            }, function (xhr, status, error) {
                                // close the details panel
                                $('#secure-port-configuration').modal('hide');

                                // handle the error
                                nf.Common.handleAjaxError(xhr, status, error);
                            });
                        }
                    }
                }, {
                    buttonText: 'Cancel',
                    handler: {
                        click: function () {
                            $('#secure-port-configuration').modal('hide');
                        }
                    }
                }],
            handler: {
                close: function () {
                    portUri = '';

                    // clear the port details
                    $('#secure-port-id').text('');
                    $('#secure-port-type').text('');
                    $('#secure-port-name').val('');
                    $('#secure-port-enabled').removeClass('checkbox-unchecked checkbox-checked');
                    $('#secure-port-concurrent-tasks').val('');
                    $('#secure-port-comments').val('');
                    $('#allowed-users').empty();
                    $('#allowed-groups').empty();
                }
            }
        });

        // listen for removal requests
        $(document).on('click', 'div.remove-allowed-entity', function () {
            $(this).closest('li').remove();
            $(this).closest('ul').sortable('refresh');
        });

        // initialize the access control auto complete
        $.widget('nf.userSearchAutocomplete', $.ui.autocomplete, {
            _normalize: function(searchResults) {
                var items = [];
                items.push(searchResults);
                return items;
            },
            _resizeMenu: function () {
                var ul = this.menu.element;
                ul.width(700);
            },
            _renderMenu: function (ul, items) {
                var self = this;

                // results are normalized into an array
                var results = items[0];

                // show all groups not currently selected
                if (!nf.Common.isEmpty(results.userGroupResults)) {
                    var allowedGroups = getAllowedGroups();
                    var groupHeaderAdded = false;

                    // go through each group result
                    $.each(results.userGroupResults, function (i, groupMatch) {

                        // see if this match is not already selected
                        if ($.inArray(groupMatch.group, allowedGroups) === -1) {

                            // only add the header for the first non selected matching group
                            if (!groupHeaderAdded) {
                                ul.append('<li class="search-users-header">Groups</li>');
                                groupHeaderAdded = true;
                            }

                            // add the group match
                            self._renderGroupItem(ul, groupMatch);
                        }
                    });
                }

                // show all users not currently selected
                if (!nf.Common.isEmpty(results.userResults)) {
                    var allowedUsers = getAllowedUsers();
                    var userHeaderAdded = false;

                    // go through each user result
                    $.each(results.userResults, function (i, userMatch) {

                        // see if this match is not already selected
                        if ($.inArray(userMatch.userDn, allowedUsers) === -1) {

                            // only add the header for the first non selected matching user
                            if (!userHeaderAdded) {
                                ul.append('<li class="search-users-header">Users</li>');
                                userHeaderAdded = true;
                            }

                            // add the user match
                            self._renderUserItem(ul, userMatch);
                        }
                    });
                }

                // ensure there were some results
                if (ul.children().length === 0) {
                    ul.append('<li class="unset search-users-no-matches">No users or groups match</li>');
                }
            },
            _renderGroupItem: function (ul, groupMatch) {
                var groupContent = $('<a></a>').append($('<div class="search-users-match-header"></div>').text(groupMatch.group));
                return $('<li></li>').data('ui-autocomplete-item', groupMatch).append(groupContent).appendTo(ul);
            },
            _renderUserItem: function (ul, userMatch) {
                var userContent = $('<a></a>').append($('<div class="search-users-match-header"></div>').text(userMatch.userDn));
                return $('<li></li>').data('ui-autocomplete-item', userMatch).append(userContent).appendTo(ul);
            }
        });

        // configure the autocomplete field
        $('#secure-port-access-control').userSearchAutocomplete({
            minLength: 0,
            appendTo: '#search-users-results',
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
                    url: '../nifi-api/controller/users/search-results'
                }).done(function (searchResponse) {
                    response(searchResponse);
                });
            },
            select: function (event, ui) {
                var item = ui.item;

                // add the item appropriately
                if (nf.Common.isDefinedAndNotNull(item.group)) {
                    addAllowedGroup(item.group);
                } else {
                    addAllowedUser(item.userDn);
                }

                // blur the search field
                $(this).blur();

                // stop event propagation
                return false;
            }
        }).focus(function () {
            // conditionally clear the text for the user to type
            if ($(this).val() === config.search) {
                $(this).val('').removeClass('search-users');
            }
        }).blur(function () {
            $(this).val(config.search).addClass('search-users');
        }).val(config.search).addClass('search-users');
    };

    /**
     * Adds the specified user to the list of allowed users.
     * 
     * @argument {string} allowedUser       The allowed user dn
     */
    var addAllowedUser = function (allowedUser) {
        var allowedUsers = $('#allowed-users');

        // append the user
        var user = $('<span></span>').addClass('allowed-entity ellipsis').text(allowedUser).ellipsis();
        var userAction = $('<div></div>').addClass('remove-allowed-entity');
        $('<li></li>').data('user', allowedUser).append(user).append(userAction).appendTo(allowedUsers);
    };

    /**
     * Adds the specified group to the list of allowed groups.
     * 
     * @argument {string} allowedGroup      The allowed group name
     */
    var addAllowedGroup = function (allowedGroup) {
        var allowedGroups = $('#allowed-groups');

        // append the group
        var group = $('<span></span>').addClass('allowed-entity ellipsis').text(allowedGroup).ellipsis();
        var groupAction = $('<div></div>').addClass('remove-allowed-entity');
        $('<li></li>').data('group', allowedGroup).append(group).append(groupAction).appendTo(allowedGroups);
    };

    /**
     * Gets the currently selected allowed users.
     */
    var getAllowedUsers = function () {
        var allowedUsers = [];
        $('#allowed-users').children('li').each(function (_, allowedUser) {
            var user = $(allowedUser).data('user');
            if (nf.Common.isDefinedAndNotNull(user)) {
                allowedUsers.push(user);
            }
        });
        return allowedUsers;
    };

    /**
     * Gets the currently selected allowed groups.
     */
    var getAllowedGroups = function () {
        var allowedGroups = [];
        $('#allowed-groups').children('li').each(function (_, allowedGroup) {
            var group = $(allowedGroup).data('group');
            if (nf.Common.isDefinedAndNotNull(group)) {
                allowedGroups.push(group);
            }
        });
        return allowedGroups;
    };

    return {
        init: function () {
            initPortConfigurationDialog();
        },
        /**
         * Shows the details for the port specified selection.
         * 
         * @argument {selection} selection      The selection
         */
        showConfiguration: function (selection) {
            // if the specified component is a port, load its properties
            if (nf.CanvasUtils.isInputPort(selection) || nf.CanvasUtils.isOutputPort(selection)) {
                var selectionData = selection.datum();

                // determine the port type
                if (selectionData.component.type === 'INPUT_PORT') {
                    $('#secure-port-type').text('inputPort');
                } else {
                    $('#secure-port-type').text('outputPort');
                }

                // store the uri
                portUri = selectionData.component.uri;

                // show concurrent tasks for root groups only
                if (nf.Canvas.getParentGroupId() === null) {
                    $('#secure-port-concurrent-task-container').show();
                } else {
                    $('#secure-port-concurrent-task-container').hide();
                }

                // determine if the enabled checkbox is checked or not
                var portEnableStyle = 'checkbox-checked';
                if (selectionData.component.state === 'DISABLED') {
                    portEnableStyle = 'checkbox-unchecked';
                }

                // populate the port settings
                $('#secure-port-id').text(selectionData.component.id);
                $('#secure-port-name').val(selectionData.component.name);
                $('#secure-port-enabled').removeClass('checkbox-unchecked checkbox-checked').addClass(portEnableStyle);
                $('#secure-port-concurrent-tasks').val(selectionData.component.concurrentlySchedulableTaskCount);
                $('#secure-port-comments').val(selectionData.component.comments);

                // add allowed users
                $.each(selectionData.component.userAccessControl, function (_, allowedUser) {
                    addAllowedUser(allowedUser);
                });

                // add allowed groups
                $.each(selectionData.component.groupAccessControl, function (_, allowedGroup) {
                    addAllowedGroup(allowedGroup);
                });

                // show the details
                $('#secure-port-configuration').modal('show');
            }
        }
    };
}());