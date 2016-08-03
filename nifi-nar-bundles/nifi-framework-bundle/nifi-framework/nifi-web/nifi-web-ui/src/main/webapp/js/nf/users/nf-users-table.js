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

/* global nf, Slick */

nf.UsersTable = (function () {

    /**
     * Configuration object used to hold a number of configuration items.
     */
    var config = {
        urls: {
            users: '../nifi-api/tenants/users',
            userGroups: '../nifi-api/tenants/user-groups'
        }
    };

    var initUserDeleteDialog = function () {
        $('#user-delete-dialog').modal({
            headerText: 'Delete User',
            buttons: [{
                buttonText: 'Delete',
                color: {
                    base: '#728E9B',
                    hover: '#004849',
                    text: '#ffffff'
                },
                handler: {
                    click: function () {
                        var userId = $('#user-id-delete-dialog').val();

                        // get the user
                        var usersGrid = $('#users-table').data('gridInstance');
                        var usersData = usersGrid.getData();
                        var user = usersData.getItemById(userId);

                        // update the user
                        $.ajax({
                            type: 'DELETE',
                            url: user.uri + '?' + $.param(nf.Client.getRevision(user)),
                            dataType: 'json'
                        }).done(function () {
                            nf.UsersTable.loadUsersTable();
                        }).fail(nf.Common.handleAjaxError);

                        // hide the dialog
                        $('#user-delete-dialog').modal('hide');
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
                        $('#user-delete-dialog').modal('hide');
                    }
                }
            }],
            handler: {
                close: function () {
                    // clear the current user
                    $('#user-id-delete-dialog').val('');
                    $('#user-name-delete-dialog').text('');
                }
            }
        });
    };

    /**
     * Gets the currently selected groups.
     *
     * @returns {Array} groups
     */
    var getSelectedGroups = function () {
        var selectedGroups = [];
        $('#available-groups div.group-check').filter(function () {
            return $(this).hasClass('checkbox-checked');
        }).each(function () {
            var id = $(this).next('span.group-id').text();
            selectedGroups.push({
                'id': id
            });
        });
        return selectedGroups;
    };

    /**
     * Gets the currently selected users.
     *
     * @returns {Array} users
     */
    var getSelectedUsers = function () {
        var selectedUsers = [];
        $('#available-users div.user-check').filter(function () {
            return $(this).hasClass('checkbox-checked');
        }).each(function () {
            var id = $(this).next('span.user-id').text();
            selectedUsers.push({
                'id': id
            });
        });
        return selectedUsers;
    };

    /**
     * Adds the specified user to the specified group.
     *
     * @param groupEntity
     * @param userEntity
     * @returns xhr
     */
    var addUserToGroup = function (groupEntity, userEntity) {
        var groupMembers = [];

        // get all the current users
        $.each(groupEntity.component.users, function (_, member) {
            groupMembers.push({
                'id': member.id
            });
        });

        // add the new user
        groupMembers.push({
            'id': userEntity.id
        });

        // build the request entity
        var updatedGroupEntity = {
            'revision': nf.Client.getRevision(groupEntity),
            'component': $.extend({}, groupEntity.component, {
                'users': groupMembers
            })
        };

        // update the group
        return $.ajax({
            type: 'PUT',
            url: groupEntity.uri,
            data: JSON.stringify(updatedGroupEntity),
            dataType: 'json',
            contentType: 'application/json'
        });
    };

    /**
     * Adds the specified user to the specified group.
     *
     * @param groupEntity
     * @param userEntity
     * @returns xhr
     */
    var removeUserFromGroup = function (groupEntity, userEntity) {
        var groupMembers = [];

        // get all the current users
        $.each(groupEntity.component.users, function (_, member) {
            // do not include the specified user
            if (member.id !== userEntity.id) {
                groupMembers.push({
                    'id': member.id
                });
            }
        });

        // build the request entity
        var updatedGroupEntity = {
            'revision': nf.Client.getRevision(groupEntity),
            'component': $.extend({}, groupEntity.component, {
                'users': groupMembers
            })
        };

        // update the group
        return $.ajax({
            type: 'PUT',
            url: groupEntity.uri,
            data: JSON.stringify(updatedGroupEntity),
            dataType: 'json',
            contentType: 'application/json'
        });
    };

    /**
     * Creates the specified user.
     *
     * @param newUserEntity
     * @param selectedGroups
     */
    var createUser = function (newUserEntity, selectedGroups) {
        // get the grid and data
        var usersGrid = $('#users-table').data('gridInstance');
        var usersData = usersGrid.getData();
        
        // create the user
        var userXhr = $.ajax({
            type: 'POST',
            url: config.urls.users,
            data: JSON.stringify(newUserEntity),
            dataType: 'json',
            contentType: 'application/json'
        });

        // if the user was successfully created
        userXhr.done(function (userEntity) {
            var xhrs = [];
            $.each(selectedGroups, function (_, selectedGroup) {
                var groupEntity = usersData.getItemById(selectedGroup.id)
                xhrs.push(addUserToGroup(groupEntity, userEntity));
            });
            
            $.when.apply(window, xhrs).always(function () {
                nf.UsersTable.loadUsersTable().done(function () {
                    // select the new user
                    var row = usersData.getRowById(userEntity.id);
                    usersGrid.setSelectedRows([row]);
                    usersGrid.scrollRowIntoView(row);
                });
            }).fail(nf.Common.handleAjaxError);
        }).fail(nf.Common.handleAjaxError);
    };

    /**
     * Updates the specified user.
     *
     * @param userId
     * @param userIdentity
     * @param selectedGroups
     */
    var updateUser = function (userId, userIdentity, selectedGroups) {
        // get the grid and data
        var usersGrid = $('#users-table').data('gridInstance');
        var usersData = usersGrid.getData();
        var userEntity = usersData.getItemById(userId);

        var updatedUserEntity = {
            'revision': nf.Client.getRevision(userEntity),
            'component': {
                'id': userId,
                'identity': userIdentity
            }
        };

        // update the user
        var userXhr = $.ajax({
            type: 'PUT',
            url: userEntity.uri,
            data: JSON.stringify(updatedUserEntity),
            dataType: 'json',
            contentType: 'application/json'
        });

        userXhr.done(function (updatedUserEntity) {
        
            // determine what to add/remove
            var groupsAdded = [];
            var groupsRemoved = [];
            $.each(updatedUserEntity.component.userGroups, function(_, currentGroup) {
                var isSelected = $.grep(selectedGroups, function (group) {
                    return group.id === currentGroup.id;
                });

                // if the current group is not selected, mark it for removed
                if (isSelected.length === 0) {
                    groupsRemoved.push(currentGroup);
                }
            });
            $.each(selectedGroups, function(_, selectedGroup) {
                var isSelected = $.grep(updatedUserEntity.component.userGroups, function (group) {
                    return group.id === selectedGroup.id;
                });

                // if the selected group is not current, mark it for addition
                if (isSelected.length === 0) {
                    groupsAdded.push(selectedGroup);
                }
            });

            // update each group
            var xhrs = [];
            $.each(groupsAdded, function (_, group) {
                var groupEntity = usersData.getItemById(group.id);
                xhrs.push(addUserToGroup(groupEntity, updatedUserEntity))
            });
            $.each(groupsRemoved, function (_, group) {
                var groupEntity = usersData.getItemById(group.id);
                xhrs.push(removeUserFromGroup(groupEntity, updatedUserEntity));
            });

            $.when.apply(window, xhrs).always(function () {
                nf.UsersTable.loadUsersTable();
            }).fail(nf.Common.handleAjaxError);
        }).fail(nf.Common.handleAjaxError);
    };

    /**
     * Creates the specified group.
     *
     * @param newGroupEntity
     */
    var createGroup = function (newGroupEntity) {
        // create the group
        $.ajax({
            type: 'POST',
            url: config.urls.userGroups,
            data: JSON.stringify(newGroupEntity),
            dataType: 'json',
            contentType: 'application/json'
        }).done(function (groupEntity) {
            nf.UsersTable.loadUsersTable().done(function () {
                // add the user
                var usersGrid = $('#users-table').data('gridInstance');
                var usersData = usersGrid.getData();

                // select the new user
                var row = usersData.getRowById(groupEntity.id);
                usersGrid.setSelectedRows([row]);
                usersGrid.scrollRowIntoView(row);
            });
        }).fail(nf.Common.handleAjaxError);
    };

    var updateGroup = function (groupId, groupIdentity, selectedUsers) {
        // get the grid and data
        var usersGrid = $('#users-table').data('gridInstance');
        var usersData = usersGrid.getData();
        var groupEntity = usersData.getItemById(groupId);

        var updatedGroupoEntity = {
            'revision': nf.Client.getRevision(groupEntity),
            'component': {
                'id': groupId,
                'identity': groupIdentity,
                'users': selectedUsers
            }
        };

        // update the user
        $.ajax({
            type: 'PUT',
            url: groupEntity.uri,
            data: JSON.stringify(updatedGroupoEntity),
            dataType: 'json',
            contentType: 'application/json'
        }).done(function (groupEntity) {
            nf.UsersTable.loadUsersTable();
        }).fail(nf.Common.handleAjaxError);
    };

    /**
     * Initializes the user table.
     */
    var initUserDialog = function () {
        $('#user-dialog').modal({
            headerText: 'User/Group',
            buttons: [{
                buttonText: 'Ok',
                color: {
                    base: '#728E9B',
                    hover: '#004849',
                    text: '#ffffff'
                },
                handler: {
                    click: function () {
                        var userId = $('#user-id-edit-dialog').text();
                        var userIdentity = $('#user-identity-edit-dialog').val();

                        // see if we should create or update this user
                        if ($.trim(userId) === '') {
                            var tenantEntity = {
                                'revision': nf.Client.getRevision({
                                    'revision': {
                                        'version': 0
                                    }
                                })
                            };

                            // handle whether it's a user or a group
                            if ($('#individual-radio-button').is(':checked')) {
                                // record the user groups
                                tenantEntity['component'] = {
                                    'identity': userIdentity
                                };

                                createUser(tenantEntity, getSelectedGroups());
                            } else {
                                // record the users
                                tenantEntity['component'] = {
                                    'identity': userIdentity,
                                    'users': getSelectedUsers()
                                };

                                createGroup(tenantEntity);
                            }

                            // update any selected policies
                        } else {
                            // handle whether it's a user or a group
                            if ($('#individual-radio-button').is(':checked')) {
                                updateUser(userId, userIdentity, getSelectedGroups());

                                // update any selected policies
                            } else {
                                updateGroup(userId, userIdentity, getSelectedUsers());

                                // update any selected policies
                            }
                        }

                        $('#user-dialog').modal('hide');
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
                        $('#user-dialog').modal('hide');
                    }
                }
            }],
            handler: {
                close: function () {
                    // reset the radio button
                    $('#user-dialog input[name="userOrGroup"]').attr('disabled', false);
                    $('#individual-radio-button').prop('checked', true);
                    $('#user-groups').show();
                    $('#group-members').hide();

                    // clear the fields
                    $('#user-id-edit-dialog').text('');
                    $('#user-identity-edit-dialog').val('');
                    $('#available-users').empty()
                    $('#available-groups').empty()
                }
            }
        });

        // listen for changes to the user/group radio
        $('#user-dialog input[name="userOrGroup"]').on('change', function () {
            if ($(this).val() === 'individual') {
                $('#user-groups').show();
                $('#group-members').hide();
            } else {
                $('#user-groups').hide();
                $('#group-members').show();
            }
        });
    };

    /**
     * Initializes the processor list.
     */
    var initUsersTable = function () {
        // define the function for filtering the list
        $('#users-filter').keyup(function () {
            applyFilter();
        });

        // filter type
        $('#users-filter-type').combo({
            options: [{
                text: 'by user',
                value: 'identity'
            }],
            select: function (option) {
                applyFilter();
            }
        });

        // function for formatting the user identity
        var identityFormatter = function (row, cell, value, columnDef, dataContext) {
            var markup = '';
            if (dataContext.type === 'group') {
                markup += '<div class="fa fa-users" style="margin-right: 5px;"></div>';
            }

            markup += dataContext.component.identity;

            return markup;
        };

        // function for formatting the members/groups
        var membersGroupsFormatter = function (row, cell, value, columnDef, dataContext) {
            if (dataContext.type === 'group') {
                return 'Members: <b>' + dataContext.component.users.map(function (user) {
                    return user.component.identity;
                }).join('</b>, <b>') + '</b>';
            } else {
                return 'Member of: <b>' + dataContext.component.userGroups.map(function (group) {
                    return group.component.identity;
                }).join('</b>, <b>') + '</b>';
            }
        };

        // function for formatting the actions column
        var actionFormatter = function (row, cell, value, columnDef, dataContext) {
            var markup = '';

            // ensure user can modify the user
            if (nf.Common.canModifyTenants()) {
                markup += '<div title="Edit" class="pointer edit-user fa fa-pencil" style="margin-right: 3px;"></div>';
                markup += '<div title="Remove" class="pointer delete-user fa fa-trash"></div>';
            }

            return markup;
        };

        // initialize the templates table
        var usersColumns = [
            {id: 'identity', name: 'User', sortable: true, resizable: true, formatter: identityFormatter},
            {id: 'membersGroups', name: '&nbsp;', sortable: true, defaultSortAsc: false, resizable: true, formatter: membersGroupsFormatter},
            {id: 'actions', name: '&nbsp;', sortable: false, resizable: false, formatter: actionFormatter, width: 100, maxWidth: 100}
        ];
        var usersOptions = {
            forceFitColumns: true,
            enableTextSelectionOnCells: true,
            enableCellNavigation: true,
            enableColumnReorder: false,
            autoEdit: false
        };

        // initialize the dataview
        var usersData = new Slick.Data.DataView({
            inlineFilters: false
        });
        usersData.setItems([]);
        usersData.setFilterArgs({
            searchString: getFilterText(),
            property: $('#users-filter-type').combo('getSelectedOption').value
        });
        usersData.setFilter(filter);

        // initialize the sort
        sort({
            columnId: 'identity',
            sortAsc: true
        }, usersData);

        // initialize the grid
        var usersGrid = new Slick.Grid('#users-table', usersData, usersColumns, usersOptions);
        usersGrid.setSelectionModel(new Slick.RowSelectionModel());
        usersGrid.registerPlugin(new Slick.AutoTooltips());
        usersGrid.setSortColumn('identity', true);
        usersGrid.onSort.subscribe(function (e, args) {
            sort({
                columnId: args.sortCol.id,
                sortAsc: args.sortAsc
            }, usersData);
        });

        // configure a click listener
        usersGrid.onClick.subscribe(function (e, args) {
            var target = $(e.target);

            // get the node at this row
            var item = usersData.getItem(args.row);

            // determine the desired action
            if (usersGrid.getColumns()[args.cell].id === 'actions') {
                if (target.hasClass('edit-user')) {
                    editUser(item);
                } else if (target.hasClass('delete-user')) {
                    deleteUser(item);
                }
            }
        });

        // wire up the dataview to the grid
        usersData.onRowCountChanged.subscribe(function (e, args) {
            usersGrid.updateRowCount();
            usersGrid.render();

            // update the total number of displayed processors
            $('#displayed-users').text(args.current);
        });
        usersData.onRowsChanged.subscribe(function (e, args) {
            usersGrid.invalidateRows(args.rows);
            usersGrid.render();
        });

        // hold onto an instance of the grid
        $('#users-table').data('gridInstance', usersGrid);

        // initialize the number of displayed items
        $('#displayed-users').text('0');
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
            if(a.permissions.canRead && b.permissions.canRead) {
                var aString = nf.Common.isDefinedAndNotNull(a.component[sortDetails.columnId]) ? a.component[sortDetails.columnId] : '';
                var bString = nf.Common.isDefinedAndNotNull(b.component[sortDetails.columnId]) ? b.component[sortDetails.columnId] : '';
                return aString === bString ? 0 : aString > bString ? 1 : -1;
            } else {
                if (!a.permissions.canRead && !b.permissions.canRead){
                    return 0;
                }
                if(a.permissions.canRead){
                    return 1;
                } else {
                    return -1;
                }
            }
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
        return $('#users-filter').val();
    };

    /**
     * Applies the filter found in the filter expression text field.
     */
    var applyFilter = function () {
        // get the dataview
        var usersGrid = $('#users-table').data('gridInstance');

        // ensure the grid has been initialized
        if (nf.Common.isDefinedAndNotNull(usersGrid)) {
            var usersData = usersGrid.getData();

            // update the search criteria
            usersData.setFilterArgs({
                searchString: getFilterText(),
                property: $('#users-filter-type').combo('getSelectedOption').value
            });
            usersData.refresh();
        }
    };

    /**
     * Performs the filtering.
     *
     * @param {object} item     The item subject to filtering
     * @param {object} args     Filter arguments
     * @returns {Boolean}       Whether or not to include the item
     */
    var filter = function (item, args) {
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

        return item.component[args.property].search(filterExp) >= 0;
    };

    /**
     * Builds a listing a users.
     */
    var buildUsersList = function () {
        var usersGrid = $('#users-table').data('gridInstance');
        var usersData = usersGrid.getData();
        var usersList = $('#available-users');

        // add a row for each user
        var count = 0;
        $.each(usersData.getItems(), function(_, user) {
            if (user.type === 'user') {
                // checkbox
                var checkbox = $('<div class="user-check nf-checkbox checkbox-unchecked"></div>').addClass('group-user-' + user.id);

                // user id
                var userId = $('<span class="user-id hidden"></span>').text(user.id);

                // identity
                var identity = $('<div class="available-identities"></div>').text(user.component.identity);

                // clear
                var clear = $('<div class="clear"></div>');

                // list item
                var li = $('<li></li>').append(checkbox).append(userId).append(identity).append(clear).appendTo(usersList);
                if (count++ % 2 === 0) {
                    li.addClass('even');
                }
            }
        });
    };

    /**
     * Builds a listing a users.
     */
    var buildGroupsList = function () {
        var usersGrid = $('#users-table').data('gridInstance');
        var usersData = usersGrid.getData();
        var groupsList = $('#available-groups');

        // add a row for each user
        var count = 0;
        $.each(usersData.getItems(), function(_, group) {
            if (group.type === 'group') {
                // checkbox
                var checkbox = $('<div class="group-check nf-checkbox checkbox-unchecked"></div>').addClass('user-group-' + group.id);

                // group id
                var groupId = $('<span class="group-id hidden"></span>').text(group.id);
                
                // icon
                var groupIcon = $('<div class="fa fa-users" style="margin-top: 6px;"></div>');

                // identity
                var identity = $('<div class="available-identities"></div>').text(group.component.identity);

                // clear
                var clear = $('<div class="clear"></div>');

                // list item
                var li = $('<li></li>').append(checkbox).append(groupId).append(groupIcon).append(identity).append(clear).appendTo(groupsList);
                if (count++ % 2 === 0) {
                    li.addClass('even');
                }
            }
        });
    };

    /**
     * Edit's the specified user's account.
     *
     * @argument {object} user        The user item
     */
    var editUser = function (user) {
        // populate the users info
        $('#user-id-edit-dialog').text(user.id);
        $('#user-identity-edit-dialog').val(user.component.identity);

        if (user.type === 'user') {
            $('#individual-radio-button').prop('checked', true);
            $('#user-groups').show();
            $('#group-members').hide();

            // list all the groups currently in the table
            buildGroupsList();

            // select each group this user belongs to
            $.each(user.component.userGroups, function (_, userGroup) {
                $('div.group-check.user-group-' + userGroup.id).removeClass('checkbox-unchecked').addClass('checkbox-checked');
            });
        } else {
            $('#group-radio-button').prop('checked', true);
            $('#user-groups').hide();
            $('#group-members').show();

            // list all the users currently in the table
            buildUsersList();

            // select each user that belongs to this group
            $.each(user.component.users, function (_, member) {
                $('div.user-check.group-user-' + member.id).removeClass('checkbox-unchecked').addClass('checkbox-checked');
            });
        }

        // disable the type radio
        $('#user-dialog input[name="userOrGroup"]').attr('disabled', true);

        // show the dialog
        $('#user-dialog').modal('show');
    };

    /**
     * Delete's the specified user's account.
     *
     * @argument {object} user        The user item
     */
    var deleteUser = function (user) {
        // populate the users info
        $('#user-id-delete-dialog').val(user.id);
        $('#user-identity-delete-dialog').text(user.component.identity);

        // show the dialog
        $('#user-delete-dialog').modal('show');
    };

    return {
        init: function () {
            initUserDialog();
            initUserDeleteDialog();
            initUsersTable();

            if (nf.Common.canModifyTenants()) {
                $('#new-user-button').on('click', function () {
                    buildUsersList();
                    buildGroupsList();
                    $('#user-dialog').modal('show');
                });

                $('#new-user-button').prop('disabled', false);
            } else {
                $('#new-user-button').prop('disabled', true);
            }
        },

        /**
         * Update the size of the grid based on its container's current size.
         */
        resetTableSize: function () {
            var usersTable = $('#users-table');
            if (usersTable.is(':visible')) {
                var grid = usersTable.data('gridInstance');
                if (nf.Common.isDefinedAndNotNull(grid)) {
                    grid.resizeCanvas();
                }
            }
        },

        /**
         * Load the processor status table.
         */
        loadUsersTable: function () {
            var users = $.ajax({
                type: 'GET',
                url: config.urls.users,
                dataType: 'json'
            });

            var groups = $.ajax({
                type: 'GET',
                url: config.urls.userGroups,
                dataType: 'json'
            });

            return $.when(users, groups).done(function (usersResults, groupsResults) {
                var usersResponse = usersResults[0];
                var groupsResponse = groupsResults[0];

                // update the refresh timestamp
                $('#users-last-refreshed').text(usersResponse.generated);

                var usersGrid = $('#users-table').data('gridInstance');
                var usersData = usersGrid.getData();

                // begin the update
                usersData.beginUpdate();

                var users = [];

                // add each user
                $.each(usersResponse.users, function (_, user) {
                    users.push($.extend({
                        type: 'user'
                    }, user));
                });

                // add each group
                $.each(groupsResponse.userGroups, function (_, group) {
                    users.push($.extend({
                        type: 'group'
                    }, group));
                });

                // set the rows
                usersData.setItems(users);

                // end the update
                usersData.endUpdate();

                // re-sort and clear selection after updating
                usersData.reSort();
                usersGrid.invalidate();
                usersGrid.getSelectionModel().setSelectedRows([]);

                $('#total-users').text(usersData.getLength());
            }).fail(nf.Common.handleAjaxError);
        }
    };
}());