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
        filterText: 'Filter',
        styles: {
            filterList: 'users-filter-list'
        },
        urls: {
            users: '../nifi-api/controller/users',
            userGroups: '../nifi-api/controller/user-groups'
        }
    };

    /**
     * Initializes the user details dialog.
     */
    var initUserDetailsDialog = function () {
        $('#user-details-dialog').modal({
            headerText: 'User Details',
            overlayBackground: false,
            buttons: [{
                    buttonText: 'Ok',
                    handler: {
                        click: function () {
                            $('#user-details-dialog').modal('hide');
                        }
                    }
                }],
            handler: {
                close: function () {
                    // clear the details
                    $('#user-name-details-dialog').text('');
                    $('#user-dn-details-dialog').text('');
                    $('#user-created-details-dialog').text('');
                    $('#user-verified-details-dialog').text('');
                    $('#user-justification-details-dialog').text('');
                }
            }
        });
    };

    /**
     * Initializes the user roles dialog.
     */
    var initUserRolesDialog = function () {
        $('#user-roles-dialog').modal({
            headerText: 'User Roles',
            overlayBackground: false,
            buttons: [{
                    buttonText: 'Apply',
                    handler: {
                        click: function () {
                            var userId = $('#user-id-roles-dialog').val();
                            var userRoles = [];

                            // function for determining if a checkbox is checked
                            var isChecked = function (domId) {
                                return $('#' + domId).hasClass('checkbox-checked');
                            };

                            // determine the user roles
                            if (isChecked('role-admin-checkbox')) {
                                userRoles.push('ROLE_ADMIN');
                            }
                            if (isChecked('role-dfm-checkbox')) {
                                userRoles.push('ROLE_DFM');
                            }
                            if (isChecked('role-provenance-checkbox')) {
                                userRoles.push('ROLE_PROVENANCE');
                            }
                            if (isChecked('role-monitor-checkbox')) {
                                userRoles.push('ROLE_MONITOR');
                            }
                            if (isChecked('role-nifi-checkbox')) {
                                userRoles.push('ROLE_NIFI');
                            }
                            if (isChecked('role-proxy-checkbox')) {
                                userRoles.push('ROLE_PROXY');
                            }

                            var userDto = {};
                            userDto['id'] = userId;
                            userDto['authorities'] = userRoles;

                            // ensure the account is active
                            userDto['status'] = 'ACTIVE';

                            var userEntity = {};
                            userEntity['user'] = userDto;

                            // update the user
                            $.ajax({
                                type: 'PUT',
                                url: config.urls.users + '/' + encodeURIComponent(userId),
                                data: JSON.stringify(userEntity),
                                contentType: 'application/json',
                                dataType: 'json'
                            }).done(function (response) {
                                if (nf.Common.isDefinedAndNotNull(response.user)) {
                                    var user = response.user;

                                    // get the table and update the row accordingly
                                    var usersGrid = $('#users-table').data('gridInstance');
                                    var usersData = usersGrid.getData();
                                    usersData.updateItem(user.id, user);
                                }
                            }).fail(nf.Common.handleAjaxError);

                            // hide the dialog
                            $('#user-roles-dialog').modal('hide');
                        }
                    }
                }, {
                    buttonText: 'Cancel',
                    handler: {
                        click: function () {
                            $('#user-roles-dialog').modal('hide');
                        }
                    }
                }],
            handler: {
                close: function () {
                    // uncheck every box
                    $('div.role-checkbox').removeClass('checkbox-checked').addClass('checkbox-unchecked');
                    $('#user-id-roles-dialog').val('');
                }
            }
        });
    };

    /**
     * Initializes the group roles dialog.
     */
    var initGroupRolesDialog = function () {
        $('#group-roles-dialog').modal({
            headerText: 'Group Roles',
            overlayBackground: false,
            buttons: [{
                    buttonText: 'Apply',
                    handler: {
                        click: function () {
                            var group = $('#group-name-roles-dialog').text();
                            var groupRoles = [];

                            // function for determining if a checkbox is checked
                            var isChecked = function (domId) {
                                return $('#' + domId).hasClass('checkbox-checked');
                            };

                            // determine the user roles
                            if (isChecked('group-role-admin-checkbox')) {
                                groupRoles.push('ROLE_ADMIN');
                            }
                            if (isChecked('group-role-dfm-checkbox')) {
                                groupRoles.push('ROLE_DFM');
                            }
                            if (isChecked('group-role-provenance-checkbox')) {
                                groupRoles.push('ROLE_PROVENANCE');
                            }
                            if (isChecked('group-role-monitor-checkbox')) {
                                groupRoles.push('ROLE_MONITOR');
                            }
                            if (isChecked('group-role-nifi-checkbox')) {
                                groupRoles.push('ROLE_NIFI');
                            }
                            if (isChecked('group-role-proxy-checkbox')) {
                                groupRoles.push('ROLE_PROXY');
                            }

                            var userGroupDto = {};
                            userGroupDto['group'] = group;
                            userGroupDto['authorities'] = groupRoles;

                            // ensure the accounts are active
                            userGroupDto['status'] = 'ACTIVE';

                            var userGroupEntity = {};
                            userGroupEntity['userGroup'] = userGroupDto;

                            // update the user
                            $.ajax({
                                type: 'PUT',
                                url: config.urls.userGroups + '/' + encodeURIComponent(group),
                                data: JSON.stringify(userGroupEntity),
                                contentType: 'application/json',
                                dataType: 'json'
                            }).done(function () {
                                nf.UsersTable.loadUsersTable();
                            }).fail(nf.Common.handleAjaxError);

                            // hide the dialog
                            $('#group-roles-dialog').modal('hide');
                        }
                    }
                }, {
                    buttonText: 'Cancel',
                    handler: {
                        click: function () {
                            $('#group-roles-dialog').modal('hide');
                        }
                    }
                }],
            handler: {
                close: function () {
                    // uncheck every box
                    $('div.role-checkbox').removeClass('checkbox-checked').addClass('checkbox-unchecked');
                    $('#group-name-roles-dialog').text('');
                }
            }
        });
    };

    var initUserDeleteDialog = function () {
        $('#user-delete-dialog').modal({
            headerText: 'Delete User',
            overlayBackground: false,
            buttons: [{
                    buttonText: 'Delete',
                    handler: {
                        click: function () {
                            var userId = $('#user-id-delete-dialog').val();

                            // update the user
                            $.ajax({
                                type: 'DELETE',
                                url: config.urls.users + '/' + encodeURIComponent(userId),
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
     * Initializes the user revoke dialog.
     */
    var initUserRevokeDialog = function () {
        $('#user-revoke-dialog').modal({
            headerText: 'Revoke Access',
            overlayBackground: false,
            buttons: [{
                    buttonText: 'Revoke',
                    handler: {
                        click: function () {
                            var userId = $('#user-id-revoke-dialog').val();

                            // update the user
                            $.ajax({
                                type: 'PUT',
                                url: config.urls.users + '/' + encodeURIComponent(userId),
                                data: {
                                    'status': 'DISABLED'
                                },
                                dataType: 'json'
                            }).done(function (response) {
                                if (nf.Common.isDefinedAndNotNull(response.user)) {
                                    var user = response.user;

                                    // get the table and update the row accordingly
                                    var usersGrid = $('#users-table').data('gridInstance');
                                    var usersData = usersGrid.getData();
                                    usersData.updateItem(user.id, user);
                                }
                            }).fail(nf.Common.handleAjaxError);

                            // hide the dialog
                            $('#user-revoke-dialog').modal('hide');
                        }
                    }
                }, {
                    buttonText: 'Cancel',
                    handler: {
                        click: function () {
                            $('#user-revoke-dialog').modal('hide');
                        }
                    }
                }],
            handler: {
                close: function () {
                    // clear the current user
                    $('#user-id-revoke-dialog').val('');
                    $('#user-name-revoke-dialog').text('');
                }
            }
        });
    };

    /**
     * Initializes the group revoke dialog.
     */
    var initGroupRevokeDialog = function () {
        $('#group-revoke-dialog').modal({
            headerText: 'Revoke Access',
            overlayBackground: false,
            buttons: [{
                    buttonText: 'Revoke',
                    handler: {
                        click: function () {
                            var groupName = $('#group-name-revoke-dialog').text();

                            // update the group
                            $.ajax({
                                type: 'PUT',
                                url: config.urls.userGroups + '/' + encodeURIComponent(groupName),
                                data: {
                                    'status': 'DISABLED'
                                },
                                dataType: 'json'
                            }).done(function () {
                                nf.UsersTable.loadUsersTable();
                            }).fail(nf.Common.handleAjaxError);

                            // hide the dialog
                            $('#group-revoke-dialog').modal('hide');
                        }
                    }
                }, {
                    buttonText: 'Cancel',
                    handler: {
                        click: function () {
                            $('#group-revoke-dialog').modal('hide');
                        }
                    }
                }],
            handler: {
                close: function () {
                    // clear the current group
                    $('#group-name-revoke-dialog').text('');
                }
            }
        });
    };

    /**
     * Initializes the user revoke dialog.
     */
    var initUserGroupDialog = function () {
        $('#user-group-dialog').modal({
            headerText: 'Set Users Group',
            overlayBackground: false,
            buttons: [{
                    buttonText: 'Group',
                    handler: {
                        click: function () {
                            var group = $.trim($('#group-name').val());

                            // ensure a group name was specified
                            if (group === '') {
                                nf.Dialog.showOkDialog({
                                    headerText: 'Group Users',
                                    dialogContent: 'Group name cannot be blank.',
                                    overlayBackground: false
                                });
                            } else {
                                var userIds = $('#group-name').data('selected-user-ids');

                                var userGroupDto = {};
                                userGroupDto['userIds'] = userIds;
                                userGroupDto['group'] = group;

                                var userGroupEntity = {};
                                userGroupEntity['userGroup'] = userGroupDto;

                                // update the user
                                $.ajax({
                                    type: 'PUT',
                                    url: config.urls.userGroups + '/' + encodeURIComponent(group),
                                    data: JSON.stringify(userGroupEntity),
                                    contentType: 'application/json',
                                    dataType: 'json'
                                }).done(function () {
                                    nf.UsersTable.loadUsersTable();
                                }).fail(nf.Common.handleAjaxError);
                            }

                            // hide the dialog
                            $('#user-group-dialog').modal('hide');
                        }
                    }
                }, {
                    buttonText: 'Cancel',
                    handler: {
                        click: function () {
                            $('#user-group-dialog').modal('hide');
                        }
                    }
                }],
            handler: {
                close: function () {
                    // clear the current configuration
                    $('#group-name').removeData('selected-user-ids');
                    $('#group-name').val('');

                    // uncheck every box
                    $('div.group-role-checkbox').removeClass('checkbox-checked').addClass('checkbox-unchecked');
                }
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
        }).focus(function () {
            if ($(this).hasClass(config.styles.filterList)) {
                $(this).removeClass(config.styles.filterList).val('');
            }
        }).blur(function () {
            if ($(this).val() === '') {
                $(this).addClass(config.styles.filterList).val(config.filterText);
            }
        }).addClass(config.styles.filterList).val(config.filterText);

        // filter type
        $('#users-filter-type').combo({
            options: [{
                    text: 'by user',
                    value: 'userName'
                }, {
                    text: 'by group',
                    value: 'userGroup'
                }, {
                    text: 'by role',
                    value: 'authorities'
                }],
            select: function (option) {
                applyFilter();
            }
        });

        // add hover effect and click handler for opening the group dialog
        nf.Common.addHoverEffect('#group-button', 'button-normal', 'button-over').click(function () {
            groupUsers();
        });

        // listen for browser resize events to update the page size
        $(window).resize(function () {
            nf.UsersTable.resetTableSize();
        });

        // define a custom formatter for the more details column
        var moreDetailsFormatter = function (row, cell, value, columnDef, dataContext) {
            return '<img src="images/iconDetails.png" title="View Details" class="pointer show-user-details" style="margin-top: 4px;"/>';
        };

        // function for formatting the last accessed time
        var valueFormatter = function (row, cell, value, columnDef, dataContext) {
            return nf.Common.formatValue(value);
        };

        // function for formatting the property name
        var roleFormatter = function (row, cell, value, columnDef, dataContext) {
            var grouped = $('#group-collaspe-checkbox').hasClass('checkbox-checked');

            // function for converting roles into human readable role names
            var convertRoleNames = function () {
                var roleNames = [];
                $.each(value, function (i, role) {
                    var roleName = role;
                    if (role === 'ROLE_ADMIN') {
                        roleName = 'Administrator';
                    } else if (role === 'ROLE_DFM') {
                        roleName = 'Data Flow Manager';
                    } else if (role === 'ROLE_PROVENANCE') {
                        roleName = 'Provenance';
                    } else if (role === 'ROLE_MONITOR') {
                        roleName = 'Read Only';
                    } else if (role === 'ROLE_NIFI') {
                        roleName = 'NiFi';
                    } else if (role === 'ROLE_PROXY') {
                        roleName = 'Proxy';
                    }
                    roleNames.push(roleName);
                });
                return roleNames.join(', ');
            };

            // generate the roles as appropriate
            if (grouped && nf.Common.isDefinedAndNotNull(dataContext.userGroup)) {
                if (dataContext.status === 'PENDING') {
                    return '<span style="color: #0081D7; font-weight: bold;">Authorization Pending</span>';
                } else if (dataContext.status === 'DISABLED') {
                    return '<span style="color: red; font-weight: bold;">Access Revoked</span>';
                } else if (nf.Common.isDefinedAndNotNull(value)) {
                    if (!nf.Common.isEmpty(value)) {
                        return convertRoleNames();
                    } else {
                        return '<span class="unset">No roles set</span>';
                    }
                } else {
                    return '<span class="unset">Multiple users with different roles</span>';
                }
            } else {
                if (dataContext.status === 'PENDING') {
                    return '<span style="color: #0081D7; font-weight: bold;">Authorization Pending</span>';
                } else if (dataContext.status === 'DISABLED') {
                    return '<span style="color: red; font-weight: bold;">Access Revoked</span>';
                } else if (!nf.Common.isEmpty(value)) {
                    return convertRoleNames();
                } else {
                    return '<span class="unset">No roles set</span>';
                }
            }
        };

        // function for formatting the status
        var statusFormatter = function (row, cell, value, columnDef, dataContext) {
            var grouped = $('#group-collaspe-checkbox').hasClass('checkbox-checked');

            // return the status as appropriate
            if (nf.Common.isDefinedAndNotNull(value)) {
                return value;
            } else if (grouped && nf.Common.isDefinedAndNotNull(dataContext.userGroup)) {
                return '<span class="unset">Multiple users with different status</span>';
            } else {
                return '<span class="unset">No status set</span>';
            }
        };

        // function for formatting the actions column
        var actionFormatter = function (row, cell, value, columnDef, dataContext) {
            var grouped = $('#group-collaspe-checkbox').hasClass('checkbox-checked');

            // if this represents a grouped row
            if (nf.Common.isDefinedAndNotNull(dataContext.userGroup) && grouped) {
                var actions = '<img src="images/iconEdit.png" title="Edit Access" class="pointer update-group-access" style="margin-top: 2px;"/>&nbsp;<img src="images/iconRevoke.png" title="Revoke Access" class="pointer revoke-group-access" style="margin-top: 2px;"/>&nbsp;&nbsp;<img src="images/ungroup.png" title="Ungroup" class="pointer ungroup"/>';
            } else {
                // return the appropriate markup for an individual user
                var actions = '<img src="images/iconEdit.png" title="Edit Access" class="pointer update-user-access" style="margin-top: 2px;"/>';

                if (dataContext.status === 'ACTIVE') {
                    actions += '&nbsp;<img src="images/iconRevoke.png" title="Revoke Access" class="pointer revoke-user-access"/>';

                    // add an ungroup active if appropriate
                    if (nf.Common.isDefinedAndNotNull(dataContext.userGroup)) {
                        actions += '&nbsp;&nbsp;<img src="images/ungroup.png" title="Ungroup" class="pointer ungroup-user" style="margin-top: 2px;"/>';
                    }
                } else {
                    actions += '&nbsp;<img src="images/iconDelete.png" title="Delete Account" class="pointer delete-user-account"/>';
                }
            }

            return actions;
        };

        // initialize the templates table
        var usersColumns = [
            {id: 'moreDetails', name: '&nbsp;', sortable: false, resizable: false, formatter: moreDetailsFormatter, width: 50, maxWidth: 50},
            {id: 'userName', name: 'User', field: 'userName', sortable: true, resizable: true},
            {id: 'userGroup', name: 'Group', field: 'userGroup', sortable: true, resizable: true, formatter: valueFormatter},
            {id: 'authorities', name: 'Roles', field: 'authorities', sortable: true, resizable: true, formatter: roleFormatter},
            {id: 'lastAccessed', name: 'Last Accessed', field: 'lastAccessed', sortable: true, defaultSortAsc: false, resizable: true, formatter: valueFormatter},
            {id: 'status', name: 'Status', field: 'status', sortable: true, resizable: false, formatter: statusFormatter},
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
            columnId: 'userName',
            sortAsc: true
        }, usersData);

        // initialize the grid
        var usersGrid = new Slick.Grid('#users-table', usersData, usersColumns, usersOptions);
        usersGrid.setSelectionModel(new Slick.RowSelectionModel());
        usersGrid.registerPlugin(new Slick.AutoTooltips());
        usersGrid.setSortColumn('userName', true);
        usersGrid.onSort.subscribe(function (e, args) {
            sort({
                columnId: args.sortCol.field,
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
                if (target.hasClass('update-group-access')) {
                    updateGroupAccess(item);
                } else if (target.hasClass('revoke-group-access')) {
                    revokeGroupAccess(item);
                } else if (target.hasClass('ungroup')) {
                    ungroup(item);
                } else if (target.hasClass('update-user-access')) {
                    updateUserAccess(item);
                } else if (target.hasClass('revoke-user-access')) {
                    revokeUserAccess(item);
                } else if (target.hasClass('ungroup-user')) {
                    ungroupUser(item);
                } else if (target.hasClass('delete-user-account')) {
                    deleteUserAccount(item);
                }
            } else if (usersGrid.getColumns()[args.cell].id === 'moreDetails') {
                if (target.hasClass('show-user-details')) {
                    showUserDetails(item);
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
            if (sortDetails.columnId === 'lastAccessed') {
                var aDate = nf.Common.parseDateTime(a[sortDetails.columnId]);
                var bDate = nf.Common.parseDateTime(b[sortDetails.columnId]);
                return aDate.getTime() - bDate.getTime();
            } else {
                var aString = nf.Common.isDefinedAndNotNull(a[sortDetails.columnId]) ? a[sortDetails.columnId] : '';
                var bString = nf.Common.isDefinedAndNotNull(b[sortDetails.columnId]) ? b[sortDetails.columnId] : '';
                return aString === bString ? 0 : aString > bString ? 1 : -1;
            }
        };

        // perform the sort
        data.sort(comparer, sortDetails.sortAsc);
    };

    /**
     * Prompts to gather user group name.
     */
    var groupUsers = function () {
        // get the table and update the row accordingly
        var usersGrid = $('#users-table').data('gridInstance');
        var selectedIndices = usersGrid.getSelectedRows();

        if ($.isArray(selectedIndices) && selectedIndices.length > 0) {
            var usersData = usersGrid.getData();

            var userIds = [];
            $.each(selectedIndices, function (_, index) {
                var user = usersData.getItem(index);

                // groups have comma separated id's
                userIds = userIds.concat(user['id'].split(','));
            });

            var groupNameField = $('#group-name');
            groupNameField.data('selected-user-ids', userIds);

            // show the dialog
            $('#user-group-dialog').modal('show');

            // set the focus
            groupNameField.focus();
        } else {
            nf.Dialog.showOkDialog({
                headerText: 'Group Users',
                dialogContent: 'Select one or more users to group.',
                overlayBackground: false
            });
        }
    };

    /**
     * Get the text out of the filter field. If the filter field doesn't
     * have any text it will contain the text 'filter list' so this method
     * accounts for that.
     */
    var getFilterText = function () {
        var filterText = '';
        var filterField = $('#users-filter');
        if (!filterField.hasClass(config.styles.filterList)) {
            filterText = filterField.val();
        }
        return filterText;
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

        // handle searching appropriately
        if (args.property === 'authorities') {
            var roles = item[args.property];

            var found = false;
            for (var i = 0; i < roles.length; i++) {
                var role = roles[i];
                var roleName = role;

                // convert the role name accordingly
                if (role === 'ROLE_ADMIN') {
                    roleName = 'Administrator';
                } else if (role === 'ROLE_DFM') {
                    roleName = 'Data Flow Manager';
                } else if (role === 'ROLE_PROVENANCE') {
                    roleName = 'Provenance';
                } else if (role === 'ROLE_MONITOR') {
                    roleName = 'Read Only';
                } else if (role === 'ROLE_NIFI') {
                    roleName = 'NiFi';
                } else if (role === 'ROLE_PROXY') {
                    roleName = 'Proxy';
                }

                // see if the string was found
                if (roleName.search(filterExp) >= 0) {
                    found = true;
                    break;
                }
            }

            return found;
        } else {
            return item[args.property].search(filterExp) >= 0;
        }
    };

    /**
     * Shows details for the specified user.
     * 
     * @param {object} user
     */
    var showUserDetails = function (user) {
        var grouped = $('#group-collaspe-checkbox').hasClass('checkbox-checked');

        // update the dialog fields
        $('#user-name-details-dialog').text(user.userName);
        $('#user-dn-details-dialog').text(user.dn);

        // handle fields that could vary for groups
        if (nf.Common.isDefinedAndNotNull(user.creation)) {
            $('#user-created-details-dialog').text(user.creation);
        } else if (grouped && nf.Common.isDefinedAndNotNull(user.userGroup)) {
            $('#user-created-details-dialog').html('<span class="unset">Multiple users with different creation timestamps.</span>');
        } else {
            $('#user-created-details-dialog').html('<span class="unset">No creation timestamp set</span>');
        }

        if (nf.Common.isDefinedAndNotNull(user.lastVerified)) {
            $('#user-verified-details-dialog').text(user.lastVerified);
        } else if (grouped && nf.Common.isDefinedAndNotNull(user.userGroup)) {
            $('#user-verified-details-dialog').html('<span class="unset">Multiple users with different last verified timestamps.</span>');
        } else {
            $('#user-verified-details-dialog').html('<span class="unset">No last verified timestamp set.</span>');
        }

        if (nf.Common.isDefinedAndNotNull(user.justification)) {
            $('#user-justification-details-dialog').text(user.justification);
        } else if (grouped && nf.Common.isDefinedAndNotNull(user.userGroup)) {
            $('#user-justification-details-dialog').html('<span class="unset">Multiple users with different justifications.</span>');
        } else {
            $('#user-justification-details-dialog').html('<span class="unset">No justification set.</span>');
        }

        // show the dialog
        $('#user-details-dialog').modal('show');
    };
    
    /**
     * Updates the specified groups level of access.
     * 
     * @argument {object} item        The user item
     */
    var updateGroupAccess = function (item) {
        // record the current group
        $('#group-name-roles-dialog').text(item.userGroup);

        // show the dialog
        $('#group-roles-dialog').modal('show');
    };
    
    /**
     * Disables the specified group's account.
     * 
     * @argument {object} item        The user item
     */
    var revokeGroupAccess = function (item) {
        // record the current group
        $('#group-name-revoke-dialog').text(item.userGroup);

        // show the dialog
        $('#group-revoke-dialog').modal('show');
    };

    /**
     * Ungroups the specified group.
     * 
     * @argument {object} item        The user item
     */
    var ungroup = function (item) {
        // prompt for ungroup
        nf.Dialog.showYesNoDialog({
            dialogContent: 'Remove all users from group \'' + nf.Common.escapeHtml(item.userGroup) + '\'?',
            overlayBackground: false,
            yesHandler: function () {
                $.ajax({
                    type: 'DELETE',
                    url: config.urls.userGroups + '/' + encodeURIComponent(item.userGroup),
                    dataType: 'json'
                }).done(function (response) {
                    nf.UsersTable.loadUsersTable();
                }).fail(nf.Common.handleAjaxError);
            }
        });
    };
    
    /**
     * Updates the specified users's level of access.
     * 
     * @argument {object} item        The user item
     */
    var updateUserAccess = function (item) {
        // populate the user info
        $('#user-id-roles-dialog').val(item.id);
        $('#user-name-roles-dialog').attr('title', item.dn).text(item.userName);
        $('#user-justification-roles-dialog').html(nf.Common.formatValue(item.justification));

        // function for checking a checkbox
        var check = function (domId) {
            $('#' + domId).removeClass('checkbox-unchecked').addClass('checkbox-checked');
        };

        // go through each user role
        $.each(item.authorities, function (i, authority) {
            if (authority === 'ROLE_ADMIN') {
                check('role-admin-checkbox');
            } else if (authority === 'ROLE_DFM') {
                check('role-dfm-checkbox');
            } else if (authority === 'ROLE_PROVENANCE') {
                check('role-provenance-checkbox');
            } else if (authority === 'ROLE_MONITOR') {
                check('role-monitor-checkbox');
            } else if (authority === 'ROLE_NIFI') {
                check('role-nifi-checkbox');
            } else if (authority === 'ROLE_PROXY') {
                check('role-proxy-checkbox');
            }
        });

        // show the dialog
        $('#user-roles-dialog').modal('show');
    };
    
    /**
     * Disables the specified user's account.
     * 
     * @argument {object} item        The user item
     */
    var revokeUserAccess = function (item) {
        // populate the users info
        $('#user-id-revoke-dialog').val(item.id);
        $('#user-name-revoke-dialog').text(item.userName);

        // show the dialog
        $('#user-revoke-dialog').modal('show');
    };
    
    /**
     * Prompts to verify group removal.
     * 
     * @argument {object} item        The user item
     */
    var ungroupUser = function (item) {
        // prompt for ungroup
        nf.Dialog.showYesNoDialog({
            dialogContent: 'Remove user \'' + nf.Common.escapeHtml(item.userName) + '\' from group \'' + nf.Common.escapeHtml(item.userGroup) + '\'?',
            overlayBackground: false,
            yesHandler: function () {
                $.ajax({
                    type: 'DELETE',
                    url: config.urls.userGroups + '/' + encodeURIComponent(item.userGroup) + '/users/' + encodeURIComponent(item.id),
                    dataType: 'json'
                }).done(function (response) {
                    nf.UsersTable.loadUsersTable();
                }).fail(nf.Common.handleAjaxError);
            }
        });
    };

    /**
     * Delete's the specified user's account.
     * 
     * @argument {object} item        The user item
     */
    var deleteUserAccount = function (item) {
        // populate the users info
        $('#user-id-delete-dialog').val(item.id);
        $('#user-name-delete-dialog').text(item.userName);

        // show the dialog
        $('#user-delete-dialog').modal('show');
    };

    return {
        init: function () {
            initUserDetailsDialog();
            initUserRolesDialog();
            initGroupRolesDialog();
            initUserRevokeDialog();
            initUserDeleteDialog();
            initUserGroupDialog();
            initGroupRevokeDialog();
            initUsersTable();
        },
        
        /**
         * Update the size of the grid based on its container's current size.
         */
        resetTableSize: function () {
            var grid = $('#users-table').data('gridInstance');
            if (nf.Common.isDefinedAndNotNull(grid)) {
                grid.resizeCanvas();
            }
        },
        
        /**
         * Load the processor status table.
         */
        loadUsersTable: function () {
            return $.ajax({
                type: 'GET',
                url: config.urls.users,
                data: {
                    'grouped': $('#group-collaspe-checkbox').hasClass('checkbox-checked')
                },
                dataType: 'json'
            }).done(function (response) {
                // ensure there are users
                if (nf.Common.isDefinedAndNotNull(response.users)) {
                    var usersGrid = $('#users-table').data('gridInstance');
                    var usersData = usersGrid.getData();

                    // set the items
                    usersData.setItems(response.users);
                    usersData.reSort();
                    usersGrid.invalidate();

                    // clear the current selection
                    usersGrid.getSelectionModel().setSelectedRows([]);

                    // update the refresh timestamp
                    $('#users-last-refreshed').text(response.generated);

                    // update the total number of processors
                    $('#total-users').text(response.users.length);
                } else {
                    $('#total-users').text('0');
                }
            }).fail(nf.Common.handleAjaxError);
        }
    };
}());