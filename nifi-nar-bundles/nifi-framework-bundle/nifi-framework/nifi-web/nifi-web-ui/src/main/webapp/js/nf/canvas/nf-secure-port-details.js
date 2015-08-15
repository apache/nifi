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

/* global nf */

nf.SecurePortDetails = (function () {

    /**
     * Adds the specified user to the list of allowed users.
     * 
     * @argument {string} allowedUser       The allowed user dn
     */
    var addAllowedUser = function (allowedUser) {
        var allowedUsers = $('#read-only-allowed-users');

        // append the user
        var user = $('<span></span>').addClass('allowed-entity ellipsis').text(allowedUser).ellipsis();
        $('<li></li>').data('user', allowedUser).append(user).appendTo(allowedUsers);
    };

    /**
     * Adds the specified group to the list of allowed groups.
     * 
     * @argument {string} allowedGroup      The allowed group name
     */
    var addAllowedGroup = function (allowedGroup) {
        var allowedGroups = $('#read-only-allowed-groups');

        // append the group
        var group = $('<span></span>').addClass('allowed-entity ellipsis').text(allowedGroup).ellipsis();
        $('<li></li>').data('group', allowedGroup).append(group).appendTo(allowedGroups);
    };

    return {
        init: function () {
            // initialize the properties tabs
            $('#secure-port-details-tabs').tabbs({
                tabStyle: 'tab',
                selectedTabStyle: 'selected-tab',
                tabs: [{
                        name: 'Settings',
                        tabContentId: 'read-only-secure-port-settings-tab-content'
                    }, {
                        name: 'Access Control',
                        tabContentId: 'read-only-secure-port-access-control-tab-content'
                    }]
            });

            // configure the processor details dialog
            $('#secure-port-details').modal({
                headerText: 'Secure Port Details',
                overlayBackground: true,
                buttons: [{
                        buttonText: 'Ok',
                        handler: {
                            click: function () {
                                // hide the dialog
                                $('#secure-port-details').modal('hide');
                            }
                        }
                    }],
                handler: {
                    close: function () {
                        // clear the processor details
                        nf.Common.clearField('read-only-secure-port-name');
                        nf.Common.clearField('read-only-secure-port-id');
                        nf.Common.clearField('read-only-secure-port-comments');
                        nf.Common.clearField('read-only-secure-port-concurrent-tasks');

                        // clear the access control
                        $('#read-only-allowed-users').empty();
                        $('#read-only-allowed-groups').empty();
                    }
                }
            }).draggable({
                containment: 'parent',
                handle: '.dialog-header'
            });
        },
        
        showDetails: function (selection) {
            // if the specified component is a port, load its properties
            if (nf.CanvasUtils.isInputPort(selection) || nf.CanvasUtils.isOutputPort(selection)) {
                var selectionData = selection.datum();

                // populate the port settings
                nf.Common.populateField('read-only-secure-port-name', selectionData.component.name);
                nf.Common.populateField('read-only-secure-port-id', selectionData.component.id);
                nf.Common.populateField('read-only-secure-port-concurrent-tasks', selectionData.component.concurrentlySchedulableTaskCount);
                nf.Common.populateField('read-only-secure-port-comments', selectionData.component.comments);

                // add allowed users
                $.each(selectionData.component.userAccessControl, function (_, allowedUser) {
                    addAllowedUser(allowedUser);
                });

                // add allowed groups
                $.each(selectionData.component.groupAccessControl, function (_, allowedGroup) {
                    addAllowedGroup(allowedGroup);
                });

                // show the details
                $('#secure-port-details').modal('show');
            }
        }
    };
}());