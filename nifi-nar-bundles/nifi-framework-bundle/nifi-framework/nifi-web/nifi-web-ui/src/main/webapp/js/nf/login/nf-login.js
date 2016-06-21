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

/* global nf, top */

$(document).ready(function () {
    nf.Login.init();
});

nf.Login = (function () {

    var config = {
        urls: {
            identity: '../nifi-api/flow/identity',
            users: '../nifi-api/controller/users',
            token: '../nifi-api/access/token',
            accessStatus: '../nifi-api/access',
            accessConfig: '../nifi-api/access/config'
        }
    };

    var initializeMessage = function () {
        $('#login-message-container').show();
    };

    var showLogin = function () {
        // reset the forms
        $('#username').val('');
        $('#password').val('');
        $('#login-submission-button').text('Log in');

        // update the form visibility
        $('#login-container').show();
        $('#nifi-registration-container').hide();

        // set the focus
        $('#username').focus();
    };

    var initializeSubmission = function () {
        $('#login-submission-button').on('click', function () {
            if ($('#login-container').is(':visible')) {
                login();
            }
        });

        $('#login-submission-container').show();
    };

    var login = function () {
        // remove focus
        $('#username, #password').blur();
        
        // show the logging message...
        $('#login-progress-label').text('Logging in...');
        $('#login-progress-container').show();
        $('#login-submission-container').hide();
        
        // login submit
        $.ajax({
            type: 'POST',
            url: config.urls.token,
            data: {
                'username': $('#username').val(),
                'password': $('#password').val()
            }
        }).done(function (jwt) {
            // get the payload and store the token with the appropirate expiration
            var token = nf.Common.getJwtPayload(jwt);
            var expiration = parseInt(token['exp'], 10) * nf.Common.MILLIS_PER_SECOND;
            nf.Storage.setItem('jwt', jwt, expiration);

            // check to see if they actually have access now
            $.ajax({
                type: 'GET',
                url: config.urls.accessStatus,
                dataType: 'json'
            }).done(function (response) {
                var accessStatus = response.accessStatus;
                
                // update the logout link appropriately
                showLogoutLink();
                
                // update according to the access status
                if (accessStatus.status === 'ACTIVE') {
                    // reload as appropriate - no need to schedule token refresh as the page is reloading
                    if (top !== window) {
                        parent.window.location = '/nifi';
                    } else {
                        window.location = '/nifi';
                    }
                } else {
                    $('#login-message-title').text('Unable to log in');
                    $('#login-message').text(accessStatus.message);

                    // update visibility
                    $('#login-container').hide();
                    $('#login-submission-container').hide();
                    $('#login-progress-container').hide();
                    $('#login-message-container').show();
                }
            }).fail(function (xhr, status, error) {
                $('#login-message-title').text('Unable to log in');
                $('#login-message').text(xhr.responseText);

                // update visibility
                $('#login-container').hide();
                $('#login-submission-container').hide();
                $('#login-progress-container').hide();
                $('#login-message-container').show();
            });
        }).fail(function (xhr, status, error) {
            nf.Dialog.showOkDialog({
                headerText: 'Login',
                dialogContent: nf.Common.escapeHtml(xhr.responseText)
            });

            // update the form visibility
            $('#login-submission-container').show();
            $('#login-progress-container').hide();
        });
    };

    var showLogoutLink = function () {
        nf.Common.showLogoutLink();
    };

    return {
        /**
         * Initializes the login page.
         */
        init: function () {
            nf.Storage.init();

            if (nf.Storage.getItem('jwt') !== null) {
                showLogoutLink();
            }
            
            // supporting logging in via enter press
            $('#username, #password').on('keyup', function (e) {
                var code = e.keyCode ? e.keyCode : e.which;
                if (code === $.ui.keyCode.ENTER) {
                    login();
                }
            });

            // access status
            var accessStatus = $.ajax({
                type: 'GET',
                url: config.urls.accessStatus,
                dataType: 'json'
            }).fail(function (xhr, status, error) {
                $('#login-message-title').text('Unable to check Access Status');
                $('#login-message').text(xhr.responseText);
                initializeMessage();
            });
            
            // access config
            var accessConfigXhr = $.ajax({
                type: 'GET',
                url: config.urls.accessConfig,
                dataType: 'json'
            });

            $.when(accessStatus, accessConfigXhr).done(function (accessStatusResult, accessConfigResult) {
                var accessStatusResponse = accessStatusResult[0];
                var accessStatus = accessStatusResponse.accessStatus;

                var accessConfigResponse = accessConfigResult[0];
                var accessConfig = accessConfigResponse.config;
                
                // possible login states
                var needsLogin = true;
                var showMessage = false;
                
                // handle the status appropriately
                if (accessStatus.status === 'UNKNOWN') {
                    needsLogin = true;
                } else if (accessStatus.status === 'ACTIVE') {
                    showMessage = true;
                    needsLogin = false;
                    
                    $('#login-message-title').text('Success');
                    $('#login-message').text(accessStatus.message);
                }
                
                // if login is required, verify its supported
                if (accessConfig.supportsLogin === false && needsLogin === true) {
                    $('#login-message-title').text('Access Denied');
                    $('#login-message').text('This NiFi is not configured to support username/password logins.');
                    showMessage = true;
                    needsLogin = false;
                }

                // initialize the page as appropriate
                if (showMessage === true) {
                    initializeMessage();
                } else if (needsLogin === true) {
                    showLogin();
                    initializeSubmission();
                }
            });
        }
    };
}());