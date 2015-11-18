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

    var supportsAnonymous = false;

    var config = {
        urls: {
            identity: '../nifi-api/controller/identity',
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

    var initializeNiFiRegistration = function () {
        $('#nifi-registration-justification').count({
            charCountField: '#remaining-characters'
        });

        // toggle between signup and login
        $('#login-to-account-link').on('click', function () {
            showLogin();
        });
    };

    var showNiFiRegistration = function () {
        // reset the forms
        $('#login-submission-button').text('Submit');
        $('#nifi-registration-justification').val('');

        // update the form visibility
        $('#login-container').hide();
        $('#nifi-registration-container').show();
    };

    var initializeSubmission = function () {
        $('#login-submission-button').on('click', function () {
            if ($('#login-container').is(':visible')) {
                login();
            } else if ($('#nifi-registration-container').is(':visible')) {
                submitJustification();
            }
        });

        $('#login-submission-container').show();
    };

    var login = function () {
        // show the logging message...
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
                url: config.urls.identity,
                dataType: 'json'
            }).done(function (response) {
                if (response.identity === 'anonymous') {
                    showLogoutLink();

                    // schedule automatic token refresh
                    nf.Common.scheduleTokenRefresh();
            
                    // show the user
                    $('#nifi-user-submit-justification').text(token['preferred_username']);

                    // show the registration form
                    initializeNiFiRegistration();
                    showNiFiRegistration();
                    
                    // update the form visibility
                    $('#login-submission-container').show();
                    $('#login-progress-container').hide();
                } else {
                    // reload as appropriate - no need to schedule token refresh as the page is reloading
                    if (top !== window) {
                        parent.window.location = '/nifi';
                    } else {
                        window.location = '/nifi';
                    }
                }
            }).fail(function (xhr, status, error) {
                showLogoutLink();

                // schedule automatic token refresh
                nf.Common.scheduleTokenRefresh();

                // show the user
                $('#nifi-user-submit-justification').text(token['preferred_username']);

                if (xhr.status === 401) {
                    initializeNiFiRegistration();
                    showNiFiRegistration();
                    
                    // update the form visibility
                    $('#login-submission-container').show();
                    $('#login-progress-container').hide();
                } else {
                    $('#login-message-title').text('Unable to log in');
                    $('#login-message').text(xhr.responseText);

                    // update visibility
                    $('#login-container').hide();
                    $('#login-submission-container').hide();
                    $('#login-progress-container').hide();
                    $('#login-message-container').show();
                }
            });
        }).fail(function (xhr, status, error) {
            if (xhr.status === 400) {
                nf.Dialog.showOkDialog({
                    dialogContent: nf.Common.escapeHtml(xhr.responseText),
                    overlayBackground: false
                });
                
                // update the form visibility
                $('#login-submission-container').show();
                $('#login-progress-container').hide();
            } else {
                $('#login-message-title').text('Unable to log in');
                $('#login-message').text(xhr.responseText);

                // update visibility
                $('#login-container').hide();
                $('#login-submission-container').hide();
                $('#login-progress-container').hide();
                $('#login-message-container').show();
            }
        });
    };

    var submitJustification = function () {
        // attempt to create the nifi account registration
        $.ajax({
            type: 'POST',
            url: config.urls.users,
            data: {
                'justification': $('#nifi-registration-justification').val()
            }
        }).done(function (response) {
            var markup = 'An administrator will process your request shortly.';
            if (supportsAnonymous === true) {
                markup += '<br/><br/>In the meantime you can continue accessing anonymously.';
            }

            $('#login-message-title').text('Thanks!');
            $('#login-message').html(markup);
        }).fail(function (xhr, status, error) {
            $('#login-message-title').text('Unable to submit justification');
            $('#login-message').text(xhr.responseText);
        }).always(function () {
            // update form visibility
            $('#nifi-registration-container').hide();
            $('#login-submission-container').hide();
            $('#login-message-container').show();
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
                
                // record whether this NiFi supports anonymous access
                supportsAnonymous = accessConfig.supportsAnonymous;
            
                // possible login states
                var needsLogin = false;
                var needsNiFiRegistration = false;
                var showMessage = false;
                
                // handle the status appropriately
                if (accessStatus.status === 'UNKNOWN') {
                    needsLogin = true;
                } else if (accessStatus.status === 'UNREGISTERED') {
                    needsNiFiRegistration = true;
                    
                    $('#nifi-user-submit-justification').text(accessStatus.username);
                } else if (accessStatus.status === 'NOT_ACTIVE') {
                    showMessage = true;
                    
                    $('#login-message-title').text('Access Denied');
                    $('#login-message').text(accessStatus.message);
                } else if (accessStatus.status === 'ACTIVE') {
                    showMessage = true;
                    
                    $('#login-message-title').text('Success');
                    $('#login-message').text('Your account is active and you are already logged in.');
                }
                
                // if login is required, verify its supported
                if (accessConfig.supportsLogin === false && needsLogin === true) {
                    $('#login-message-title').text('Access Denied');
                    $('#login-message').text('This NiFi is not configured to support login.');
                    showMessage = true;
                    needsLogin = false;
                }

                // initialize the page as appropriate
                if (showMessage === true) {
                    initializeMessage();
                } else if (needsLogin === true) {
                    showLogin();
                } else if (needsNiFiRegistration === true) {
                    initializeNiFiRegistration();
                    showNiFiRegistration();
                }

                if (needsLogin === true || needsNiFiRegistration === true) {
                    initializeSubmission();
                }
            });
        }
    };
}());