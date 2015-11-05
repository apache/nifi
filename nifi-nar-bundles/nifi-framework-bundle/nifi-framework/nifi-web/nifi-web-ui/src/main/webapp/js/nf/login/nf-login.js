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

    var isAnonymous = false;

    var config = {
        urls: {
            registrationStatus: '../nifi-api/registration/status',
            registration: '../nifi-api/registration',
            identity: '../nifi-api/controller/identity',
            users: '../nifi-api/controller/users',
            token: '../nifi-api/token',
            loginConfig: '../nifi-api/controller/login/config'
        }
    };

    var initializeMessage = function () {
        $('#login-message-container').show();
    };

    var initializeLogin = function (supportsRegistration) {
        // if this nifi supports registration, render the registration form
        if (supportsRegistration === true) {
            initializeUserRegistration();
            initializeNiFiRegistration();

            // show the create account message
            $('#create-account-message').show();

            // toggle between login and signup
            $('#create-account-link').on('click', function () {
                showUserRegistration();
            });
        }
    };

    var showLogin = function () {
        $('#login-container').show();
        $('#user-registration-container').hide();
        $('#nifi-registration-container').hide();
        $('#login-submission-button').text('Log in');
    };

    var initializeUserRegistration = function () {
    };

    var initializeNiFiRegistration = function () {
        $('#nifi-registration-justification').count({
            charCountField: '#remaining-characters'
        });
    };

    var showUserRegistration = function () {
        showNiFiRegistration();

        $('div.nifi-submit-justification').hide();
        $('#user-registration-container').show();
        $('#login-submission-button').text('Create');
    };

    var showNiFiRegistration = function () {
        $('#login-container').hide();
        $('#nifi-registration-container').show();
        $('#login-submission-button').text('Submit');
    };

    var initializeSubmission = function () {
        $('#login-submission-button').on('click', function () {
            if ($('#login-container').is(':visible')) {
                login();
            } else if ($('#user-registration-container').is(':visible')) {
                createUserAccount();
            } else if ($('#nifi-registration-container').is(':visible')) {
                submitJustification();
            }
        });

        $('#login-submission-container').show();
    };

    var login = function () {
        // login submit
        $.ajax({
            type: 'POST',
            url: config.urls.token,
            data: {
                'username': $('#username').val(),
                'password': $('#password').val()
            }
        }).done(function (jwt) {
            // store the jwt and reload the page
            nf.Storage.setItem('jwt', jwt);

            // reload as appropriate
            if (top !== window) {
                parent.window.location = '/nifi';
            } else {
                window.location = '/nifi';
            }
        }).fail(function (xhr, status, error) {
            if (xhr.status === 400) {
                nf.Dialog.showOkDialog({
                    dialogContent: nf.Common.escapeHtml(xhr.responseText),
                    overlayBackground: false
                });
            } else {
                $('#login-message-title').text('Unable to log in');
                $('#login-message').text(xhr.responseText);

                // update visibility
                $('#login-container').hide();
                $('#login-submission-container').hide();
                $('#login-message-container').show();
            }
        });
    };

    var createUserAccount = function () {
        var password = $('#registration-password').val();
        var passwordConfirmation = $('#registration-password-confirmation').val();

        // ensure the password matches
        if (password !== passwordConfirmation) {
            nf.Dialog.showOkDialog({
                dialogContent: 'The specified passwords do not match.',
                overlayBackground: false
            });
            return;
        }

        // attempt to create the user account registration
        $.ajax({
            type: 'POST',
            url: config.urls.registration,
            data: {
                'username': $('#registration-username').val(),
                'password': password,
                'justification': $('#nifi-registration-justification').val()
            }
        }).done(function (response, status, xhr) {
            var markup = 'An administrator will process your request shortly.';
            if (isAnonymous === true) {
                markup += '<br/><br/>In the meantime you can continue accessing anonymously.';
            }

            $('#login-message-title').text('Thanks!');
            $('#login-message').html(markup);
        }).fail(function (xhr, status, error) {
            $('#login-message-title').text('Unable to create user account');
            $('#login-message').text(xhr.responseText);
        }).always(function () {
            // update form visibility
            $('#user-registration-container').hide();
            $('#nifi-registration-container').hide();
            $('#login-submission-container').hide();
            $('#login-message-container').show();
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
            if (isAnonymous === true) {
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

    /**
     * Extracts the subject from the specified jwt. If the jwt is not as expected
     * an empty string is returned.
     * 
     * @param {string} jwt
     * @returns {string}
     */
    var getJwtSubject = function (jwt) {
        if (nf.Common.isDefinedAndNotNull(jwt)) {
            var segments = jwt.split(/\./);
            if (segments.length !== 3) {
                return '';
            }

            var rawPayload = $.base64.atob(segments[1]);
            var payload = JSON.parse(rawPayload);

            if (nf.Common.isDefinedAndNotNull(payload['preferred_username'])) {
                return payload['preferred_username'];
            } else {
                '';
            }
        }

        return '';
    };

    return {
        /**
         * Initializes the login page.
         */
        init: function () {
            nf.Storage.init();

            var showMessage = false;
            var needsLogin = false;
            var needsNiFiRegistration = false;

            var logout = function () {
                nf.Storage.removeItem('jwt');
            };
            
            // handle logout
            $('#nifi-user-submit-justification-logout').on('click', function () {
                logout();
                window.location = '/nifi/login';
            });

            var token = $.ajax({
                type: 'GET',
                url: config.urls.token
            });

            var identity = $.ajax({
                type: 'GET',
                url: config.urls.identity,
                dataType: 'json'
            });

            var pageStateInit = $.Deferred(function (deferred) {
                // get the current user's identity
                identity.done(function (response) {
                    // if the user is anonymous see if they need to login or if they are working with a certificate
                    if (response.identity === 'anonymous') {
                        isAnonymous = true;

                        // request a token without including credentials, if successful then the user is using a certificate
                        token.done(function (jwt) {

                            // the user is using a certificate/token, see if their account is active/pending/revoked/etc
                            $.ajax({
                                type: 'GET',
                                url: config.urls.registrationStatus
                            }).done(function () {
                                showMessage = true;

                                // account is active and good
                                $('#login-message-title').text('Success');
                                $('#login-message').text('Your account is active and you are already logged in.');
                            }).fail(function (xhr, status, error) {
                                if (xhr.status === 401) {
                                    var user = getJwtSubject(jwt);
                                    
                                    // show the user
                                    $('#nifi-user-submit-justification').text(user);

                                    // render the logout button if there is a token locally
                                    if (nf.Storage.getItem('jwt') !== null) {
                                        $('#nifi-user-submit-justification-logout').show();
                                    }

                                    // anonymous user and 401 means they need nifi registration
                                    needsNiFiRegistration = true;
                                } else {
                                    showMessage = true;

                                    // anonymous user and non-401 means they already have an account and it's pending/revoked
                                    $('#login-message-title').text('Access Denied');
                                    if ($.trim(xhr.responseText) === '') {
                                        $('#login-message').text('Unable to check registration status.');
                                    } else {
                                        $('#login-message').text(xhr.responseText);
                                    }
                                }
                            }).always(function () {
                                deferred.resolve();
                            });
                        }).fail(function (tokenXhr) {
                            if (tokenXhr.status === 400) {
                                // no credentials supplied so 400 must be due to an invalid/expired token
                                logout();
                            }

                            // no token granted, user has no certificate and needs to login with their credentials
                            needsLogin = true;
                            deferred.resolve();
                        });
                    } else {
                        showMessage = true;

                        // the user is not anonymous and has an active account (though maybe role-less)
                        $('#login-message-title').text('Success');
                        $('#login-message').text('Your account is active and you are already logged in.');
                        deferred.resolve();
                    }
                }).fail(function (xhr, status, error) {
                    // unable to get identity (and no anonymous user) see if we can offer login
                    if (xhr.status === 401) {
                        // attempt to get a token for the current user without passing login credentials
                        token.done(function (jwt) {
                            var user = getJwtSubject(jwt);

                            // show the user
                            $('#nifi-user-submit-justification').text(user);

                            // render the logout button if there is a token locally
                            if (nf.Storage.getItem('jwt') !== null) {
                                $('#nifi-user-submit-justification-logout').show();
                            }

                            // 401 from identity request and 200 from token means they have a certificate/token but have not yet requested an account 
                            needsNiFiRegistration = true;
                        }).fail(function (tokenXhr) {
                            if (tokenXhr.status === 400) {
                                // no credentials supplied so 400 must be due to an invalid/expired token
                                logout();
                            }

                            // no token granted, user needs to login with their credentials
                            needsLogin = true;
                        }).always(function () {
                            deferred.resolve();
                        });
                    } else {
                        showMessage = true;

                        // the user is logged in with certificate or credentials but their account is pending/revoked. error message should indicate
                        $('#login-message-title').text('Access Denied');
                        if ($.trim(xhr.responseText) === '') {
                            $('#login-message').text('Unable to authorize you to use this NiFi and anonymous access is disabled.');
                        } else {
                            $('#login-message').text(xhr.responseText);
                        }

                        deferred.resolve();
                    }
                });
            }).promise();

            var loginConfigXhr = $.ajax({
                type: 'GET',
                url: config.urls.loginConfig,
                dataType: 'json'
            });

            // render the page accordingly
            $.when(loginConfigXhr, pageStateInit).done(function (loginResult) {
                var loginResponse = loginResult[0];
                var loginConfig = loginResponse.config;

                // if login is required, verify its supported
                if (loginConfig.supportsLogin === false && needsLogin === true) {
                    $('#login-message-title').text('Access Denied');
                    $('#login-message').text('This NiFi is not configured to support login.');
                    showMessage = true;
                    needsLogin = false;
                }

                if (showMessage === true) {
                    initializeMessage();
                } else if (needsLogin === true) {
                    initializeLogin(loginConfig.supportsRegistration);
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