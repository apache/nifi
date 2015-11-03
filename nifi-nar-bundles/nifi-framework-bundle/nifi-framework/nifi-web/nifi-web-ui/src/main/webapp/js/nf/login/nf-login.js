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

    var initializeLogin = function () {
        return $.ajax({
            type: 'GET',
            url: config.urls.loginConfig,
            dataType: 'json'
        }).done(function (response) {
            var config = response.config;
            
            // if this nifi supports login, render the login form
            if (config.supportsLogin === true) {
                
                // handle login click
                $('#login-button').on('click', function () {
                    login().done(function (response, status, xhr) {
                        var authorization = xhr.getResponseHeader('Authorization');
                        var badToken = false;
                        
                        // ensure there was a token in the response
                        if (authorization) {
                            var tokens = authorization.split(/ /);
                            
                            // ensure the token is the appropriate length
                            if (tokens.length === 2) {
                                // store the jwt and reload the page
                                nf.Storage.setItem('jwt', tokens[1]);
                                window.location = '/nifi';
                            } else {
                                badToken = true;
                            }
                        } else {
                            badToken = true;
                        }
                        
                        if (badToken === true) {
                            // TODO - show unable to parse response token
                        }
                    });
                });
                
                // show the login form
                $('#login-container').show();
            }
            
            // if this nifi supports registration, render the registration form
            if (config.supportsRegistration === true) {
                initializeUserRegistration();
                
                // automatically include support for nifi registration
                initializeNiFiRegistration();
            }
        });
    };
    
    var initializeUserRegistration = function () {
        
        // show the user registration form
        $('#user-registration-container').show();
    };
    
    var login = function () {
        var username = $('#username').val();
        var password = $('#password').val();
        
        return $.ajax({
            type: 'POST',
            url: '../nifi-api/token',
            data: {
                'username': username,
                'password': password
            },
            dataType: 'json'
        });
    };
    
    var initializeNiFiRegistration = function () {
        $('#nifi-registration-justification').count({
            charCountField: '#remaining-characters'
        });

        // show the nifi registration container
        $('#nifi-registration-container').show();
    };
    
    var initializeSubmission = function () {
        $('#login-submission-button').one('click', function () {
            if ($('#login-container').is(':visible')) {
                // login submit
            } else if ($('#user-registration-container').is(':visible')) {
                // new user account submit
            } else if ($('#nifi-registration-container').is(':visible')) {
                // new nifi account submit
                var justification = $('#registration-justification').val();

                // attempt to create the user account registration
                $.ajax({
                    type: 'POST',
                    url: config.urls.users,
                    data: {
                        'justification': justification
                    }
                }).done(function (response) {
                    $('#login-message').text('Thanks! Your request will be processed shortly.');
                }).fail(function (xhr, status, error) {
                    $('#login-message').text(xhr.responseText);
                }).always(function () {
                    // update form visibility
                    $('#nifi-registration-container').hide();
                    $('#login-submission-container').hide();
                    $('#login-message-container').show();
                });
            }
        });
        
        $('#login-submission-container').show();
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
            
            var token = $.ajax({
                type: 'GET',
                url: config.urls.token
            });
            
            var identity = $.ajax({
                type: 'GET',
                url: config.urls.identity,
                dataType: 'json'
            });
            
            var pageStateInit = $.Deferred(function(deferred) {
                // get the current user's identity
                identity.done(function (response) {
                    // if the user is anonymous see if they need to login or if they are working with a certificate
                    if (response.identity === 'anonymous') {
                        // request a token without including credentials, if successful then the user is using a certificate
                        token.done(function () {
                            // the user is using a certificate, see if their account is active/pending/revoked/etc
                            $.ajax({
                                type: 'GET',
                                url: config.urls.registrationStatus
                            }).done(function () {
                                showMessage = true;
                                
                                // account is active and good
                                $('#login-message').text('Your account is active and you are already logged in.');
                                deferred.resolve();
                            }).fail(function (xhr, status, error) {
                                if (xhr.status === 401) {
                                    // anonymous user and 401 means they need nifi registration
                                    needsNiFiRegistration = true;
                                } else {
                                    showMessage = true;
                                    
                                    // anonymous user and non-401 means they already have an account and it's pending/revoked 
                                    if ($.trim(xhr.responseText) === '') {
                                        $('#login-message').text('Unable to check registration status.');
                                    } else {
                                        $('#login-message').text(xhr.responseText);
                                    }
                                }
                                deferred.resolve();
                            });
                        }).fail(function () {
                            // no token granted, user has no certificate and needs to login with their credentials
                            needsLogin = true;
                            deferred.resolve();
                        });
                    } else {
                        showMessage = true;
                        
                        // the user is not anonymous and has an active account (though maybe role-less)
                        $('#login-message').text('Your account is active and you are already logged in.');
                        deferred.resolve();
                    }
                }).fail(function (xhr, status, error) {
                    // unable to get identity (and no anonymous user) see if we can offer login
                    if (xhr.status === 401) {
                        // attempt to get a token for the current user without passing login credentials
                        token.done(function () {
                            // 401 from identity request and 200 from token means they have a certificate but have not yet requested an account 
                            needsNiFiRegistration = true;
                        }).fail(function () {
                            // no token granted, user needs to login with their credentials
                            needsLogin = true;
                        }).always(function () {
                            deferred.resolve();
                        });
                    } else {
                        showMessage = true;
                        
                        // the user is logged in with certificate or credentials but their account is pending/revoked. error message should indicate
                        if ($.trim(xhr.responseText) === '') {
                            $('#login-message').text('Unable to authorize you to use this NiFi and anonymous access is disabled.');
                        } else {
                            $('#login-message').text(xhr.responseText);
                        }
                        
                        deferred.resolve();
                    }
                });
            }).promise();
            
            // render the page accordingly
            pageStateInit.done(function () {
                if (showMessage === true) {
                    initializeMessage();
                } else if (needsLogin === true) {
                    initializeLogin();
                } else if (needsNiFiRegistration === true) {
                    initializeNiFiRegistration();
                }
                
                if (needsLogin === true || needsNiFiRegistration === true) {
                    initializeSubmission();
                }
            });
        }
    };
}());