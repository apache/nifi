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
            identity: '../nifi-api/controller/identity',
            users: '../nifi-api/controller/users',
            token: '../nifi-api/token',
            loginConfig: '../nifi-api/controller/login/config'
        }
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
                    login().done(function (response) {
                        
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
                    // TODO
    //                // hide the registration pane
    //                $('#registration-pane').hide();
    //
    //                // show the message pane
    //                $('#message-pane').show();
    //                $('#message-title').text('Thanks');
    //                $('#message-content').text('Your request will be processed shortly.');
                }).fail(nf.Common.handleAjaxError);
            }
        });
    };

    return {
        /**
         * Initializes the login page.
         */
        init: function () {
            nf.Storage.init();
            
            var needsLogin = false;
            var needsNiFiRegistration = false;
            
            var token = $.ajax({
                type: 'GET',
                url: config.urls.token
            });
            
            var pageStateInit = $.Deferred(function(deferred) {
                // get the current user's identity
                $.ajax({
                    type: 'GET',
                    url: config.urls.identity,
                    dataType: 'json'
                }).done(function (response) {
                    var identity = response.identity;

                    // if the user is anonymous they need to login
                    if (identity === 'anonymous') {
                        token.done(function () {
                            // anonymous user and 200 from token means they have a certificate but have not yet requested an account
                            needsNiFiRegistration = true;
                        }).fail(function (xhr, status, error) {
                            // no token granted, user needs to login with their credentials
                            needsLogin = true;
                        });
                    }
                }).fail(function (xhr, status, error) {
                    if (xhr.status === 401) {
                        // attempt to get a token for the current user without passing login credentials
                        token.done(function () {
                            // 401 from identity request and 200 from token means they have a certificate but have not yet requested an account 
                            needsNiFiRegistration = true;
                        }).fail(function (xhr, status, error) {
                            // no token granted, user needs to login with their credentials
                            needsLogin = true;
                        });
                    } else if (xhr.status === 403) {
                        // the user is logged in with certificate or credentials but their account is still pending. error message should indicate
                        // TODO - show error
                    }
                }).always(function () {
                    deferred.resolve();
                });
            }).promise();
            
            // render the page accordingly
            $.when(pageStateInit).done(function () {
                if (needsLogin === true) {
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