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
    var loadControllerConfiguration = function () {
        return $.ajax({
            type: 'GET',
            url: '../nifi-api/controller/login/config',
            dataType: 'json'
        });
    };

    var initializePage = function () {
        return loadControllerConfiguration().done(function (response) {
            var config = response.config;
            
            if (config.supportsLogin === true) {
                
            }
            
            if (config.supportsRegistration === true) {
                
            }
        });
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

    return {
        /**
         * Initializes the login page.
         */
        init: function () {
            initializePage();
            
            // handle login click
            $('#login-button').on('click', function () {
                login().done(function (response) {
                   console.log(response); 
                });
            });
        }
    };
}());