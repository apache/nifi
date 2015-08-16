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

nf.Registration = (function () {

    var config = {
        urls: {
            users: '../nifi-api/controller/users'
        }
    };

    return {
        /**
         * Initializes the user account registration form.
         */
        init: function () {
            $('#registration-justification').count({
                charCountField: '#remaining-characters'
            });

            // register a click listener to expand/collapse the registration form
            $('#expand-registration-button, #expand-registration-text').click(function () {
                var registrationForm = $('#registration-form');
                if (registrationForm.is(':visible')) {
                    $('#expand-registration-button').removeClass('registration-expanded').addClass('collapsed');
                } else {
                    $('#expand-registration-button').removeClass('registration-collapsed').addClass('expanded');
                }
                registrationForm.toggle();
            });

            // register a click listener for submitting user account requests
            $('#registration-form-submit').one('click', function () {
                var justification = $('#registration-justification').val();

                // attempt to create the user account registration
                $.ajax({
                    type: 'POST',
                    url: config.urls.users,
                    data: {
                        'justification': justification
                    }
                }).done(function (response) {
                    // hide the registration pane
                    $('#registration-pane').hide();

                    // show the message pane
                    $('#message-pane').show();
                    $('#message-title').text('Thanks');
                    $('#message-content').text('Your request will be processed shortly.');
                }).fail(nf.Common.handleAjaxError);
            });
        }
    };
}());