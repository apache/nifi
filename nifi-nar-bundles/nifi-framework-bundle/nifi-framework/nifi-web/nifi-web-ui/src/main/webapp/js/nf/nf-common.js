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

/* global nf, parseFloat */

$(document).ready(function () {
    // preload the image for the error page - this is preloaded because the system
    // may be unavailable to return the image when the error page is rendered
    var imgSrc = 'images/bg-error.png';
    $('<img/>').attr('src', imgSrc).on('load', function () {
        $('div.message-pane').css('background-image', imgSrc);
    });

    // mouse over for links
    $(document).on('mouseenter', 'span.link', function () {
        $(this).addClass('link-over');
    }).on('mouseleave', 'span.link', function () {
        $(this).removeClass('link-over');
    });

    // setup custom checkbox
    $(document).on('click', 'div.nf-checkbox', function () {
        var checkbox = $(this);
        if (checkbox.hasClass('checkbox-unchecked')) {
            checkbox.removeClass('checkbox-unchecked').addClass('checkbox-checked');
        } else {
            checkbox.removeClass('checkbox-checked').addClass('checkbox-unchecked');
        }
    });

    // show the loading icon when appropriate
    $(document).ajaxStart(function () {
        // show the loading indicator 
        $('div.loading-container').addClass('ajax-loading');
    }).ajaxStop(function () {
        // hide the loading indicator 
        $('div.loading-container').removeClass('ajax-loading');
    });
    
    // shows the logout link in the message-pane when appropriate and schedule token refresh
    if (nf.Storage.getItem('jwt') !== null) {
        $('#user-logout-container').css('display', 'block');
        nf.Common.scheduleTokenRefresh();
    }
    
    // handle logout
    $('#user-logout').on('click', function () {
        nf.Storage.removeItem('jwt');
        window.location = '/nifi/login';
    });
    
    // handle home
    $('#user-home').on('click', function () {
        if (top !== window) {
            parent.window.location = '/nifi';
        } else {
            window.location = '/nifi';
        }
    });
});

// Define a common utility class used across the entire application.
nf.Common = (function () {
    // interval for cancelling token refresh when necessary
    var tokenRefreshInterval = null;
    
    return {
        ANONYMOUS_USER_TEXT: 'Anonymous user',

        config: {
            sensitiveText: 'Sensitive value set',
            tooltipConfig: {
                style: {
                    classes: 'nifi-tooltip'
                },
                show: {
                    solo: true,
                    effect: function(offset) {
                        $(this).slideDown(100);
                    }
                },
                hide: {
                    effect: function(offset) {
                        $(this).slideUp(100);
                    }
                },
                position: {
                    at: 'bottom center',
                    my: 'top center',
                    adjust: {
                        y: 5
                    }
                }
            }
        },

        /**
         * Determines if the current broswer supports SVG.
         */
        SUPPORTS_SVG: !!document.createElementNS && !!document.createElementNS('http://www.w3.org/2000/svg', 'svg').createSVGRect,

        /**
         * The current user.
         */
        currentUser: undefined,

        /**
         * Sets the current user.
         * 
         * @param currentUser
         */
        setCurrentUser: function (currentUser) {
            nf.Common.currentUser = currentUser;
        },

        /**
         * Automatically refresh tokens by checking once an hour if its going to expire soon.
         */
        scheduleTokenRefresh: function () {
            // if we are currently polling for token refresh, cancel it
            if (tokenRefreshInterval !== null) {
                clearInterval(tokenRefreshInterval);
            }
            
            // set the interval to one hour
            var interval = nf.Common.MILLIS_PER_MINUTE;
            
            var checkExpiration = function () {
                var expiration = nf.Storage.getItemExpiration('jwt');

                // ensure there is an expiration and token present
                if (expiration !== null) {
                    var expirationDate = new Date(expiration);
                    var now = new Date();

                    // get the time remainging plus a little bonus time to reload the token
                    var timeRemaining = expirationDate.valueOf() - now.valueOf() - (30 * nf.Common.MILLIS_PER_SECOND);
                    if (timeRemaining < interval) {
                        if ($('#current-user').text() !== nf.Common.ANONYMOUS_USER_TEXT && !$('#anonymous-user-alert').is(':visible')) {
                            // if the token will expire before the next interval minus some bonus time, notify the user to re-login
                            $('#anonymous-user-alert').show().qtip($.extend({}, nf.Common.config.tooltipConfig, {
                                content: 'Your session will expire soon. Please log in again to avoid being automatically logged out.',
                                position: {
                                    my: 'top right',
                                    at: 'bottom left'
                                }
                            }));
                        }
                    }
                }
            };
            
            // perform initial check
            checkExpiration();
            
            // schedule subsequent checks
            tokenRefreshInterval = setInterval(checkExpiration, interval);
        },

        /**
         * Sets the anonymous user label.
         */
        setAnonymousUserLabel: function () {
            var anonymousUserAlert = $('#anonymous-user-alert');
            if (anonymousUserAlert.data('qtip')) {
                anonymousUserAlert.qtip('api').destroy(true);
            }
                        
            // alert user's of anonymous access
            anonymousUserAlert.show().qtip($.extend({}, nf.Common.config.tooltipConfig, {
                content: 'You are accessing with limited authority. Log in or request an account to access with additional authority granted to you by an administrator.',
                position: {
                    my: 'top right',
                    at: 'bottom left'
                }
            }));

            // render the anonymous user text
            $('#current-user').text(nf.Common.ANONYMOUS_USER_TEXT).show();  
        },

        /**
         * Extracts the subject from the specified jwt. If the jwt is not as expected
         * an empty string is returned.
         * 
         * @param {string} jwt
         * @returns {string}
         */
        getJwtPayload: function (jwt) {
            if (nf.Common.isDefinedAndNotNull(jwt)) {
                var segments = jwt.split(/\./);
                if (segments.length !== 3) {
                    return '';
                }

                var rawPayload = $.base64.atob(segments[1]);
                var payload = JSON.parse(rawPayload);

                if (nf.Common.isDefinedAndNotNull(payload)) {
                    return payload;
                } else {
                    return null;
                }
            }

            return null;
        },

        /**
         * Determines whether the current user can access provenance.
         * 
         * @returns {boolean}
         */
        canAccessProvenance: function () {
            if (nf.Common.isDefinedAndNotNull(nf.Common.currentUser)) {
                return nf.Common.currentUser.provenancePermissions.canRead === true;
            } else {
                return false;
            }
        },

        /**
         * Determines whether the current user can access counters.
         * 
         * @returns {boolean}
         */
        canAccessCounters: function () {
            if (nf.Common.isDefinedAndNotNull(nf.Common.currentUser)) {
                return nf.Common.currentUser.countersPermissions.canRead === true;
            } else {
                return false;
            }
        },

        /**
         * Determines whether the current user can modify counters.
         * 
         * @returns {boolean}
         */
        canModifyCounters: function () {
            if (nf.Common.isDefinedAndNotNull(nf.Common.currentUser)) {
                return nf.Common.currentUser.countersPermissions.canRead === true && nf.Common.currentUser.countersPermissions.canWrite === true;
            } else {
                return false;
            }
        },

        /**
         * Determines whether the current user can access tenants.
         *
         * @returns {boolean}
         */
        canAccessTenants: function () {
            if (nf.Common.isDefinedAndNotNull(nf.Common.currentUser)) {
                return nf.Common.currentUser.tenantsPermissions.canRead === true;
            } else {
                return false;
            }
        },

        /**
         * Determines whether the current user can modify tenants.
         *
         * @returns {boolean}
         */
        canModifyTenants: function () {
            if (nf.Common.isDefinedAndNotNull(nf.Common.currentUser)) {
                return nf.Common.currentUser.tenantsPermissions.canRead === true && nf.Common.currentUser.tenantsPermissions.canWrite === true;
            } else {
                return false;
            }
        },

        /**
         * Determines whether the current user can access counters.
         *
         * @returns {boolean}
         */
        canAccessPolicies: function () {
            if (nf.Common.isDefinedAndNotNull(nf.Common.currentUser)) {
                return nf.Common.currentUser.policiesPermissions.canRead === true;
            } else {
                return false;
            }
        },

        /**
         * Determines whether the current user can modify counters.
         *
         * @returns {boolean}
         */
        canModifyPolicies: function () {
            if (nf.Common.isDefinedAndNotNull(nf.Common.currentUser)) {
                return nf.Common.currentUser.policiesPermissions.canRead === true && nf.Common.currentUser.policiesPermissions.canWrite === true;
            } else {
                return false;
            }
        },

        /**
         * Determines whether the current user can access the controller.
         *
         * @returns {boolean}
         */
        canAccessController: function () {
            if (nf.Common.isDefinedAndNotNull(nf.Common.currentUser)) {
                return nf.Common.currentUser.controllerPermissions.canRead === true;
            } else {
                return false;
            }
        },

        /**
         * Determines whether the current user can modify the controller.
         *
         * @returns {boolean}
         */
        canModifyController: function () {
            if (nf.Common.isDefinedAndNotNull(nf.Common.currentUser)) {
                return nf.Common.currentUser.controllerPermissions.canRead === true && nf.Common.currentUser.controllerPermissions.canWrite === true;
            } else {
                return false;
            }
        },

        /**
         * Adds a mouse over effect for the specified selector using
         * the specified styles.
         * 
         * @argument {string} selector      The selector for the element to add a hover effect for
         * @argument {string} normalStyle   The css style for the normal state
         * @argument {string} overStyle     The css style for the over state
         */
        addHoverEffect: function (selector, normalStyle, overStyle) {
            $(document).on('mouseenter', selector, function () {
                $(this).removeClass(normalStyle).addClass(overStyle);
            }).on('mouseleave', selector, function () {
                $(this).removeClass(overStyle).addClass(normalStyle);
            });
            return $(selector).addClass(normalStyle);
        },

        /**
         * Determine if an `element` has content overflow and adds the `.scrollable` class if it does.
         *
         * @param {HTMLElement} element The DOM element to toggle .scrollable upon.
         */
        toggleScrollable: function (element) {
            if ($(element).is(':visible')){
                if (element.offsetHeight < element.scrollHeight ||
                    element.offsetWidth < element.scrollWidth) {
                    // your element has overflow
                    $(element).addClass('scrollable');
                } else {
                    $(element).removeClass('scrollable');
                }
            }
        },

        /**
         * Method for handling ajax errors.
         * 
         * @argument {object} xhr       The XmlHttpRequest
         * @argument {string} status    The status of the request
         * @argument {string} error     The error
         */
        handleAjaxError: function (xhr, status, error) {
            if (status === 'canceled') {
                if ($('#splash').is(':visible')) {
                    $('#message-title').text('Session Expired');
                    $('#message-content').text('Your session has expired. Please reload to log in again.');

                    // show the error pane
                    $('#message-pane').show();
                } else {
                    nf.Dialog.showOkDialog({
                        headerText: 'Session Expired',
                        dialogContent: 'Your session has expired. Please press Ok to log in again.',
                        okHandler: function () {
                            window.location = '/nifi';
                        }
                    });
                }
                
                // close the canvas
                nf.Common.closeCanvas();
                return;
            }
            
            // if an error occurs while the splash screen is visible close the canvas show the error message
            if ($('#splash').is(':visible')) {
                if (xhr.status === 401) {
                    $('#message-title').text('Unauthorized');
                } else if (xhr.status === 403) {
                    $('#message-title').text('Access Denied');
                } else if (xhr.status === 409) {
                    $('#message-title').text('Invalid State');
                } else {
                    $('#message-title').text('An unexpected error has occurred');
                }

                if ($.trim(xhr.responseText) === '') {
                    $('#message-content').text('Please check the logs.');
                } else {
                    $('#message-content').text(xhr.responseText);
                }

                // show the error pane
                $('#message-pane').show();

                // close the canvas
                nf.Common.closeCanvas();
                return;
            }

            // status code 400, 403, 404, and 409 are expected response codes for common errors.
            if (xhr.status === 400 || xhr.status === 403 || xhr.status === 404 || xhr.status === 409 || xhr.status === 503) {
                nf.Dialog.showOkDialog({
                    headerText: 'Error',
                    dialogContent: nf.Common.escapeHtml(xhr.responseText)
                });
            } else {
                if (xhr.status < 99 || xhr.status === 12007 || xhr.status === 12029) {
                    var content = 'Please ensure the application is running and check the logs for any errors.';
                    if (nf.Common.isDefinedAndNotNull(status)) {
                        if (status === 'timeout') {
                            content = 'Request has timed out. Please ensure the application is running and check the logs for any errors.';
                        } else if (status === 'abort') {
                            content = 'Request has been aborted.';
                        } else if (status === 'No Transport') {
                            content = 'Request transport mechanism failed. Please ensure the host where the application is running is accessible.';
                        }
                    }
                    $('#message-title').text('Unable to communicate with NiFi');
                    $('#message-content').text(content);
                } else if (xhr.status === 401) {
                    $('#message-title').text('Unauthorized');
                    if ($.trim(xhr.responseText) === '') {
                        $('#message-content').text('Authentication is required to use this NiFi.');
                    } else {
                        $('#message-content').text(xhr.responseText);
                    }
                } else if (xhr.status === 500) {
                    $('#message-title').text('An unexpected error has occurred');
                    if ($.trim(xhr.responseText) === '') {
                        $('#message-content').text('An error occurred communicating with the application core. Please check the logs and fix any configuration issues before restarting.');
                    } else {
                        $('#message-content').text(xhr.responseText);
                    }
                } else if (xhr.status === 200 || xhr.status === 201) {
                    $('#message-title').text('Parse Error');
                    if ($.trim(xhr.responseText) === '') {
                        $('#message-content').text('Unable to interpret response from NiFi.');
                    } else {
                        $('#message-content').text(xhr.responseText);
                    }
                } else {
                    $('#message-title').text(xhr.status + ': Unexpected Response');
                    $('#message-content').text('An unexpected error has occurred. Please check the logs.');
                }

                // show the error pane
                $('#message-pane').show();

                // close the canvas
                nf.Common.closeCanvas();
            }
        },

        /**
         * Closes the canvas by removing the splash screen and stats poller.
         */
        closeCanvas: function () {
            nf.Common.showLogoutLink();
            
            // ensure this javascript has been loaded in the nf canvas page
            if (nf.Common.isDefinedAndNotNull(nf.Canvas)) {
                // hide the splash screen if required
                if ($('#splash').is(':visible')) {
                    nf.Canvas.hideSplash();
                }

                // hide the context menu
                nf.ContextMenu.hide();

                // shut off the auto refresh
                nf.Canvas.stopPolling();
            }
        },

        /**
         * Shows the logout link if appropriate.
         */
        showLogoutLink: function () {
            if (nf.Storage.getItem('jwt') === null) {
                $('#user-logout-container').css('display', 'none');
            } else {
                $('#user-logout-container').css('display', 'block');
            }
        },

        /**
         * Returns whether a content viewer has been configured.
         *
         * @returns {boolean}
         */
        isContentViewConfigured: function () {
            var contentViewerUrl = $('#nifi-content-viewer-url').text();
            return !nf.Common.isBlank(contentViewerUrl);
        },

        /**
         * Populates the specified field with the specified value. If the value is 
         * undefined, the field will read 'No value set.' If the value is an empty
         * string, the field will read 'Empty string set.'
         * 
         * @argument {string} target        The dom Id of the target
         * @argument {string} value         The value
         */
        populateField: function (target, value) {
            if (nf.Common.isUndefined(value) || nf.Common.isNull(value)) {
                return $('#' + target).addClass('unset').text('No value previously set');
            } else if (value === '') {
                return $('#' + target).addClass('blank').text('Empty string previously set');
            } else {
                return $('#' + target).text(value);
            }
        },

        /**
         * Clears the specified field. Removes any style that may have been applied
         * by a preceeding call to populateField.
         * 
         * @argument {string} target        The dom Id of the target
         */
        clearField: function (target) {
            return $('#' + target).removeClass('unset blank').text('');
        },

        /**
         * Cleans up any tooltips that have been created for the specified container.
         * 
         * @param {jQuery} container
         * @param {string} tooltipTarget
         */
        cleanUpTooltips: function(container, tooltipTarget) {
            container.find(tooltipTarget).each(function () {
                var tip = $(this);
                if (tip.data('qtip')) {
                    var api = tip.qtip('api');
                    api.destroy(true);
                }
            });
        },

        /**
         * Formats the tooltip for the specified property.
         * 
         * @param {object} propertyDescriptor      The property descriptor
         * @param {object} propertyHistory         The property history
         * @returns {string}
         */
        formatPropertyTooltip: function (propertyDescriptor, propertyHistory) {
            var tipContent = [];

            // show the property description if applicable
            if (nf.Common.isDefinedAndNotNull(propertyDescriptor)) {
                if (!nf.Common.isBlank(propertyDescriptor.description)) {
                    tipContent.push(nf.Common.escapeHtml(propertyDescriptor.description));
                }
                if (!nf.Common.isBlank(propertyDescriptor.defaultValue)) {
                    tipContent.push('<b>Default value:</b> ' + nf.Common.escapeHtml(propertyDescriptor.defaultValue));
                }
                if (!nf.Common.isBlank(propertyDescriptor.supportsEl)) {
                    tipContent.push('<b>Supports expression language:</b> ' + nf.Common.escapeHtml(propertyDescriptor.supportsEl));
                }
            }

            if (nf.Common.isDefinedAndNotNull(propertyHistory)) {
                if (!nf.Common.isEmpty(propertyHistory.previousValues)) {
                    var history = [];
                    $.each(propertyHistory.previousValues, function (_, previousValue) {
                        history.push('<li>' + nf.Common.escapeHtml(previousValue.previousValue) + ' - ' + nf.Common.escapeHtml(previousValue.timestamp) + ' (' + nf.Common.escapeHtml(previousValue.userIdentity) + ')</li>');
                    });
                    tipContent.push('<b>History:</b><ul class="property-info">' + history.join('') + '</ul>');
                }
            }

            if (tipContent.length > 0) {
                return tipContent.join('<br/><br/>');
            } else {
                return null;
            }
        },

        /**
         * Formats the specified property (name and value) accordingly.
         * 
         * @argument {string} name      The name of the property
         * @argument {string} value     The value of the property
         */
        formatProperty: function (name, value) {
            return '<div><span class="label">' + nf.Common.formatValue(name) + ': </span>' + nf.Common.formatValue(value) + '</div>';
        },

        /**
         * Formats the specified value accordingly.
         * 
         * @argument {string} value     The value of the property
         */
        formatValue: function (value) {
            if (nf.Common.isDefinedAndNotNull(value)) {
                if (value === '') {
                    return '<span class="blank" style="font-size: 13px; padding-top: 2px;">Empty string previously set</span>';
                } else {
                    return nf.Common.escapeHtml(value);
                }
            } else {
                return '<span class="unset" style="font-size: 13px; padding-top: 2px;">No value previously set</span>';
            }
        },

        /**
         * HTML escapes the specified string. If the string is null 
         * or undefined, an empty string is returned.
         * 
         * @returns {string}
         */
        escapeHtml: (function () {
            var entityMap = {
                '&': '&amp;',
                '<': '&lt;',
                '>': '&gt;',
                '"': '&quot;',
                "'": '&#39;',
                '/': '&#x2f;'
            };

            return function (string) {
                if (nf.Common.isDefinedAndNotNull(string)) {
                    return String(string).replace(/[&<>"'\/]/g, function (s) {
                        return entityMap[s];
                    });
                } else {
                    return '';
                }
            };
        }()),

        /**
         * Determines if the specified property is sensitive.
         * 
         * @argument {object} propertyDescriptor        The property descriptor
         */
        isSensitiveProperty: function (propertyDescriptor) {
            if (nf.Common.isDefinedAndNotNull(propertyDescriptor)) {
                return propertyDescriptor.sensitive === true;
            } else {
                return false;
            }
        },

        /**
         * Determines if the specified property is required.
         * 
         * @param {object} propertyDescriptor           The property descriptor
         */
        isRequiredProperty: function (propertyDescriptor) {
            if (nf.Common.isDefinedAndNotNull(propertyDescriptor)) {
                return propertyDescriptor.required === true;
            } else {
                return false;
            }
        },

        /**
         * Determines if the specified property is required.
         * 
         * @param {object} propertyDescriptor           The property descriptor
         */
        isDynamicProperty: function (propertyDescriptor) {
            if (nf.Common.isDefinedAndNotNull(propertyDescriptor)) {
                return propertyDescriptor.dynamic === true;
            } else {
                return false;
            }
        },

        /**
         * Gets the allowable values for the specified property.
         * 
         * @argument {object} propertyDescriptor        The property descriptor
         */
        getAllowableValues: function (propertyDescriptor) {
            if (nf.Common.isDefinedAndNotNull(propertyDescriptor)) {
                return propertyDescriptor.allowableValues;
            } else {
                return null;
            }
        },

        /**
         * Returns whether the specified property supports EL.
         * 
         * @param {object} propertyDescriptor           The property descriptor
         */
        supportsEl: function (propertyDescriptor) {
            if (nf.Common.isDefinedAndNotNull(propertyDescriptor)) {
                return propertyDescriptor.supportsEl === true;
            } else {
                return false;
            }
        },

        /**
         * Formats the specified array as an unordered list. If the array is not an 
         * array, null is returned.
         * 
         * @argument {array} array      The array to convert into an unordered list
         */
        formatUnorderedList: function (array) {
            if ($.isArray(array)) {
                var ul = $('<ul class="result"></ul>');
                $.each(array, function (_, item) {
                    var li = $('<li></li>').appendTo(ul);
                    if (item instanceof jQuery) {
                        li.append(item);
                    } else {
                        li.text(item);
                    }
                });
                return ul;
            } else {
                return null;
            }
        },

        /**
         * Extracts the contents of the specified str after the strToFind. If the
         * strToFind is not found or the last part of the str, an empty string is
         * returned.
         * 
         * @argument {string} str       The full string
         * @argument {string} strToFind The substring to find
         */
        substringAfterLast: function (str, strToFind) {
            var result = '';
            var indexOfStrToFind = str.lastIndexOf(strToFind);
            if (indexOfStrToFind >= 0) {
                var indexAfterStrToFind = indexOfStrToFind + strToFind.length;
                if (indexAfterStrToFind < str.length) {
                    result = str.substr(indexAfterStrToFind);
                }
            }
            return result;
        },

        /**
         * Extracts the contents of the specified str after the strToFind. If the
         * strToFind is not found or the last part of the str, an empty string is
         * returned.
         *
         * @argument {string} str       The full string
         * @argument {string} strToFind The substring to find
         */
        substringAfterFirst: function (str, strToFind) {
            var result = '';
            var indexOfStrToFind = str.indexOf(strToFind);
            if (indexOfStrToFind >= 0) {
                var indexAfterStrToFind = indexOfStrToFind + strToFind.length;
                if (indexAfterStrToFind < str.length) {
                    result = str.substr(indexAfterStrToFind);
                }
            }
            return result;
        },

        /**
         * Extracts the contents of the specified str before the strToFind. If the
         * strToFind is not found or the first part of the str, an empty string is
         * returned.
         *
         * @argument {string} str       The full string
         * @argument {string} strToFind The substring to find
         */
        substringBeforeFirst: function(str, strToFind) {
            var result = '';
            var indexOfStrToFind = str.indexOf(strToFind);
            if (indexOfStrToFind >= 0) {
                result = str.substr(0, indexOfStrToFind);
            }
            return result
        },

        /**
         * Updates the mouse pointer.
         * 
         * @argument {string} domId         The id of the element for the new cursor style
         * @argument {boolean} isMouseOver  Whether or not the mouse is over the element
         */
        setCursor: function (domId, isMouseOver) {
            if (isMouseOver) {
                $('#' + domId).addClass('pointer');
            } else {
                $('#' + domId).removeClass('pointer');
            }
        },

        /**
         * Gets an access token from the specified url.
         *
         * @param accessTokenUrl    The access token
         * @returns the access token as a deferred
         */
        getAccessToken: function (accessTokenUrl) {
            return $.Deferred(function (deferred) {
                if (nf.Storage.hasItem('jwt')) {
                    $.ajax({
                        type: 'POST',
                        url: accessTokenUrl
                    }).done(function (token) {
                        deferred.resolve(token);
                    }).fail(function () {
                        deferred.reject();
                    })
                } else {
                    deferred.resolve('');
                }
            }).promise();
        },

        /**
         * Constants for time duration formatting.
         */
        MILLIS_PER_DAY: 86400000,
        MILLIS_PER_HOUR: 3600000,
        MILLIS_PER_MINUTE: 60000,
        MILLIS_PER_SECOND: 1000,

        /**
         * Formats the specified duration.
         * 
         * @param {integer} duration in millis
         */
        formatDuration: function (duration) {
            // don't support sub millisecond resolution
            duration = duration < 1 ? 0 : duration;

            // determine the number of days in the specified duration
            var days = duration / nf.Common.MILLIS_PER_DAY;
            days = days >= 1 ? parseInt(days, 10) : 0;
            duration %= nf.Common.MILLIS_PER_DAY;

            // remaining duration should be less than 1 day, get number of hours
            var hours = duration / nf.Common.MILLIS_PER_HOUR;
            hours = hours >= 1 ? parseInt(hours, 10) : 0;
            duration %= nf.Common.MILLIS_PER_HOUR;

            // remaining duration should be less than 1 hour, get number of minutes
            var minutes = duration / nf.Common.MILLIS_PER_MINUTE;
            minutes = minutes >= 1 ? parseInt(minutes, 10) : 0;
            duration %= nf.Common.MILLIS_PER_MINUTE;

            // remaining duration should be less than 1 minute, get number of seconds
            var seconds = duration / nf.Common.MILLIS_PER_SECOND;
            seconds = seconds >= 1 ? parseInt(seconds, 10) : 0;

            // remaining duration is the number millis (don't support sub millisecond resolution)
            duration = Math.floor(duration % nf.Common.MILLIS_PER_SECOND);

            // format the time
            var time = nf.Common.pad(hours, 2, '0') +
                    ':' +
                    nf.Common.pad(minutes, 2, '0') +
                    ':' +
                    nf.Common.pad(seconds, 2, '0') +
                    '.' +
                    nf.Common.pad(duration, 3, '0');

            // only include days if appropriate
            if (days > 0) {
                return days + ' days and ' + time;
            } else {
                return time;
            }
        },

        /**
         * Constants for formatting data size.
         */
        BYTES_IN_KILOBYTE: 1024,
        BYTES_IN_MEGABYTE: 1048576,
        BYTES_IN_GIGABYTE: 1073741824,
        BYTES_IN_TERABYTE: 1099511627776,

        /**
         * Formats the specified number of bytes into a human readable string.
         * 
         * @param {integer} dataSize
         * @returns {string}
         */
        formatDataSize: function (dataSize) {
            // check terabytes
            var dataSizeToFormat = parseFloat(dataSize / nf.Common.BYTES_IN_TERABYTE);
            if (dataSizeToFormat > 1) {
                return dataSizeToFormat.toFixed(2) + " TB";
            }

            // check gigabytes
            dataSizeToFormat = parseFloat(dataSize / nf.Common.BYTES_IN_GIGABYTE);
            if (dataSizeToFormat > 1) {
                return dataSizeToFormat.toFixed(2) + " GB";
            }

            // check megabytes
            dataSizeToFormat = parseFloat(dataSize / nf.Common.BYTES_IN_MEGABYTE);
            if (dataSizeToFormat > 1) {
                return dataSizeToFormat.toFixed(2) + " MB";
            }

            // check kilobytes
            dataSizeToFormat = parseFloat(dataSize / nf.Common.BYTES_IN_KILOBYTE);
            if (dataSizeToFormat > 1) {
                return dataSizeToFormat.toFixed(2) + " KB";
            }

            // default to bytes
            return parseFloat(dataSize).toFixed(2) + " bytes";
        },

        /**
         * Formats the specified integer as a string (adding commas). At this
         * point this does not take into account any locales.
         * 
         * @param {integer} integer
         */
        formatInteger: function (integer) {
            var string = integer + '';
            var regex = /(\d+)(\d{3})/;
            while (regex.test(string)) {
                string = string.replace(regex, '$1' + ',' + '$2');
            }
            return string;
        },

        /**
         * Formats the specified float using two demical places.
         * 
         * @param {float} f
         */
        formatFloat: function (f) {
            if (nf.Common.isUndefinedOrNull(f)) {
                return 0.00 + '';
            }
            return f.toFixed(2) + '';
        },

        /**
         * Pads the specified value to the specified width with the specified character.
         * If the specified value is already wider than the specified width, the original
         * value is returned.
         * 
         * @param {integer} value
         * @param {integer} width
         * @param {string} character
         * @returns {string}
         */
        pad: function (value, width, character) {
            var s = value + '';

            // pad until wide enough
            while (s.length < width) {
                s = character + s;
            }

            return s;
        },

        /**
         * Formats the specified DateTime.
         * 
         * @param {Date} date
         * @returns {String}
         */
        formatDateTime: function (date) {
            return nf.Common.pad(date.getMonth() + 1, 2, '0') +
                    '/' +
                    nf.Common.pad(date.getDate(), 2, '0') +
                    '/' +
                    nf.Common.pad(date.getFullYear(), 2, '0') +
                    ' ' +
                    nf.Common.pad(date.getHours(), 2, '0') +
                    ':' +
                    nf.Common.pad(date.getMinutes(), 2, '0') +
                    ':' +
                    nf.Common.pad(date.getSeconds(), 2, '0') +
                    '.' +
                    nf.Common.pad(date.getMilliseconds(), 3, '0');
        },

        /**
         * Parses the specified date time into a Date object. The resulting
         * object does not account for timezone and should only be used for
         * performing relative comparisons.
         * 
         * @param {string} rawDateTime
         * @returns {Date}
         */
        parseDateTime: function (rawDateTime) {
            // handle non date values
            if (!nf.Common.isDefinedAndNotNull(rawDateTime)) {
                return new Date();
            }
            if (rawDateTime === 'No value previously set') {
                return new Date();
            }
            if (rawDateTime === 'Empty string previously set') {
                return new Date();
            }

            // parse the date time
            var dateTime = rawDateTime.split(/ /);

            // ensure the correct number of tokens
            if (dateTime.length !== 3) {
                return new Date();
            }

            // get the date and time
            var date = dateTime[0].split(/\//);
            var time = dateTime[1].split(/:/);

            // ensure the correct number of tokens
            if (date.length !== 3 || time.length !== 3) {
                return new Date();
            }

            // detect if there is millis
            var seconds = time[2].split(/\./);
            if (seconds.length === 2) {
                return new Date(parseInt(date[2], 10), parseInt(date[0], 10), parseInt(date[1], 10), parseInt(time[0], 10), parseInt(time[1], 10), parseInt(seconds[0], 10), parseInt(seconds[1], 10));
            } else {
                return new Date(parseInt(date[2], 10), parseInt(date[0], 10), parseInt(date[1], 10), parseInt(time[0], 10), parseInt(time[1], 10), parseInt(time[2], 10), 0);
            }
        },

        /**
         * Parses the specified duration and returns the total number of millis.
         * 
         * @param {string} rawDuration
         * @returns {number}        The number of millis
         */
        parseDuration: function (rawDuration) {
            var duration = rawDuration.split(/:/);

            // ensure the appropriate number of tokens
            if (duration.length !== 3) {
                return 0;
            }

            // detect if there is millis
            var seconds = duration[2].split(/\./);
            if (seconds.length === 2) {
                return new Date(1970, 0, 1, parseInt(duration[0], 10), parseInt(duration[1], 10), parseInt(seconds[0], 10), parseInt(seconds[1], 10)).getTime();
            } else {
                return new Date(1970, 0, 1, parseInt(duration[0], 10), parseInt(duration[1], 10), parseInt(duration[2], 10), 0).getTime();
            }
        },

        /**
         * Parses the specified size.
         * 
         * @param {string} rawSize
         * @returns {int}
         */
        parseSize: function (rawSize) {
            var tokens = rawSize.split(/ /);
            var size = parseFloat(tokens[0].replace(/,/g, ''));
            var units = tokens[1];

            if (units === 'KB') {
                return size * 1024;
            } else if (units === 'MB') {
                return size * 1024 * 1024;
            } else if (units === 'GB') {
                return size * 1024 * 1024 * 1024;
            } else if (units === 'TB') {
                return size * 1024 * 1024 * 1024 * 1024;
            } else {
                return size;
            }
        },

        /**
         * Parses the specified count.
         * 
         * @param {string} rawCount
         * @returns {int}
         */
        parseCount: function (rawCount) {
            // extract the count
            var count = rawCount.split(/ /, 1);

            // ensure the string was split successfully
            if (count.length !== 1) {
                return 0;
            }

            // convert the count to an integer
            var intCount = parseInt(count[0].replace(/,/g, ''), 10);

            // ensure it was parsable as an integer
            if (isNaN(intCount)) {
                return 0;
            }
            return intCount;
        },

        /**
         * Determines if the specified object is defined and not null.
         * 
         * @argument {object} obj   The object to test
         */
        isDefinedAndNotNull: function (obj) {
            return !nf.Common.isUndefined(obj) && !nf.Common.isNull(obj);
        },

        /**
         * Determines if the specified object is undefined or null.
         * 
         * @param {object} obj      The object to test
         */
        isUndefinedOrNull: function (obj) {
            return nf.Common.isUndefined(obj) || nf.Common.isNull(obj);
        },

        /**
         * Determines if the specified object is undefined.
         * 
         * @argument {object} obj   The object to test
         */
        isUndefined: function (obj) {
            return typeof obj === 'undefined';
        },

        /**
         * Determines whether the specified string is blank (or null or undefined).
         * 
         * @argument {string} str   The string to test
         */
        isBlank: function (str) {
            return nf.Common.isUndefined(str) || nf.Common.isNull(str) || $.trim(str) === '';
        },

        /**
         * Determines if the specified object is null.
         * 
         * @argument {object} obj   The object to test
         */
        isNull: function (obj) {
            return obj === null;
        },

        /**
         * Determines if the specified array is empty. If the specified arg is not an
         * array, then true is returned.
         * 
         * @argument {array} arr    The array to test
         */
        isEmpty: function (arr) {
            return $.isArray(arr) ? arr.length === 0 : true;
        },

        /**
         * Determines if these are the same bulletins. If both arguments are not
         * arrays, false is returned.
         * 
         * @param {array} bulletins
         * @param {array} otherBulletins
         * @returns {boolean}
         */
        doBulletinsDiffer: function (bulletins, otherBulletins) {
            if ($.isArray(bulletins) && $.isArray(otherBulletins)) {
                if (bulletins.length === otherBulletins.length) {
                    for (var i = 0; i < bulletins.length; i++) {
                        if (bulletins[i].id !== otherBulletins[i].id || bulletins[i].canRead !== otherBulletins[i].canRead) {
                            return true;
                        }
                    }
                } else {
                    return true;
                }
            } else if ($.isArray(bulletins) || $.isArray(otherBulletins)) {
                return true;
            }
            return false;
        },

        /**
         * Formats the specified bulletin list.
         * 
         * @argument {array} bulletins      The bulletins
         * @return {array}                  The jQuery objects
         */
        getFormattedBulletins: function (bulletinEntities) {
            var formattedBulletinEntities = [];
            $.each(bulletinEntities, function (j, bulletinEntity) {
                if (bulletinEntity.canRead === true) {
                    var bulletin = bulletinEntity.bulletin;

                    // format the node address
                    var nodeAddress = '';
                    if (nf.Common.isDefinedAndNotNull(bulletin.nodeAddress)) {
                        nodeAddress = '-&nbsp' + nf.Common.escapeHtml(bulletin.nodeAddress) + '&nbsp;-&nbsp;';
                    }

                    // set the bulletin message (treat as text)
                    var bulletinMessage = $('<pre></pre>').css({
                        'white-space': 'pre-wrap'
                    }).text(bulletin.message);

                    // create the bulletin message
                    var formattedBulletin = $('<div>' +
                            nf.Common.escapeHtml(bulletin.timestamp) + '&nbsp;' +
                            nodeAddress + '&nbsp;' +
                            '<b>' + nf.Common.escapeHtml(bulletin.level) + '</b>&nbsp;' +
                            '</div>').append(bulletinMessage);

                    formattedBulletinEntities.push(formattedBulletin);
                }
            });
            return formattedBulletinEntities;
        }
    };
}());
