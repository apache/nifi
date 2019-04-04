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

/* global define, module, require, exports, parseFloat */

// Define a common utility class used across the entire application.
(function (root, factory) {
    if (typeof define === 'function' && define.amd) {
        define(['jquery',
                'd3',
                'nf.Storage'],
            function ($, d3, nfStorage) {
                return (nf.Common = factory($, d3, nfStorage));
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.Common = factory(require('jquery'),
            require('d3'),
            require('nf.Storage')));
    } else {
        nf.Common = factory(root.$,
            root.d3,
            root.nf.Storage);
    }
}(this, function ($, d3, nfStorage) {
    'use strict';

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
            // emit a state change event
            checkbox.trigger('change');
        });

        // setup click areas for custom checkboxes
        $(document).on('click', '.nf-checkbox-label', function (e) {
            $(e.target).parent().find('.nf-checkbox').click();
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
        if (nfStorage.getItem('jwt') !== null) {
            $('#user-logout-container').css('display', 'block');
            nfCommon.scheduleTokenRefresh();
        }

        // handle logout
        $('#user-logout').on('click', function () {
            nfStorage.removeItem('jwt');
            window.location = '../nifi/logout';
        });

        // handle home
        $('#user-home').on('click', function () {
            if (top !== window) {
                parent.window.location = '../nifi';
            } else {
                window.location = '../nifi';
            }
        });
    });

    // interval for cancelling token refresh when necessary
    var tokenRefreshInterval = null;

    var policyTypeListing = [{
        text: 'view the user interface',
        value: 'flow',
        description: 'Allows users to view the user interface'
    }, {
        text: 'access the controller',
        value: 'controller',
        description: 'Allows users to view/modify the controller including Reporting Tasks, Controller Services, and Nodes in the Cluster'
    }, {
        text: 'query provenance',
        value: 'provenance',
        description: 'Allows users to submit a Provenance Search and request Event Lineage'
    }, {
        text: 'access restricted components',
        value: 'restricted-components',
        description: 'Allows users to create/modify restricted components assuming other permissions are sufficient'
    }, {
        text: 'access all policies',
        value: 'policies',
        description: 'Allows users to view/modify the policies for all components'
    }, {
        text: 'access users/user groups',
        value: 'tenants',
        description: 'Allows users to view/modify the users and user groups'
    }, {
        text: 'retrieve site-to-site details',
        value: 'site-to-site',
        description: 'Allows other NiFi instances to retrieve Site-To-Site details of this NiFi'
    }, {
        text: 'view system diagnostics',
        value: 'system',
        description: 'Allows users to view System Diagnostics'
    }, {
        text: 'proxy user requests',
        value: 'proxy',
        description: 'Allows proxy machines to send requests on the behalf of others'
    }, {
        text: 'access counters',
        value: 'counters',
        description: 'Allows users to view/modify Counters'
    }];

    var nfCommon = {
        ANONYMOUS_USER_TEXT: 'Anonymous user',

        config: {
            sensitiveText: 'Sensitive value set',
            tooltipConfig: {
                style: {
                    classes: 'nifi-tooltip'
                },
                show: {
                    solo: true,
                    effect: function (offset) {
                        $(this).slideDown(100);
                    }
                },
                hide: {
                    effect: function (offset) {
                        $(this).slideUp(100);
                    }
                },
                position: {
                    at: 'top center',
                    my: 'bottom center'
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
         * Sorts the specified version strings.
         *
         * @param aRawVersion version string
         * @param bRawVersion version string
         * @returns {number} negative if a before b, positive if a after b, 0 otherwise
         */
        sortVersion: function (aRawVersion, bRawVersion) {
            if (aRawVersion === bRawVersion) {
                return 0;
            }

            // attempt to parse the raw strings
            var aTokens = aRawVersion.split(/-/);
            var bTokens = bRawVersion.split(/-/);

            // ensure there is at least one token
            if (aTokens.length >= 1 && bTokens.length >= 1) {
                var aVersionTokens = aTokens[0].split(/\./);
                var bVersionTokens = bTokens[0].split(/\./);

                // ensure both versions have at least one token
                if (aVersionTokens.length >= 1 && bVersionTokens.length >= 1) {
                    // find the number of tokens a and b have in common
                    var commonTokenLength = Math.min(aVersionTokens.length, bVersionTokens.length);

                    // consider all tokens in common
                    for (var i = 0; i < commonTokenLength; i++) {
                        var aVersionSegment = parseInt(aVersionTokens[i], 10);
                        var bVersionSegment = parseInt(bVersionTokens[i], 10);

                        // if both are non numeric, consider the next token
                        if (isNaN(aVersionSegment) && isNaN(bVersionSegment)) {
                            continue;
                        }  else if (isNaN(aVersionSegment)) {
                            // NaN is considered less
                            return -1;
                        } else if (isNaN(bVersionSegment)) {
                            // NaN is considered less
                            return 1;
                        }

                        // if a version at any point does not match
                        if (aVersionSegment !== bVersionSegment) {
                            return aVersionSegment - bVersionSegment;
                        }
                    }

                    if (aVersionTokens.length === bVersionTokens.length) {
                        if (aTokens.length === bTokens.length) {
                            // same version for all tokens so consider the trailing bits (1.1-RC vs 1.1-SNAPSHOT)
                            var aExtraBits = nfCommon.substringAfterFirst(aRawVersion, aTokens[0]);
                            var bExtraBits = nfCommon.substringAfterFirst(bRawVersion, bTokens[0]);
                            return aExtraBits === bExtraBits ? 0 : aExtraBits > bExtraBits ? 1 : -1;
                        } else {
                            // in this case, extra bits means it's consider less than no extra bits (1.1 vs 1.1-SNAPSHOT)
                            return bTokens.length - aTokens.length;
                        }
                    } else {
                        // same version for all tokens in common (ie 1.1 vs 1.1.1)
                        return aVersionTokens.length - bVersionTokens.length;
                    }
                } else if (aVersionTokens.length >= 1) {
                    // presence of version tokens is considered greater
                    return 1;
                } else if (bVersionTokens.length >= 1) {
                    // presence of version tokens is considered greater
                    return -1;
                } else {
                    return 0;
                }
            } else if (aTokens.length >= 1) {
                // presence of tokens is considered greater
                return 1;
            } else if (bTokens.length >= 1) {
                // presence of tokens is considered greater
                return -1;
            } else {
                return 0;
            }
        },

        /**
         * Sorts the specified type data using the specified sort details.
         *
         * @param {object} sortDetails
         * @param {object} data
         */
        sortType: function (sortDetails, data) {
            // compares two bundles
            var compareBundle = function (a, b) {
                var aBundle = nfCommon.formatBundle(a.bundle);
                var bBundle = nfCommon.formatBundle(b.bundle);
                return aBundle === bBundle ? 0 : aBundle > bBundle ? 1 : -1;
            };

            // defines a function for sorting
            var comparer = function (a, b) {
                if (sortDetails.columnId === 'version') {
                    var aVersion = nfCommon.isDefinedAndNotNull(a.bundle[sortDetails.columnId]) ? a.bundle[sortDetails.columnId] : '';
                    var bVersion = nfCommon.isDefinedAndNotNull(b.bundle[sortDetails.columnId]) ? b.bundle[sortDetails.columnId] : '';
                    var versionResult = nfCommon.sortVersion(aVersion, bVersion);
                    return versionResult === 0 ? compareBundle(a, b) : versionResult;
                } else if (sortDetails.columnId === 'type') {
                    var aType = nfCommon.substringAfterLast(a[sortDetails.columnId], '.');
                    var bType = nfCommon.substringAfterLast(b[sortDetails.columnId], '.');
                    return aType === bType ? 0 : aType > bType ? 1 : -1;
                } else {
                    var aString = nfCommon.isDefinedAndNotNull(a[sortDetails.columnId]) ? a[sortDetails.columnId] : '';
                    var bString = nfCommon.isDefinedAndNotNull(b[sortDetails.columnId]) ? b[sortDetails.columnId] : '';
                    return aString === bString ? compareBundle(a, b) : aString > bString ? 1 : -1;
                }
            };

            // perform the sort
            data.sort(comparer, sortDetails.sortAsc);
        },

        /**
         * Formats type of a component for a new instance dialog.
         *
         * @param row
         * @param cell
         * @param value
         * @param columnDef
         * @param dataContext
         * @returns {string}
         */
        typeFormatter: function (row, cell, value, columnDef, dataContext) {
            var markup = '';

            // restriction
            if (dataContext.restricted === true) {
                markup += '<div class="view-usage-restriction fa fa-shield"></div><span class="hidden row-id">' + nfCommon.escapeHtml(dataContext.id) + '</span>';
            } else {
                markup += '<div class="fa"></div>';
            }

            // type
            markup += nfCommon.escapeHtml(value);

            return markup;
        },

        /**
         * Escapes any malicious HTML characters from the value.
         *
         * @param row
         * @param cell
         * @param value
         * @param columnDef
         * @param dataContext
         * @returns {string}
         */
        genericValueFormatter: function (row, cell, value, columnDef, dataContext) {
            return nfCommon.escapeHtml(value);
        },

        /**
         * Formats the bundle of a component type for the new instance dialog.
         *
         * @param row
         * @param cell
         * @param value
         * @param columnDef
         * @param dataContext
         * @returns {string}
         */
        typeBundleFormatter: function (row, cell, value, columnDef, dataContext) {
            return nfCommon.escapeHtml(nfCommon.formatBundle(dataContext.bundle));
        },

        /**
         * Formats the bundle of a component type for the new instance dialog.
         *
         * @param row
         * @param cell
         * @param value
         * @param columnDef
         * @param dataContext
         * @returns {string}
         */
        typeVersionFormatter: function (row, cell, value, columnDef, dataContext) {
            var markup = '';

            if (nfCommon.isDefinedAndNotNull(dataContext.bundle)) {
                markup += ('<div style="float: left;">' + nfCommon.escapeHtml(dataContext.bundle.version) + '</div>');
            } else {
                markup += '<div style="float: left;">unversioned</div>';
            }

            if (!nfCommon.isEmpty(dataContext.controllerServiceApis)) {
                markup += '<div class="controller-service-apis fa fa-list" title="Compatible Controller Service" style="margin-left: 4px;"></div><span class="hidden row-id">' + nfCommon.escapeHtml(dataContext.id) + '</span>';
            }

            markup += '<div class="clear"></div>';

            return markup;
        },

        /**
         * Formatter for the type column.
         *
         * @param {type} row
         * @param {type} cell
         * @param {type} value
         * @param {type} columnDef
         * @param {type} dataContext
         * @returns {String}
         */
        instanceTypeFormatter: function (row, cell, value, columnDef, dataContext) {
            if (!dataContext.permissions.canRead) {
                return '';
            }

            return nfCommon.escapeHtml(nfCommon.formatType(dataContext.component));
        },

        /**
         * Formats the bundle of a component instance for the component listing table.
         *
         * @param row
         * @param cell
         * @param value
         * @param columnDef
         * @param dataContext
         * @returns {string}
         */
        instanceBundleFormatter: function (row, cell, value, columnDef, dataContext) {
            if (!dataContext.permissions.canRead) {
                return '';
            }

            return nfCommon.typeBundleFormatter(row, cell, value, columnDef, dataContext.component);
        },

        /**
         * Gets the version control tooltip.
         *
         * @param versionControlInformation
         */
        getVersionControlTooltip: function (versionControlInformation) {
            return versionControlInformation.stateExplanation;
        },

        /**
         * Formats the class name of this component.
         *
         * @param dataContext component datum
         */
        formatClassName: function (dataContext) {
            return nfCommon.substringAfterLast(dataContext.type, '.');
        },

        /**
         * Formats the type of this component.
         *
         * @param dataContext component datum
         */
        formatType: function (dataContext) {
            var typeString = nfCommon.formatClassName(dataContext);
            if (dataContext.bundle.version !== 'unversioned') {
                typeString += (' ' + dataContext.bundle.version);
            }
            return typeString;
        },

        /**
         * Formats the bundle label.
         *
         * @param bundle
         */
        formatBundle: function (bundle) {
            var groupString = '';
            if (bundle.group !== 'default') {
                groupString = bundle.group + ' - ';
            }
            return groupString + bundle.artifact;
        },

        /**
         * Sets the current user.
         *
         * @param currentUser
         */
        setCurrentUser: function (currentUser) {
            nfCommon.currentUser = currentUser;
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
            var interval = nfCommon.MILLIS_PER_MINUTE;

            var checkExpiration = function () {
                var expiration = nfStorage.getItemExpiration('jwt');

                // ensure there is an expiration and token present
                if (expiration !== null) {
                    var expirationDate = new Date(expiration);
                    var now = new Date();

                    // get the time remainging plus a little bonus time to reload the token
                    var timeRemaining = expirationDate.valueOf() - now.valueOf() - (30 * nfCommon.MILLIS_PER_SECOND);
                    if (timeRemaining < interval) {
                        if ($('#current-user').text() !== nfCommon.ANONYMOUS_USER_TEXT && !$('#anonymous-user-alert').is(':visible')) {
                            // if the token will expire before the next interval minus some bonus time, notify the user to re-login
                            $('#anonymous-user-alert').show().qtip($.extend({}, nfCommon.config.tooltipConfig, {
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
            anonymousUserAlert.show().qtip($.extend({}, nfCommon.config.tooltipConfig, {
                content: 'You are accessing with limited authority. Log in or request an account to access with additional authority granted to you by an administrator.',
                position: {
                    my: 'top right',
                    at: 'bottom left'
                }
            }));

            // render the anonymous user text
            $('#current-user').text(nfCommon.ANONYMOUS_USER_TEXT).show();
        },

        /**
         * Extracts the subject from the specified jwt. If the jwt is not as expected
         * an empty string is returned.
         *
         * @param {string} jwt
         * @returns {string}
         */
        getJwtPayload: function (jwt) {
            if (nfCommon.isDefinedAndNotNull(jwt)) {
                var segments = jwt.split(/\./);
                if (segments.length !== 3) {
                    return '';
                }

                var rawPayload = $.base64.atob(segments[1]);
                var payload = JSON.parse(rawPayload);

                if (nfCommon.isDefinedAndNotNull(payload)) {
                    return payload;
                } else {
                    return null;
                }
            }

            return null;
        },

        /**
         * Determines whether the current user can version flows.
         */
        canVersionFlows: function () {
            if (nfCommon.isDefinedAndNotNull(nfCommon.currentUser)) {
                return nfCommon.currentUser.canVersionFlows === true;
            } else {
                return false;
            }
        },

        /**
         * Determines whether the current user can access provenance.
         *
         * @returns {boolean}
         */
        canAccessProvenance: function () {
            if (nfCommon.isDefinedAndNotNull(nfCommon.currentUser)) {
                return nfCommon.currentUser.provenancePermissions.canRead === true;
            } else {
                return false;
            }
        },

        /**
         * Determines whether the current user can access restricted components.
         *
         * @returns {boolean}
         */
        canAccessRestrictedComponents: function () {
            if (nfCommon.isDefinedAndNotNull(nfCommon.currentUser)) {
                return nfCommon.currentUser.restrictedComponentsPermissions.canWrite === true;
            } else {
                return false;
            }
        },

        /**
         * Determines whether the current user can access the specific explicit component restrictions.
         *
         * @param {object} explicitRestrictions
         * @returns {boolean}
         */
        canAccessComponentRestrictions: function (explicitRestrictions) {
            if (nfCommon.isDefinedAndNotNull(nfCommon.currentUser)) {
                if (nfCommon.currentUser.restrictedComponentsPermissions.canWrite === true) {
                    return true;
                }

                var satisfiesRequiredPermission = function (requiredPermission) {
                    if (nfCommon.isEmpty(nfCommon.currentUser.componentRestrictionPermissions)) {
                        return false;
                    }

                    var hasPermission = false;

                    $.each(nfCommon.currentUser.componentRestrictionPermissions, function (_, componentRestrictionPermission) {
                        if (componentRestrictionPermission.requiredPermission.id === requiredPermission.id) {
                            if (componentRestrictionPermission.permissions.canWrite === true) {
                                hasPermission = true;
                                return false;
                            }
                        }
                    });

                    return hasPermission;
                };

                var satisfiesRequiredPermissions = true;

                if (nfCommon.isEmpty(explicitRestrictions)) {
                    satisfiesRequiredPermissions = false;
                } else {
                    $.each(explicitRestrictions, function (_, explicitRestriction) {
                        if (!satisfiesRequiredPermission(explicitRestriction.requiredPermission)) {
                            satisfiesRequiredPermissions = false;
                            return false;
                        }
                    });
                }

                return satisfiesRequiredPermissions;
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
            if (nfCommon.isDefinedAndNotNull(nfCommon.currentUser)) {
                return nfCommon.currentUser.countersPermissions.canRead === true;
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
            if (nfCommon.isDefinedAndNotNull(nfCommon.currentUser)) {
                return nfCommon.currentUser.countersPermissions.canRead === true && nfCommon.currentUser.countersPermissions.canWrite === true;
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
            if (nfCommon.isDefinedAndNotNull(nfCommon.currentUser)) {
                return nfCommon.currentUser.tenantsPermissions.canRead === true;
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
            if (nfCommon.isDefinedAndNotNull(nfCommon.currentUser)) {
                return nfCommon.currentUser.tenantsPermissions.canRead === true && nfCommon.currentUser.tenantsPermissions.canWrite === true;
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
            if (nfCommon.isDefinedAndNotNull(nfCommon.currentUser)) {
                return nfCommon.currentUser.policiesPermissions.canRead === true;
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
            if (nfCommon.isDefinedAndNotNull(nfCommon.currentUser)) {
                return nfCommon.currentUser.policiesPermissions.canRead === true && nfCommon.currentUser.policiesPermissions.canWrite === true;
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
            if (nfCommon.isDefinedAndNotNull(nfCommon.currentUser)) {
                return nfCommon.currentUser.controllerPermissions.canRead === true;
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
            if (nfCommon.isDefinedAndNotNull(nfCommon.currentUser)) {
                return nfCommon.currentUser.controllerPermissions.canRead === true && nfCommon.currentUser.controllerPermissions.canWrite === true;
            } else {
                return false;
            }
        },

        /**
         * Determines whether the current user can access system diagnostics.
         *
         * @returns {boolean}
         */
        canAccessSystem: function () {
            if (nfCommon.isDefinedAndNotNull(nfCommon.currentUser)) {
                return nfCommon.currentUser.systemPermissions.canRead === true;
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
            if ($(element).is(':visible')) {
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
         * Determines the contrast color of a given hex color.
         *
         * @param {string} hex  The hex color to test.
         * @returns {string} The contrasting color string.
         */
        determineContrastColor: function (hex) {
            if (parseInt(hex, 16) > 0xffffff / 1.5) {
                return '#000000';
            }
            return '#ffffff';
        },

        /**
         * Shows the logout link if appropriate.
         */
        showLogoutLink: function () {
            if (nfStorage.getItem('jwt') === null) {
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
            return !nfCommon.isBlank(contentViewerUrl);
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
            if (nfCommon.isUndefined(value) || nfCommon.isNull(value)) {
                return $('#' + target).addClass('unset').text('No value set');
            } else if (value === '') {
                return $('#' + target).addClass('blank').text('Empty string set');
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
        cleanUpTooltips: function (container, tooltipTarget) {
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
            if (nfCommon.isDefinedAndNotNull(propertyDescriptor)) {
                if (!nfCommon.isBlank(propertyDescriptor.description)) {
                    tipContent.push(nfCommon.escapeHtml(propertyDescriptor.description));
                }
                if (!nfCommon.isBlank(propertyDescriptor.defaultValue)) {
                    tipContent.push('<b>Default value:</b> ' + nfCommon.escapeHtml(propertyDescriptor.defaultValue));
                }
                if (!nfCommon.isBlank(propertyDescriptor.supportsEl)) {
                    tipContent.push('<b>Expression language scope:</b> ' + nfCommon.escapeHtml(propertyDescriptor.expressionLanguageScope));
                }
                if (!nfCommon.isBlank(propertyDescriptor.identifiesControllerService)) {
                    var formattedType = nfCommon.formatType({
                        'type': propertyDescriptor.identifiesControllerService,
                        'bundle': propertyDescriptor.identifiesControllerServiceBundle
                    });
                    var formattedBundle = nfCommon.formatBundle(propertyDescriptor.identifiesControllerServiceBundle);
                    tipContent.push('<b>Requires Controller Service:</b> ' + nfCommon.escapeHtml(formattedType + ' from ' + formattedBundle));
                }
            }

            if (nfCommon.isDefinedAndNotNull(propertyHistory)) {
                if (!nfCommon.isEmpty(propertyHistory.previousValues)) {
                    var history = [];
                    $.each(propertyHistory.previousValues, function (_, previousValue) {
                        history.push('<li>' + nfCommon.escapeHtml(previousValue.previousValue) + ' - ' + nfCommon.escapeHtml(previousValue.timestamp) + ' (' + nfCommon.escapeHtml(previousValue.userIdentity) + ')</li>');
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
            return '<div><span class="label">' + nfCommon.formatValue(name) + ': </span>' + nfCommon.formatValue(value) + '</div>';
        },

        /**
         * Formats the specified value accordingly.
         *
         * @argument {string} value     The value of the property
         */
        formatValue: function (value) {
            if (nfCommon.isDefinedAndNotNull(value)) {
                if (value === '') {
                    return '<span class="blank" style="font-size: 13px; padding-top: 2px;">Empty string set</span>';
                } else {
                    return nfCommon.escapeHtml(value);
                }
            } else {
                return '<span class="unset" style="font-size: 13px; padding-top: 2px;">No value set</span>';
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
                if (nfCommon.isDefinedAndNotNull(string)) {
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
            if (nfCommon.isDefinedAndNotNull(propertyDescriptor)) {
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
            if (nfCommon.isDefinedAndNotNull(propertyDescriptor)) {
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
            if (nfCommon.isDefinedAndNotNull(propertyDescriptor)) {
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
            if (nfCommon.isDefinedAndNotNull(propertyDescriptor)) {
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
            if (nfCommon.isDefinedAndNotNull(propertyDescriptor)) {
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
         * Extracts the contents of the specified str after the last strToFind. If the
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
         * Extracts the contents of the specified str before the last strToFind. If the
         * strToFind is not found or the first part of the str, an empty string is
         * returned.
         *
         * @argument {string} str       The full string
         * @argument {string} strToFind The substring to find
         */
        substringBeforeLast: function (str, strToFind) {
            var result = '';
            var indexOfStrToFind = str.lastIndexOf(strToFind);
            if (indexOfStrToFind >= 0) {
                result = str.substr(0, indexOfStrToFind);
            }
            return result;
        },

        /**
         * Extracts the contents of the specified str before the strToFind. If the
         * strToFind is not found or the first path of the str, an empty string is
         * returned.
         *
         * @argument {string} str       The full string
         * @argument {string} strToFind The substring to find
         */
        substringBeforeFirst: function (str, strToFind) {
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
                if (nfStorage.hasItem('jwt')) {
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
         * Constants for combo options.
         */
        loadBalanceStrategyOptions: [{
                text: 'Do not load balance',
                value: 'DO_NOT_LOAD_BALANCE',
                description: 'Do not load balance FlowFiles between nodes in the cluster.'
            }, {
                text: 'Partition by attribute',
                value: 'PARTITION_BY_ATTRIBUTE',
                description: 'Determine which node to send a given FlowFile to based on the value of a user-specified FlowFile Attribute.'
                                + ' All FlowFiles that have the same value for said Attribute will be sent to the same node in the cluster.'
            }, {
                text: 'Round robin',
                value: 'ROUND_ROBIN',
                description: 'FlowFiles will be distributed to nodes in the cluster in a Round-Robin fashion.'
            }, {
                text: 'Single node',
                value: 'SINGLE_NODE',
                description: 'All FlowFiles will be sent to the same node. Which node they are sent to is not defined.'
        }],

        loadBalanceCompressionOptions: [{
                text: 'Do not compress',
                value: 'DO_NOT_COMPRESS',
                description: 'FlowFiles will not be compressed'
            }, {
                text: 'Compress attributes only',
                value: 'COMPRESS_ATTRIBUTES_ONLY',
                description: 'FlowFiles\' attributes will be compressed, but the FlowFiles\' contents will not be'
            }, {
                text: 'Compress attributes and content',
                value: 'COMPRESS_ATTRIBUTES_AND_CONTENT',
                description: 'FlowFiles\' attributes and content will be compressed'
        }],

        /**
         * Formats the specified duration.
         *
         * @param {integer} duration in millis
         */
        formatDuration: function (duration) {
            // don't support sub millisecond resolution
            duration = duration < 1 ? 0 : duration;

            // determine the number of days in the specified duration
            var days = duration / nfCommon.MILLIS_PER_DAY;
            days = days >= 1 ? parseInt(days, 10) : 0;
            duration %= nfCommon.MILLIS_PER_DAY;

            // remaining duration should be less than 1 day, get number of hours
            var hours = duration / nfCommon.MILLIS_PER_HOUR;
            hours = hours >= 1 ? parseInt(hours, 10) : 0;
            duration %= nfCommon.MILLIS_PER_HOUR;

            // remaining duration should be less than 1 hour, get number of minutes
            var minutes = duration / nfCommon.MILLIS_PER_MINUTE;
            minutes = minutes >= 1 ? parseInt(minutes, 10) : 0;
            duration %= nfCommon.MILLIS_PER_MINUTE;

            // remaining duration should be less than 1 minute, get number of seconds
            var seconds = duration / nfCommon.MILLIS_PER_SECOND;
            seconds = seconds >= 1 ? parseInt(seconds, 10) : 0;

            // remaining duration is the number millis (don't support sub millisecond resolution)
            duration = Math.floor(duration % nfCommon.MILLIS_PER_SECOND);

            // format the time
            var time = nfCommon.pad(hours, 2, '0') +
                ':' +
                nfCommon.pad(minutes, 2, '0') +
                ':' +
                nfCommon.pad(seconds, 2, '0') +
                '.' +
                nfCommon.pad(duration, 3, '0');

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
            var dataSizeToFormat = parseFloat(dataSize / nfCommon.BYTES_IN_TERABYTE);
            if (dataSizeToFormat > 1) {
                return dataSizeToFormat.toFixed(2) + " TB";
            }

            // check gigabytes
            dataSizeToFormat = parseFloat(dataSize / nfCommon.BYTES_IN_GIGABYTE);
            if (dataSizeToFormat > 1) {
                return dataSizeToFormat.toFixed(2) + " GB";
            }

            // check megabytes
            dataSizeToFormat = parseFloat(dataSize / nfCommon.BYTES_IN_MEGABYTE);
            if (dataSizeToFormat > 1) {
                return dataSizeToFormat.toFixed(2) + " MB";
            }

            // check kilobytes
            dataSizeToFormat = parseFloat(dataSize / nfCommon.BYTES_IN_KILOBYTE);
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
            return nfCommon.escapeHtml(string);
        },

        /**
         * Formats the specified float using two decimal places.
         *
         * @param {float} f
         */
        formatFloat: function (f) {
            if (nfCommon.isUndefinedOrNull(f)) {
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
            return nfCommon.pad(date.getMonth() + 1, 2, '0') +
                '/' +
                nfCommon.pad(date.getDate(), 2, '0') +
                '/' +
                nfCommon.pad(date.getFullYear(), 2, '0') +
                ' ' +
                nfCommon.pad(date.getHours(), 2, '0') +
                ':' +
                nfCommon.pad(date.getMinutes(), 2, '0') +
                ':' +
                nfCommon.pad(date.getSeconds(), 2, '0') +
                '.' +
                nfCommon.pad(date.getMilliseconds(), 3, '0');
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
            if (!nfCommon.isDefinedAndNotNull(rawDateTime)) {
                return new Date();
            }
            if (rawDateTime === 'No value set') {
                return new Date();
            }
            if (rawDateTime === 'Empty string set') {
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
            var year = parseInt(date[2], 10);
            var month = parseInt(date[0], 10) - 1; // new Date() accepts months 0 - 11
            var day = parseInt(date[1], 10);
            var hours = parseInt(time[0], 10);
            var minutes = parseInt(time[1], 10);

            // detect if there is millis
            var secondsSpec = time[2].split(/\./);
            var seconds = parseInt(secondsSpec[0], 10);
            var milliseconds = 0;
            if (secondsSpec.length === 2) {
                milliseconds = parseInt(secondsSpec[1], 10);
            }
            return new Date(year, month, day, hours, minutes, seconds, milliseconds);
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
            return !nfCommon.isUndefined(obj) && !nfCommon.isNull(obj);
        },

        /**
         * Determines if the specified object is undefined or null.
         *
         * @param {object} obj      The object to test
         */
        isUndefinedOrNull: function (obj) {
            return nfCommon.isUndefined(obj) || nfCommon.isNull(obj);
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
            return nfCommon.isUndefined(str) || nfCommon.isNull(str) || $.trim(str) === '';
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
                    if (nfCommon.isDefinedAndNotNull(bulletin.nodeAddress)) {
                        nodeAddress = '-&nbsp;' + nfCommon.escapeHtml(bulletin.nodeAddress) + '&nbsp;-&nbsp;';
                    }

                    // set the bulletin message (treat as text)
                    var bulletinMessage = $('<pre></pre>').css({
                        'white-space': 'pre-wrap'
                    }).text(bulletin.message);

                    // create the bulletin message
                    var formattedBulletin = $('<div>' +
                        nfCommon.escapeHtml(bulletin.timestamp) + '&nbsp;' +
                        nodeAddress + '&nbsp;' +
                        '<b>' + nfCommon.escapeHtml(bulletin.level) + '</b>&nbsp;' +
                        '</div>').append(bulletinMessage);

                    formattedBulletinEntities.push(formattedBulletin);
                }
            });
            return formattedBulletinEntities;
        },

        /**
         * Formats the specified controller service list.
         *
         * @param {array} controllerServiceApis
         * @returns {array}
         */
        getFormattedServiceApis: function (controllerServiceApis) {
            var formattedControllerServiceApis = [];
            $.each(controllerServiceApis, function (i, controllerServiceApi) {
                var formattedType = nfCommon.formatType({
                    'type': controllerServiceApi.type,
                    'bundle': controllerServiceApi.bundle
                });
                var formattedBundle = nfCommon.formatBundle(controllerServiceApi.bundle);
                formattedControllerServiceApis.push($('<div></div>').text(formattedType + ' from ' + formattedBundle));
            });
            return formattedControllerServiceApis;
        },

        /**
         * Formats the specified garbage collections list.
         *
         * @param {array} garbageCollections    The garbage collections
         * @returns {array}                     The formatted messages
         */
        getFormattedGarbageCollections: function (garbageCollections) {
            // sort the garbage collections
            garbageCollections.sort(function (a, b) {
                return b.collectionCount - a.collectionCount;
            });

            var formattedGarbageCollections = [];
            $.each(garbageCollections, function (_, garbageCollection) {
                var name = $('<span style="font-weight: bold;"></span>').text(garbageCollection.name);
                var stats = $('<span></span>').text(' - ' + garbageCollection.collectionCount + ' times (' + garbageCollection.collectionTime + ')');
                var gc = $('<div></div>').append(name).append(stats);
                formattedGarbageCollections.push(gc);
            });
            return formattedGarbageCollections;
        },

        getPolicyTypeListing: function (value) {
            var nest = d3.nest()
                .key(function (d) {
                    return d.value;
                })
                .map(policyTypeListing, d3.map);
            return nest.get(value)[0];
        },

        /**
         * Get component name from an entity safely.
         *
         * @param {object} entity    The component entity
         * @returns {String}         The component name if it can be read, otherwise entity id
         */
        getComponentName: function (entity) {
            return entity.permissions.canRead === true ? entity.component.name : entity.id;
        },

        /**
         * Find the corresponding combo option text from a combo option values.
         *
         * @param {object} options    The combo option array
         * @param {string} value      The target value
         * @returns {string}          The matched option text or undefined if not found
         */
        getComboOptionText: function (options, value) {
            var matchedOption = options.find(function (option) {
                return option.value === value;
            });
            return nfCommon.isDefinedAndNotNull(matchedOption) ? matchedOption.text : undefined;
        }

    };

    return nfCommon;
}));
