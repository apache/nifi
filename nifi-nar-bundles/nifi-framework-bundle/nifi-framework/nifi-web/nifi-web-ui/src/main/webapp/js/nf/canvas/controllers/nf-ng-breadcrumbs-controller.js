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

/* global define, module, require, exports */

(function (root, factory) {
    if (typeof define === 'function' && define.amd) {
        define(['jquery',
                'nf.Common'],
            function ($, nfCommon) {
                return (nf.ng.BreadcrumbsCtrl = factory($, nfCommon));
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.ng.BreadcrumbsCtrl =
            factory(require('jquery'),
                require('nf.Common')));
    } else {
        nf.ng.BreadcrumbsCtrl = factory(root.$,
            root.nf.Common);
    }
}(this, function ($, nfCommon) {
    'use strict';

    return function (serviceProvider) {
        'use strict';

        function BreadcrumbsCtrl() {
            this.breadcrumbs = [];
        }

        BreadcrumbsCtrl.prototype = {
            constructor: BreadcrumbsCtrl,

            /**
             *  Register the breadcrumbs controller.
             */
            register: function () {
                if (serviceProvider.breadcrumbsCtrl === undefined) {
                    serviceProvider.register('breadcrumbsCtrl', breadcrumbsCtrl);
                }
            },

            /**
             * Generate the breadcrumbs.
             *
             * @param {object} breadcrumbEntity  The breadcrumb
             */
            generateBreadcrumbs: function (breadcrumbEntity) {
                var label = breadcrumbEntity.id;
                if (breadcrumbEntity.permissions.canRead) {
                    label = breadcrumbEntity.breadcrumb.name;
                }

                this.breadcrumbs.unshift($.extend({
                    'label': label
                }, breadcrumbEntity));

                if (nfCommon.isDefinedAndNotNull(breadcrumbEntity.parentBreadcrumb)) {
                    this.generateBreadcrumbs(breadcrumbEntity.parentBreadcrumb);
                }
            },

            /**
             * Updates the version control information for the specified process group.
             *
             * @param processGroupId
             * @param versionControlInformation
             */
            updateVersionControlInformation: function (processGroupId, versionControlInformation) {
                $.each(this.breadcrumbs, function (_, breadcrumbEntity) {
                    if (breadcrumbEntity.id === processGroupId) {
                        breadcrumbEntity.breadcrumb.versionControlInformation = versionControlInformation;
                        return false;
                    }
                });
            },

            /**
             * Reset the breadcrumbs.
             */
            resetBreadcrumbs: function () {
                this.breadcrumbs = [];
            },

            /**
             * Whether this crumb is tracking.
             *
             * @param breadcrumbEntity
             * @returns {*}
             */
            isTracking: function (breadcrumbEntity) {
                return nfCommon.isDefinedAndNotNull(breadcrumbEntity.versionedFlowState);
            },

            /**
             * Returns the class string to use for the version control of the specified breadcrumb.
             *
             * @param breadcrumbEntity
             * @returns {string}
             */
            getVersionControlClass: function (breadcrumbEntity) {
                if (nfCommon.isDefinedAndNotNull(breadcrumbEntity.versionedFlowState)) {
                    var vciState = breadcrumbEntity.versionedFlowState;
                    if (vciState === 'SYNC_FAILURE') {
                        return 'breadcrumb-version-control-gray fa fa-question'
                    } else if (vciState === 'LOCALLY_MODIFIED_AND_STALE') {
                        return 'breadcrumb-version-control-red fa fa-exclamation-circle';
                    } else if (vciState === 'STALE') {
                        return 'breadcrumb-version-control-red fa fa-arrow-circle-up';
                    } else if (vciState === 'LOCALLY_MODIFIED') {
                        return 'breadcrumb-version-control-gray fa fa-asterisk';
                    } else {
                        return 'breadcrumb-version-control-green fa fa-check';
                    }
                } else {
                    return '';
                }
            },

            /**
             * Gets the content for the version control tooltip for the specified breadcrumb.
             *
             * @param breadcrumbEntity
             */
            getVersionControlTooltip: function (breadcrumbEntity) {
                if (nfCommon.isDefinedAndNotNull(breadcrumbEntity.versionedFlowState) && breadcrumbEntity.permissions.canRead) {
                    return nfCommon.getVersionControlTooltip(breadcrumbEntity.breadcrumb.versionControlInformation);
                } else {
                    return 'This Process Group is not under version control.'
                }
            },

            /**
             * Get the breadcrumbs.
             */
            getBreadcrumbs: function () {
                return this.breadcrumbs;
            },

            /**
             * Update the breadcrumbs css.
             *
             * @param {object} style  The style to be applied.
             */
            updateBreadcrumbsCss: function (style) {
                $('#breadcrumbs').css(style);
            },

            /**
             * Reset initial scroll position.
             */
            resetScrollPosition: function () {
                var title = $('#data-flow-title-container');
                var titlePosition = title.position();
                var titleWidth = title.outerWidth();
                var titleRight = titlePosition.left + titleWidth;

                var padding = $('#breadcrumbs-right-border').width();
                var viewport = $('#data-flow-title-viewport');
                var viewportWidth = viewport.width();
                var viewportRight = viewportWidth - padding;

                // if the title's right is past the viewport's right, shift accordingly
                if (titleRight > viewportRight) {
                    // adjust the position
                    title.css('left', (titlePosition.left - (titleRight - viewportRight)) + 'px');
                } else {
                    title.css('left', '10px');
                }
            },

            /**
             * Registers a scroll event on the `element`
             *
             * @param {object} element    The element event listener will be registered upon.
             */
            registerMouseWheelEvent: function (element) {
                // mousewheel -> IE, Chrome
                // DOMMouseScroll -> FF
                // wheel -> FF, IE

                // still having issues with this in IE :/
                element.on('DOMMouseScroll mousewheel', function (evt, d) {
                    if (nfCommon.isUndefinedOrNull(evt.originalEvent)) {
                        return;
                    }

                    var title = $('#data-flow-title-container');
                    var titlePosition = title.position();
                    var titleWidth = title.outerWidth();
                    var titleRight = titlePosition.left + titleWidth;

                    var padding = $('#breadcrumbs-right-border').width();
                    var viewport = $('#data-flow-title-viewport');
                    var viewportWidth = viewport.width();
                    var viewportRight = viewportWidth - padding;

                    // if the width of the title is larger than the viewport
                    if (titleWidth > viewportWidth) {
                        var adjust = false;

                        var delta = 0;

                        //Chrome and Safari both have evt.originalEvent.detail defined but
                        //evt.originalEvent.wheelDelta holds the correct value so we must
                        //check for evt.originalEvent.wheelDelta first!
                        if (nfCommon.isDefinedAndNotNull(evt.originalEvent.wheelDelta)) {
                            delta = evt.originalEvent.wheelDelta;
                        } else if (nfCommon.isDefinedAndNotNull(evt.originalEvent.detail)) {
                            delta = -evt.originalEvent.detail;
                        }

                        // determine the increment
                        if (delta > 0 && titleRight > viewportRight) {
                            var increment = -25;
                            adjust = true;
                        } else if (delta < 0 && (titlePosition.left - padding) < 0) {
                            increment = 25;

                            // don't shift too far
                            if (titlePosition.left + increment > padding) {
                                increment = padding - titlePosition.left;
                            }

                            adjust = true;
                        }

                        if (adjust) {
                            // adjust the position
                            title.css('left', (titlePosition.left + increment) + 'px');
                        }
                    }
                });
            }
        }

        var breadcrumbsCtrl = new BreadcrumbsCtrl();
        breadcrumbsCtrl.register();
        return breadcrumbsCtrl;
    }
}));