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

/* global nf, d3 */

nf.ng.BreadcrumbsCtrl = function (serviceProvider) {
    'use strict';

    function BreadcrumbsCtrl() {
        this.breadcrumbs = [];
    }
    BreadcrumbsCtrl.prototype = {
        constructor: BreadcrumbsCtrl,

        /**
         *  Register the breadcrumbs controller.
         */
        register: function() {
            if (serviceProvider.breadcrumbsCtrl === undefined) {
                serviceProvider.register('breadcrumbsCtrl', breadcrumbsCtrl);
            }
        },

        /**
         * Generate the breadcrumbs.
         *
         * @param {object} breadcrumbEntity  The breadcrumb
         */
        generateBreadcrumbs: function(breadcrumbEntity) {
            var label = breadcrumbEntity.id;
            if (breadcrumbEntity.permissions.canRead) {
                label = breadcrumbEntity.breadcrumb.name;
            }

            this.breadcrumbs.unshift($.extend({
                'label': label
            }, breadcrumbEntity));

            if (nf.Common.isDefinedAndNotNull(breadcrumbEntity.parentBreadcrumb)) {
                this.generateBreadcrumbs(breadcrumbEntity.parentBreadcrumb);
            }
        },

        /**
         * Reset the breadcrumbs.
         */
        resetBreadcrumbs: function() {
            this.breadcrumbs = [];
        },

        /**
         * Get the breadcrumbs.
         */
        getBreadcrumbs: function() {
            return this.breadcrumbs;
        },

        /**
         * Update the breadcrumbs css.
         *
         * @param {object} style  The style to be applied.
         */
        updateBreadcrumbsCss: function(style) {
            $('#breadcrumbs').css(style);
        },

        /**
         * Reset initial scroll position.
         */
        resetScrollPosition: function() {
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
        registerMouseWheelEvent: function(element) {
            // mousewheel -> IE, Chrome
            // DOMMouseScroll -> FF
            // wheel -> FF, IE

            // still having issues with this in IE :/
            element.on('DOMMouseScroll mousewheel', function (evt, d) {
                if (nf.Common.isUndefinedOrNull(evt.originalEvent)) {
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
                    if (nf.Common.isDefinedAndNotNull(evt.originalEvent.wheelDelta)) {
                        delta = evt.originalEvent.wheelDelta;
                    } else if (nf.Common.isDefinedAndNotNull(evt.originalEvent.detail)) {
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
};