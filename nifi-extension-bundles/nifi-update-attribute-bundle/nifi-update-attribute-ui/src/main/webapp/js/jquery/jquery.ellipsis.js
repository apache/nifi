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

/**
 * Plugin to provide support for generating ellipsis. This plugin comprises
 * a number of suggested techniques found on StackOverflow.
 * 
 * @param {type} $
 * @returns {undefined}
 */
(function ($) {

    var entityMap = {
        '&': '&amp;',
        '<': '&lt;',
        '>': '&gt;',
        '"': '&quot;',
        "'": '&#39;',
        '/': '&#x2f;'
    };

    var escapeHtml = function (string) {
        return String(string).replace(/[&<>"'\/]/g, function (s) {
            return entityMap[s];
        });
    };

    /**
     * Inserts ellipsis in elements where the contents exceed the width. The
     * original text is preserved in the title attribute of the element. The
     * element should have the following css properties:
     * 
     * overflow: hidden;
     * white-space: nowrap;
     * width: 50px (value must be set in 'px')
     * 
     * If multiline is desired, add a 'multiline' class to the element.
     * 
     * @param {type} addTooltip
     */
    $.fn.ellipsis = function (addTooltip) {
        addTooltip = (typeof addTooltip === 'undefined' || addTooltip === null) ? true : addTooltip;

        // function for determining if the browser supports ellipsis via css
        function supportsOverflowProperty() {
            var s = document.documentElement.style;
            return ('textOverflow' in s || 'OTextOverflow' in s);
        }

        function binarySearch(length, funct) {
            var low = 0;
            var high = length - 1;
            var best = -1;
            var mid;

            while (low <= high) {
                mid = ~~((low + high) / 2);
                var result = funct(mid);
                if (result < 0) {
                    high = mid - 1;
                } else if (result > 0) {
                    low = mid + 1;
                } else {
                    best = mid;
                    low = mid + 1;
                }
            }

            return best;
        }

        // chain and process each matched element
        return this.each(function () {
            var element = $(this);

            // preserve the original text in the title attribute
            var text = element.text();
            if (addTooltip) {
                element.attr('title', text);
            }

            // determine if this is a multiline element
            var multiline = element.hasClass('multiline');

            // if its not multiline and the browser supports ellipsis via css, use it
            if (!multiline && supportsOverflowProperty()) {
                element.css({
                    'text-overflow': 'ellipsis',
                    'o-text-overflow': 'ellipsis'
                });
            } else {
                // clone the element and add to the body - this addresses issues where
                // the element may not have been added to the dom (causing its size and 
                // styles to be incorrect)
                var cloneElement = $(this.cloneNode(true)).hide().appendTo('body');

                // make a scratch element to adjusting the text size
                var scratchElement = $(this.cloneNode(true)).hide().css({
                    'position': 'absolute',
                    'overflow': 'visible'
                }).width(multiline ? cloneElement.width() : 'auto').height(multiline ? 'auto' : cloneElement.height()).appendTo('body');

                // function for determining if the scratch element is too tall
                var isTooTall = function () {
                    return scratchElement.height() >= cloneElement.height();
                };

                // function for determining if the scratch element is too wide
                var isTooWide = function () {
                    return scratchElement.width() >= cloneElement.width();
                };

                // determine the appropriate function to use
                var isTooBig = multiline ? isTooTall : isTooWide;

                // if the text is too big, adjust it accordingly
                if (isTooBig()) {
                    var originalText = escapeHtml(text);

                    // function for creating the text within the scratch element
                    var createContent = function (i) {
                        var t = originalText.substr(0, i);
                        scratchElement.html(t + '&#8230;');
                    };

                    // function for performing the search
                    var search = function (i) {
                        createContent(i);
                        if (isTooBig()) {
                            return -1;
                        }
                        return 0;
                    };

                    // determine the appropriate length
                    var length = binarySearch(originalText.length - 1, search);

                    // create the content at the appropriate length
                    createContent(length);
                }

                // update the actual elements value
                element.html(scratchElement.html());

                // remove the cloned and scratch elements
                cloneElement.remove();
                scratchElement.remove();
            }
        });
    };
})(jQuery);