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
 * Adds a simple character counter functionality to the matched elements. The 
 * options are specified in the following format:
 * 
 * {
 *   charCountField: element or dom id for displaying the remaining characters (must support .text())
 *   maxLength: maxlength in case the matched element does not have a max length set
 * }
 * 
 * Implementation specifics suggested on StackOverflow.
 * 
 * @param {type} $
 * @returns {undefined}
 */
(function ($) {

    var isUndefined = function (obj) {
        return typeof obj === 'undefined';
    };

    var isNull = function (obj) {
        return obj === null;
    };

    var isDefinedAndNotNull = function (obj) {
        return !isUndefined(obj) && !isNull(obj);
    };

    var methods = {
        
        /**
         * Initializes the count widget.
         * 
         * @param {type} options
         */
        init: function (options) {
            return this.each(function () {
                // ensure the options have been properly specified
                if (isDefinedAndNotNull(options) && isDefinedAndNotNull(options.charCountField)) {

                    // get the field
                    var field = $(this);

                    // determine the appropriate max length
                    var maxLength = $(field).attr('maxlength');
                    if (isUndefined(maxLength) || isNull(maxLength)) {
                        maxLength = options.maxLength;
                    }

                    // ensure we found a max length
                    if (isDefinedAndNotNull(maxLength)) {
                        // listen for keyup and blur events
                        $(field).bind('keyup blur', function () {
                            // get the current value
                            var value = $(field).val();

                            // determine if the length is too long
                            if (value.length > maxLength) {
                                // update the value accordingly
                                $(field).val(value.slice(0, maxLength));

                                // update the count field
                                $(options.charCountField).text(0);
                            } else {
                                // update the count field
                                $(options.charCountField).text(maxLength - value.length);
                            }
                        }).keyup();
                    }
                }
            });
        }
    };

    $.fn.count = function (method) {
        if (methods[method]) {
            return methods[method].apply(this, Array.prototype.slice.call(arguments, 1));
        } else {
            return methods.init.apply(this, arguments);
        }
    };
})(jQuery);
