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
 * Simple jQuery plugin to center any matched elements. Implementation
 * specifics suggested on StackOverflow.
 * 
 * @param {type} $
 * @returns {undefined}
 */
(function ($) {
    $.fn.center = function () {
        return this.each(function () {
            $(this).css({
                'position': 'absolute',
                'top': ($(window).height() - $(this).outerHeight()) / 2 + $(window).scrollTop() + 'px',
                'left': ($(window).width() - $(this).outerWidth()) / 2 + $(window).scrollLeft() + 'px'
            });
        });
    };
})(jQuery);
