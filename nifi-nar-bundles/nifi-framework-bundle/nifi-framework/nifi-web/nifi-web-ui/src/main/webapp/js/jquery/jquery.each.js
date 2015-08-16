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
(function ($) {
    // get a reference to the original each method
    var origEach = $.each;

    // override the jQuery each method (not $.fn.each as
    // we are not calling with a jQuery 'instance', $.fn.each
    // defers to this function with 'this' as the object param)
    $.each = function (object, callback, args) {
        // ensure the object specified is not undefined or null
        if (typeof object !== 'undefined' && object !== null) {
            return origEach.call(this, object, callback, args);
        }
        return object;
    };
})(jQuery);