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
 * Plugin to support entering tab characters into textarea's or textfield's. 
 * Code heavily borrowed from stackoverflow post by user tim-down
 * (http://stackoverflow.com/questions/4379535/tab-within-a-text-area-without-changing-focus-jquery/4380032#4380032)
 */
(function ($) {
    /**
     * Adds key listeners for capturing tab keys.
     */
    function allowTabChar(element) {
        $(element).keydown(function (e) {
            if (e.which == 9) {
                if (e.shiftKey == false && e.ctrlKey == false) {
                    insertText(this, '\t');
                }
                return false;
            }
        }).keypress(function (e) {
            if (e.which == 9) {
                return false;
            }
        });
    }

    /**
     * Inserts the specified text into the specified element.
     */
    function insertText(element, text) {
        element.focus();
        if (typeof element.selectionStart == 'number') {
            var value = element.value;
            var selectionStart = element.selectionStart;
            element.value = value.slice(0, selectionStart) + text + value.slice(element.selectionEnd);
            element.selectionEnd = element.selectionStart = selectionStart + text.length;
        } else if (typeof document.selection != 'undefined') {
            var textRange = document.selection.createRange();
            textRange.text = text;
            textRange.collapse(false);
            textRange.select();
        }
    }

    $.fn.tab = function () {
        return this.each(function () {
            if (this.nodeType == 1) {
                var nodeName = this.nodeName.toLowerCase();
                if (nodeName == 'textarea' || (nodeName == 'input' && this.type == 'text')) {
                    allowTabChar(this);
                }
            }
        });
    }
})(jQuery);
