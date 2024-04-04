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

/* requires qtip plugin to be loaded first*/

(function (root, factory) {
    if (typeof define === 'function' && define.amd) {
        define(['jquery',
                'CodeMirror'],
            function ($, CodeMirror) {
                return (nf.nfpr = factory($, CodeMirror));
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.nfpr =
            factory(require('jquery'),
                require('CodeMirror')));
    } else {
        nf.nfpr = factory(root.$,
            root.CodeMirror);
    }
}(this, function ($, CodeMirror) {
    'use strict';

    /**
     * Formats the specified parameter definition.
     *
     * @param parameterDefinition
     * @returns {jQuery|HTMLElement}
     */
    var formatParameter = function (parameterDefinition) {
        var parameterContainer = $('<div></div>');

        // add the function name
        $('<div class="el-name el-section"></div>').text(parameterDefinition.name).appendTo(parameterContainer);

        // add the function description
        if (typeof parameterDefinition.description !== 'undefined' && parameterDefinition.description !== null) {
            $('<div class="el-section"></div>').text(parameterDefinition.description).appendTo(parameterContainer);
        }

        return parameterContainer;
    };

    var parameterKeyRegex = /^[a-zA-Z0-9-_. ]+/;

    var parameters = [];
    var parameterRegex = new RegExp('^$');

    var parameterDetails = {};
    var parametersSupported = false;

    // valid context states
    var PARAMETER = 'parameter';
    var INVALID = 'invalid';

    /**
     * Handles pound identifies on the stream.
     *
     * @param {object} stream   The character stream
     * @param {object} states    The states
     */
    var handlePound = function (stream, states) {
        // determine the number of sequential pounds
        var poundCount = 0;
        stream.eatWhile(function (ch) {
            if (ch === '#') {
                poundCount++;
                return true;
            }
            return false;
        });

        // if there is an even number of consecutive pounds this expression is escaped
        if (poundCount % 2 === 0) {
            // do not style an escaped expression
            return null;
        }

        // if there was an odd number of consecutive pounds and there was more than 1
        if (poundCount > 1) {
            // back up one char so we can process the start sequence next iteration
            stream.backUp(1);

            // do not style the preceding pounds
            return null;
        }

        // if the next character isn't the start of an expression
        if (stream.peek() === '{') {
            // consume the open curly
            stream.next();

            // new expression start
            states.push({
                context: PARAMETER
            });

            // consume any addition whitespace
            stream.eatSpace();

            return 'bracket';
        } else {
            // not a valid start sequence
            return null;
        }
    };

    // the api for the currently selected completion
    var currentApi = null;

    // the identifier to cancel showing the tip for the next completion
    var showTip = null;

    // the apis of every completion rendered
    var apis = [];

    /**
     * Listens for select event on the auto complete.
     *
     * @param {type} completion
     * @param {type} element
     * @returns {undefined}
     */
    var select = function(completion, element) {
        hide();

        currentApi = $(element).qtip('api');
        showTip = setTimeout(function() {
            currentApi.show();
        }, 500);
    };

    /**
     * Cancels the next tip to show, if applicable. Hides the currently
     * visible tip, if applicable.
     *
     * @returns {undefined}
     */
    var hide = function() {
        if (showTip !== null) {
            clearInterval(showTip);
            showTip = null;
        }

        if (currentApi !== null) {
            currentApi.hide();
        }
    };

    /**
     * Listens for close events for the auto complete.
     *
     * @returns {undefined}
     */
    var close = function() {
        if (showTip !== null) {
            clearInterval(showTip);
            showTip = null;
        }

        // clear the current api (since its in the apis array)
        currentApi = null;

        // destroy the tip from every applicable function
        $.each(apis, function(_, api) {
            api.destroy(true);
        });

        // reset the apis
        apis = [];
    };

    /**
     * Renders an auto complete item.
     *
     * @param {type} element
     * @param {type} self
     * @param {type} data
     * @returns {undefined}
     */
    var renderer = function(element, self, data) {
        var item = $('<div></div>').text(data.text);
        var li = $(element).qtip({
            content: formatParameter(parameterDetails[data.text]),
            style: {
                classes: 'nifi-tooltip nf-tooltip',
                tip: false,
                width: 350
            },
            show: {
                event: false,
                effect: false
            },
            hide: {
                event: false,
                effect: false
            },
            position: {
                at: 'bottom right',
                my: 'bottom left',
                adjust: {
                    x: 20
                }
            }
        }).append(item);

        // record the api for destruction later
        apis.push(li.qtip('api'));
    };

    return {

        /**
         * Enables parameter referencing.
         */
        enableParameters: function () {
            parameters = [];
            parameterRegex = new RegExp('^$');
            parameterDetails = {};

            parametersSupported = true;
        },

        /**
         * Sets the available parameters.
         *
         * @param parameterListing
         */
        setParameters: function (parameterListing) {
            parameterListing.forEach(function (parameter) {
                parameters.push(parameter.name);
                parameterDetails[parameter.name] = parameter;
            });

            parameterRegex = new RegExp('^((' + parameters.join(')|(') + '))$');
        },

        /**
         * Disables parameter referencing.
         */
        disableParameters: function () {
            parameters = [];
            parameterRegex = new RegExp('^$');
            parameterDetails = {};

            parametersSupported = false;
        },

        /**
         * Returns an object that provides syntax highlighting for NiFi expression language.
         */
        color: function () {
            // builds the states based off the specified initial value
            var buildStates = function (initialStates) {
                // each state has a context
                var states = initialStates;

                return {
                    copy: function () {
                        var copy = [];
                        for (var i = 0; i < states.length; i++) {
                            copy.push({
                                context: states[i].context
                            });
                        }
                        return copy;
                    },
                    get: function () {
                        if (states.length === 0) {
                            return {
                                context: null
                            };
                        } else {
                            return states[states.length - 1];
                        }
                    },
                    push: function (state) {
                        return states.push(state);
                    },
                    pop: function () {
                        return states.pop();
                    }
                };
            };

            return {
                startState: function () {
                    // build states with an empty array
                    return buildStates([]);
                },

                copyState: function (state) {
                    // build states with
                    return buildStates(state.copy());
                },

                token: function (stream, states) {
                    // consume any whitespace
                    if (stream.eatSpace()) {
                        return null;
                    }

                    // if we've hit the end of the line
                    if (stream.eol()) {
                        return null;
                    }

                    // get the current character
                    var current = stream.peek();

                    // get the current state
                    var state = states.get();

                    // the current input is invalid
                    if (state.context === INVALID) {
                        stream.skipToEnd();
                        return null;
                    }

                    // within a parameter reference
                    if (state.context === PARAMETER) {
                        // attempt to extract a parameter name
                        var parameterName = stream.match(parameterKeyRegex, false);

                        // if the result returned a match
                        if (parameterName !== null && parameterName.length === 1) {
                            // consume the entire token to ensure the whole function
                            // name is matched. this is an issue with functions like
                            // substring and substringAfter since 'substringA' would
                            // match the former and when we really want to autocomplete
                            // against the latter.
                            stream.match(parameterKeyRegex);

                            // see if this matches a known function and is followed by (
                            if (parameterRegex.test(parameterName)) {
                                // ------------------
                                // resolved parameter
                                // ------------------

                                // style for function
                                return 'builtin';
                            } else {
                                // --------------------
                                // unresolved parameter
                                // --------------------

                                // style for function
                                return 'string';
                            }
                        }
                        else if (current === '}') {
                            // -----------------
                            // end of expression
                            // -----------------

                            // consume the close
                            stream.next();

                            // signifies the end of an parameter reference
                            if (typeof states.pop() === 'undefined') {
                                return null;
                            } else {
                                // style as expression
                                return 'bracket';
                            }
                        } else {
                            // ----------
                            // unexpected
                            // ----------

                            // consume and move along
                            stream.skipToEnd();
                            state.context = INVALID;

                            // unexpected...
                            return null;
                        }
                    }

                    // signifies the potential start of an expression
                    if (current === '#' && parametersSupported) {
                        return handlePound(stream, states);
                    }

                    // signifies the end of an expression
                    if (current === '}') {
                        stream.next();
                        if (typeof states.pop() === 'undefined') {
                            return null;
                        } else {
                            return 'bracket';
                        }
                    }

                    // ----------------------------------------------------------
                    // extra characters that are around expression[s] end up here
                    // ----------------------------------------------------------

                    // consume the character to keep things moving along
                    stream.next();
                    return null;
                }
            };
        },

        /**
         * Returns the suggestions for the content at the current cursor.
         *
         * @param {type} editor
         */
        suggest: function (editor) {
            // Find the token at the cursor
            var cursor = editor.getCursor();
            var token = editor.getTokenAt(cursor);
            var includeAll = false;
            var state = token.state.get();

            // whether or not the current context is within a function
            var isParameterContext = function (context) {
                // attempting to match a function name or already successfully matched a function name
                return context === PARAMETER;
            };

            // only support suggestion in certain cases
            var context = state.context;
            if (!isParameterContext(context)) {
                return null;
            }

            // lower case for case insensitive comparison
            var value = token.string.toLowerCase();

            // trim to ignore extra whitespace
            var trimmed = $.trim(value);

            // identify potential patterns and increment the start location appropriately
            if (trimmed === '#{') {
                includeAll = true;
                token.start += value.length;
            }

            var getCompletions = function(parameterList) {
                var found = [];

                $.each(parameterList, function (i, parameter) {
                    if ($.inArray(parameter, found) === -1) {
                        if (includeAll || parameter.toLowerCase().indexOf(value) === 0) {
                            found.push({
                                text: parameter,
                                render: renderer
                            });
                        }
                    }
                });

                return found;
            };

            // get the suggestions for the current context
            var completionList = getCompletions(parameters);
            completionList = completionList.sort(function (a, b) {
                var aLower = a.text.toLowerCase();
                var bLower = b.text.toLowerCase();
                return aLower === bLower ? 0 : aLower > bLower ? 1 : -1;
            });

            var completions = {
                list: completionList,
                from: {
                    line: cursor.line,
                    ch: token.start
                },
                to: {
                    line: cursor.line,
                    ch: token.end
                }
            };

            CodeMirror.on(completions, 'select', select);
            CodeMirror.on(completions, 'close', close);

            return completions;
        },

        /**
         * Returns the language id.
         *
         * @returns {string} language id
         */
        getLanguageId: function () {
            return 'nfpr';
        },

        /**
         * Returns whether this editor mode supports EL.
         *
         * @returns {boolean}   Whether the editor supports EL
         */
        supportsEl: function () {
            return false;
        },

        /**
         * Returns whether this editor mode supports parameter reference.
         *
         * @returns {boolean}   Whether the editor supports parameter reference
         */
        supportsParameterReference: function () {
            return parametersSupported;
        }
    };
}));
