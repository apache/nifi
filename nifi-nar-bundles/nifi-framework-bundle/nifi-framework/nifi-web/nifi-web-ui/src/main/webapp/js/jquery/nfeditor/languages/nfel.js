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
                return (nf.nfel = factory($, CodeMirror));
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.nfel =
            factory(require('jquery'),
                require('CodeMirror')));
    } else {
        nf.nfel = factory(root.$,
            root.CodeMirror);
    }
}(this, function ($, CodeMirror) {
    'use strict';
    
    /**
     * Formats the specified arguments for the EL function tooltip.
     * 
     * @param {type} args
     * @returns {String}
     */
    var formatArguments = function(args) {
        if ($.isEmptyObject(args)) {
            return '<span class="unset">None</span>';
        } else {
            var formatted = '<div class="clear"></div><ul class="el-arguments">';
            $.each(args, function(key, value) {
                formatted += (
                    '<li>' +
                        '<span class="el-argument-name">' + key + '</span> - ' +
                        value +
                    '</li>'
                );
            });
            formatted += '</ul>';
            return formatted;
        }
    };
    
    var subjectlessFunctions = [];
    var functions = [];
    
    var subjectlessFunctionRegex = new RegExp('^$');
    var functionRegex = new RegExp('^$');
    
    var functionDetails = {};
    
    $.ajax({
        type: 'GET',
        url: '../nifi-docs/html/expression-language-guide.html',
        dataType: 'html'
    }).done(function(response) {
        $(response).find('div.function').each(function() {
            var elFunction = $(this);
            
            var name = elFunction.find('h3').text();
            var description = elFunction.find('span.description').text();
            var returnType = elFunction.find('span.returnType').text();
            
            var subject;
            var subjectSpan = subject = elFunction.find('span.subject');
            var subjectless = elFunction.find('span.subjectless');
            
            // Determine if this function supports running subjectless
            if (subjectless.length) {
                subjectlessFunctions.push(name);
                subject = '<span class="unset">None</span>';
            }

            // Determine if this function supports running with a subject
            if (subjectSpan.length) {
                functions.push(name);
                subject = elFunction.find('span.subject').text();
            }

            // find the arguments
            var args = {};
            elFunction.find('span.argName').each(function() {
                var argName = $(this);
                var argDescription = argName.next('span.argDesc');
                args[argName.text()] = argDescription.text(); 
            });
            
            // format the function tooltip
            functionDetails[name] = 
                '<div>' + 
                    '<div class="el-name el-section">' + name + '</div>' +
                    '<div class="el-section">' + description + '</div>' +
                    '<div class="el-section">' + 
                        '<div class="el-header">Arguments</div>' +
                        formatArguments(args) + 
                    '</div>' +
                    '<div class="el-section">' + 
                        '<div class="el-header">Subject</div>' +
                        '<p>' + subject + '</p>' + 
                        '<div class="clear"></div>' +
                    '</div>' +
                    '<div class="el-section">' + 
                        '<div class="el-header">Returns</div>' +
                        '<p>' + returnType + '</p>' +
                        '<div class="clear"></div>' +
                    '</div>' +
                '</div>';
        });
    }).always(function() {
        // build the regex for all functions discovered
        subjectlessFunctionRegex = new RegExp('^((' + subjectlessFunctions.join(')|(') + '))$');
        functionRegex = new RegExp('^((' + functions.join(')|(') + '))$');
    });
    
    // valid context states
    var SUBJECT = 'subject';
    var FUNCTION = 'function';
    var SUBJECT_OR_FUNCTION = 'subject-or-function';
    var EXPRESSION = 'expression';
    var ARGUMENTS = 'arguments';
    var ARGUMENT = 'argument';
    var INVALID = 'invalid';
    
    /**
     * Handles dollars identifies on the stream.
     * 
     * @param {object} stream   The character stream
     * @param {object} states    The states
     */
    var handleDollar = function (stream, states) {
        // determine the number of sequential dollars
        var dollarCount = 0;
        stream.eatWhile(function (ch) {
            if (ch === '$') {
                dollarCount++;
                return true;
            }
            return false;
        });

        // if there is an even number of consecutive dollars this expression is escaped
        if (dollarCount % 2 === 0) {
            // do not style an escaped expression
            return null;
        }

        // if there was an odd number of consecutive dollars and there was more than 1
        if (dollarCount > 1) {
            // back up one char so we can process the start sequence next iteration
            stream.backUp(1);

            // do not style the preceeding dollars
            return null;
        }

        // if the next character isn't the start of an expression
        if (stream.peek() === '{') {
            // consume the open curly
            stream.next();

            // new expression start
            states.push({
                context: EXPRESSION
            });

            // consume any addition whitespace
            stream.eatSpace();

            return 'bracket';
        } else {
            // not a valid start sequence
            return null;
        }
    };

    /**
     * Handles dollars identifies on the stream.
     * 
     * @param {object} stream   The character stream
     * @param {object} state    The current state
     */
    var handleStringLiteral = function (stream, state) {
        var current = stream.next();
        var foundTrailing = false;
        var foundEscapeChar = false;

        // locate a closing string delimitor
        var foundStringLiteral = stream.eatWhile(function (ch) {
            // we've just found the trailing delimitor, stop
            if (foundTrailing === true) {
                return false;
            }

            // if this is the trailing delimitor, only consume
            // if we did not see the escape character on the 
            // previous iteration
            if (ch === current) {
                foundTrailing = foundEscapeChar === false;
            }

            // reset the escape character flag
            foundEscapeChar = false;

            // if this is the escape character, set the flag
            if (ch === '\\') {
                foundEscapeChar = true;
            }

            // consume this character
            return true;
        });

        // if we found the trailing delimitor
        if (foundStringLiteral) {
            return 'string';
        }

        // there is no trailing delimitor... clear the current context
        state.context = INVALID;
        stream.skipToEnd();
        return null;
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
            content: functionDetails[data.text],
            style: {
                classes: 'nifi-tooltip nfel-tooltip',
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

                    // if we've hit some comments... will consume the remainder of the line
                    if (current === '#') {
                        stream.skipToEnd();
                        return 'comment';
                    }

                    // get the current state
                    var state = states.get();

                    // the current input is invalid
                    if (state.context === INVALID) {
                        stream.skipToEnd();
                        return null;
                    }

                    // within an expression
                    if (state.context === EXPRESSION) {
                        var attributeOrSubjectlessFunctionExpression = /^[^'"#${}()[\],:;\/*\\\s\t\r\n0-9][^'"#${}()[\],:;\/*\\\s\t\r\n]*/;

                        // attempt to extract a function name
                        var attributeOrSubjectlessFunctionName = stream.match(attributeOrSubjectlessFunctionExpression, false);

                        // if the result returned a match
                        if (attributeOrSubjectlessFunctionName !== null && attributeOrSubjectlessFunctionName.length === 1) {
                            // consume the entire token to better support suggest below
                            stream.match(attributeOrSubjectlessFunctionExpression);

                            // if the result returned a match and is followed by a (
                            if (subjectlessFunctionRegex.test(attributeOrSubjectlessFunctionName) && stream.peek() === '(') {
                                // --------------------
                                // subjectless function
                                // --------------------

                                // context change to function
                                state.context = ARGUMENTS;

                                // style for function
                                return 'builtin';
                            } else {
                                // ---------------------
                                // attribute or function
                                // ---------------------

                                // context change to function or subject... not sure yet
                                state.context = SUBJECT_OR_FUNCTION;

                                // this could be an attribute or a partial function name... style as attribute until we know
                                return 'variable-2';
                            }
                        } else if (current === '\'' || current === '"') {
                            // --------------
                            // string literal
                            // --------------

                            // handle the string literal
                            var expressionStringResult = handleStringLiteral(stream, state);

                            // considered a quoted variable
                            if (expressionStringResult !== null) {
                                // context change to function
                                state.context = SUBJECT;
                            }

                            return expressionStringResult;
                        } else if (current === '$') {
                            // -----------------
                            // nested expression
                            // -----------------

                            var expressionDollarResult = handleDollar(stream, states);

                            // if we've found an embedded expression we need to...
                            if (expressionDollarResult !== null) {
                                // transition back to subject when this expression completes
                                state.context = SUBJECT;
                            }

                            return expressionDollarResult;
                        } else if (current === '}') {
                            // -----------------
                            // end of expression
                            // -----------------

                            // consume the close
                            stream.next();

                            // signifies the end of an expression
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

                            // consume to move along
                            stream.skipToEnd();
                            state.context = INVALID;

                            // unexpected...
                            return null;
                        }
                    }

                    // within a subject
                    if (state.context === SUBJECT || state.context === SUBJECT_OR_FUNCTION) {
                        // if the next character indicates the start of a function call
                        if (current === ':') {
                            // -------------------------
                            // trigger for function name
                            // -------------------------

                            // consume the colon and update the context
                            stream.next();
                            state.context = FUNCTION;

                            // consume any addition whitespace
                            stream.eatSpace();

                            // don't style
                            return null;
                        } else if (current === '}') {
                            // -----------------
                            // end of expression
                            // -----------------

                            // consume the close
                            stream.next();

                            // signifies the end of an expression
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

                            // consume to move along
                            stream.skipToEnd();
                            state.context = INVALID;

                            // unexpected...
                            return null;
                        }
                    }

                    // within a function
                    if (state.context === FUNCTION) {
                        // attempt to extract a function name
                        var functionName = stream.match(/^[a-zA-Z]+/, false);

                        // if the result returned a match
                        if (functionName !== null && functionName.length === 1) {
                            // consume the entire token to ensure the whole function
                            // name is matched. this is an issue with functions like
                            // substring and substringAfter since 'substringA' would
                            // match the former and when we really want to autocomplete
                            // against the latter.
                            stream.match(/^[a-zA-Z]+/);

                            // see if this matches a known function and is followed by (
                            if (functionRegex.test(functionName) && stream.peek() === '(') {
                                // --------
                                // function
                                // --------

                                // change context to arugments
                                state.context = ARGUMENTS;

                                // style for function
                                return 'builtin';
                            } else {
                                // ------------------------------
                                // maybe function... not sure yet
                                // ------------------------------

                                // not sure yet... 
                                return null;
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

                    // within arguments
                    if (state.context === ARGUMENTS) {
                        if (current === '(') {
                            // --------------
                            // argument start
                            // --------------

                            // consume the open paranthesis
                            stream.next();

                            // change context to handle an argument
                            state.context = ARGUMENT;

                            // start of arguments
                            return null;
                        } else if (current === ')') {
                            // --------------
                            // argument close
                            // --------------

                            // consume the close paranthesis
                            stream.next();

                            // change context to subject for potential chaining
                            state.context = SUBJECT;

                            // end of arguments
                            return null;
                        } else if (current === ',') {
                            // ------------------
                            // argument separator
                            // ------------------

                            // consume the comma
                            stream.next();

                            // change context back to argument
                            state.context = ARGUMENT;

                            // argument separator
                            return null;
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

                    // within a specific argument
                    if (state.context === ARGUMENT) {
                        if (current === '\'' || current === '"') {
                            // --------------
                            // string literal
                            // --------------

                            // handle the string literal
                            var argumentStringResult = handleStringLiteral(stream, state);

                            // successfully processed a string literal... 
                            if (argumentStringResult !== null) {
                                // change context back to arguments
                                state.context = ARGUMENTS;
                            }

                            return argumentStringResult;
                        } else if (stream.match(/^[-\+]?((([0-9]+\.[0-9]*)([eE][+-]?([0-9])+)?)|((\.[0-9]+)([eE][+-]?([0-9])+)?)|(([0-9]+)([eE][+-]?([0-9])+)))/)) {
                            // -------------
                            // Decimal value
                            // -------------
                            // This matches the following ANTLR spec for deciamls
                            //
                            // DECIMAL :     OP? ('0'..'9')+ '.' ('0'..'9')* EXP?    ^([0-9]+\.[0-9]*)([eE][+-]?([0-9])+)?
                            //             | OP? '.' ('0'..'9')+ EXP?
                            //             | OP? ('0'..'9')+ EXP;
                            //
                            // fragment OP: ('+'|'-');
                            // fragment EXP : ('e'|'E') ('+'|'-')? ('0'..'9')+ ;

                            // change context back to arguments
                            state.context = ARGUMENTS;

                            // style for decimal (use same as number)
                            return 'number';
                        } else if (stream.match(/^[-\+]?[0-9]+/)) {
                            // -------------
                            // integer value
                            // -------------

                            // change context back to arguments
                            state.context = ARGUMENTS;

                            // style for integers
                            return 'number';
                        } else if (stream.match(/^((true)|(false))/)) {
                            // -------------
                            // boolean value
                            // -------------

                            // change context back to arguments
                            state.context = ARGUMENTS;

                            // style for boolean (use same as number)
                            return 'number';
                        } else if (current === ')') {
                            // ----------------------------------
                            // argument close (zero arg function)
                            // ----------------------------------

                            // consume the close paranthesis
                            stream.next();

                            // change context to subject for potential chaining
                            state.context = SUBJECT;

                            // end of arguments
                            return null;
                        } else if (current === '$') {
                            // -----------------
                            // nested expression
                            // -----------------

                            // handle the nested expression
                            var argumentDollarResult = handleDollar(stream, states);

                            // if we've found an embedded expression we need to...
                            if (argumentDollarResult !== null) {
                                // transition back to arguments when then expression completes
                                state.context = ARGUMENTS;
                            }

                            return argumentDollarResult;
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
                    if (current === '$') {
                        return handleDollar(stream, states);
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
                    return;
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
            var isFunction = function (context) {
                // attempting to match a function name or already successfully matched a function name
                return context === FUNCTION || context === ARGUMENTS;
            };

            // whether or not the current context is within a subject-less funciton
            var isSubjectlessFunction = function (context) {
                // within an expression when no characters are found or when the string may be an attribute or a function
                return context === EXPRESSION || context === SUBJECT_OR_FUNCTION;
            };

            // only support suggestion in certain cases
            var context = state.context;
            if (!isSubjectlessFunction(context) && !isFunction(context)) {
                return null;
            }

            // lower case for case insensitive comparison
            var value = token.string.toLowerCase();

            // trim to ignore extra whitespace
            var trimmed = $.trim(value);

            // identify potential patterns and increment the start location appropriately
            if (trimmed === '${' || trimmed === ':') {
                includeAll = true;
                token.start += value.length;
            }

            var getCompletions = function(functions) {
                var found = [];

                $.each(functions, function (i, funct) {
                    if ($.inArray(funct, found) === -1) {
                        if (includeAll || funct.toLowerCase().indexOf(value) === 0) {
                            found.push({
                                text: funct,
                                render: renderer
                            });
                        }
                    }
                });

                return found;
            };

            // get the suggestions for the current context
            var completionList = getCompletions(isSubjectlessFunction(context) ? subjectlessFunctions : functions);
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
        }
    };
}));