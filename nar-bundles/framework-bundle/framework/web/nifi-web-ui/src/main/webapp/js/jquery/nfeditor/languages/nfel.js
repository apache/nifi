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
nf.nfel = {
    subjectlessFunctionRegex: null,
    subjectlessFunctions: ['anyAttribute', 'anyMatchingAttribute', 'allAttributes', 'allMatchingAttributes', 'allDelineatedValues',
        'anyDelineatedValue', 'nextInt', 'ip', 'UUID', 'hostname', 'now'],
    functionRegex: null,
    functions: ['toUpper', 'toLower', 'toString', 'length', 'trim', 'isNull', 'notNull', 'toNumber', 'urlEncode', 'urlDecode',
        'not', 'substringAfterLast', 'substringBeforeLast', 'substringAfter', 'substringBefore', 'substring', 'startsWith',
        'endsWith', 'contains', 'prepend', 'append', 'indexOf', 'lastIndexOf', 'replaceNull', 'find', 'matches', 'equalsIgnoreCase',
        'equals', 'gt', 'lt', 'ge', 'le', 'format', 'toDate', 'mod', 'plus', 'minus', 'multiply', 'divide', 'toRadix', 'or', 'and',
        'replaceAll', 'replace'],
    // valid context states
    SUBJECT: 'subject',
    FUNCTION: 'function',
    SUBJECT_OR_FUNCTION: 'subject-or-function',
    EXPRESSION: 'expression',
    ARGUMENTS: 'arguments',
    ARGUMENT: 'argument',
    INVALID: 'invalid',
    /**
     * Initializes the function regular expressions. 
     */
    init: function () {
        nf.nfel.subjectlessFunctionRegex = new RegExp('^((' + nf.nfel.subjectlessFunctions.join(')|(') + '))$');
        nf.nfel.functionRegex = new RegExp('^((' + nf.nfel.functions.join(')|(') + '))$');
    },
    /**
     * Handles dollars identifies on the stream.
     * 
     * @param {object} stream   The character stream
     * @param {object} states    The states
     */
    handleDollar: function (stream, states) {
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
                context: nf.nfel.EXPRESSION
            });

            return 'bracket';
        } else {
            // not a valid start sequence
            return null;
        }
    },
    /**
     * Handles dollars identifies on the stream.
     * 
     * @param {object} stream   The character stream
     * @param {object} state    The current state
     */
    handleStringLiteral: function (stream, state) {
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
        state.context = nf.nfel.INVALID;
        stream.skipToEnd();
        return null;
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

                // if we've hit some comments... will consume the remainder of the line
                if (current === '#') {
                    stream.skipToEnd();
                    return 'comment';
                }

                // get the current state
                var state = states.get();

                // the current input is invalid
                if (state.context === nf.nfel.INVALID) {
                    stream.skipToEnd();
                    return null;
                }

                // within an expression
                if (state.context === nf.nfel.EXPRESSION) {
                    var attributeOrSubjectlessFunctionExpression = /^[^'"#${}()[\],:;\/*\\\s\t\r\n0-9][^'"#${}()[\],:;\/*\\\s\t\r\n]*/;

                    // attempt to extract a function name
                    var attributeOrSubjectlessFunctionName = stream.match(attributeOrSubjectlessFunctionExpression, false);

                    // if the result returned a match
                    if (attributeOrSubjectlessFunctionName !== null && attributeOrSubjectlessFunctionName.length === 1) {
                        // consume the entire token to better support suggest below
                        stream.match(attributeOrSubjectlessFunctionExpression);

                        // if the result returned a match
                        if (nf.nfel.subjectlessFunctionRegex.test(attributeOrSubjectlessFunctionName)) {
                            // --------------------
                            // subjectless function
                            // --------------------

                            // context change to function
                            state.context = nf.nfel.ARGUMENTS;

                            // style for function
                            return 'builtin';
                        } else {
                            // ---------------------
                            // attribute or function
                            // ---------------------

                            // context change to function or subject... not sure yet
                            state.context = nf.nfel.SUBJECT_OR_FUNCTION;

                            // this could be an attribute or a partial function name... style as attribute until we know
                            return 'variable-2';
                        }
                    } else if (current === '\'' || current === '"') {
                        // --------------
                        // string literal
                        // --------------

                        // handle the string literal
                        var expressionStringResult = nf.nfel.handleStringLiteral(stream, state);

                        // considered a quoted variable
                        if (expressionStringResult !== null) {
                            // context change to function
                            state.context = nf.nfel.SUBJECT;
                        }

                        return expressionStringResult;
                    } else if (current === '$') {
                        // -----------------
                        // nested expression
                        // -----------------

                        var expressionDollarResult = nf.nfel.handleDollar(stream, states);

                        // if we've found an embedded expression we need to...
                        if (expressionDollarResult !== null) {
                            // transition back to subject when this expression completes
                            state.context = nf.nfel.SUBJECT;
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
                        state.context = nf.nfel.INVALID;

                        // unexpected...
                        return null;
                    }
                }

                // within a subject
                if (state.context === nf.nfel.SUBJECT || state.context === nf.nfel.SUBJECT_OR_FUNCTION) {
                    // if the next character indicates the start of a function call
                    if (current === ':') {
                        // -------------------------
                        // trigger for function name
                        // -------------------------

                        // consume the colon and update the context
                        stream.next();
                        state.context = nf.nfel.FUNCTION;

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
                        state.context = nf.nfel.INVALID;

                        // unexpected...
                        return null;
                    }
                }

                // within a function
                if (state.context === nf.nfel.FUNCTION) {
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

                        // see if this matches a known function
                        if (nf.nfel.functionRegex.test(functionName)) {
                            // --------
                            // function
                            // --------

                            // change context to arugments
                            state.context = nf.nfel.ARGUMENTS;

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
                        state.context = nf.nfel.INVALID;

                        // unexpected...
                        return null;
                    }
                }

                // within arguments
                if (state.context === nf.nfel.ARGUMENTS) {
                    if (current === '(') {
                        // --------------
                        // argument start
                        // --------------

                        // consume the open paranthesis
                        stream.next();

                        // change context to handle an argument
                        state.context = nf.nfel.ARGUMENT;

                        // start of arguments
                        return null;
                    } else if (current === ')') {
                        // --------------
                        // argument close
                        // --------------

                        // consume the close paranthesis
                        stream.next();

                        // change context to subject for potential chaining
                        state.context = nf.nfel.SUBJECT;

                        // end of arguments
                        return null;
                    } else if (current === ',') {
                        // ------------------
                        // argument separator
                        // ------------------

                        // consume the comma
                        stream.next();

                        // change context back to argument
                        state.context = nf.nfel.ARGUMENT;

                        // argument separator
                        return null;
                    } else {
                        // ----------
                        // unexpected
                        // ----------

                        // consume and move along
                        stream.skipToEnd();
                        state.context = nf.nfel.INVALID;

                        // unexpected...
                        return null;
                    }
                }

                // within a specific argument
                if (state.context === nf.nfel.ARGUMENT) {
                    if (current === '\'' || current === '"') {
                        // --------------
                        // string literal
                        // --------------

                        // handle the string literal
                        var argumentStringResult = nf.nfel.handleStringLiteral(stream, state);

                        // successfully processed a string literal... 
                        if (argumentStringResult !== null) {
                            // change context back to arguments
                            state.context = nf.nfel.ARGUMENTS;
                        }

                        return argumentStringResult;
                    } else if (stream.match(/^[0-9]+/)) {
                        // -------------
                        // integer value
                        // -------------

                        // change context back to arguments
                        state.context = nf.nfel.ARGUMENTS;

                        // style for integers
                        return 'number';
                    } else if (stream.match(/^((true)|(false))/)) {
                        // -------------
                        // boolean value
                        // -------------

                        // change context back to arguments
                        state.context = nf.nfel.ARGUMENTS;

                        // style for boolean (use same as number)
                        return 'number';
                    } else if (current === ')') {
                        // ----------------------------------
                        // argument close (zero arg function)
                        // ----------------------------------

                        // consume the close paranthesis
                        stream.next();

                        // change context to subject for potential chaining
                        state.context = nf.nfel.SUBJECT;

                        // end of arguments
                        return null;
                    } else if (current === '$') {
                        // -----------------
                        // nested expression
                        // -----------------

                        // handle the nested expression
                        var argumentDollarResult = nf.nfel.handleDollar(stream, states);

                        // if we've found an embedded expression we need to...
                        if (argumentDollarResult !== null) {
                            // transition back to arguments when then expression completes
                            state.context = nf.nfel.ARGUMENTS;
                        }

                        return argumentDollarResult;
                    } else {
                        // ----------
                        // unexpected
                        // ----------

                        // consume and move along
                        stream.skipToEnd();
                        state.context = nf.nfel.INVALID;

                        // unexpected...
                        return null;
                    }
                }

                // signifies the potential start of an expression
                if (current === '$') {
                    return nf.nfel.handleDollar(stream, states);
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
            return context === nf.nfel.FUNCTION || context === nf.nfel.ARGUMENTS;
        };

        // whether or not the current context is within a subject-less funciton
        var isSubjectlessFunction = function (context) {
            // within an expression when no characters are found or when the string may be an attribute or a function
            return context === nf.nfel.EXPRESSION || context === nf.nfel.SUBJECT_OR_FUNCTION;
        };

        // only support suggestion in certain cases
        var context = state.context;
        if (!isSubjectlessFunction(context) && !isFunction(context)) {
            return null;
        }

        // lower case for case insensitive comparison
        var value = token.string.toLowerCase();

        // identify potential patterns and increment the start location appropriately
        if (value === '${' || value === ':' || value === '') {
            includeAll = true;
            token.start += value.length;
        }

        function getCompletions(functions) {
            var found = [];

            $.each(functions, function (i, funct) {
                if ($.inArray(funct, found) === -1) {
                    if (includeAll || funct.toLowerCase().indexOf(value) === 0) {
                        found.push(funct);
                    }
                }
            });

            return found;
        }

        // get the suggestions for the current context
        var completionList = getCompletions(isSubjectlessFunction(context) ? nf.nfel.subjectlessFunctions : nf.nfel.functions);
        completionList = completionList.sort(function (a, b) {
            var aLower = a.toLowerCase();
            var bLower = b.toLowerCase();
            return aLower === bLower ? 0 : aLower > bLower ? 1 : -1;
        });

        return {
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
    }
};

(function () {
    nf.nfel.init();
})();