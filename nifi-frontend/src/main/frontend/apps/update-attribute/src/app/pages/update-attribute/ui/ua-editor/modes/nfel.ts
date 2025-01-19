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

import { ComponentRef, Injectable, Renderer2, ViewContainerRef } from '@angular/core';
import * as CodeMirror from 'codemirror';
import { Editor, Hint, Hints, StringStream } from 'codemirror';
import { ElService } from './el.service';
import { take } from 'rxjs';
import { NiFiCommon, ElFunction, Parameter, ParameterTip, ElFunctionTip } from '@nifi/shared';

export interface NfElHint extends Hint {
    parameterDetails?: Parameter;
    functionDetails?: ElFunction;
}

@Injectable({ providedIn: 'root' })
export class NfEl {
    private viewContainerRef: ViewContainerRef | undefined;
    private renderer: Renderer2 | undefined;

    constructor(
        private elService: ElService,
        private nifiCommon: NiFiCommon
    ) {
        const self: NfEl = this;

        this.elService
            .getElGuide()
            .pipe(take(1))
            .subscribe({
                next: (response) => {
                    const document: Document = new DOMParser().parseFromString(response, 'text/html');
                    const elFunctions: Element[] = Array.from(document.querySelectorAll('div.function'));

                    elFunctions.forEach((elFunction) => {
                        const name: string = elFunction.querySelector('h3')?.textContent ?? '';
                        const description: string = elFunction.querySelector('span.description')?.textContent ?? '';
                        const returnType: string = elFunction.querySelector('span.returnType')?.textContent ?? '';

                        let subject = '';
                        const subjectSpan: Element | null = elFunction.querySelector('span.subject');
                        const subjectless: Element | null = elFunction.querySelector('span.subjectless');

                        // Determine if this function supports running subjectless
                        if (subjectless) {
                            self.subjectlessFunctions.push(name);
                            subject = 'None';
                        }

                        // Determine if this function supports running with a subject
                        if (subjectSpan) {
                            self.functions.push(name);
                            subject = subjectSpan.textContent ?? '';
                        }

                        // find the arguments
                        const args: { [key: string]: string } = {};
                        const argElements: Element[] = Array.from(elFunction.querySelectorAll('span.argName'));
                        argElements.forEach((argElement) => {
                            const argName: string = argElement.textContent ?? '';
                            const next: Element | null = argElement.nextElementSibling;
                            if (next && next.matches('span.argDesc')) {
                                args[argName] = next.textContent ?? '';
                            }
                        });

                        // record the function details
                        self.functionDetails[name] = {
                            name: name,
                            description: description,
                            args: args,
                            subject: subject,
                            returnType: returnType
                        };
                    });
                },
                complete: () => {
                    // build the regex for all functions discovered
                    self.subjectlessFunctionRegex = new RegExp('^((' + self.subjectlessFunctions.join(')|(') + '))$');
                    self.functionRegex = new RegExp('^((' + self.functions.join(')|(') + '))$');
                }
            });

        // register this custom mode
        CodeMirror.defineMode(this.getLanguageId(), function () {
            // builds the states based off the specified initial value
            const buildStates = function (initialStates: any): any {
                // each state has a context
                const states = initialStates;

                return {
                    copy: function () {
                        const copy: any[] = [];
                        for (let i = 0; i < states.length; i++) {
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
                    push: function (state: any) {
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

                copyState: function (state: any) {
                    // build states with
                    return buildStates(state.copy());
                },

                token: function (stream: StringStream, states: any) {
                    // consume any whitespace
                    if (stream.eatSpace()) {
                        return null;
                    }

                    // if we've hit the end of the line
                    if (stream.eol()) {
                        return null;
                    }

                    // get the current character
                    const current: string | null = stream.peek();

                    // if we've hit some comments... will consume the remainder of the line
                    if (current === '#') {
                        // consume the pound
                        stream.next();

                        const afterPound: string | null = stream.peek();
                        if (afterPound !== '{') {
                            stream.skipToEnd();
                            return 'comment';
                        } else {
                            // unconsume the pound
                            stream.backUp(1);
                        }
                    }

                    // get the current state
                    const state: any = states.get();

                    // the current input is invalid
                    if (state.context === NfEl.INVALID) {
                        stream.skipToEnd();
                        return null;
                    }

                    // within an expression
                    if (state.context === NfEl.EXPRESSION) {
                        const attributeOrSubjectlessFunctionExpression =
                            /^[^'"#${}()[\],:;\/*\\\s\t\r\n0-9][^'"#${}()[\],:;\/*\\\s\t\r\n]*/;

                        // attempt to extract a function name
                        const attributeOrSubjectlessFunctionName: string[] = stream.match(
                            attributeOrSubjectlessFunctionExpression,
                            false
                        );

                        // if the result returned a match
                        if (
                            attributeOrSubjectlessFunctionName !== null &&
                            attributeOrSubjectlessFunctionName.length === 1
                        ) {
                            // consume the entire token to better support suggest below
                            stream.match(attributeOrSubjectlessFunctionExpression);

                            // if the result returned a match and is followed by a (
                            if (
                                self.subjectlessFunctionRegex.test(attributeOrSubjectlessFunctionName[0]) &&
                                stream.peek() === '('
                            ) {
                                // --------------------
                                // subjectless function
                                // --------------------

                                // context change to function
                                state.context = NfEl.ARGUMENTS;

                                // style for function
                                return 'builtin';
                            } else {
                                // ---------------------
                                // attribute or function
                                // ---------------------

                                // context change to function or subject... not sure yet
                                state.context = NfEl.SUBJECT_OR_FUNCTION;

                                // this could be an attribute or a partial function name... style as attribute until we know
                                return 'variable-2';
                            }
                        } else if (current === "'" || current === '"') {
                            // --------------
                            // string literal
                            // --------------

                            // handle the string literal
                            const expressionStringResult: string | null = self.handleStringLiteral(stream, state);

                            // considered a quoted variable
                            if (expressionStringResult !== null) {
                                // context change to function
                                state.context = NfEl.SUBJECT;
                            }

                            return expressionStringResult;
                        } else if (current === '$') {
                            // -----------------
                            // nested expression
                            // -----------------

                            const expressionDollarResult: string | null = self.handleStart(
                                '$',
                                NfEl.EXPRESSION,
                                stream,
                                states
                            );

                            // if we've found an embedded expression we need to...
                            if (expressionDollarResult !== null) {
                                // transition back to subject when this expression completes
                                state.context = NfEl.SUBJECT;
                            }

                            return expressionDollarResult;
                        } else if (current === '#' && self.parametersSupported) {
                            // --------------------------
                            // nested parameter reference
                            // --------------------------

                            // handle the nested parameter reference
                            const parameterReferenceResult = self.handleStart('#', NfEl.PARAMETER, stream, states);

                            // if we've found an embedded parameter reference we need to...
                            if (parameterReferenceResult !== null) {
                                // transition back to subject when this parameter reference completes
                                state.context = NfEl.SUBJECT;
                            }

                            return parameterReferenceResult;
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
                            state.context = NfEl.INVALID;

                            // unexpected...
                            return null;
                        }
                    }

                    // within a subject
                    if (state.context === NfEl.SUBJECT || state.context === NfEl.SUBJECT_OR_FUNCTION) {
                        // if the next character indicates the start of a function call
                        if (current === ':') {
                            // -------------------------
                            // trigger for function name
                            // -------------------------

                            // consume the colon and update the context
                            stream.next();
                            state.context = NfEl.FUNCTION;

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
                            state.context = NfEl.INVALID;

                            // unexpected...
                            return null;
                        }
                    }

                    // within a function
                    if (state.context === NfEl.FUNCTION) {
                        // attempt to extract a function name
                        const functionName = stream.match(/^[a-zA-Z]+/, false);

                        // if the result returned a match
                        if (functionName !== null && functionName.length === 1) {
                            // consume the entire token to ensure the whole function
                            // name is matched. this is an issue with functions like
                            // substring and substringAfter since 'substringA' would
                            // match the former and when we really want to autocomplete
                            // against the latter.
                            stream.match(/^[a-zA-Z]+/);

                            // see if this matches a known function and is followed by (
                            if (self.functionRegex.test(functionName[0]) && stream.peek() === '(') {
                                // --------
                                // function
                                // --------

                                // change context to arguments
                                state.context = NfEl.ARGUMENTS;

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
                            state.context = NfEl.INVALID;

                            // unexpected...
                            return null;
                        }
                    }

                    // within arguments
                    if (state.context === NfEl.ARGUMENTS) {
                        if (current === '(') {
                            // --------------
                            // argument start
                            // --------------

                            // consume the open paranthesis
                            stream.next();

                            // change context to handle an argument
                            state.context = NfEl.ARGUMENT;

                            // start of arguments
                            return null;
                        } else if (current === ')') {
                            // --------------
                            // argument close
                            // --------------

                            // consume the close paranthesis
                            stream.next();

                            // change context to subject for potential chaining
                            state.context = NfEl.SUBJECT;

                            // end of arguments
                            return null;
                        } else if (current === ',') {
                            // ------------------
                            // argument separator
                            // ------------------

                            // consume the comma
                            stream.next();

                            // change context back to argument
                            state.context = NfEl.ARGUMENT;

                            // argument separator
                            return null;
                        } else {
                            // ----------
                            // unexpected
                            // ----------

                            // consume and move along
                            stream.skipToEnd();
                            state.context = NfEl.INVALID;

                            // unexpected...
                            return null;
                        }
                    }

                    // within a specific argument
                    if (state.context === NfEl.ARGUMENT) {
                        if (current === "'" || current === '"') {
                            // --------------
                            // string literal
                            // --------------

                            // handle the string literal
                            const argumentStringResult: string | null = self.handleStringLiteral(stream, state);

                            // successfully processed a string literal...
                            if (argumentStringResult !== null) {
                                // change context back to arguments
                                state.context = NfEl.ARGUMENTS;
                            }

                            return argumentStringResult;
                        } else if (
                            stream.match(
                                /^[-\+]?((([0-9]+\.[0-9]*)([eE][+-]?([0-9])+)?)|((\.[0-9]+)([eE][+-]?([0-9])+)?)|(([0-9]+)([eE][+-]?([0-9])+)))/
                            )
                        ) {
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
                            state.context = NfEl.ARGUMENTS;

                            // style for decimal (use same as number)
                            return 'number';
                        } else if (stream.match(/^[-\+]?[0-9]+/)) {
                            // -------------
                            // integer value
                            // -------------

                            // change context back to arguments
                            state.context = NfEl.ARGUMENTS;

                            // style for integers
                            return 'number';
                        } else if (stream.match(/^((true)|(false))/)) {
                            // -------------
                            // boolean value
                            // -------------

                            // change context back to arguments
                            state.context = NfEl.ARGUMENTS;

                            // style for boolean (use same as number)
                            return 'number';
                        } else if (current === ')') {
                            // ----------------------------------
                            // argument close (zero arg function)
                            // ----------------------------------

                            // consume the close parenthesis
                            stream.next();

                            // change context to subject for potential chaining
                            state.context = NfEl.SUBJECT;

                            // end of arguments
                            return null;
                        } else if (current === '$') {
                            // -----------------
                            // nested expression
                            // -----------------

                            // handle the nested expression
                            const argumentDollarResult: string | null = self.handleStart(
                                '$',
                                NfEl.EXPRESSION,
                                stream,
                                states
                            );

                            // if we've found an embedded expression we need to...
                            if (argumentDollarResult !== null) {
                                // transition back to arguments when then expression completes
                                state.context = NfEl.ARGUMENTS;
                            }

                            return argumentDollarResult;
                        } else if (current === '#' && self.parametersSupported) {
                            // --------------------------
                            // nested parameter reference
                            // --------------------------

                            // handle the nested parameter reference
                            const parameterReferenceResult: string | null = self.handleStart(
                                '#',
                                NfEl.PARAMETER,
                                stream,
                                states
                            );

                            // if we've found an embedded parameter reference we need to...
                            if (parameterReferenceResult !== null) {
                                // transition back to arguments when this parameter reference completes
                                state.context = NfEl.ARGUMENTS;
                            }

                            return parameterReferenceResult;
                        } else {
                            // ----------
                            // unexpected
                            // ----------

                            // consume and move along
                            stream.skipToEnd();
                            state.context = NfEl.INVALID;

                            // unexpected...
                            return null;
                        }
                    }

                    // within a parameter reference
                    if (
                        state.context === NfEl.PARAMETER ||
                        state.context === NfEl.SINGLE_QUOTE_PARAMETER ||
                        state.context === NfEl.DOUBLE_QUOTE_PARAMETER
                    ) {
                        // attempt to extract a parameter name
                        const parameterName: string[] = stream.match(self.parameterKeyRegex, false);

                        // if the result returned a match
                        if (parameterName !== null && parameterName.length === 1) {
                            // consume the entire token to ensure the whole function
                            // name is matched. this is an issue with functions like
                            // substring and substringAfter since 'substringA' would
                            // match the former and when we really want to autocomplete
                            // against the latter.
                            stream.match(self.parameterKeyRegex);

                            // see if this matches a known function and is followed by (
                            if (self.parameterRegex.test(parameterName[0])) {
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

                        if (state.context === NfEl.SINGLE_QUOTE_PARAMETER) {
                            return self.handleParameterEnd(stream, state, states, () => current === "'");
                        }

                        if (state.context === NfEl.DOUBLE_QUOTE_PARAMETER) {
                            return self.handleParameterEnd(stream, state, states, () => current === '"');
                        }

                        if (current === '}') {
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
                            state.context = NfEl.INVALID;

                            // unexpected...
                            return null;
                        }
                    }

                    // signifies the potential start of an expression
                    if (current === '$') {
                        return self.handleStart('$', NfEl.EXPRESSION, stream, states);
                    }

                    // signifies the potential start of a parameter reference
                    if (current === '#' && self.parametersSupported) {
                        return self.handleStart('#', NfEl.PARAMETER, stream, states);
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
        });
    }

    private parameterKeyRegex = /^[a-zA-Z0-9-_. ]+/;

    private parameters: string[] = [];
    private parameterRegex = new RegExp('^$');

    private parameterDetails: { [key: string]: Parameter } = {};
    private parametersSupported = false;

    private subjectlessFunctions: string[] = [];
    private functions: string[] = [];

    private subjectlessFunctionRegex = new RegExp('^$');
    private functionRegex = new RegExp('^$');

    private functionDetails: { [key: string]: ElFunction } = {};

    // valid context states
    private static readonly SUBJECT: string = 'subject';
    private static readonly FUNCTION: string = 'function';
    private static readonly SUBJECT_OR_FUNCTION: string = 'subject-or-function';
    private static readonly EXPRESSION: string = 'expression';
    private static readonly ARGUMENTS: string = 'arguments';
    private static readonly ARGUMENT: string = 'argument';
    private static readonly PARAMETER: string = 'parameter';
    private static readonly SINGLE_QUOTE_PARAMETER: string = 'single-quote-parameter';
    private static readonly DOUBLE_QUOTE_PARAMETER: string = 'double-quote-parameter';
    private static readonly INVALID: string = 'invalid';

    /**
     * Handles dollars identifies on the stream.
     *
     * @param {string} startChar    The start character
     * @param {string} context      The context to transition to if we match on the specified start character
     * @param {object} stream       The character stream
     * @param {object} states       The states
     */
    private handleStart(startChar: string, context: string, stream: StringStream, states: any): null | string {
        // determine the number of sequential start chars
        let startCharCount = 0;
        stream.eatWhile(function (ch: string) {
            if (ch === startChar) {
                startCharCount++;
                return true;
            }
            return false;
        });

        // if there is an even number of consecutive start chars this expression is escaped
        if (startCharCount % 2 === 0) {
            // do not style an escaped expression
            return null;
        }

        // if there was an odd number of consecutive start chars and there was more than 1
        if (startCharCount > 1) {
            // back up one char so we can process the start sequence next iteration
            stream.backUp(1);

            // do not style the preceding start chars
            return null;
        }

        // if the next character isn't the start of an expression
        if (stream.peek() === '{') {
            // consume the open curly
            stream.next();

            if (NfEl.PARAMETER === context) {
                // there may be an optional single/double quote
                if (stream.peek() === "'") {
                    // consume the single quote
                    stream.next();

                    // new expression start
                    states.push({
                        context: NfEl.SINGLE_QUOTE_PARAMETER
                    });
                } else if (stream.peek() === '"') {
                    // consume the double quote
                    stream.next();

                    // new expression start
                    states.push({
                        context: NfEl.DOUBLE_QUOTE_PARAMETER
                    });
                } else {
                    // new expression start
                    states.push({
                        context: NfEl.PARAMETER
                    });
                }
            } else {
                // new expression start
                states.push({
                    context: context
                });
            }

            // consume any addition whitespace
            stream.eatSpace();

            return 'bracket';
        }
        // not a valid start sequence
        return null;
    }

    /**
     * Handles dollars identifies on the stream.
     *
     * @param {StringStream} stream   The character stream
     * @param {object} state    The current state
     */
    private handleStringLiteral(stream: StringStream, state: any): string | null {
        const current: string | null = stream.next();
        let foundTrailing = false;
        let foundEscapeChar = false;

        // locate a closing string delimitor
        const foundStringLiteral: boolean = stream.eatWhile(function (ch: string) {
            // we've just found the trailing delimitor, stop
            if (foundTrailing) {
                return false;
            }

            // if this is the trailing delimitor, only consume
            // if we did not see the escape character on the
            // previous iteration
            if (ch === current) {
                foundTrailing = !foundEscapeChar;
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
        state.context = NfEl.INVALID;
        stream.skipToEnd();
        return null;
    }

    private handleParameterEnd(
        stream: StringStream,
        state: any,
        states: any,
        parameterPredicate: () => boolean
    ): string | null {
        if (parameterPredicate()) {
            // consume the single/double quote
            stream.next();

            // verify the next character closes the parameter reference
            if (stream.peek() === '}') {
                // -----------------
                // end of expression
                // -----------------

                // consume the close
                stream.next();

                // signifies the end of a parameter reference
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
                state.context = NfEl.INVALID;

                // unexpected...
                return null;
            }
        } else {
            // ----------
            // unexpected
            // ----------

            // consume and move along
            stream.skipToEnd();
            state.context = NfEl.INVALID;

            // unexpected...
            return null;
        }
    }

    public setViewContainerRef(viewContainerRef: ViewContainerRef, renderer: Renderer2): void {
        this.viewContainerRef = viewContainerRef;
        this.renderer = renderer;
    }

    public configureAutocomplete(): void {
        const self: NfEl = this;

        CodeMirror.commands.autocomplete = function (cm: Editor) {
            CodeMirror.showHint(cm, function (editor: Editor) {
                // Find the token at the cursor
                const cursor: CodeMirror.Position = editor.getCursor();
                const token: CodeMirror.Token = editor.getTokenAt(cursor);
                let includeAll = false;
                const state = token.state.get();

                // whether the current context is within a function
                const isFunction = function (context: string): boolean {
                    // attempting to match a function name or already successfully matched a function name
                    return context === NfEl.FUNCTION || context === NfEl.ARGUMENTS;
                };

                // whether the current context is within a subject-less funciton
                const isSubjectlessFunction = function (context: string): boolean {
                    // within an expression when no characters are found or when the string may be an attribute or a function
                    return context === NfEl.EXPRESSION || context === NfEl.SUBJECT_OR_FUNCTION;
                };

                // whether the current context is within a parameter reference
                const isParameterReference = function (context: string): boolean {
                    // attempting to match a function name or already successfully matched a function name
                    return (
                        context === NfEl.PARAMETER ||
                        context === NfEl.SINGLE_QUOTE_PARAMETER ||
                        context === NfEl.DOUBLE_QUOTE_PARAMETER
                    );
                };

                // only support suggestion in certain cases
                const context: string = state.context;
                if (!isSubjectlessFunction(context) && !isFunction(context) && !isParameterReference(context)) {
                    return null;
                }

                // lower case for case-insensitive comparison
                const value: string = token.string.toLowerCase();

                // trim to ignore extra whitespace
                const trimmed: string = value.trim();

                // identify potential patterns and increment the start location appropriately
                if (trimmed === '${' || trimmed === ':' || trimmed === '#{' || trimmed === "#{'" || trimmed === '#{"') {
                    includeAll = true;
                    token.start += value.length;
                }

                let options: string[] = self.functions;
                let useFunctionDetails = true;
                if (isSubjectlessFunction(context)) {
                    options = self.subjectlessFunctions;
                } else if (isParameterReference(context)) {
                    options = self.parameters;
                    useFunctionDetails = false;
                }

                const getCompletions = function (options: string[]): Hint[] {
                    const found: NfElHint[] = [];

                    options.forEach((opt: string) => {
                        if (!found.some((item) => item.text == opt)) {
                            if (includeAll || opt.toLowerCase().indexOf(value) === 0) {
                                if (useFunctionDetails) {
                                    found.push({
                                        text: opt,
                                        functionDetails: self.functionDetails[opt],
                                        render: function (element: HTMLElement, hints: Hints, data: any): void {
                                            element.textContent = data.text;
                                        }
                                    });
                                } else {
                                    found.push({
                                        text: opt,
                                        parameterDetails: self.parameterDetails[opt],
                                        render: function (element: HTMLElement, hints: Hints, data: any): void {
                                            element.textContent = data.text;
                                        }
                                    });
                                }
                            }
                        }
                    });

                    return found;
                };

                // get the suggestions for the current context
                let completionList: Hint[] = getCompletions(options);
                completionList = completionList.sort((a, b) => {
                    return self.nifiCommon.compareString(a.text, b.text);
                });

                const completions: Hints = {
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

                CodeMirror.on(completions, 'select', function (completion: any, element: any) {
                    if (self.viewContainerRef && self.renderer) {
                        const { x, y, width, height } = element.getBoundingClientRect();

                        // clear any existing tooltips
                        self.viewContainerRef.clear();

                        // create and configure the tooltip
                        let componentRef: ComponentRef<any> | undefined;
                        if (completion.parameterDetails) {
                            componentRef = self.viewContainerRef.createComponent(ParameterTip);
                            componentRef.setInput('bottom', window.innerHeight - (y + height));
                            componentRef.setInput('left', x + width + 20);
                            componentRef.setInput('data', {
                                parameter: completion.parameterDetails
                            });
                        } else if (completion.functionDetails) {
                            componentRef = self.viewContainerRef.createComponent(ElFunctionTip);
                            componentRef.setInput('bottom', window.innerHeight - (y + height));
                            componentRef.setInput('left', x + width + 20);
                            componentRef.setInput('data', {
                                elFunction: completion.functionDetails
                            });
                        }

                        if (componentRef) {
                            // move the newly created component, so it's co-located with the completion list... this
                            // is helpful when it comes to ensuring the tooltip is cleaned up appropriately
                            self.renderer.appendChild(element.parentElement, componentRef.location.nativeElement);
                        }
                    }
                });
                CodeMirror.on(completions, 'close', function () {
                    self.viewContainerRef?.clear();
                });

                return completions;
            });
        };
    }

    /**
     * Enables parameter referencing.
     */
    public enableParameters(): void {
        this.parameters = [];
        this.parameterRegex = new RegExp('^$');
        this.parameterDetails = {};

        this.parametersSupported = true;
    }

    /**
     * Sets the available parameters.
     *
     * @param parameterListing
     */
    public setParameters(parameterListing: Parameter[]): void {
        parameterListing.forEach((parameter) => {
            this.parameters.push(parameter.name);
            this.parameterDetails[parameter.name] = parameter;
        });

        this.parameterRegex = new RegExp('^((' + this.parameters.join(')|(') + '))$');
    }

    /**
     * Disables parameter referencing.
     */
    public disableParameters(): void {
        this.parameters = [];
        this.parameterRegex = new RegExp('^$');
        this.parameterDetails = {};

        this.parametersSupported = false;
    }

    /**
     * Returns the language id.
     *
     * @returns {string} language id
     */
    public getLanguageId(): string {
        return 'nfel';
    }

    /**
     * Returns whether this editor mode supports EL.
     *
     * @returns {boolean}   Whether the editor supports EL
     */
    public supportsEl(): boolean {
        return true;
    }

    /**
     * Returns whether this editor mode supports parameter reference.
     *
     * @returns {boolean}   Whether the editor supports parameter reference
     */
    public supportsParameterReference(): boolean {
        return this.parametersSupported;
    }
}
