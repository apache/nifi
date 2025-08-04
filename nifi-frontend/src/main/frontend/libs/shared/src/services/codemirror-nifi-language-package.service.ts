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

import { ComponentRef, Injectable, ViewContainerRef } from '@angular/core';
import { ElService } from './el.service';
import { take } from 'rxjs';
import { StreamParser, StringStream } from '@codemirror/language';
import { Completion, CompletionContext, CompletionInfo, CompletionResult } from '@codemirror/autocomplete';
import { ElFunction, ElFunctionTipInput, Parameter, ParameterTipInput } from '../types';
import { ElFunctionTip, ParameterTip } from '../components';

export interface NfLanguageDefinition {
    supportsEl: boolean;
    parameterListing?: Parameter[];
}

export interface NfLanguageConfig {
    streamParser: StreamParser<unknown>;
    getAutocompletions: (viewContainerRef: ViewContainerRef) => (context: CompletionContext) => CompletionResult | null;
}

@Injectable({ providedIn: 'root' })
export class CodemirrorNifiLanguagePackage {
    constructor(private elService: ElService) {
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
                            this.subjectlessFunctions.push(name);
                            subject = 'None';
                        }

                        // Determine if this function supports running with a subject
                        if (subjectSpan) {
                            this.functions.push(name);
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
                        this.functionDetails[name] = {
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
                    this.subjectlessFunctionRegex = new RegExp('^((' + this.subjectlessFunctions.join(')|(') + '))$');
                    this.functionRegex = new RegExp('^((' + this.functions.join(')|(') + '))$');
                }
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
    private functionSupported = false;

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
    private handleStart(startChar: string, context: string, stream: any, states: any): null | string {
        // determine the number of sequential start chars
        let startCharCount = 0;
        stream.eatWhile((ch: string) => {
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

            if (CodemirrorNifiLanguagePackage.PARAMETER === context) {
                // there may be an optional single/double quote
                if (stream.peek() === "'") {
                    // consume the single quote
                    stream.next();

                    // new expression start
                    states.push({
                        context: CodemirrorNifiLanguagePackage.SINGLE_QUOTE_PARAMETER
                    });
                } else if (stream.peek() === '"') {
                    // consume the double quote
                    stream.next();

                    // new expression start
                    states.push({
                        context: CodemirrorNifiLanguagePackage.DOUBLE_QUOTE_PARAMETER
                    });
                } else {
                    // new expression start
                    states.push({
                        context: CodemirrorNifiLanguagePackage.PARAMETER
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

    public getLanguageMode(config: NfLanguageDefinition): NfLanguageConfig {
        const self: CodemirrorNifiLanguagePackage = this;
        let currentState: string = CodemirrorNifiLanguagePackage.INVALID;

        // establish parameter support
        if (config.parameterListing) {
            this.enableParameters();
            this.setParameters(config.parameterListing);
        } else {
            this.disableParameters();
        }

        // establish el support
        if (config.supportsEl) {
            this.enableEl();
        } else {
            this.disableEl();
        }

        /**
         * Handles dollars identifies on the stream.
         *
         * @param {StringStream} stream   The character stream
         * @param {object} state    The current state
         */
        const handleStringLiteral = (stream: any, state: any): string | null => {
            const current: string | null = stream.next();
            let foundTrailing = false;
            let foundEscapeChar = false;

            // locate a closing string delimitor
            const foundStringLiteral: boolean = stream.eatWhile((ch: string) => {
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
            state.context = CodemirrorNifiLanguagePackage.INVALID;
            currentState = CodemirrorNifiLanguagePackage.INVALID;

            stream.skipToEnd();
            return null;
        };

        const handleParameterEnd = (
            stream: any,
            state: any,
            states: any,
            parameterPredicate: () => boolean
        ): string | null => {
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
                    state.context = CodemirrorNifiLanguagePackage.INVALID;
                    currentState = CodemirrorNifiLanguagePackage.INVALID;

                    // unexpected...
                    return null;
                }
            } else {
                // ----------
                // unexpected
                // ----------

                // consume and move along
                stream.skipToEnd();
                state.context = CodemirrorNifiLanguagePackage.INVALID;
                currentState = CodemirrorNifiLanguagePackage.INVALID;

                // unexpected...
                return null;
            }
        };

        // builds the states based off the specified initial value
        const buildStates = (initialStates: any): any => {
            // each state has a context
            const states = initialStates;

            return {
                copy: () => {
                    const copy: any[] = [];
                    for (let i = 0; i < states.length; i++) {
                        copy.push({
                            context: states[i].context
                        });
                    }
                    return copy;
                },
                get: () => {
                    if (states.length === 0) {
                        return {
                            context: null
                        };
                    } else {
                        return states[states.length - 1];
                    }
                },
                push: (state: any) => {
                    currentState = state.context;
                    return states.push(state);
                },
                pop: () => {
                    const state = states.pop();

                    // now get the next last item and save the context if appropriate
                    const current = states.at(-1);
                    if (current) {
                        currentState = current.context;
                    }

                    return state;
                }
            };
        };

        return {
            streamParser: {
                startState: () => {
                    // build states with an empty array
                    return buildStates([]);
                },

                copyState: (state: any) => {
                    // build states with
                    return buildStates(state.copy());
                },

                token: (stream: StringStream, states: any) => {
                    // consume any whitespace
                    if (stream.eatSpace()) {
                        return null;
                    }

                    // if we've hit the end of the line
                    if (stream.eol()) {
                        return null;
                    }

                    // get the current character
                    const current: string | undefined = stream.peek();

                    // if we've hit some comments... will consume the remainder of the line
                    if (current === '#') {
                        // consume the pound
                        stream.next();

                        const afterPound: string | undefined = stream.peek();
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
                    if (state.context === CodemirrorNifiLanguagePackage.INVALID) {
                        stream.skipToEnd();
                        return null;
                    }

                    // within an expression
                    if (state.context === CodemirrorNifiLanguagePackage.EXPRESSION) {
                        const attributeOrSubjectlessFunctionExpression =
                            /^[^'"#${}()[\],:;\/*\\\s\t\r\n0-9][^'"#${}()[\],:;\/*\\\s\t\r\n]*/;

                        // attempt to extract a function name
                        const attributeOrSubjectlessFunctionName: RegExpMatchArray | null = stream.match(
                            attributeOrSubjectlessFunctionExpression,
                            false
                        ) as RegExpMatchArray | null;

                        // if the result returned a match
                        if (
                            attributeOrSubjectlessFunctionName !== null &&
                            attributeOrSubjectlessFunctionName.length === 1
                        ) {
                            // consume the entire token to better support suggest below
                            stream.match(attributeOrSubjectlessFunctionExpression);

                            // if the result returned a match and is followed by a (
                            if (
                                self.functionSupported &&
                                self.subjectlessFunctionRegex.test(attributeOrSubjectlessFunctionName[0]) &&
                                stream.peek() === '('
                            ) {
                                // --------------------
                                // subjectless function
                                // --------------------

                                // context change to function
                                state.context = CodemirrorNifiLanguagePackage.ARGUMENTS;
                                currentState = CodemirrorNifiLanguagePackage.ARGUMENTS;

                                // style for function
                                return 'builtin';
                            } else {
                                // ---------------------
                                // attribute or function
                                // ---------------------

                                // context change to function or subject... not sure yet
                                state.context = CodemirrorNifiLanguagePackage.SUBJECT_OR_FUNCTION;
                                currentState = CodemirrorNifiLanguagePackage.SUBJECT_OR_FUNCTION;

                                // this could be an attribute or a partial function name... style as attribute until we know
                                return 'variable-2';
                            }
                        } else if (current === "'" || current === '"') {
                            // --------------
                            // string literal
                            // --------------

                            // handle the string literal
                            const expressionStringResult: string | null = handleStringLiteral(stream, state);

                            // considered a quoted variable
                            if (expressionStringResult !== null) {
                                // context change to function
                                state.context = CodemirrorNifiLanguagePackage.SUBJECT;
                                currentState = CodemirrorNifiLanguagePackage.SUBJECT;
                            }

                            return expressionStringResult;
                        } else if (current === '$') {
                            // -----------------
                            // nested expression
                            // -----------------

                            const expressionDollarResult: string | null = self.handleStart(
                                '$',
                                CodemirrorNifiLanguagePackage.EXPRESSION,
                                stream,
                                states
                            );

                            // if we've found an embedded expression we need to...
                            if (expressionDollarResult !== null) {
                                // transition back to subject when this expression completes
                                state.context = CodemirrorNifiLanguagePackage.SUBJECT;
                                currentState = CodemirrorNifiLanguagePackage.SUBJECT;
                            }

                            return expressionDollarResult;
                        } else if (current === '#' && self.parametersSupported) {
                            // --------------------------
                            // nested parameter reference
                            // --------------------------

                            // handle the nested parameter reference
                            const parameterReferenceResult = self.handleStart(
                                '#',
                                CodemirrorNifiLanguagePackage.PARAMETER,
                                stream,
                                states
                            );

                            // if we've found an embedded parameter reference we need to...
                            if (parameterReferenceResult !== null) {
                                // transition back to subject when this parameter reference completes
                                state.context = CodemirrorNifiLanguagePackage.SUBJECT;
                                currentState = CodemirrorNifiLanguagePackage.SUBJECT;
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
                            state.context = CodemirrorNifiLanguagePackage.INVALID;
                            currentState = CodemirrorNifiLanguagePackage.INVALID;

                            // unexpected...
                            return null;
                        }
                    }

                    // within a subject
                    if (
                        state.context === CodemirrorNifiLanguagePackage.SUBJECT ||
                        state.context === CodemirrorNifiLanguagePackage.SUBJECT_OR_FUNCTION
                    ) {
                        // if the next character indicates the start of a function call
                        if (current === ':') {
                            // -------------------------
                            // trigger for function name
                            // -------------------------

                            // consume the colon and update the context
                            stream.next();
                            state.context = CodemirrorNifiLanguagePackage.FUNCTION;
                            currentState = CodemirrorNifiLanguagePackage.FUNCTION;

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
                            state.context = CodemirrorNifiLanguagePackage.INVALID;
                            currentState = CodemirrorNifiLanguagePackage.INVALID;

                            // unexpected...
                            return null;
                        }
                    }

                    // within a function
                    if (state.context === CodemirrorNifiLanguagePackage.FUNCTION) {
                        // attempt to extract a function name
                        const functionName: RegExpMatchArray | null = stream.match(
                            /^[a-zA-Z]+/,
                            false
                        ) as RegExpMatchArray | null;

                        // if the result returned a match
                        if (functionName !== null && functionName.length === 1) {
                            // consume the entire token to ensure the whole function
                            // name is matched. this is an issue with functions like
                            // substring and substringAfter since 'substringA' would
                            // match the former and when we really want to autocomplete
                            // against the latter.
                            stream.match(/^[a-zA-Z]+/);

                            // see if this matches a known function and is followed by (
                            if (
                                self.functionSupported &&
                                self.functionRegex.test(functionName[0]) &&
                                stream.peek() === '('
                            ) {
                                // --------
                                // function
                                // --------

                                // change context to arguments
                                state.context = CodemirrorNifiLanguagePackage.ARGUMENTS;
                                currentState = CodemirrorNifiLanguagePackage.ARGUMENTS;

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
                            state.context = CodemirrorNifiLanguagePackage.INVALID;
                            currentState = CodemirrorNifiLanguagePackage.INVALID;

                            // unexpected...
                            return null;
                        }
                    }

                    // within arguments
                    if (state.context === CodemirrorNifiLanguagePackage.ARGUMENTS) {
                        if (current === '(') {
                            // --------------
                            // argument start
                            // --------------

                            // consume the open paranthesis
                            stream.next();

                            // change context to handle an argument
                            state.context = CodemirrorNifiLanguagePackage.ARGUMENT;
                            currentState = CodemirrorNifiLanguagePackage.ARGUMENT;

                            // start of arguments
                            return null;
                        } else if (current === ')') {
                            // --------------
                            // argument close
                            // --------------

                            // consume the close paranthesis
                            stream.next();

                            // change context to subject for potential chaining
                            state.context = CodemirrorNifiLanguagePackage.SUBJECT;
                            currentState = CodemirrorNifiLanguagePackage.SUBJECT;

                            // end of arguments
                            return null;
                        } else if (current === ',') {
                            // ------------------
                            // argument separator
                            // ------------------

                            // consume the comma
                            stream.next();

                            // change context back to argument
                            state.context = CodemirrorNifiLanguagePackage.ARGUMENT;
                            currentState = CodemirrorNifiLanguagePackage.ARGUMENT;

                            // argument separator
                            return null;
                        } else {
                            // ----------
                            // unexpected
                            // ----------

                            // consume and move along
                            stream.skipToEnd();
                            state.context = CodemirrorNifiLanguagePackage.INVALID;
                            currentState = CodemirrorNifiLanguagePackage.INVALID;

                            // unexpected...
                            return null;
                        }
                    }

                    // within a specific argument
                    if (state.context === CodemirrorNifiLanguagePackage.ARGUMENT) {
                        if (current === "'" || current === '"') {
                            // --------------
                            // string literal
                            // --------------

                            // handle the string literal
                            const argumentStringResult: string | null = handleStringLiteral(stream, state);

                            // successfully processed a string literal...
                            if (argumentStringResult !== null) {
                                // change context back to arguments
                                state.context = CodemirrorNifiLanguagePackage.ARGUMENTS;
                                currentState = CodemirrorNifiLanguagePackage.ARGUMENTS;
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
                            state.context = CodemirrorNifiLanguagePackage.ARGUMENTS;
                            currentState = CodemirrorNifiLanguagePackage.ARGUMENTS;

                            // style for decimal (use same as number)
                            return 'number';
                        } else if (stream.match(/^[-\+]?[0-9]+/)) {
                            // -------------
                            // integer value
                            // -------------

                            // change context back to arguments
                            state.context = CodemirrorNifiLanguagePackage.ARGUMENTS;
                            currentState = CodemirrorNifiLanguagePackage.ARGUMENTS;

                            // style for integers
                            return 'number';
                        } else if (stream.match(/^((true)|(false))/)) {
                            // -------------
                            // boolean value
                            // -------------

                            // change context back to arguments
                            state.context = CodemirrorNifiLanguagePackage.ARGUMENTS;
                            currentState = CodemirrorNifiLanguagePackage.ARGUMENTS;

                            // style for boolean (use same as number)
                            return 'number';
                        } else if (current === ')') {
                            // ----------------------------------
                            // argument close (zero arg function)
                            // ----------------------------------

                            // consume the close parenthesis
                            stream.next();

                            // change context to subject for potential chaining
                            state.context = CodemirrorNifiLanguagePackage.SUBJECT;
                            currentState = CodemirrorNifiLanguagePackage.SUBJECT;

                            // end of arguments
                            return null;
                        } else if (current === '$') {
                            // -----------------
                            // nested expression
                            // -----------------

                            // handle the nested expression
                            const argumentDollarResult: string | null = self.handleStart(
                                '$',
                                CodemirrorNifiLanguagePackage.EXPRESSION,
                                stream,
                                states
                            );

                            // if we've found an embedded expression we need to...
                            if (argumentDollarResult !== null) {
                                // transition back to arguments when then expression completes
                                state.context = CodemirrorNifiLanguagePackage.ARGUMENTS;
                                currentState = CodemirrorNifiLanguagePackage.ARGUMENTS;
                            }

                            return argumentDollarResult;
                        } else if (current === '#' && self.parametersSupported) {
                            // --------------------------
                            // nested parameter reference
                            // --------------------------

                            // handle the nested parameter reference
                            const parameterReferenceResult: string | null = self.handleStart(
                                '#',
                                CodemirrorNifiLanguagePackage.PARAMETER,
                                stream,
                                states
                            );

                            // if we've found an embedded parameter reference we need to...
                            if (parameterReferenceResult !== null) {
                                // transition back to arguments when this parameter reference completes
                                state.context = CodemirrorNifiLanguagePackage.ARGUMENTS;
                                currentState = CodemirrorNifiLanguagePackage.ARGUMENTS;
                            }

                            return parameterReferenceResult;
                        } else {
                            // ----------
                            // unexpected
                            // ----------

                            // consume and move along
                            stream.skipToEnd();
                            state.context = CodemirrorNifiLanguagePackage.INVALID;
                            currentState = CodemirrorNifiLanguagePackage.INVALID;

                            // unexpected...
                            return null;
                        }
                    }

                    // within a parameter reference
                    if (
                        state.context === CodemirrorNifiLanguagePackage.PARAMETER ||
                        state.context === CodemirrorNifiLanguagePackage.SINGLE_QUOTE_PARAMETER ||
                        state.context === CodemirrorNifiLanguagePackage.DOUBLE_QUOTE_PARAMETER
                    ) {
                        // attempt to extract a parameter name
                        const parameterName: RegExpMatchArray | null = stream.match(
                            self.parameterKeyRegex,
                            false
                        ) as RegExpMatchArray | null;

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

                        if (state.context === CodemirrorNifiLanguagePackage.SINGLE_QUOTE_PARAMETER) {
                            return handleParameterEnd(stream, state, states, () => current === "'");
                        }

                        if (state.context === CodemirrorNifiLanguagePackage.DOUBLE_QUOTE_PARAMETER) {
                            return handleParameterEnd(stream, state, states, () => current === '"');
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
                            state.context = CodemirrorNifiLanguagePackage.INVALID;
                            currentState = CodemirrorNifiLanguagePackage.INVALID;

                            // unexpected...
                            return null;
                        }
                    }

                    // signifies the potential start of an expression
                    if (current === '$') {
                        return self.handleStart('$', CodemirrorNifiLanguagePackage.EXPRESSION, stream, states);
                    }

                    // signifies the potential start of a parameter reference
                    if (current === '#' && self.parametersSupported) {
                        return self.handleStart('#', CodemirrorNifiLanguagePackage.PARAMETER, stream, states);
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
            },
            getAutocompletions: (viewContainerRef: ViewContainerRef) => {
                return (context: CompletionContext): CompletionResult | null => {
                    if (!context.explicit) {
                        return null;
                    }

                    // Analyze the text around the cursor position to determine context
                    const cursorPos = context.pos;
                    const doc = context.state.doc;
                    const lineInfo = doc.lineAt(cursorPos);
                    const lineText = lineInfo.text;
                    const cursorInLine = cursorPos - lineInfo.from;

                    // Function to determine if cursor is within specific syntax
                    const getCursorContext = (): string => {
                        // Look backward from cursor to find opening syntax
                        let parameterStart = -1;
                        let elStart = -1;
                        let parameterEnd = -1;
                        let elEnd = -1;

                        // Find the most recent parameter or EL function start before cursor
                        for (let i = cursorInLine - 1; i >= 0; i--) {
                            if (lineText.charAt(i) === '{' && i > 0) {
                                if (lineText.charAt(i - 1) === '#' && parameterStart === -1) {
                                    parameterStart = i - 1;
                                }
                                if (lineText.charAt(i - 1) === '$' && elStart === -1) {
                                    elStart = i - 1;
                                }
                            }
                        }

                        // Find the next closing brace after cursor
                        for (let i = cursorInLine; i < lineText.length; i++) {
                            if (lineText.charAt(i) === '}') {
                                if (parameterStart !== -1 && parameterEnd === -1) {
                                    parameterEnd = i;
                                }
                                if (elStart !== -1 && elEnd === -1) {
                                    elEnd = i;
                                }
                                break;
                            }
                        }

                        // Determine which context we're in based on the most recent opening
                        if (parameterStart !== -1 && parameterEnd !== -1) {
                            if (elStart === -1 || parameterStart > elStart) {
                                return CodemirrorNifiLanguagePackage.PARAMETER;
                            }
                        }

                        if (elStart !== -1 && elEnd !== -1) {
                            if (parameterStart === -1 || elStart > parameterStart) {
                                return CodemirrorNifiLanguagePackage.EXPRESSION;
                            }
                        }

                        return CodemirrorNifiLanguagePackage.INVALID;
                    };

                    const detectedContext = getCursorContext();

                    // whether the current context is within a function
                    const isFunction = (context: string): boolean => {
                        // attempting to match a function name or already successfully matched a function name
                        return (
                            context === CodemirrorNifiLanguagePackage.FUNCTION ||
                            context === CodemirrorNifiLanguagePackage.ARGUMENTS ||
                            context === CodemirrorNifiLanguagePackage.EXPRESSION ||
                            context === CodemirrorNifiLanguagePackage.SUBJECT_OR_FUNCTION
                        );
                    };

                    // whether the current context is within a parameter reference
                    const isParameterReference = (context: string): boolean => {
                        // attempting to match a parameter name or already successfully matched a parameter name
                        return (
                            context === CodemirrorNifiLanguagePackage.PARAMETER ||
                            context === CodemirrorNifiLanguagePackage.SINGLE_QUOTE_PARAMETER ||
                            context === CodemirrorNifiLanguagePackage.DOUBLE_QUOTE_PARAMETER
                        );
                    };

                    let options: string[] = [];
                    let useFunctionDetails = true;

                    // determine the options based on the detected context
                    console.log('detectedContext', detectedContext);

                    if (isParameterReference(detectedContext) && self.parametersSupported) {
                        options = self.parameters;
                        useFunctionDetails = false;
                    } else if (isFunction(detectedContext) && self.functionSupported) {
                        // Prefer subjectless functions in expressions, but include all functions
                        options = [...self.subjectlessFunctions, ...self.functions];
                    }

                    // if there are no options, return null
                    if (options.length === 0) {
                        return null;
                    }

                    const word = context.matchBefore(/\w*/);
                    if (!word) {
                        return null;
                    }

                    const tokenValue = word.text.toLowerCase().trim();

                    const completions: Completion[] = options
                        .filter((opt) => tokenValue === '' || opt.toLowerCase().startsWith(tokenValue))
                        .map((opt) => ({
                            label: opt,
                            type: useFunctionDetails ? 'function' : 'variable',
                            info: (): CompletionInfo => {
                                // create and configure the tooltip
                                let componentRef: ComponentRef<any>;
                                if (useFunctionDetails) {
                                    componentRef = viewContainerRef.createComponent(ElFunctionTip);
                                } else {
                                    componentRef = viewContainerRef.createComponent(ParameterTip);
                                }

                                if (useFunctionDetails) {
                                    const data: ElFunctionTipInput = {
                                        elFunction: self.functionDetails[opt]
                                    };
                                    componentRef.setInput('data', data);
                                } else {
                                    const data: ParameterTipInput = {
                                        parameter: self.parameterDetails[opt]
                                    };
                                    componentRef.setInput('data', data);
                                }

                                componentRef.changeDetectorRef.detectChanges();

                                return {
                                    dom: componentRef.location.nativeElement,
                                    destroy: () => {
                                        viewContainerRef.clear();
                                    }
                                };
                            }
                        }));

                    return {
                        from: word.from,
                        options: completions
                    };
                };
            }
        };
    }

    private enableEl(): void {
        this.functionSupported = true;
    }

    private disableEl(): void {
        this.functionSupported = false;
    }

    /**
     * Enables parameter referencing.
     */
    private enableParameters(): void {
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
    private setParameters(parameterListing: Parameter[]): void {
        parameterListing.forEach((parameter) => {
            this.parameters.push(parameter.name);
            this.parameterDetails[parameter.name] = parameter;
        });

        this.parameterRegex = new RegExp('^((' + this.parameters.join(')|(') + '))$');
    }

    /**
     * Disables parameter referencing.
     */
    private disableParameters(): void {
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
        return 'nf';
    }
}
