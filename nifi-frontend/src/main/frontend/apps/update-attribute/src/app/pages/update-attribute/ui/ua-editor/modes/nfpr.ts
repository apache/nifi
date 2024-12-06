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
import { Editor, Hint, Hints, StringStream } from 'codemirror';
import * as CodeMirror from 'codemirror';
import { NiFiCommon, Parameter, ParameterTip } from '@nifi/shared';

@Injectable({ providedIn: 'root' })
export class NfPr {
    private viewContainerRef: ViewContainerRef | undefined;
    private renderer: Renderer2 | undefined;

    constructor(private nifiCommon: NiFiCommon) {
        const self: NfPr = this;

        CodeMirror.defineMode(this.getLanguageId(), function (): any {
            // builds the states based off the specified initial value
            const buildStates = function (initialStates: any) {
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

                    // get the current state
                    const state = states.get();

                    // the current input is invalid
                    if (state.context === NfPr.INVALID) {
                        stream.skipToEnd();
                        return null;
                    }

                    // within a parameter reference
                    if (
                        state.context === NfPr.PARAMETER ||
                        state.context === NfPr.SINGLE_QUOTE_PARAMETER ||
                        state.context === NfPr.DOUBLE_QUOTE_PARAMETER
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

                        if (state.context === NfPr.SINGLE_QUOTE_PARAMETER) {
                            return self.handleParameterEnd(stream, state, states, () => current === "'");
                        }

                        if (state.context === NfPr.DOUBLE_QUOTE_PARAMETER) {
                            return self.handleParameterEnd(stream, state, states, () => current === '"');
                        }

                        if (current === '}') {
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
                            state.context = NfPr.INVALID;

                            // unexpected...
                            return null;
                        }
                    }

                    // signifies the potential start of an expression
                    if (current === '#' && self.parametersSupported) {
                        return self.handlePound(stream, states);
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

    // valid context states
    private static readonly PARAMETER: string = 'parameter';
    private static readonly SINGLE_QUOTE_PARAMETER: string = 'single-quote-parameter';
    private static readonly DOUBLE_QUOTE_PARAMETER: string = 'double-quote-parameter';
    private static readonly INVALID: string = 'invalid';

    /**
     * Handles pound identifies on the stream.
     *
     * @param {object} stream   The character stream
     * @param {object} states    The states
     */
    private handlePound(stream: StringStream, states: any): string | null {
        // determine the number of sequential pounds
        let poundCount = 0;
        stream.eatWhile(function (ch: string): boolean {
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

            // there may be an optional single/double quote
            if (stream.peek() === "'") {
                // consume the single quote
                stream.next();

                // new expression start
                states.push({
                    context: NfPr.SINGLE_QUOTE_PARAMETER
                });
            } else if (stream.peek() === '"') {
                // consume the double quote
                stream.next();

                // new expression start
                states.push({
                    context: NfPr.DOUBLE_QUOTE_PARAMETER
                });
            } else {
                // new expression start
                states.push({
                    context: NfPr.PARAMETER
                });
            }

            // consume any addition whitespace
            stream.eatSpace();

            return 'bracket';
        } else {
            // not a valid start sequence
            return null;
        }
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
                state.context = NfPr.INVALID;

                // unexpected...
                return null;
            }
        } else {
            // ----------
            // unexpected
            // ----------

            // consume and move along
            stream.skipToEnd();
            state.context = NfPr.INVALID;

            // unexpected...
            return null;
        }
    }

    public setViewContainerRef(viewContainerRef: ViewContainerRef, renderer: Renderer2): void {
        this.viewContainerRef = viewContainerRef;
        this.renderer = renderer;
    }

    public configureAutocomplete(): void {
        const self: NfPr = this;

        CodeMirror.commands.autocomplete = function (cm: Editor) {
            CodeMirror.showHint(cm, function (editor: Editor) {
                // Find the token at the cursor
                const cursor: CodeMirror.Position = editor.getCursor();
                const token: CodeMirror.Token = editor.getTokenAt(cursor);
                let includeAll = false;
                const state = token.state.get();

                // whether the current context is within a function
                const isParameterContext = function (context: string) {
                    // attempting to match a function name or already successfully matched a function name
                    return (
                        context === NfPr.PARAMETER ||
                        context === NfPr.SINGLE_QUOTE_PARAMETER ||
                        context === NfPr.DOUBLE_QUOTE_PARAMETER
                    );
                };

                // only support suggestion in certain cases
                const context = state.context;
                if (!isParameterContext(context)) {
                    return null;
                }

                // lower case for case-insensitive comparison
                const value: string = token.string.toLowerCase();

                // trim to ignore extra whitespace
                const trimmed: string = value.trim();

                // identify potential patterns and increment the start location appropriately
                if (trimmed === '#{' || trimmed === "#{'" || trimmed === '#{"') {
                    includeAll = true;
                    token.start += value.length;
                }

                const getCompletions = function (parameterList: string[]) {
                    const found: Hint[] = [];

                    parameterList.forEach((parameter: string) => {
                        if (!found.some((item) => item.text == parameter)) {
                            if (includeAll || parameter.toLowerCase().indexOf(value) === 0) {
                                found.push({
                                    text: parameter,
                                    render: function (element: HTMLElement, hints: Hints, data: any): void {
                                        element.textContent = data.text;
                                    }
                                });
                            }
                        }
                    });

                    return found;
                };

                // get the suggestions for the current context
                let completionList: any[] = getCompletions(self.parameters);
                completionList = completionList.sort((a, b) => {
                    return self.nifiCommon.compareString(a.text, b.text);
                });

                const completions: any = {
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
                        const componentRef: ComponentRef<ParameterTip> =
                            self.viewContainerRef.createComponent(ParameterTip);
                        componentRef.setInput('bottom', window.innerHeight - (y + height));
                        componentRef.setInput('left', x + width + 20);
                        componentRef.setInput('data', {
                            parameter: self.parameterDetails[completion.text]
                        });

                        // move the newly created component, so it's co-located with the completion list... this
                        // is helpful when it comes to ensuring the tooltip is cleaned up appropriately
                        self.renderer.appendChild(element.parentElement, componentRef.location.nativeElement);
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
        parameterListing.forEach((parameter: any) => {
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
        return 'nfpr';
    }

    /**
     * Returns whether this editor mode supports EL.
     *
     * @returns {boolean}   Whether the editor supports EL
     */
    public supportsEl(): boolean {
        return false;
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
