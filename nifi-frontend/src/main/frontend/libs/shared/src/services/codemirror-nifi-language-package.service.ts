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
import { ElFunctionTip } from '../components/tooltips/el-function-tip/el-function-tip.component';
import { ParameterTip } from '../components/tooltips/parameter-tip/parameter-tip.component';

export interface NfLanguageDefinition {
    supportsEl: boolean;
    parameterListing?: Parameter[];
}

export interface NfLanguageConfig {
    streamParser: StreamParser<unknown>;
    getAutocompletions: (viewContainerRef: ViewContainerRef) => (context: CompletionContext) => CompletionResult | null;
}

/**
 * Service providing CodeMirror language support for NiFi Expression Language and Parameter references.
 * Handles syntax highlighting, parsing, and autocompletion for both EL functions and parameters.
 */
@Injectable({ providedIn: 'root' })
export class CodemirrorNifiLanguagePackage {
    // Parser context constants
    private static readonly CONTEXT_SUBJECT: string = 'subject';
    private static readonly CONTEXT_FUNCTION: string = 'function';
    private static readonly CONTEXT_SUBJECT_OR_FUNCTION: string = 'subject-or-function';
    private static readonly CONTEXT_EXPRESSION: string = 'expression';
    private static readonly CONTEXT_ARGUMENTS: string = 'arguments';
    private static readonly CONTEXT_ARGUMENT: string = 'argument';
    private static readonly CONTEXT_PARAMETER: string = 'parameter';
    private static readonly CONTEXT_SINGLE_QUOTE_PARAMETER: string = 'single-quote-parameter';
    private static readonly CONTEXT_DOUBLE_QUOTE_PARAMETER: string = 'double-quote-parameter';
    private static readonly CONTEXT_INVALID: string = 'invalid';

    // Regular expressions for parsing
    private static readonly PARAMETER_KEY_REGEX = /^[a-zA-Z0-9-_. ]+/;
    private static readonly ATTRIBUTE_OR_FUNCTION_REGEX =
        /^[^'"#${}()[\],:;\/*\\\s\t\r\n0-9][^'"#${}()[\],:;\/*\\\s\t\r\n]*/;
    private static readonly FUNCTION_NAME_REGEX = /^[a-zA-Z]+/;
    private static readonly DECIMAL_REGEX =
        /^[-\+]?((([0-9]+\.[0-9]*)([eE][+-]?([0-9])+)?)|((\.[0-9]+)([eE][+-]?([0-9])+)?)|(([0-9]+)([eE][+-]?([0-9])+)))/;
    private static readonly INTEGER_REGEX = /^[-\+]?[0-9]+/;
    private static readonly BOOLEAN_REGEX = /^((true)|(false))/;

    // Token style constants
    private static readonly STYLE_BRACKET = 'bracket';
    private static readonly STYLE_BUILTIN = 'builtin';
    private static readonly STYLE_VARIABLE_2 = 'variable-2';
    private static readonly STYLE_STRING = 'string';
    private static readonly STYLE_NUMBER = 'number';
    private static readonly STYLE_COMMENT = 'comment';

    // Tooltip configuration
    private static readonly TOOLTIP_ATTRIBUTE = 'data-nifi-tooltip';

    // Expression Language function and parameter state
    private readonly parameterKeyRegex = CodemirrorNifiLanguagePackage.PARAMETER_KEY_REGEX;
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

    constructor(private elService: ElService) {
        this.initializeElFunctions();
    }

    /**
     * Initializes Expression Language functions by loading and parsing the EL guide.
     */
    private initializeElFunctions(): void {
        this.elService
            .getElGuide()
            .pipe(take(1))
            .subscribe({
                next: (response) => {
                    this.parseElGuide(response);
                },
                complete: () => {
                    this.buildFunctionRegexes();
                }
            });
    }

    /**
     * Parses the Expression Language guide HTML to extract function definitions.
     *
     * @param response - The HTML response containing EL function documentation
     */
    private parseElGuide(response: string): void {
        const document: Document = new DOMParser().parseFromString(response, 'text/html');
        const elFunctions: Element[] = Array.from(document.querySelectorAll('div.function'));

        elFunctions.forEach((elFunction) => {
            const functionData = this.extractFunctionData(elFunction);
            if (functionData) {
                this.storeFunctionData(functionData);
            }
        });
    }

    /**
     * Extracts function data from a function element in the EL guide.
     *
     * @param elFunction - The DOM element containing function information
     * @returns The extracted function data or null if invalid
     */
    private extractFunctionData(elFunction: Element): any | null {
        const name: string = elFunction.querySelector('h3')?.textContent ?? '';
        const description: string = elFunction.querySelector('span.description')?.textContent ?? '';
        const returnType: string = elFunction.querySelector('span.returnType')?.textContent ?? '';

        if (!name) {
            return null;
        }

        let subject = '';
        const subjectSpan: Element | null = elFunction.querySelector('span.subject');
        const subjectless: Element | null = elFunction.querySelector('span.subjectless');

        // Determine function type and subject requirements
        let isSubjectless = false;
        let hasSubject = false;

        if (subjectless) {
            isSubjectless = true;
            subject = 'None';
        }

        if (subjectSpan) {
            hasSubject = true;
            subject = subjectSpan.textContent ?? '';
        }

        // Extract function arguments
        const args: { [key: string]: string } = {};
        const argElements: Element[] = Array.from(elFunction.querySelectorAll('span.argName'));
        argElements.forEach((argElement) => {
            const argName: string = argElement.textContent ?? '';
            const next: Element | null = argElement.nextElementSibling;
            if (next && next.matches('span.argDesc')) {
                args[argName] = next.textContent ?? '';
            }
        });

        return {
            name,
            description,
            returnType,
            subject,
            args,
            isSubjectless,
            hasSubject
        };
    }

    /**
     * Stores function data in the appropriate collections.
     *
     * @param functionData - The function data to store
     */
    private storeFunctionData(functionData: any): void {
        const { name, description, returnType, subject, args, isSubjectless, hasSubject } = functionData;

        if (isSubjectless) {
            this.subjectlessFunctions.push(name);
        }

        if (hasSubject) {
            this.functions.push(name);
        }

        this.functionDetails[name] = {
            name,
            description,
            args,
            subject,
            returnType
        };
    }

    /**
     * Builds regular expressions for function matching after all functions are loaded.
     */
    private buildFunctionRegexes(): void {
        this.subjectlessFunctionRegex = new RegExp('^((' + this.subjectlessFunctions.join(')|(') + '))$');
        this.functionRegex = new RegExp('^((' + this.functions.join(')|(') + '))$');
    }

    /**
     * Creates a language configuration for CodeMirror with the specified capabilities.
     *
     * @param config - Configuration specifying EL and parameter support
     * @returns Language configuration for CodeMirror
     */
    public getLanguageMode(config: NfLanguageDefinition): NfLanguageConfig {
        this.configureLanguageSupport(config);

        return {
            streamParser: this.createStreamParser(),
            getAutocompletions: (viewContainerRef: ViewContainerRef) =>
                this.createAutocompletionHandler(viewContainerRef)
        };
    }

    /**
     * Configures parameter and EL function support based on the provided configuration.
     *
     * @param config - The language definition configuration
     */
    private configureLanguageSupport(config: NfLanguageDefinition): void {
        if (config.parameterListing) {
            this.enableParameters();
            this.setParameters(config.parameterListing);
        } else {
            this.disableParameters();
        }

        if (config.supportsEl) {
            this.enableEl();
        } else {
            this.disableEl();
        }
    }

    /**
     * Creates the stream parser for tokenizing NiFi expressions.
     *
     * @returns The configured stream parser
     */
    private createStreamParser(): StreamParser<unknown> {
        const self: CodemirrorNifiLanguagePackage = this;
        let currentState: string = CodemirrorNifiLanguagePackage.CONTEXT_INVALID;

        /**
         * Handles string literals in the stream.
         */
        const handleStringLiteral = (stream: any, state: any): string | null => {
            const current: string | null = stream.next();
            let foundTrailing = false;
            let foundEscapeChar = false;

            const foundStringLiteral: boolean = stream.eatWhile((ch: string) => {
                if (foundTrailing) {
                    return false;
                }

                if (ch === current) {
                    foundTrailing = !foundEscapeChar;
                }

                foundEscapeChar = false;

                if (ch === '\\') {
                    foundEscapeChar = true;
                }

                return true;
            });

            if (foundStringLiteral) {
                return CodemirrorNifiLanguagePackage.STYLE_STRING;
            }

            state.context = CodemirrorNifiLanguagePackage.CONTEXT_INVALID;
            currentState = CodemirrorNifiLanguagePackage.CONTEXT_INVALID;

            stream.skipToEnd();
            return null;
        };

        /**
         * Handles the end of parameter references.
         */
        const handleParameterEnd = (
            stream: any,
            state: any,
            states: any,
            parameterPredicate: () => boolean
        ): string | null => {
            if (parameterPredicate()) {
                stream.next();

                if (stream.peek() === '}') {
                    stream.next();

                    if (typeof states.pop() === 'undefined') {
                        return null;
                    } else {
                        return CodemirrorNifiLanguagePackage.STYLE_BRACKET;
                    }
                } else {
                    stream.skipToEnd();
                    state.context = CodemirrorNifiLanguagePackage.CONTEXT_INVALID;
                    currentState = CodemirrorNifiLanguagePackage.CONTEXT_INVALID;
                    return null;
                }
            } else {
                stream.skipToEnd();
                state.context = CodemirrorNifiLanguagePackage.CONTEXT_INVALID;
                currentState = CodemirrorNifiLanguagePackage.CONTEXT_INVALID;
                return null;
            }
        };

        return {
            startState: () => this.buildStates([]),
            copyState: (state: any) => this.buildStates(state.copy()),
            token: (stream: StringStream, states: any) => {
                // Skip whitespace
                if (stream.eatSpace()) {
                    return null;
                }

                // End of line
                if (stream.eol()) {
                    return null;
                }

                const current: string | undefined = stream.peek();

                // Handle comments
                if (current === '#') {
                    stream.next();
                    const afterPound: string | undefined = stream.peek();
                    if (afterPound !== '{') {
                        stream.skipToEnd();
                        return CodemirrorNifiLanguagePackage.STYLE_COMMENT;
                    } else {
                        stream.backUp(1);
                    }
                }

                const state: any = states.get();

                // Handle invalid context
                if (state.context === CodemirrorNifiLanguagePackage.CONTEXT_INVALID) {
                    stream.skipToEnd();
                    return null;
                }

                // Within an expression
                if (state.context === CodemirrorNifiLanguagePackage.CONTEXT_EXPRESSION) {
                    const attributeOrSubjectlessFunctionExpression =
                        CodemirrorNifiLanguagePackage.ATTRIBUTE_OR_FUNCTION_REGEX;

                    const attributeOrSubjectlessFunctionName: RegExpMatchArray | null = stream.match(
                        attributeOrSubjectlessFunctionExpression,
                        false
                    ) as RegExpMatchArray | null;

                    if (
                        attributeOrSubjectlessFunctionName !== null &&
                        attributeOrSubjectlessFunctionName.length === 1
                    ) {
                        stream.match(attributeOrSubjectlessFunctionExpression);

                        if (
                            self.functionSupported &&
                            self.subjectlessFunctionRegex.test(attributeOrSubjectlessFunctionName[0]) &&
                            stream.peek() === '('
                        ) {
                            state.context = CodemirrorNifiLanguagePackage.CONTEXT_ARGUMENTS;
                            currentState = CodemirrorNifiLanguagePackage.CONTEXT_ARGUMENTS;
                            return CodemirrorNifiLanguagePackage.STYLE_BUILTIN;
                        } else {
                            state.context = CodemirrorNifiLanguagePackage.CONTEXT_SUBJECT_OR_FUNCTION;
                            currentState = CodemirrorNifiLanguagePackage.CONTEXT_SUBJECT_OR_FUNCTION;
                            return CodemirrorNifiLanguagePackage.STYLE_VARIABLE_2;
                        }
                    } else if (current === "'" || current === '"') {
                        const expressionStringResult: string | null = handleStringLiteral(stream, state);

                        if (expressionStringResult !== null) {
                            state.context = CodemirrorNifiLanguagePackage.CONTEXT_SUBJECT;
                            currentState = CodemirrorNifiLanguagePackage.CONTEXT_SUBJECT;
                        }

                        return expressionStringResult;
                    } else if (current === '$') {
                        const expressionDollarResult: string | null = self.handleStart(
                            '$',
                            CodemirrorNifiLanguagePackage.CONTEXT_EXPRESSION,
                            stream,
                            states
                        );

                        if (expressionDollarResult !== null) {
                            state.context = CodemirrorNifiLanguagePackage.CONTEXT_SUBJECT;
                            currentState = CodemirrorNifiLanguagePackage.CONTEXT_SUBJECT;
                        }

                        return expressionDollarResult;
                    } else if (current === '#' && self.parametersSupported) {
                        const parameterReferenceResult = self.handleStart(
                            '#',
                            CodemirrorNifiLanguagePackage.CONTEXT_PARAMETER,
                            stream,
                            states
                        );

                        if (parameterReferenceResult !== null) {
                            state.context = CodemirrorNifiLanguagePackage.CONTEXT_SUBJECT;
                            currentState = CodemirrorNifiLanguagePackage.CONTEXT_SUBJECT;
                        }

                        return parameterReferenceResult;
                    } else if (current === '}') {
                        stream.next();

                        if (typeof states.pop() === 'undefined') {
                            return null;
                        } else {
                            return CodemirrorNifiLanguagePackage.STYLE_BRACKET;
                        }
                    } else {
                        stream.skipToEnd();
                        state.context = CodemirrorNifiLanguagePackage.CONTEXT_INVALID;
                        currentState = CodemirrorNifiLanguagePackage.CONTEXT_INVALID;
                        return null;
                    }
                }

                // Within a subject
                if (
                    state.context === CodemirrorNifiLanguagePackage.CONTEXT_SUBJECT ||
                    state.context === CodemirrorNifiLanguagePackage.CONTEXT_SUBJECT_OR_FUNCTION
                ) {
                    if (current === ':') {
                        stream.next();
                        state.context = CodemirrorNifiLanguagePackage.CONTEXT_FUNCTION;
                        currentState = CodemirrorNifiLanguagePackage.CONTEXT_FUNCTION;
                        stream.eatSpace();
                        return null;
                    } else if (current === '}') {
                        stream.next();

                        if (typeof states.pop() === 'undefined') {
                            return null;
                        } else {
                            return CodemirrorNifiLanguagePackage.STYLE_BRACKET;
                        }
                    } else {
                        stream.skipToEnd();
                        state.context = CodemirrorNifiLanguagePackage.CONTEXT_INVALID;
                        currentState = CodemirrorNifiLanguagePackage.CONTEXT_INVALID;
                        return null;
                    }
                }

                // Within a function
                if (state.context === CodemirrorNifiLanguagePackage.CONTEXT_FUNCTION) {
                    const functionName: RegExpMatchArray | null = stream.match(
                        CodemirrorNifiLanguagePackage.FUNCTION_NAME_REGEX,
                        false
                    ) as RegExpMatchArray | null;

                    if (functionName !== null && functionName.length === 1) {
                        stream.match(CodemirrorNifiLanguagePackage.FUNCTION_NAME_REGEX);

                        if (
                            self.functionSupported &&
                            self.functionRegex.test(functionName[0]) &&
                            stream.peek() === '('
                        ) {
                            state.context = CodemirrorNifiLanguagePackage.CONTEXT_ARGUMENTS;
                            currentState = CodemirrorNifiLanguagePackage.CONTEXT_ARGUMENTS;
                            return CodemirrorNifiLanguagePackage.STYLE_BUILTIN;
                        } else {
                            return null;
                        }
                    } else {
                        stream.skipToEnd();
                        state.context = CodemirrorNifiLanguagePackage.CONTEXT_INVALID;
                        currentState = CodemirrorNifiLanguagePackage.CONTEXT_INVALID;
                        return null;
                    }
                }

                // Within arguments
                if (state.context === CodemirrorNifiLanguagePackage.CONTEXT_ARGUMENTS) {
                    if (current === '(') {
                        stream.next();
                        state.context = CodemirrorNifiLanguagePackage.CONTEXT_ARGUMENT;
                        currentState = CodemirrorNifiLanguagePackage.CONTEXT_ARGUMENT;
                        return null;
                    } else if (current === ')') {
                        stream.next();
                        state.context = CodemirrorNifiLanguagePackage.CONTEXT_SUBJECT;
                        currentState = CodemirrorNifiLanguagePackage.CONTEXT_SUBJECT;
                        return null;
                    } else if (current === ',') {
                        stream.next();
                        state.context = CodemirrorNifiLanguagePackage.CONTEXT_ARGUMENT;
                        currentState = CodemirrorNifiLanguagePackage.CONTEXT_ARGUMENT;
                        return null;
                    } else {
                        stream.skipToEnd();
                        state.context = CodemirrorNifiLanguagePackage.CONTEXT_INVALID;
                        currentState = CodemirrorNifiLanguagePackage.CONTEXT_INVALID;
                        return null;
                    }
                }

                // Within a specific argument
                if (state.context === CodemirrorNifiLanguagePackage.CONTEXT_ARGUMENT) {
                    if (current === "'" || current === '"') {
                        const argumentStringResult: string | null = handleStringLiteral(stream, state);

                        if (argumentStringResult !== null) {
                            state.context = CodemirrorNifiLanguagePackage.CONTEXT_ARGUMENTS;
                            currentState = CodemirrorNifiLanguagePackage.CONTEXT_ARGUMENTS;
                        }

                        return argumentStringResult;
                    } else if (stream.match(CodemirrorNifiLanguagePackage.DECIMAL_REGEX)) {
                        state.context = CodemirrorNifiLanguagePackage.CONTEXT_ARGUMENTS;
                        currentState = CodemirrorNifiLanguagePackage.CONTEXT_ARGUMENTS;
                        return CodemirrorNifiLanguagePackage.STYLE_NUMBER;
                    } else if (stream.match(CodemirrorNifiLanguagePackage.INTEGER_REGEX)) {
                        state.context = CodemirrorNifiLanguagePackage.CONTEXT_ARGUMENTS;
                        currentState = CodemirrorNifiLanguagePackage.CONTEXT_ARGUMENTS;
                        return CodemirrorNifiLanguagePackage.STYLE_NUMBER;
                    } else if (stream.match(CodemirrorNifiLanguagePackage.BOOLEAN_REGEX)) {
                        state.context = CodemirrorNifiLanguagePackage.CONTEXT_ARGUMENTS;
                        currentState = CodemirrorNifiLanguagePackage.CONTEXT_ARGUMENTS;
                        return CodemirrorNifiLanguagePackage.STYLE_NUMBER;
                    } else if (current === ')') {
                        stream.next();
                        state.context = CodemirrorNifiLanguagePackage.CONTEXT_SUBJECT;
                        currentState = CodemirrorNifiLanguagePackage.CONTEXT_SUBJECT;
                        return null;
                    } else if (current === '$') {
                        const argumentDollarResult: string | null = self.handleStart(
                            '$',
                            CodemirrorNifiLanguagePackage.CONTEXT_EXPRESSION,
                            stream,
                            states
                        );

                        if (argumentDollarResult !== null) {
                            state.context = CodemirrorNifiLanguagePackage.CONTEXT_ARGUMENTS;
                            currentState = CodemirrorNifiLanguagePackage.CONTEXT_ARGUMENTS;
                        }

                        return argumentDollarResult;
                    } else if (current === '#' && self.parametersSupported) {
                        const parameterReferenceResult: string | null = self.handleStart(
                            '#',
                            CodemirrorNifiLanguagePackage.CONTEXT_PARAMETER,
                            stream,
                            states
                        );

                        if (parameterReferenceResult !== null) {
                            state.context = CodemirrorNifiLanguagePackage.CONTEXT_ARGUMENTS;
                            currentState = CodemirrorNifiLanguagePackage.CONTEXT_ARGUMENTS;
                        }

                        return parameterReferenceResult;
                    } else {
                        stream.skipToEnd();
                        state.context = CodemirrorNifiLanguagePackage.CONTEXT_INVALID;
                        currentState = CodemirrorNifiLanguagePackage.CONTEXT_INVALID;
                        return null;
                    }
                }

                // Within a parameter reference
                if (
                    state.context === CodemirrorNifiLanguagePackage.CONTEXT_PARAMETER ||
                    state.context === CodemirrorNifiLanguagePackage.CONTEXT_SINGLE_QUOTE_PARAMETER ||
                    state.context === CodemirrorNifiLanguagePackage.CONTEXT_DOUBLE_QUOTE_PARAMETER
                ) {
                    const parameterName: RegExpMatchArray | null = stream.match(
                        self.parameterKeyRegex,
                        false
                    ) as RegExpMatchArray | null;

                    if (parameterName !== null && parameterName.length === 1) {
                        stream.match(self.parameterKeyRegex);

                        if (self.parameterRegex.test(parameterName[0])) {
                            return CodemirrorNifiLanguagePackage.STYLE_BUILTIN;
                        } else {
                            return CodemirrorNifiLanguagePackage.STYLE_STRING;
                        }
                    }

                    if (state.context === CodemirrorNifiLanguagePackage.CONTEXT_SINGLE_QUOTE_PARAMETER) {
                        return handleParameterEnd(stream, state, states, () => current === "'");
                    }

                    if (state.context === CodemirrorNifiLanguagePackage.CONTEXT_DOUBLE_QUOTE_PARAMETER) {
                        return handleParameterEnd(stream, state, states, () => current === '"');
                    }

                    if (current === '}') {
                        stream.next();

                        if (typeof states.pop() === 'undefined') {
                            return null;
                        } else {
                            return CodemirrorNifiLanguagePackage.STYLE_BRACKET;
                        }
                    } else {
                        stream.skipToEnd();
                        state.context = CodemirrorNifiLanguagePackage.CONTEXT_INVALID;
                        currentState = CodemirrorNifiLanguagePackage.CONTEXT_INVALID;
                        return null;
                    }
                }

                // Default context - signifies the potential start of an expression
                if (current === '$') {
                    return self.handleStart('$', CodemirrorNifiLanguagePackage.CONTEXT_EXPRESSION, stream, states);
                }

                // Signifies the potential start of a parameter reference
                if (current === '#' && self.parametersSupported) {
                    return self.handleStart('#', CodemirrorNifiLanguagePackage.CONTEXT_PARAMETER, stream, states);
                }

                // Signifies the end of an expression
                if (current === '}') {
                    stream.next();
                    if (typeof states.pop() === 'undefined') {
                        return null;
                    } else {
                        return CodemirrorNifiLanguagePackage.STYLE_BRACKET;
                    }
                }

                // Extra characters that are around expression[s] end up here
                // consume the character to keep things moving along
                stream.next();
                return null;
            }
        };
    }

    /**
     * Creates the state management system for the parser.
     *
     * @param initialStates - Initial state array
     * @returns State management object
     */
    private buildStates(initialStates: any[]): any {
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
                    return { context: null };
                } else {
                    return states[states.length - 1];
                }
            },
            push: (state: any) => {
                return states.push(state);
            },
            pop: () => {
                return states.pop();
            }
        };
    }

    /**
     * Main tokenization method that processes the input stream.
     *
     * @param stream - The character stream to tokenize
     * @param states - The current parser states
     * @returns The token style or null
     */
    private tokenize(stream: StringStream, states: any): string | null {
        // Skip whitespace
        if (stream.eatSpace()) {
            return null;
        }

        // End of line
        if (stream.eol()) {
            return null;
        }

        const current: string | undefined = stream.peek();
        if (!current) {
            return null;
        }

        // Handle comments
        if (this.isCommentStart(stream, current)) {
            return CodemirrorNifiLanguagePackage.STYLE_COMMENT;
        }

        const state: any = states.get();

        // Handle invalid context
        if (state.context === CodemirrorNifiLanguagePackage.CONTEXT_INVALID) {
            stream.skipToEnd();
            return null;
        }

        // Route to appropriate handler based on context
        return this.handleTokenByContext(stream, states, state, current);
    }

    /**
     * Checks if the current position is the start of a comment.
     *
     * @param stream - The character stream
     * @param current - The current character
     * @returns True if this is a comment start
     */
    private isCommentStart(stream: StringStream, current: string): boolean {
        if (current === '#') {
            stream.next();
            const afterPound: string | undefined = stream.peek();
            if (afterPound !== '{') {
                stream.skipToEnd();
                return true;
            } else {
                stream.backUp(1);
            }
        }
        return false;
    }

    /**
     * Routes token handling based on the current parser context.
     *
     * @param stream - The character stream
     * @param states - The parser states
     * @param state - The current state
     * @param current - The current character
     * @returns The token style or null
     */
    private handleTokenByContext(stream: StringStream, states: any, state: any, current: string): string | null {
        switch (state.context) {
            case CodemirrorNifiLanguagePackage.CONTEXT_EXPRESSION:
                return this.handleExpressionContext(stream, states, state, current);
            case CodemirrorNifiLanguagePackage.CONTEXT_SUBJECT:
            case CodemirrorNifiLanguagePackage.CONTEXT_SUBJECT_OR_FUNCTION:
                return this.handleSubjectContext(stream, states, state, current);
            case CodemirrorNifiLanguagePackage.CONTEXT_FUNCTION:
                return this.handleFunctionContext(stream, states, state, current);
            case CodemirrorNifiLanguagePackage.CONTEXT_ARGUMENTS:
                return this.handleArgumentsContext(stream, states, state, current);
            case CodemirrorNifiLanguagePackage.CONTEXT_ARGUMENT:
                return this.handleArgumentContext(stream, states, state, current);
            case CodemirrorNifiLanguagePackage.CONTEXT_PARAMETER:
            case CodemirrorNifiLanguagePackage.CONTEXT_SINGLE_QUOTE_PARAMETER:
            case CodemirrorNifiLanguagePackage.CONTEXT_DOUBLE_QUOTE_PARAMETER:
                return this.handleParameterContext(stream, states, state, current);
            default:
                return this.handleDefaultContext(stream, states, current);
        }
    }

    /**
     * Handles tokenization for the expression context.
     *
     * @param stream - The character stream
     * @param states - The parser states
     * @param state - The current state
     * @param current - The current character
     * @returns The token style or null
     */
    private handleExpressionContext(stream: StringStream, states: any, state: any, current: string): string | null {
        if (current === '{') {
            stream.next();
            if (CodemirrorNifiLanguagePackage.CONTEXT_PARAMETER === state.context) {
                if (stream.peek() === "'") {
                    stream.next();
                    states.push({ context: CodemirrorNifiLanguagePackage.CONTEXT_SINGLE_QUOTE_PARAMETER });
                } else if (stream.peek() === '"') {
                    stream.next();
                    states.push({ context: CodemirrorNifiLanguagePackage.CONTEXT_DOUBLE_QUOTE_PARAMETER });
                } else {
                    states.push({ context: CodemirrorNifiLanguagePackage.CONTEXT_PARAMETER });
                }
            } else {
                states.push({ context: state.context });
            }
            stream.eatSpace();
            return CodemirrorNifiLanguagePackage.STYLE_BRACKET;
        }
        return null;
    }

    /**
     * Handles tokenization for the subject context.
     *
     * @param stream - The character stream
     * @param states - The parser states
     * @param state - The current state
     * @param current - The current character
     * @returns The token style or null
     */
    private handleSubjectContext(stream: StringStream, states: any, state: any, current: string): string | null {
        if (current === ':') {
            stream.next();
            states.context = CodemirrorNifiLanguagePackage.CONTEXT_FUNCTION;
            return null;
        } else if (current === '}') {
            stream.next();
            if (typeof states.pop() === 'undefined') {
                return null;
            } else {
                return CodemirrorNifiLanguagePackage.STYLE_BRACKET;
            }
        }
        return null;
    }

    /**
     * Handles tokenization for the function context.
     *
     * @param stream - The character stream
     * @param states - The parser states
     * @param state - The current state
     * @param current - The current character
     * @returns The token style or null
     */
    private handleFunctionContext(stream: StringStream, states: any, state: any, current: string): string | null {
        const functionNameMatch = stream.match(CodemirrorNifiLanguagePackage.FUNCTION_NAME_REGEX, false);
        if (functionNameMatch && Array.isArray(functionNameMatch) && functionNameMatch.length === 1) {
            stream.match(CodemirrorNifiLanguagePackage.FUNCTION_NAME_REGEX);
            if (this.functionSupported && this.functionRegex.test(functionNameMatch[0]) && stream.peek() === '(') {
                states.context = CodemirrorNifiLanguagePackage.CONTEXT_ARGUMENTS;
                return CodemirrorNifiLanguagePackage.STYLE_BUILTIN;
            }
        }
        return null;
    }

    /**
     * Handles tokenization for the arguments context.
     *
     * @param stream - The character stream
     * @param states - The parser states
     * @param state - The current state
     * @param current - The current character
     * @returns The token style or null
     */
    private handleArgumentsContext(stream: StringStream, states: any, state: any, current: string): string | null {
        if (current === '(') {
            stream.next();
            states.context = CodemirrorNifiLanguagePackage.CONTEXT_ARGUMENT;
            return null;
        } else if (current === ')') {
            stream.next();
            states.context = CodemirrorNifiLanguagePackage.CONTEXT_SUBJECT;
            return null;
        } else if (current === ',') {
            stream.next();
            states.context = CodemirrorNifiLanguagePackage.CONTEXT_ARGUMENT;
            return null;
        }
        return null;
    }

    /**
     * Handles tokenization for the argument context.
     *
     * @param stream - The character stream
     * @param states - The parser states
     * @param state - The current state
     * @param current - The current character
     * @returns The token style or null
     */
    private handleArgumentContext(stream: StringStream, states: any, state: any, current: string): string | null {
        if (current === "'" || current === '"') {
            const stringLiteralResult = this.handleStringLiteral(stream, states);
            if (stringLiteralResult !== null) {
                states.context = CodemirrorNifiLanguagePackage.CONTEXT_ARGUMENTS;
                return stringLiteralResult;
            }
        } else if (stream.match(CodemirrorNifiLanguagePackage.DECIMAL_REGEX)) {
            stream.match(CodemirrorNifiLanguagePackage.DECIMAL_REGEX);
            states.context = CodemirrorNifiLanguagePackage.CONTEXT_ARGUMENTS;
            return CodemirrorNifiLanguagePackage.STYLE_NUMBER;
        } else if (stream.match(CodemirrorNifiLanguagePackage.INTEGER_REGEX)) {
            stream.match(CodemirrorNifiLanguagePackage.INTEGER_REGEX);
            states.context = CodemirrorNifiLanguagePackage.CONTEXT_ARGUMENTS;
            return CodemirrorNifiLanguagePackage.STYLE_NUMBER;
        } else if (stream.match(CodemirrorNifiLanguagePackage.BOOLEAN_REGEX)) {
            stream.match(CodemirrorNifiLanguagePackage.BOOLEAN_REGEX);
            states.context = CodemirrorNifiLanguagePackage.CONTEXT_ARGUMENTS;
            return CodemirrorNifiLanguagePackage.STYLE_NUMBER;
        } else if (current === ')') {
            stream.next();
            states.context = CodemirrorNifiLanguagePackage.CONTEXT_SUBJECT;
            return null;
        } else if (current === '$') {
            const expressionDollarResult = this.handleStart(
                '$',
                CodemirrorNifiLanguagePackage.CONTEXT_EXPRESSION,
                stream,
                states
            );
            if (expressionDollarResult !== null) {
                states.context = CodemirrorNifiLanguagePackage.CONTEXT_SUBJECT;
                return expressionDollarResult;
            }
        } else if (current === '#' && this.parametersSupported) {
            const parameterReferenceResult = this.handleStart(
                '#',
                CodemirrorNifiLanguagePackage.CONTEXT_PARAMETER,
                stream,
                states
            );
            if (parameterReferenceResult !== null) {
                states.context = CodemirrorNifiLanguagePackage.CONTEXT_SUBJECT;
                return parameterReferenceResult;
            }
        }
        return null;
    }

    /**
     * Handles tokenization for the parameter context.
     *
     * @param stream - The character stream
     * @param states - The parser states
     * @param state - The current state
     * @param current - The current character
     * @returns The token style or null
     */
    private handleParameterContext(stream: StringStream, states: any, state: any, current: string): string | null {
        const parameterNameMatch = stream.match(CodemirrorNifiLanguagePackage.PARAMETER_KEY_REGEX, false);
        if (parameterNameMatch && Array.isArray(parameterNameMatch) && parameterNameMatch.length === 1) {
            stream.match(CodemirrorNifiLanguagePackage.PARAMETER_KEY_REGEX);
            if (this.parameterRegex.test(parameterNameMatch[0])) {
                return CodemirrorNifiLanguagePackage.STYLE_BUILTIN;
            }
        }

        if (state.context === CodemirrorNifiLanguagePackage.CONTEXT_SINGLE_QUOTE_PARAMETER) {
            return this.handleParameterEnd(stream, states, () => current === "'");
        }

        if (state.context === CodemirrorNifiLanguagePackage.CONTEXT_DOUBLE_QUOTE_PARAMETER) {
            return this.handleParameterEnd(stream, states, () => current === '"');
        }

        if (current === '}') {
            stream.next();
            if (typeof states.pop() === 'undefined') {
                return null;
            } else {
                return CodemirrorNifiLanguagePackage.STYLE_BRACKET;
            }
        }
        return null;
    }

    /**
     * Handles tokenization for the default context.
     *
     * @param stream - The character stream
     * @param states - The parser states
     * @param current - The current character
     * @returns The token style or null
     */
    private handleDefaultContext(stream: StringStream, states: any, current: string): string | null {
        if (current === '$') {
            return this.handleStart('$', CodemirrorNifiLanguagePackage.CONTEXT_EXPRESSION, stream, states);
        }

        if (current === '#' && this.parametersSupported) {
            return this.handleStart('#', CodemirrorNifiLanguagePackage.CONTEXT_PARAMETER, stream, states);
        }

        if (current === '}') {
            stream.next();
            if (typeof states.pop() === 'undefined') {
                return null;
            } else {
                return CodemirrorNifiLanguagePackage.STYLE_BRACKET;
            }
        }

        // Consume any additional characters that are around expression[s] end up here
        stream.next();
        return null;
    }

    /**
     * Handles the start of a string literal.
     *
     * @param stream - The character stream
     * @param states - The parser states
     * @returns The token style or null
     */
    private handleStringLiteral(stream: StringStream, states: any): string | null {
        const current = stream.next();
        let foundTrailing = false;
        let foundEscapeChar = false;

        const foundStringLiteral = stream.eatWhile((ch: string) => {
            if (foundTrailing) {
                return false;
            }

            if (ch === current) {
                foundTrailing = !foundEscapeChar;
            }

            foundEscapeChar = false;

            if (ch === '\\') {
                foundEscapeChar = true;
            }

            return true;
        });

        if (foundStringLiteral) {
            return 'string';
        }

        states.context = CodemirrorNifiLanguagePackage.CONTEXT_INVALID;
        stream.skipToEnd();
        return null;
    }

    /**
     * Handles the end of a parameter reference.
     *
     * @param stream - The character stream
     * @param states - The parser states
     * @param parameterPredicate - Predicate to check if the current character is the closing delimiter
     * @returns The token style or null
     */
    private handleParameterEnd(stream: StringStream, states: any, parameterPredicate: () => boolean): string | null {
        if (parameterPredicate()) {
            stream.next();
            if (stream.peek() === '}') {
                stream.next();
                if (typeof states.pop() === 'undefined') {
                    return null;
                } else {
                    return CodemirrorNifiLanguagePackage.STYLE_BRACKET;
                }
            } else {
                stream.skipToEnd();
                states.context = CodemirrorNifiLanguagePackage.CONTEXT_INVALID;
                return null;
            }
        } else {
            stream.skipToEnd();
            states.context = CodemirrorNifiLanguagePackage.CONTEXT_INVALID;
            return null;
        }
    }

    /**
     * Handles the start of a nested expression or parameter reference.
     *
     * @param startChar - The character that starts the nested expression/parameter
     * @param context - The context to transition to if we match on the specified start character
     * @param stream - The character stream
     * @param states - The parser states
     * @returns The token style or null
     */
    private handleStart(startChar: string, context: string, stream: StringStream, states: any): string | null {
        // Determine the number of sequential start chars
        let startCharCount = 0;
        stream.eatWhile((ch: string) => {
            if (ch === startChar) {
                startCharCount++;
                return true;
            }
            return false;
        });

        // If there is an even number of consecutive start chars this expression is escaped
        if (startCharCount % 2 === 0) {
            // Do not style an escaped expression
            return null;
        }

        // If there was an odd number of consecutive start chars and there was more than 1
        if (startCharCount > 1) {
            // Back up one char so we can process the start sequence next iteration
            stream.backUp(1);
            // Do not style the preceding start chars
            return null;
        }

        // If the next character is the start of an expression
        if (stream.peek() === '{') {
            // Consume the open curly
            stream.next();

            if (CodemirrorNifiLanguagePackage.CONTEXT_PARAMETER === context) {
                // There may be an optional single/double quote
                if (stream.peek() === "'") {
                    // Consume the single quote
                    stream.next();
                    // New expression start
                    states.push({
                        context: CodemirrorNifiLanguagePackage.CONTEXT_SINGLE_QUOTE_PARAMETER
                    });
                } else if (stream.peek() === '"') {
                    // Consume the double quote
                    stream.next();
                    // New expression start
                    states.push({
                        context: CodemirrorNifiLanguagePackage.CONTEXT_DOUBLE_QUOTE_PARAMETER
                    });
                } else {
                    // New expression start
                    states.push({
                        context: CodemirrorNifiLanguagePackage.CONTEXT_PARAMETER
                    });
                }
            } else {
                // New expression start
                states.push({
                    context: context
                });
            }

            // Consume any additional whitespace
            stream.eatSpace();

            return CodemirrorNifiLanguagePackage.STYLE_BRACKET;
        }
        // Not a valid start sequence
        return null;
    }

    /**
     * Creates the autocompletion handler for the language.
     *
     * @param viewContainerRef - Angular view container for creating tooltip components
     * @returns The autocompletion handler function
     */
    private createAutocompletionHandler(
        viewContainerRef: ViewContainerRef
    ): (context: CompletionContext) => CompletionResult | null {
        return (context: CompletionContext): CompletionResult | null => {
            if (!context.explicit) {
                return null;
            }

            const cursorContext = this.analyzeCursorContext(context);
            const options = this.getCompletionOptions(cursorContext);

            if (options.length === 0) {
                return null;
            }

            // Find the expression boundaries and current text
            const expressionInfo = this.findExpressionBoundaries(context, cursorContext);
            if (!expressionInfo) {
                return null;
            }

            const completions = this.createCompletions(
                options,
                expressionInfo.currentText,
                cursorContext,
                viewContainerRef,
                context
            );

            // Auto-insert if there's only one exact match
            if (completions.length === 1 && context.explicit && context.view) {
                const completion = completions[0];
                context.view.dispatch({
                    changes: {
                        from: expressionInfo.from,
                        to: expressionInfo.to,
                        insert: completion.label
                    }
                });
                return null; // Return null to close the completion popup
            }

            return {
                from: expressionInfo.from,
                to: expressionInfo.to,
                options: completions
            };
        };
    }

    /**
     * Finds the boundaries of the current expression being edited.
     *
     * @param context - The completion context
     * @param cursorContext - The analyzed cursor context
     * @returns Expression boundary information or null
     */
    private findExpressionBoundaries(
        context: CompletionContext,
        cursorContext: any
    ): {
        from: number;
        to: number;
        currentText: string;
    } | null {
        const cursorPos = context.pos;
        const doc = context.state.doc;
        const lineInfo = doc.lineAt(cursorPos);
        const cursorInLine = cursorPos - lineInfo.from;

        // Find the expression boundaries based on context type
        if (cursorContext.type === CodemirrorNifiLanguagePackage.CONTEXT_PARAMETER) {
            return this.findParameterBoundaries(lineInfo.text, cursorInLine, lineInfo.from);
        } else if (cursorContext.type === CodemirrorNifiLanguagePackage.CONTEXT_EXPRESSION) {
            return this.findElFunctionBoundaries(lineInfo.text, cursorInLine, lineInfo.from);
        }

        return null;
    }

    /**
     * Finds parameter expression boundaries.
     */
    private findParameterBoundaries(
        lineText: string,
        cursorInLine: number,
        lineFrom: number
    ): {
        from: number;
        to: number;
        currentText: string;
    } | null {
        // Find the most recent #{
        let startPos = -1;
        for (let i = cursorInLine - 1; i >= 1; i--) {
            if (lineText.charAt(i) === '{' && lineText.charAt(i - 1) === '#') {
                startPos = i + 1; // Position after #{
                break;
            }
        }

        if (startPos === -1) {
            return null;
        }

        // Find the closing }
        let endPos = lineText.indexOf('}', startPos);
        if (endPos === -1) {
            endPos = lineText.length; // If no closing brace, go to end of line
        }

        const currentText = lineText.substring(startPos, Math.min(endPos, cursorInLine));

        return {
            from: lineFrom + startPos,
            to: lineFrom + endPos,
            currentText: currentText
        };
    }

    /**
     * Finds EL function expression boundaries.
     */
    private findElFunctionBoundaries(
        lineText: string,
        cursorInLine: number,
        lineFrom: number
    ): {
        from: number;
        to: number;
        currentText: string;
    } | null {
        // Find the most recent ${
        let startPos = -1;
        for (let i = cursorInLine - 1; i >= 1; i--) {
            if (lineText.charAt(i) === '{' && lineText.charAt(i - 1) === '$') {
                startPos = i + 1; // Position after ${
                break;
            }
        }

        if (startPos === -1) {
            return null;
        }

        // Find the closing }
        let endPos = lineText.indexOf('}', startPos);
        if (endPos === -1) {
            endPos = lineText.length; // If no closing brace, go to end of line
        }

        const currentText = lineText.substring(startPos, Math.min(endPos, cursorInLine));

        return {
            from: lineFrom + startPos,
            to: lineFrom + endPos,
            currentText: currentText
        };
    }

    /**
     * Analyzes the cursor context to determine what type of completion to provide.
     *
     * @param context - The completion context
     * @returns The detected context information
     */
    private analyzeCursorContext(context: CompletionContext): any {
        const cursorPos = context.pos;
        const doc = context.state.doc;
        const lineInfo = doc.lineAt(cursorPos);
        const lineText = lineInfo.text;
        const cursorInLine = cursorPos - lineInfo.from;

        let parameterStart = -1;
        let elStart = -1;

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

        // Determine context based on most recent opening
        if (parameterStart !== -1 && cursorInLine > parameterStart + 1) {
            if (elStart === -1 || parameterStart > elStart) {
                return {
                    type: CodemirrorNifiLanguagePackage.CONTEXT_PARAMETER,
                    useFunctionDetails: false
                };
            }
        }

        if (elStart !== -1 && cursorInLine > elStart + 1) {
            if (parameterStart === -1 || elStart > parameterStart) {
                return {
                    type: CodemirrorNifiLanguagePackage.CONTEXT_EXPRESSION,
                    useFunctionDetails: true
                };
            }
        }

        return { type: CodemirrorNifiLanguagePackage.CONTEXT_INVALID };
    }

    /**
     * Gets the appropriate completion options based on the cursor context.
     *
     * @param cursorContext - The analyzed cursor context
     * @returns Array of completion options
     */
    private getCompletionOptions(cursorContext: any): string[] {
        const { type, useFunctionDetails } = cursorContext;

        if (type === CodemirrorNifiLanguagePackage.CONTEXT_PARAMETER && this.parametersSupported) {
            return this.parameters;
        } else if (type === CodemirrorNifiLanguagePackage.CONTEXT_EXPRESSION && this.functionSupported) {
            return [...this.subjectlessFunctions, ...this.functions];
        }

        return [];
    }

    /**
     * Creates completion objects with tooltips for the given options.
     *
     * @param options - Available completion options
     * @param tokenValue - The current token being typed
     * @param cursorContext - The cursor context information
     * @param viewContainerRef - Angular view container for tooltips
     * @param context - The completion context
     * @returns Array of completion objects
     */
    private createCompletions(
        options: string[],
        tokenValue: string,
        cursorContext: any,
        viewContainerRef: ViewContainerRef,
        context: CompletionContext
    ): Completion[] {
        // Use improved partial matching that handles spaces
        const filteredOptions = this.filterOptionsWithSpaceSupport(options, tokenValue.trim());

        return filteredOptions.map((opt) => ({
            label: opt,
            type: cursorContext.useFunctionDetails ? 'function' : 'variable',
            boost: filteredOptions.length === 1 ? 1 : 0,
            info: (): CompletionInfo => this.createTooltipInfo(opt, cursorContext, viewContainerRef, context)
        }));
    }

    /**
     * Filters options with improved matching that supports partial matches with spaces.
     *
     * @param options - Available options to filter
     * @param input - User input to match against
     * @returns Filtered array of matching options
     */
    private filterOptionsWithSpaceSupport(options: string[], input: string): string[] {
        if (!input) {
            return options;
        }

        const lowerInput = input.toLowerCase();

        return options.filter((option) => {
            const lowerOption = option.toLowerCase();

            // Exact match or starts with match
            if (lowerOption === lowerInput || lowerOption.startsWith(lowerInput)) {
                return true;
            }

            // For partial matches with spaces, check if the input matches the beginning
            // of words in the option (space-separated matching)
            const inputWords = lowerInput.split(/\s+/);
            const optionWords = lowerOption.split(/\s+/);

            // Check if input words match the beginning of option words in order
            let inputIndex = 0;
            for (let i = 0; i < optionWords.length && inputIndex < inputWords.length; i++) {
                if (optionWords[i].startsWith(inputWords[inputIndex])) {
                    inputIndex++;
                }
            }

            return inputIndex === inputWords.length;
        });
    }

    /**
     * Checks if a parameter exists in the current parameter listing.
     *
     * @param parameterName - The parameter name to check
     * @returns True if the parameter exists
     */
    public isValidParameter(parameterName: string): boolean {
        return this.parametersSupported && this.parameters.includes(parameterName);
    }

    /**
     * Checks if an EL function exists in the current function listing.
     *
     * @param functionName - The function name to check
     * @returns True if the function exists
     */
    public isValidElFunction(functionName: string): boolean {
        // If EL is not supported, consider all functions valid (no error styling)
        if (!this.functionSupported) {
            return true;
        }

        // If functions haven't been loaded yet, consider all functions valid to avoid false errors
        if (this.subjectlessFunctions.length === 0 && this.functions.length === 0) {
            return true;
        }

        // Extract just the function name from complex expressions like "allAttributes()" or "attribute:contains('test')"
        const baseFunctionName = this.extractBaseFunctionName(functionName);

        return this.subjectlessFunctions.includes(baseFunctionName) || this.functions.includes(baseFunctionName);
    }

    /**
     * Extracts the base function name from a complex expression.
     * Examples: "allAttributes()" -> "allAttributes", "attribute:contains('test')" -> "contains"
     *
     * @param functionExpression - The full function expression
     * @returns The base function name
     */
    private extractBaseFunctionName(functionExpression: string): string {
        // Handle subject:function() format - extract function name after colon
        if (functionExpression.includes(':')) {
            const parts = functionExpression.split(':');
            if (parts.length >= 2) {
                const functionPart = parts[1].trim();
                // Extract function name before parentheses if present
                const match = functionPart.match(/^([a-zA-Z][a-zA-Z0-9]*)/);
                return match ? match[1] : functionPart;
            }
        }

        // Handle simple function() format - extract function name before parentheses
        const match = functionExpression.match(/^([a-zA-Z][a-zA-Z0-9]*)/);
        return match ? match[1] : functionExpression;
    }

    /**
     * Creates tooltip information for a completion option.
     *
     * @param option - The completion option
     * @param cursorContext - The cursor context
     * @param viewContainerRef - Angular view container
     * @param context - The completion context
     * @param matchInfo - Information about the content to be replaced
     * @returns The tooltip information object
     */
    private createTooltipInfo(
        option: string,
        cursorContext: any,
        viewContainerRef: ViewContainerRef,
        context: CompletionContext
    ): CompletionInfo {
        const componentRef = this.createTooltipComponent(option, cursorContext, viewContainerRef);
        const tooltipElement = componentRef.location.nativeElement;

        return {
            dom: tooltipElement,
            destroy: () => {
                this.cleanupTooltip(tooltipElement);
                viewContainerRef.clear();
            }
        };
    }

    /**
     * Creates the appropriate tooltip component for the completion option.
     *
     * @param option - The completion option
     * @param cursorContext - The cursor context
     * @param viewContainerRef - Angular view container
     * @returns The created component reference
     */
    private createTooltipComponent(
        option: string,
        cursorContext: any,
        viewContainerRef: ViewContainerRef
    ): ComponentRef<any> {
        let componentRef: ComponentRef<any>;

        if (cursorContext.useFunctionDetails) {
            componentRef = viewContainerRef.createComponent(ElFunctionTip);
            const data: ElFunctionTipInput = {
                elFunction: this.functionDetails[option]
            };
            componentRef.setInput('data', data);
        } else {
            componentRef = viewContainerRef.createComponent(ParameterTip);
            const data: ParameterTipInput = {
                parameter: this.parameterDetails[option]
            };
            componentRef.setInput('data', data);
        }

        componentRef.changeDetectorRef.detectChanges();
        return componentRef;
    }

    /**
     * Cleans up tooltip-specific attributes and event listeners.
     *
     * @param tooltipElement - The tooltip element to clean up
     */
    private cleanupTooltip(tooltipElement: HTMLElement): void {
        tooltipElement.removeAttribute(CodemirrorNifiLanguagePackage.TOOLTIP_ATTRIBUTE);
    }

    // Parameter and EL function support methods

    /**
     * Enables Expression Language function support.
     */
    private enableEl(): void {
        this.functionSupported = true;
    }

    /**
     * Disables Expression Language function support.
     */
    private disableEl(): void {
        this.functionSupported = false;
    }

    /**
     * Enables parameter reference support.
     */
    private enableParameters(): void {
        this.parameters = [];
        this.parameterRegex = new RegExp('^$');
        this.parameterDetails = {};
        this.parametersSupported = true;
    }

    /**
     * Sets the available parameters for autocompletion.
     *
     * @param parameterListing - Array of available parameters
     */
    private setParameters(parameterListing: Parameter[]): void {
        parameterListing.forEach((parameter) => {
            this.parameters.push(parameter.name);
            this.parameterDetails[parameter.name] = parameter;
        });

        this.parameterRegex = new RegExp('^((' + this.parameters.join(')|(') + '))$');
    }

    /**
     * Disables parameter reference support.
     */
    private disableParameters(): void {
        this.parameters = [];
        this.parameterRegex = new RegExp('^$');
        this.parameterDetails = {};
        this.parametersSupported = false;
    }

    /**
     * Returns the language identifier for this language mode.
     *
     * @returns The language identifier
     */
    public getLanguageId(): string {
        return 'nf';
    }
}
