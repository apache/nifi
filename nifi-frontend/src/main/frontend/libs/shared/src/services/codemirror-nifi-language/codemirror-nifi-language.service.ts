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

import { EnvironmentInjector, Injectable, createComponent } from '@angular/core';
import { LRLanguage, LanguageSupport, syntaxTree, HighlightStyle } from '@codemirror/language';
import { parser } from './nfel/nfel';
import {
    CompletionContext,
    autocompletion,
    Completion,
    CompletionResult,
    acceptCompletion,
    completionKeymap
} from '@codemirror/autocomplete';
import { styleTags, tags as t } from '@lezer/highlight';
import { syntaxHighlighting } from '@codemirror/language';
import { firstValueFrom } from 'rxjs';
import { NiFiCommon } from '../nifi-common.service';
import { ElFunction, Parameter } from '../../index';
import { SyntaxNode } from '@lezer/common';
import { Prec } from '@codemirror/state';
import { keymap } from '@codemirror/view';
import { ElService } from '../el.service';
import { ElFunctionTip } from '../../components/codemirror/autocomplete/el-function-tip/el-function-tip.component';
import { ParameterTip } from '../../components/codemirror/autocomplete/parameter-tip/parameter-tip.component';

export interface NifiLanguageOptions {
    functionsEnabled?: boolean;
    parametersEnabled?: boolean;
    parameters?: Parameter[];
}

@Injectable({ providedIn: 'root' })
export class CodemirrorNifiLanguageService {
    private functions: string[] = [];
    private subjectlessFunctions: string[] = [];
    private functionDetails: { [key: string]: ElFunction } = {};
    private functionSupported = false;

    private parameters: string[] = [];
    private parameterDetails: { [key: string]: Parameter } = {};
    private parametersSupported = false;

    private language!: LRLanguage;
    private languageSupport!: LanguageSupport;

    private functionsLoaded = false;
    private functionsLoadPromise: Promise<void> | null = null;

    constructor(
        private elService: ElService,
        private nifiCommon: NiFiCommon,
        private environmentInjector: EnvironmentInjector
    ) {
        this.updateLanguageConfiguration();
    }

    private createDynamicHighlighting() {
        // base highlighting
        const baseHighlighting = {
            Comment: t.comment,
            WholeNumber: t.number,
            Decimal: t.number,
            StringLiteral: t.string,
            BooleanLiteral: t.bool
        };

        // Add EL function highlighting only if EL is enabled
        if (this.functionSupported) {
            Object.assign(baseHighlighting, {
                EscapedDollar: t.content,

                // Expression Delimiters ${...}
                ExpressionStart: t.special(t.brace),

                // EL Functions function syntax highlighting
                functionName: t.function(t.variableName),
                standaloneFunctionName: t.function(t.variableName),
                multiAttrFunctionName: t.function(t.variableName), // Delineated and multi-attribute functions

                // EL Functions attribute syntax highlighting
                attributeName: t.attributeName
            });
        }

        // Add parameter highlighting only if parameters are enabled
        if (this.parametersSupported) {
            Object.assign(baseHighlighting, {
                // Parameter Delimiters #{...}
                ParameterStart: t.special(t.brace),

                // Parameter syntax highlighting
                ParameterReference: t.special(t.variableName),
                ParameterName: t.special(t.variableName),
                'ParameterName/StringLiteral': t.special(t.variableName) // for quoted parameter names
            });
        }

        return styleTags(baseHighlighting);
    }

    private updateLanguageConfiguration(): void {
        // Create the language with our style tags
        this.language = LRLanguage.define({
            parser: parser.configure({
                props: [this.createDynamicHighlighting()]
            }),
            languageData: {
                commentTokens: { line: '#' }
            }
        });

        // Create a highlighter that maps our tags to CSS variables
        const nfelHighlighter = HighlightStyle.define([
            // base highlighting
            { tag: [t.number, t.bool], color: 'var(--editor-number)' },
            { tag: t.string, color: 'var(--editor-string)' },
            { tag: [t.comment], color: 'var(--editor-comment)' },
            { tag: t.special(t.brace), color: 'var(--editor-bracket)' },
            // EL Functions syntax highlighting t.function(t.variableName)
            { tag: t.function(t.variableName), color: 'var(--editor-el-function)' },
            // EL Functions attribute syntax highlighting t.attributeName
            { tag: t.attributeName, color: 'var(--editor-attribute-name)' },
            // Parameter syntax highlighting t.special(t.variableName)
            { tag: t.special(t.variableName), color: 'var(--editor-parameter)' }
        ]);

        this.languageSupport = new LanguageSupport(this.language, [
            // Add our highlighter
            syntaxHighlighting(nfelHighlighter),
            // Add autocompletion
            autocompletion({
                override: [this.nfelCompletions.bind(this)],
                activateOnTyping: false
            }),
            Prec.highest(keymap.of([{ key: 'Tab', run: acceptCompletion }])),
            keymap.of([...completionKeymap])
        ]);
    }

    private async ensureFunctionsLoaded(): Promise<void> {
        // functions are already loaded, no need to re-fetch
        if (this.functionsLoaded) {
            return;
        }

        // functions are not loaded but the promise is already in progress, wait for it to resolve
        if (this.functionsLoadPromise) {
            await this.functionsLoadPromise;
            return;
        }

        // define the promise to load functions
        this.functionsLoadPromise = (async () => {
            const response = await firstValueFrom(this.elService.getElGuide());
            const document: Document = new DOMParser().parseFromString(response, 'text/html');
            const elFunctions: Element[] = Array.from(document.querySelectorAll('div.function'));

            // Reset in case of re-load
            this.functions = [];
            this.subjectlessFunctions = [];
            this.functionDetails = {};

            elFunctions.forEach((elFunction) => {
                const name: string = elFunction.querySelector('h3')?.textContent ?? '';
                const description: string = elFunction.querySelector('span.description')?.textContent ?? '';
                const returnType: string = elFunction.querySelector('span.returnType')?.textContent ?? '';

                let subject = '';
                const subjectSpan: Element | null = elFunction.querySelector('span.subject');
                const subjectless: Element | null = elFunction.querySelector('span.subjectless');

                if (subjectless) {
                    this.subjectlessFunctions.push(name);
                    subject = 'None';
                }
                if (subjectSpan) {
                    this.functions.push(name);
                    subject = subjectSpan.textContent ?? '';
                }

                const args: { [key: string]: string } = {};
                const argElements: Element[] = Array.from(elFunction.querySelectorAll('span.argName'));
                argElements.forEach((argElement) => {
                    const argName: string = argElement.textContent ?? '';
                    const next: Element | null = argElement.nextElementSibling;
                    if (next && next.matches('span.argDesc')) {
                        args[argName] = next.textContent ?? '';
                    }
                });

                this.functionDetails[name] = {
                    name,
                    description,
                    args,
                    subject,
                    returnType
                };
            });
            this.functionsLoaded = true;
        })();

        // await for the promise to resolve loading the functions
        await this.functionsLoadPromise;
    }

    private async nfelCompletions(context: CompletionContext): Promise<CompletionResult | null> {
        // Ensure EL functions are available before computing completions
        if (this.functionSupported && !this.functionsLoaded) {
            await this.ensureFunctionsLoaded();
        }

        const { state, pos } = context;
        const tree = syntaxTree(context.state);
        const node = tree.resolveInner(pos, -1);

        // Get the text around the cursor
        const line = state.doc.lineAt(pos);
        const textBefore = line.text.slice(0, pos - line.from);

        // First check if we're inside a string literal - if so, don't offer completions
        if (this.isInsideStringLiteral(node, textBefore)) {
            return null;
        }

        // Check if the expression is escaped and should not show autocompletion
        if (this.isExpressionEscaped(textBefore)) {
            return null;
        }

        // Check if the parameter start is escaped and should not show autocompletion
        if (this.isParameterEscaped(textBefore)) {
            return null;
        }

        // Check if we're in an expression context, respecting enablement flags
        const isStandaloneFunction = this.functionSupported ? this.isStandaloneFunctionContext(node, context) : false;
        const isFunction = this.functionSupported ? this.isFunctionContext(node, context) : false;
        const isInParameterReference = this.parametersSupported ? this.isInParameterContext(node, context) : false;

        // Check for incomplete expressions by analyzing text patterns, respecting enablement flags
        const incompleteExpression = this.detectIncompleteExpression(textBefore, node);

        if (!isStandaloneFunction && !isFunction && !isInParameterReference && !incompleteExpression.detected) {
            return null;
        }

        // Override context detection if we have incomplete expression
        let finalIsStandaloneFunction = isStandaloneFunction;
        let finalIsFunction = isFunction;
        let finalIsInParameterReference = isInParameterReference;

        if (incompleteExpression.detected) {
            finalIsStandaloneFunction = incompleteExpression.type === 'standalone';
            finalIsFunction = incompleteExpression.type === 'chained';
            finalIsInParameterReference = incompleteExpression.type === 'parameter';
        }

        // Determine what type of completions to offer, respecting enablement flags
        let options: string[];
        let getDetails: (name: string) => ElFunction | Parameter | undefined;

        if (finalIsStandaloneFunction) {
            if (!this.functionSupported) {
                return null;
            }
            options = this.subjectlessFunctions;
            getDetails = (name: string) => this.functionDetails[name];
        } else if (finalIsFunction) {
            if (!this.functionSupported) {
                return null;
            }
            options = this.functions;
            getDetails = (name: string) => this.functionDetails[name];
        } else {
            if (!this.parametersSupported) {
                return null;
            }
            options = this.parameters;
            getDetails = (name: string) => this.parameterDetails[name];
        }

        // Extract the current token being typed
        const match = textBefore.match(/[a-zA-Z0-9_-]*$/);
        const prefix = match ? match[0] : '';

        // If no valid context detected, return null
        if (!finalIsStandaloneFunction && !finalIsFunction && !finalIsInParameterReference) {
            return null;
        }

        // Filter options based on prefix
        const completions: Completion[] = options
            .filter((option) => option.toLowerCase().startsWith(prefix.toLowerCase()))
            .map((option) => {
                const details = getDetails(option);
                return {
                    label: option,
                    type: finalIsInParameterReference ? 'variable' : 'function',
                    info: details ? () => this.buildCompletionInfo(details) : undefined
                };
            })
            .sort((a, b) => this.nifiCommon.compareString(a.label, b.label));

        return {
            from: pos - prefix.length,
            options: completions
        };
    }

    private isStandaloneFunctionContext(node: any, context?: CompletionContext): boolean {
        return (
            (node.type.name === '{' && node.parent?.type.name === 'ReferenceOrFunction') ||
            (node.type.name === 'identifier' && node.parent?.type.name === 'standaloneFunctionName') ||
            (node.type.name === 'standaloneFunctionName' && node.parent?.type.name === 'StandaloneFunction') ||
            // Enhanced: Handle error nodes in expression context
            this.isErrorNodeInExpressionContext(node, 'standalone', context)
        );
    }

    private isFunctionContext(node: any, context?: CompletionContext): boolean {
        return (
            (node.type.name === ':' && node.parent?.type.name === 'ReferenceOrFunction') ||
            (node.type.name === 'identifier' && node.parent?.type.name === 'functionName') ||
            (node.type.name === 'functionName' && node.parent?.type.name === 'FunctionCall') ||
            // Handle FunctionCall nodes directly - when cursor is in the function name area
            (node.type.name === 'FunctionCall' && this.isCursorInFunctionName(node, context)) ||
            // Handle cases where we're immediately after a colon for chained function calls
            this.isImmediatelyAfterColonInExpression(node, context) ||
            // Enhanced: Handle error nodes in chained function context
            this.isErrorNodeInExpressionContext(node, 'chained', context)
        );
    }

    private isInParameterContext(node: any, context?: CompletionContext): boolean {
        // Check for well-formed parameter contexts first
        const isWellFormedParameter =
            (node.type.name === '{' && node.parent?.type.name === 'ParameterReference') ||
            (node.type.name === 'identifier' && node.parent?.type.name === 'ParameterName') ||
            (node.type.name === 'ParameterName' && node.parent?.type.name === 'ParameterReference') ||
            // Handle quoted parameter names: StringLiteral inside ParameterReference, but NOT on closing quotes
            this.isValidStringLiteralInParameterContext(node, context) ||
            // Handle incomplete quoted parameter names: Text nodes inside incomplete parameter references
            this.isValidTextInParameterContext(node, context) ||
            // Enhanced: Handle error nodes in parameter context
            this.isErrorNodeInParameterContext(node, context);

        if (isWellFormedParameter) {
            return true;
        }

        // For incomplete expressions, check if cursor is actually within a parameter reference
        if (context) {
            const cursorPos = context.pos;

            // Walk up the tree to find nodes that might contain parameter references
            let current: SyntaxNode | null = node;
            while (current) {
                const nodeText = this.getNodeText(current, context);

                // Only check if this node contains parameter patterns
                if (nodeText.includes('#{')) {
                    // Check if cursor is within the bounds of a parameter reference
                    if (this.isCursorWithinParameterReference(current, cursorPos, context)) {
                        return true;
                    }
                }

                current = current.parent;
            }
        }

        return false;
    }

    private isCursorWithinParameterReference(node: any, cursorPos: number, context?: CompletionContext): boolean {
        const nodeText = this.getNodeText(node, context);
        const nodeStart = node.from;

        // Find all parameter reference patterns in this node
        const paramRegex = /#\{[^}]*\}?/g;
        let match;

        while ((match = paramRegex.exec(nodeText)) !== null) {
            const paramStart = nodeStart + match.index;
            const paramText = match[0];

            // Check if this parameter reference is complete (has closing brace)
            const hasClosingBrace = paramText.endsWith('}');
            const paramEnd = paramStart + paramText.length;

            if (hasClosingBrace) {
                // For complete parameter references, cursor must be INSIDE the braces
                // e.g., for "#{asdf}", cursor should be between positions of { and }
                const openBracePos = paramStart + 2; // position after #{
                const closeBracePos = paramEnd - 1; // position of }

                if (cursorPos > openBracePos && cursorPos < closeBracePos) {
                    // Additional check for quoted parameter names: ensure cursor is not on quote characters
                    if (this.isCursorOnQuoteInParameterReference(paramText, cursorPos, paramStart)) {
                        return false;
                    }
                    return true;
                }
            } else {
                // For incomplete parameter references (no closing brace),
                // cursor should be after the opening #{
                const openBracePos = paramStart + 2; // position after #{

                if (cursorPos >= openBracePos && cursorPos >= paramEnd) {
                    return true;
                }
            }
        }

        return false;
    }

    private isValidStringLiteralInParameterContext(node: any, context?: CompletionContext): boolean {
        // First check if this is a StringLiteral inside a ParameterReference
        if (node.type.name !== 'StringLiteral' || node.parent?.type.name !== 'ParameterReference') {
            return false;
        }

        // If no context provided, use basic check
        if (!context) {
            return true;
        }

        const cursorPos = context.pos;
        const nodeStart = node.from;
        const nodeEnd = node.to;

        // Check if cursor is on the closing quote
        // For quoted strings, the closing quote is at nodeEnd - 1
        // We should NOT provide autocompletion when cursor is exactly on the closing quote
        const closingQuotePos = nodeEnd - 1;

        if (cursorPos === closingQuotePos) {
            return false;
        }

        // Check if cursor is on the opening quote
        // For quoted strings, the opening quote is at nodeStart
        const openingQuotePos = nodeStart;

        if (cursorPos === openingQuotePos) {
            return false;
        }

        // Cursor is inside the quoted parameter name (between quotes) - allow autocompletion
        return true;
    }

    private isCursorOnQuoteInParameterReference(paramText: string, cursorPos: number, paramStart: number): boolean {
        // Check if the parameter text has quotes (either single or double)
        // paramText is something like: #{\"param\"} or #{'param'} or #{param}

        // Find quote positions within the parameter text
        const contentStart = 2; // after #{
        const contentEnd = paramText.length - 1; // before }
        const content = paramText.substring(contentStart, contentEnd);

        // Check if content is quoted
        const isDoubleQuoted = content.startsWith('"') && content.endsWith('"');
        const isSingleQuoted = content.startsWith("'") && content.endsWith("'");

        if (!isDoubleQuoted && !isSingleQuoted) {
            // Not quoted, cursor can't be on a quote
            return false;
        }

        // Calculate absolute positions of quotes
        const openingQuotePos = paramStart + contentStart; // position of opening quote
        const closingQuotePos = paramStart + contentEnd - 1; // position of closing quote

        // Check if cursor is exactly on either quote
        return cursorPos === openingQuotePos || cursorPos === closingQuotePos;
    }

    private isValidTextInParameterContext(node: any, context?: CompletionContext): boolean {
        // Handle incomplete quoted parameter references where the content is parsed as Text
        // e.g., #{"myPar (incomplete, no closing quote or brace)

        if (node.type.name !== 'Text' || !context) {
            return false;
        }

        const cursorPos = context.pos;
        const textBefore = context.state.doc.sliceString(0, cursorPos);

        // Check if we're in an incomplete parameter reference with quotes
        // Look for the pattern #{" or #{' without a matching closing quote and brace
        const paramStartMatch = textBefore.match(/#\{["'][^"'}]*$/);
        if (paramStartMatch) {
            return true;
        }

        return false;
    }

    private detectIncompleteExpression(
        textBefore: string,
        node: SyntaxNode
    ): { detected: boolean; type?: 'standalone' | 'chained' | 'parameter' } {
        // Look for incomplete expression patterns in the text, respecting enablement flags

        // Pattern: ${something (incomplete standalone function or attribute)
        const incompleteExpressionMatch = textBefore.match(/\$\{([a-zA-Z0-9_.-]*)$/);
        if (incompleteExpressionMatch && this.functionSupported) {
            // If we're inside an expression and there's no colon, it's likely a standalone function
            return { detected: true, type: 'standalone' };
        }

        // Pattern: #{something (incomplete parameter reference)
        const incompleteParameterMatch = textBefore.match(/#\{([a-zA-Z0-9_.-]*)$/);
        if (incompleteParameterMatch && this.parametersSupported) {
            return { detected: true, type: 'parameter' };
        }

        // Pattern: #{"something or #{'something (incomplete quoted parameter reference)
        const incompleteQuotedParameterMatch = textBefore.match(/#\{["']([^"']*?)$/);
        if (incompleteQuotedParameterMatch && this.parametersSupported) {
            return { detected: true, type: 'parameter' };
        }

        // Pattern: ${attr:something (incomplete chained function)
        const incompleteChainedMatch = textBefore.match(/\$\{[a-zA-Z0-9_.-]+:([a-zA-Z0-9_.-]*)$/);
        if (incompleteChainedMatch && this.functionSupported) {
            return { detected: true, type: 'chained' };
        }

        // Pattern: ${#{param}:something (incomplete chained function with embedded parameter)
        const incompleteChainedWithParamMatch = textBefore.match(/\$\{#\{[^}]*\}:([a-zA-Z0-9_.-]*)$/);
        if (incompleteChainedWithParamMatch && this.functionSupported) {
            return { detected: true, type: 'chained' };
        }

        // Pattern: ${attr:func1():func2 (multiple chained functions)
        const incompleteMultipleChainedMatch = textBefore.match(/\$\{[^}]*:[^}]*\([^}]*\):([a-zA-Z0-9_.-]*)$/);
        if (incompleteMultipleChainedMatch && this.functionSupported) {
            return { detected: true, type: 'chained' };
        }

        // Pattern: ${#{param}:func1():func2 (multiple chained functions with embedded parameter)
        const incompleteMultipleChainedWithParamMatch = textBefore.match(
            /\$\{#\{[^}]*\}:[^}]*\([^}]*\):([a-zA-Z0-9_.-]*)$/
        );
        if (incompleteMultipleChainedWithParamMatch && this.functionSupported) {
            return { detected: true, type: 'chained' };
        }

        // Also check if we're in a parse error context that looks like an incomplete expression
        if (node.type.name === '⚠') {
            // Walk up to see if we can find expression context
            let current: SyntaxNode | null = node;
            while (current) {
                const nodeText =
                    current.from !== undefined
                        ? textBefore.slice(Math.max(0, current.from - textBefore.length + current.from))
                        : '';

                if (nodeText.includes('${') && !nodeText.includes('}') && this.functionSupported) {
                    // Incomplete expression
                    if (nodeText.includes(':')) {
                        return { detected: true, type: 'chained' };
                    } else {
                        return { detected: true, type: 'standalone' };
                    }
                } else if (nodeText.includes('#{') && !nodeText.includes('}') && this.parametersSupported) {
                    return { detected: true, type: 'parameter' };
                }
                current = current.parent;
            }
        }

        return { detected: false };
    }

    /**
     * Enhanced error node analysis: Determines if an error node represents an incomplete expression
     * by analyzing the parse tree structure and the text content contextually.
     */
    private isErrorNodeInExpressionContext(
        node: SyntaxNode,
        expectedType: 'standalone' | 'chained',
        context?: CompletionContext
    ): boolean {
        if (node.type.name !== '⚠' || !context) {
            return false;
        }

        const cursorPos = context.pos;
        const doc = context.state.doc;
        const fullText = doc.sliceString(0, doc.length);

        // Find all expression patterns in the text and check if cursor is within one
        const expressionRegex = /\$\{[^}]*\}/g;
        let match;

        while ((match = expressionRegex.exec(fullText)) !== null) {
            const exprStart = match.index;
            const exprEnd = exprStart + match[0].length;

            // Check if cursor is within this expression bounds
            if (cursorPos > exprStart && cursorPos < exprEnd) {
                const expressionText = match[0];

                if (expectedType === 'standalone') {
                    // Standalone function: has ${ but no colon, or colon is after our cursor position
                    return (
                        !expressionText.includes(':') ||
                        !this.hasColonBeforeCursorInExpression(expressionText, cursorPos - exprStart)
                    );
                } else if (expectedType === 'chained') {
                    // Chained function: has ${ and colon before our position within this specific expression
                    return this.hasColonBeforeCursorInExpression(expressionText, cursorPos - exprStart);
                }
            }
        }

        return false;
    }

    private hasColonBeforeCursorInExpression(expressionText: string, relativePos: number): boolean {
        // Check if there's a colon before the relative cursor position within this expression
        const textBeforeCursor = expressionText.slice(0, relativePos);
        return textBeforeCursor.includes(':');
    }

    private isErrorNodeInParameterContext(node: SyntaxNode, context?: CompletionContext): boolean {
        if (node.type.name !== '⚠') {
            return false;
        }

        // Walk up the tree to find parameter context
        let current: SyntaxNode | null = node;
        while (current) {
            // Look for ParameterStart (#) in the ancestry
            if (this.nodeContainsParameterStart(current)) {
                const nodeText = this.getNodeText(current, context);
                // Parameter reference: has #{ pattern
                return nodeText.includes('#{');
            }
            current = current.parent;
        }

        return false;
    }

    private isImmediatelyAfterColonInExpression(node: SyntaxNode, context?: CompletionContext): boolean {
        if (!context) {
            return false;
        }

        const cursorPos = context.pos;
        const doc = context.state.doc;

        // Get more context around the cursor position
        const textBefore = doc.sliceString(Math.max(0, cursorPos - 20), cursorPos);

        // Check if the cursor is immediately after a colon (allowing for whitespace)
        const immediateColonPattern = /:[\s]*$/;
        if (immediateColonPattern.test(textBefore)) {
            // Find the nearest expression start to the left of cursor
            const fullTextBefore = doc.sliceString(0, cursorPos);
            const lastExpressionStart = fullTextBefore.lastIndexOf('${');

            if (lastExpressionStart !== -1) {
                // Check if there's a closing brace between the expression start and cursor
                const textFromExprStart = fullTextBefore.slice(lastExpressionStart);
                const hasClosingBrace = textFromExprStart.includes('}');

                if (!hasClosingBrace) {
                    // We're inside an incomplete expression and immediately after a colon
                    return true;
                }
            }
        }

        // Also check if the current node is a colon in a ReferenceOrFunction
        if (node.type.name === ':' && node.parent?.type.name === 'ReferenceOrFunction') {
            return true;
        }

        return false;
    }

    private isCursorInFunctionName(node: SyntaxNode, context?: CompletionContext): boolean {
        if (!context) {
            return false;
        }

        // For a FunctionCall node, check if the cursor is in the function name part
        // (not in the parentheses or arguments)
        const cursorPos = context.pos;
        const nodeText = this.getNodeText(node, context);

        // Find the opening parenthesis to determine where the function name ends
        const parenIndex = nodeText.indexOf('(');
        if (parenIndex === -1) {
            // No parenthesis found, cursor might be in function name
            return true;
        }

        // Check if cursor is before the opening parenthesis (in function name area)
        const functionNameEnd = node.from + parenIndex;
        return cursorPos <= functionNameEnd;
    }

    private nodeContainsParameterStart(node: SyntaxNode): boolean {
        // Check if this node or its children contain ParameterStart
        const cursor = node.cursor();
        do {
            if (cursor.type.name === 'ParameterStart') {
                return true;
            }
        } while (cursor.next());
        return false;
    }

    private getNodeText(node: SyntaxNode, context?: CompletionContext): string {
        // Get the text content of a node from the document
        // Priority 1: Use the editor state if available (runtime environment)
        if (context && context.state) {
            return context.state.doc.sliceString(node.from, node.to);
        }

        // Priority 2: Use node's text property if available (test environment)
        if ('text' in node && typeof node.text === 'string') {
            return node.text;
        }

        // Fallback: empty string
        return '';
    }

    private isInsideStringLiteral(node: SyntaxNode, textBefore: string): boolean {
        // Method 1: Check if current node or parent is a complete StringLiteral
        let current: SyntaxNode | null = node;
        while (current) {
            if (current.type.name === 'StringLiteral') {
                // Check if this string literal is inside a parameter reference
                // If so, we should allow autocompletion for parameter names
                if (this.isStringLiteralInParameterContext(current)) {
                    return false; // Don't suppress
                }

                return true; // Suppress
            }
            current = current.parent;
        }

        // Method 2: Pattern-based detection for incomplete strings
        // This is cleaner than the previous complex regex approach
        // Look for unclosed quotes at the end of the line before cursor
        const hasUnclosedSingleQuote = this.hasUnclosedQuote(textBefore, "'");
        const hasUnclosedDoubleQuote = this.hasUnclosedQuote(textBefore, '"');

        if (hasUnclosedSingleQuote || hasUnclosedDoubleQuote) {
            // Check if the unclosed quote is in a parameter context
            if (this.isUnclosedQuoteInParameterContext(textBefore)) {
                return false; // Don't suppress
            }

            return true; // Suppress
        }

        return false;
    }

    private hasUnclosedQuote(text: string, quoteChar: string): boolean {
        // Count quotes, accounting for escaped quotes
        let count = 0;
        let escaped = false;

        for (let i = 0; i < text.length; i++) {
            const char = text[i];

            if (escaped) {
                escaped = false;
                continue;
            }

            if (char === '\\') {
                escaped = true;
                continue;
            }

            if (char === quoteChar) {
                count++;
            }
        }

        // Odd count means unclosed quote
        return count % 2 === 1;
    }

    private isExpressionEscaped(textBefore: string): boolean {
        // Find the last occurrence of ${ in the text
        const lastExpressionStart = textBefore.lastIndexOf('${');
        if (lastExpressionStart === -1) {
            return false; // No expression found
        }

        // Count consecutive $ characters before the {
        let dollarCount = 0;
        let pos = lastExpressionStart - 1; // Start just before the $

        // Count backwards through consecutive $ characters
        while (pos >= 0 && textBefore[pos] === '$') {
            dollarCount++;
            pos--;
        }

        // Add 1 for the $ in ${
        dollarCount += 1;
        // Pairs of $$ escape each other, leaving the remainder
        // If remainder is even (including 0), the expression is escaped
        // If remainder is odd, the expression is valid
        const remainder = dollarCount % 2;
        return remainder === 0;
    }

    private isParameterEscaped(textBefore: string): boolean {
        // Find the last occurrence of #{ in the text
        const lastParamStart = textBefore.lastIndexOf('#{');
        if (lastParamStart === -1) {
            return false; // No parameter start found
        }

        // Count consecutive # characters immediately before the {
        // We consider sequences like '##{' where an even number of preceding # means escaped
        let hashCount = 0;
        let pos = lastParamStart - 1; // Start just before the # in #{
        while (pos >= 0 && textBefore[pos] === '#') {
            hashCount++;
            pos--;
        }

        // Add 1 for the # in #{
        hashCount += 1;
        const remainder = hashCount % 2;
        // Even means escaped (##{, ####{, ...), odd means real parameter start
        return remainder === 0;
    }

    private isStringLiteralInParameterContext(stringLiteralNode: SyntaxNode): boolean {
        // Walk up the parse tree to see if this StringLiteral is inside a ParameterReference
        let current: SyntaxNode | null = stringLiteralNode.parent;
        while (current) {
            if (current.type.name === 'ParameterReference') {
                return true;
            }
            current = current.parent;
        }
        return false;
    }

    private isUnclosedQuoteInParameterContext(textBefore: string): boolean {
        // Look for the pattern #{" or #{' indicating we're starting a quoted parameter name
        // This is a simple heuristic - check if the last #{  appears after the last }
        const lastParameterStart = textBefore.lastIndexOf('#{');
        const lastClosingBrace = textBefore.lastIndexOf('}');

        if (lastParameterStart === -1) {
            return false; // No parameter reference
        }

        // If there's a closing brace after the parameter start, we're not in a parameter
        if (lastClosingBrace > lastParameterStart) {
            return false;
        }

        // Check if there's a quote after the parameter start
        const textAfterParameter = textBefore.substring(lastParameterStart + 2); // Skip #{
        return textAfterParameter.includes('"') || textAfterParameter.includes("'");
    }

    private buildCompletionInfo(details: ElFunction | Parameter): HTMLElement {
        const host = document.createElement('div');
        host.className = 'nfel-completion-info-host';

        if ('args' in (details as any)) {
            const compRef = createComponent(ElFunctionTip, {
                environmentInjector: this.environmentInjector,
                hostElement: host
            });
            (compRef.instance as any).data = { elFunction: details };
            compRef.changeDetectorRef.detectChanges();
        } else {
            const compRef = createComponent(ParameterTip, {
                environmentInjector: this.environmentInjector,
                hostElement: host
            });
            (compRef.instance as any).data = { parameter: details };
            compRef.changeDetectorRef.detectChanges();
        }

        return host;
    }

    public getLanguageSupport(): LanguageSupport {
        return this.languageSupport;
    }

    public setLanguageOptions(options: NifiLanguageOptions): void {
        let configurationChanged = false;

        // Handle function enablement
        if (options.functionsEnabled !== undefined) {
            const wasEnabled = this.functionSupported;
            this.functionSupported = options.functionsEnabled;
            if (wasEnabled !== this.functionSupported) {
                configurationChanged = true;
            }
        }

        // Handle parameter enablement and data
        if (options.parametersEnabled !== undefined) {
            const wasEnabled = this.parametersSupported;
            this.parametersSupported = options.parametersEnabled;

            if (!this.parametersSupported) {
                // Clear parameter data when disabled
                this.parameters = [];
                this.parameterDetails = {};
            }

            if (wasEnabled !== this.parametersSupported) {
                configurationChanged = true;
            }
        }

        // Handle parameter data
        if (options.parameters !== undefined) {
            // Clear existing parameter data
            this.parameters = [];
            this.parameterDetails = {};

            // Set new parameter data
            options.parameters.forEach((parameter) => {
                this.parameters.push(parameter.name);
                this.parameterDetails[parameter.name] = parameter;
            });

            // If parameters are being provided, ensure they're enabled
            if (options.parameters.length > 0 && options.parametersEnabled === undefined) {
                this.parametersSupported = true;
                configurationChanged = true;
            }
        }

        // Update language configuration only if something changed
        if (configurationChanged) {
            this.updateLanguageConfiguration();
        }
    }

    public supportsEl(): boolean {
        return this.functionSupported;
    }

    public supportsParameterReference(): boolean {
        return this.parametersSupported;
    }
}
