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
package org.apache.nifi.attribute.expression.language;

import org.antlr.runtime.tree.Tree;
import org.apache.nifi.attribute.expression.language.compile.ExpressionCompiler;
import org.apache.nifi.attribute.expression.language.evaluation.Evaluator;
import org.apache.nifi.attribute.expression.language.evaluation.EvaluatorState;
import org.apache.nifi.attribute.expression.language.evaluation.QueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.selection.AttributeEvaluator;
import org.apache.nifi.attribute.expression.language.exception.AttributeExpressionLanguageParsingException;
import org.apache.nifi.expression.AttributeExpression.ResultType;
import org.apache.nifi.expression.AttributeValueDecorator;
import org.apache.nifi.parameter.ExpressionLanguageAwareParameterParser;
import org.apache.nifi.parameter.ParameterLookup;
import org.apache.nifi.parameter.ParameterParser;
import org.apache.nifi.parameter.ParameterReference;
import org.apache.nifi.parameter.ParameterToken;
import org.apache.nifi.parameter.ParameterTokenList;
import org.apache.nifi.processor.exception.ProcessException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Class used for creating and evaluating NiFi Expression Language. Once a Query
 * has been created, it may be evaluated using the evaluate methods exactly
 * once.
 */
public class Query {

    private final String query;
    private final Tree tree;
    private final Evaluator<?> evaluator;
    private final AtomicBoolean evaluated = new AtomicBoolean(false);
    private final EvaluatorState context = new EvaluatorState();

    private Query(final String query, final Tree tree, final Evaluator<?> evaluator) {
        this.query = query;
        this.tree = tree;
        this.evaluator = evaluator;
    }

    public static boolean isValidExpression(final String value) {
        try {
            validateExpression(value, false);
            return true;
        } catch (final AttributeExpressionLanguageParsingException | ProcessException e) {
            return false;
        }
    }

    public static ResultType getResultType(final String value) throws AttributeExpressionLanguageParsingException {
        return Query.compile(value).getResultType();
    }

    public static List<ResultType> extractResultTypes(final String value) throws AttributeExpressionLanguageParsingException {
        final List<ResultType> types = new ArrayList<>();

        for (final Range range : extractExpressionRanges(value)) {
            final String text = value.substring(range.getStart(), range.getEnd() + 1);
            types.add(getResultType(text));
        }

        return types;
    }

    public static List<String> extractExpressions(final String value) throws AttributeExpressionLanguageParsingException {
        final List<String> expressions = new ArrayList<>();

        for (final Range range : extractExpressionRanges(value)) {
            expressions.add(value.substring(range.getStart(), range.getEnd() + 1));
        }

        return expressions;
    }


    public static List<Range> extractExpressionRanges(final String value) throws AttributeExpressionLanguageParsingException {
        return extractExpressionRanges(value, false);
    }

    public static List<Range> extractEscapedRanges(final String value) throws AttributeExpressionLanguageParsingException {
        return extractExpressionRanges(value, true);
    }

    private static List<Range> extractExpressionRanges(final String value, final boolean extractEscapeSequences) throws AttributeExpressionLanguageParsingException {
        final List<Range> ranges = new ArrayList<>();
        char lastChar = 0;
        int embeddedCount = 0;
        int expressionStart = -1;
        int dollarCount = 0;
        int backslashCount = 0;

        charLoop:
            for (int i = 0; i < value.length(); i++) {
                final char c = value.charAt(i);

                if (expressionStart > -1 && (c == '\'' || c == '"') && (lastChar != '\\' || backslashCount % 2 == 0)) {
                    final int endQuoteIndex = findEndQuoteChar(value, i);
                    if (endQuoteIndex < 0) {
                        break charLoop;
                    }

                    i = endQuoteIndex;
                    continue;
                }

                if (c == '{') {
                    final boolean evenDollarCount = dollarCount % 2 == 0;
                    if ((evenDollarCount == extractEscapeSequences) && lastChar == '$') {
                        if (embeddedCount == 0) {
                            expressionStart = i - (extractEscapeSequences ? dollarCount : 1);
                        }
                    }

                    // Keep track of the number of opening curly braces that we are embedded within,
                    // if we are within an Expression. If we are outside of an Expression, we can just ignore
                    // curly braces. This allows us to ignore the first character if the value is something
                    // like: { ${abc} }
                    // However, we will count the curly braces if we have something like: ${ $${abc} }
                    if (expressionStart > -1) {
                        embeddedCount++;
                    }
                } else if (c == '}') {
                    if (embeddedCount <= 0) {
                        continue;
                    }

                    if (--embeddedCount == 0) {
                        if (expressionStart > -1) {
                            // ended expression. Add a new range.
                            final Range range = new Range(expressionStart, i);
                            ranges.add(range);
                        }

                        expressionStart = -1;
                    }
                } else if (c == '$') {
                    dollarCount++;
                } else if (c == '\\') {
                    backslashCount++;
                } else {
                    dollarCount = 0;
                }

                lastChar = c;
            }

        return ranges;
    }

    /**
     * @param value expression to validate
     * @param allowSurroundingCharacters whether to allow surrounding chars
     * @throws AttributeExpressionLanguageParsingException if problems parsing given expression
     */
    public static void validateExpression(final String value, final boolean allowSurroundingCharacters) throws AttributeExpressionLanguageParsingException {
        if (!allowSurroundingCharacters) {
            final List<Range> ranges = extractExpressionRanges(value);
            if (ranges.size() > 1) {
                throw new AttributeExpressionLanguageParsingException("Found multiple Expressions but expected only 1");
            }

            if (ranges.isEmpty()) {
                throw new AttributeExpressionLanguageParsingException("No Expressions found");
            }

            final Range range = ranges.get(0);
            final String expression = value.substring(range.getStart(), range.getEnd() + 1);
            Query.compile(expression);

            if (range.getStart() > 0 || range.getEnd() < value.length() - 1) {
                throw new AttributeExpressionLanguageParsingException("Found characters outside of Expression");
            }
        } else {
            for (final Range range : extractExpressionRanges(value)) {
                final String expression = value.substring(range.getStart(), range.getEnd() + 1);
                Query.compile(expression);
            }
        }
    }

    static int findEndQuoteChar(final String value, final int quoteStart) {
        final char quoteChar = value.charAt(quoteStart);

        int backslashCount = 0;
        char lastChar = 0;
        for (int i = quoteStart + 1; i < value.length(); i++) {
            final char c = value.charAt(i);

            if (c == '\\') {
                backslashCount++;
            } else if (c == quoteChar && (backslashCount % 2 == 0 || lastChar != '\\')) {
                return i;
            }

            lastChar = c;
        }

        return -1;
    }

    static String evaluateExpression(final Tree tree, final Evaluator<?> rootEvaluator, final String queryText, final EvaluationContext evaluationContext, final AttributeValueDecorator decorator)
                throws ProcessException {

        Query query = new Query(queryText, tree, rootEvaluator);
        final Object evaluated = query.evaluate(evaluationContext).getValue();
        if (evaluated == null) {
            return null;
        }

        final String value = evaluated.toString();
        return decorator == null ? value : decorator.decorate(value);
    }

    static String evaluateExpressions(final String rawValue, Map<String, String> expressionMap, final AttributeValueDecorator decorator, final Map<String, String> stateVariables,
                                      final ParameterLookup parameterLookup) throws ProcessException {
        return Query.prepare(rawValue).evaluateExpressions(new StandardEvaluationContext(expressionMap, stateVariables, parameterLookup), decorator);
    }

    static String evaluateExpressions(final String rawValue, final Map<String, String> valueLookup, final ParameterLookup parameterLookup) throws ProcessException {
        return evaluateExpressions(rawValue, valueLookup, null, parameterLookup);
    }

    static String evaluateExpressions(final String rawValue, final Map<String, String> valueLookup, final AttributeValueDecorator decorator, final ParameterLookup parameterLookup)
            throws ProcessException {
        return Query.prepare(rawValue).evaluateExpressions(new StandardEvaluationContext(valueLookup, Collections.emptyMap(), parameterLookup), decorator);
    }


    /**
     * Un-escapes ${...} patterns that were escaped
     *
     * @param value to un-escape
     * @return un-escaped value
     */
    public static String unescape(final String value) {
        return value.replaceAll("\\$\\$(?=\\$*\\{.*?\\})", "\\$");
    }

    public static Query fromTree(final Tree tree, final String text) {
        final ExpressionCompiler compiler = new ExpressionCompiler();
        return new Query(text, tree, compiler.buildEvaluator(tree));
    }

    private static String unescapeLeadingDollarSigns(final String value) {
        final int index = value.indexOf("{");
        if (index < 0) {
            return value.replace("$$", "$");
        } else {
            final String prefix = value.substring(0, index);
            return prefix.replace("$$", "$") + value.substring(index);
        }
    }

    private static String unescapeTrailingDollarSigns(final String value, final boolean escapeIfAllDollars) {
        if (!value.endsWith("$")) {
            return value;
        }

        // count number of $$ at end of string
        int dollars = 0;
        for (int i=value.length()-1; i >= 0; i--) {
            final char c = value.charAt(i);
            if (c == '$') {
                dollars++;
            } else {
                break;
            }
        }

        // If the given argument consists solely of $ characters, then we
        if (dollars == value.length() && !escapeIfAllDollars) {
            return value;
        }

        final int charsToRemove = dollars / 2;
        final int newLength = value.length() - charsToRemove;
        return value.substring(0, newLength);
    }


    public static PreparedQuery prepareWithParametersPreEvaluated(final String query) throws AttributeExpressionLanguageParsingException {
        return prepare(query, true);
    }

    public static PreparedQuery prepare(final String query) throws AttributeExpressionLanguageParsingException {
        return prepare(query, false);
    }

    private static PreparedQuery prepare(final String rawQuery, final boolean escapeParameterReferences) throws AttributeExpressionLanguageParsingException {
        if (rawQuery == null) {
            return new EmptyPreparedQuery(null);
        }

        final ParameterParser parameterParser = new ExpressionLanguageAwareParameterParser();

        final String query;
        if (escapeParameterReferences) {
            query = parameterParser.parseTokens(rawQuery).escape();
        } else {
            query = rawQuery;
        }

        final List<Range> ranges = extractExpressionRanges(query);

        if (ranges.isEmpty()) {
            final List<Expression> expressions = new ArrayList<>();

            final List<Range> escapedRanges = extractEscapedRanges(query);
            int lastIndex = 0;
            for (final Range range : escapedRanges) {
                final String treeText = unescapeLeadingDollarSigns(query.substring(range.getStart(), range.getEnd() + 1));

                if (range.getStart() > lastIndex) {
                    String substring = unescapeLeadingDollarSigns(query.substring(lastIndex, range.getStart()));
                    addLiteralsAndParameters(parameterParser, substring, expressions, true);
                }

                addLiteralsAndParameters(parameterParser, treeText, expressions, true);
                lastIndex = range.getEnd() + 1;
            }

            if (escapedRanges.isEmpty()) {
                addLiteralsAndParameters(parameterParser, query, expressions, true);
            } else {
                final Range lastRange = escapedRanges.get(escapedRanges.size() - 1);
                if (lastRange.getEnd() + 1 < query.length()) {
                    final String treeText = unescapeLeadingDollarSigns(query.substring(lastRange.getEnd() + 1));
                    addLiteralsAndParameters(parameterParser, treeText, expressions, true);
                }
            }

            if (expressions.isEmpty()) {
                return new EmptyPreparedQuery(query);
            }

            return new StandardPreparedQuery(expressions);
        }

        final ExpressionCompiler compiler = new ExpressionCompiler();

        try {
            final List<Expression> expressions = new ArrayList<>();

            int lastIndex = 0;
            for (final Range range : ranges) {
                final String treeText = unescapeLeadingDollarSigns(query.substring(range.getStart(), range.getEnd() + 1));
                final CompiledExpression compiledExpression = compiler.compile(treeText);

                if (range.getStart() > lastIndex) {
                    String substring = unescapeLeadingDollarSigns(query.substring(lastIndex, range.getStart()));

                    // If this string literal evaluator immediately precedes an Attribute Reference, then we need to consider the String Literal to be
                    // Escaping if it ends with $$'s, otherwise not. We also want to avoid un-escaping if the expression consists solely of $$, because
                    // those would have been addressed by the previous #unescapeLeadingDollarSigns() call already.
                    if (compiledExpression.getRootEvaluator() instanceof AttributeEvaluator) {
                        substring = unescapeTrailingDollarSigns(substring, false);
                    }

                    // Do not allow sensitive parameters to be referenced because this is within an actual Expression.
                    // For example, ${#{sensitiveParam}} is not allowed. However, we do support referencing sensitive parameters
                    // for the use case of simply #{sensitiveParam} outside of an Expression. In such a case, the PreparedQuery will
                    // still be used to evaluate this, since all Property Values are evaluated through PreparedQueries.
                    addLiteralsAndParameters(parameterParser, substring, expressions, false);
                }

                expressions.add(compiledExpression);

                lastIndex = range.getEnd() + 1;
            }

            final Range lastRange = ranges.get(ranges.size() - 1);
            if (lastRange.getEnd() + 1 < query.length()) {
                final String treeText = unescapeLeadingDollarSigns(query.substring(lastRange.getEnd() + 1));
                addLiteralsAndParameters(parameterParser, treeText, expressions, false);
            }

            return new StandardPreparedQuery(expressions);
        } catch (final AttributeExpressionLanguageParsingException e) {
            return new InvalidPreparedQuery(query, e.getMessage());
        }
    }

    private static void addLiteralsAndParameters(final ParameterParser parser, final String input, final List<Expression> expressions, final boolean allowSensitiveParameterReference) {
        final ParameterTokenList references = parser.parseTokens(input);
        int index = 0;

        ParameterToken lastReference = null;
        for (final ParameterToken token : references) {
            if (token.isEscapeSequence()) {
                expressions.add(new StringLiteralExpression(token.getValue(ParameterLookup.EMPTY)));
                index = token.getEndOffset() + 1;
                lastReference = token;
                continue;
            }

            final int start = token.getStartOffset();

            if (start > index) {
                expressions.add(new StringLiteralExpression(input.substring(index, start)));
            }

            if (token.isParameterReference()) {
                final ParameterReference parameterReference = (ParameterReference) token;
                expressions.add(new ParameterExpression(parameterReference.getParameterName(), allowSensitiveParameterReference));
            } else {
                expressions.add(new StringLiteralExpression(token.getValue(ParameterLookup.EMPTY)));
            }

            index = token.getEndOffset() + 1;
            lastReference = token;
        }

        if (lastReference == null) {
            expressions.add(new StringLiteralExpression(input));
        } else if (input.length() > lastReference.getEndOffset() + 1) {
            expressions.add(new StringLiteralExpression(input.substring(lastReference.getEndOffset() + 1)));
        }
    }

    public static Query compile(final String query) throws AttributeExpressionLanguageParsingException {
        try {
            final ExpressionCompiler compiler = new ExpressionCompiler();
            final CompiledExpression compiledExpression = compiler.compile(query);

            return new Query(compiledExpression.getExpression(), compiledExpression.getTree(), compiledExpression.getRootEvaluator());
        } catch (final AttributeExpressionLanguageParsingException e) {
            throw e;
        } catch (final Exception e) {
            throw new AttributeExpressionLanguageParsingException(e);
        }
    }

    public ResultType getResultType() {
        return evaluator.getResultType();
    }

    QueryResult<?> evaluate(final EvaluationContext evaluationContext) {
        if (evaluated.getAndSet(true)) {
            throw new IllegalStateException("A Query cannot be evaluated more than once");
        }

        return evaluator.evaluate(evaluationContext);
    }


    Tree getTree() {
        return this.tree;
    }

    @Override
    public String toString() {
        return "Query [" + query + "]";
    }



    public static class Range {

        private final int start;
        private final int end;

        public Range(final int start, final int end) {
            this.start = start;
            this.end = end;
        }

        public int getStart() {
            return start;
        }

        public int getEnd() {
            return end;
        }

        @Override
        public String toString() {
            return start + " - " + end;
        }
    }
}
