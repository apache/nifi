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
import org.apache.nifi.attribute.expression.language.evaluation.QueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.selection.AttributeEvaluator;
import org.apache.nifi.attribute.expression.language.exception.AttributeExpressionLanguageParsingException;
import org.apache.nifi.expression.AttributeExpression.ResultType;
import org.apache.nifi.expression.AttributeValueDecorator;
import org.apache.nifi.processor.exception.ProcessException;

import java.util.ArrayList;
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
        final List<Range> ranges = new ArrayList<>();
        char lastChar = 0;
        int embeddedCount = 0;
        int expressionStart = -1;
        boolean oddDollarCount = false;
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
                    if (oddDollarCount && lastChar == '$') {
                        if (embeddedCount == 0) {
                            expressionStart = i - 1;
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
                    oddDollarCount = !oddDollarCount;
                } else if (c == '\\') {
                    backslashCount++;
                } else {
                    oddDollarCount = false;
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

    static String evaluateExpression(final Tree tree, final String queryText, final Map<String, String> valueMap, final AttributeValueDecorator decorator,
                                     final Map<String, String> stateVariables) throws ProcessException {
        final Object evaluated = Query.fromTree(tree, queryText).evaluate(valueMap, stateVariables).getValue();
        if (evaluated == null) {
            return null;
        }

        final String value = evaluated.toString();
        return decorator == null ? value : decorator.decorate(value);
    }

    static String evaluateExpressions(final String rawValue, Map<String, String> expressionMap, final AttributeValueDecorator decorator, final Map<String, String> stateVariables)
            throws ProcessException {
        return Query.prepare(rawValue).evaluateExpressions(expressionMap, decorator, stateVariables);
    }

    static String evaluateExpressions(final String rawValue, final Map<String, String> valueLookup) throws ProcessException {
        return evaluateExpressions(rawValue, valueLookup, null);
    }

    static String evaluateExpressions(final String rawValue, final Map<String, String> valueLookup, final AttributeValueDecorator decorator) throws ProcessException {
        return Query.prepare(rawValue).evaluateExpressions(valueLookup, decorator);
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


    public static PreparedQuery prepare(final String query) throws AttributeExpressionLanguageParsingException {
        if (query == null) {
            return new EmptyPreparedQuery(null);
        }

        final List<Range> ranges = extractExpressionRanges(query);

        if (ranges.isEmpty()) {
            // While in the other cases below, we are simply replacing "$$" with "$", we have to do this
            // a bit differently. We want to treat $$ as an escaped $ only if it immediately precedes the
            // start of an Expression, which is the case below. Here, we did not detect the start of an Expression
            // and as such as must use the #unescape method instead of a simple replace() function.
            return new EmptyPreparedQuery(unescape(query));
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

                    expressions.add(new StringLiteralExpression(substring));
                }

                expressions.add(compiledExpression);

                lastIndex = range.getEnd() + 1;
            }

            final Range lastRange = ranges.get(ranges.size() - 1);
            if (lastRange.getEnd() + 1 < query.length()) {
                final String treeText = unescapeLeadingDollarSigns(query.substring(lastRange.getEnd() + 1));
                expressions.add(new StringLiteralExpression(treeText));
            }

            return new StandardPreparedQuery(expressions);
        } catch (final AttributeExpressionLanguageParsingException e) {
            return new InvalidPreparedQuery(query, e.getMessage());
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

    QueryResult<?> evaluate(final Map<String, String> map) {
        return evaluate(map, null);
    }

    QueryResult<?> evaluate(final Map<String, String> attributes, final Map<String, String> stateMap) {
        if (evaluated.getAndSet(true)) {
            throw new IllegalStateException("A Query cannot be evaluated more than once");
        }
        if (stateMap != null) {
            AttributesAndState attributesAndState = new AttributesAndState(attributes, stateMap);
            return evaluator.evaluate(attributesAndState);
        } else {
            return evaluator.evaluate(attributes);
        }
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
