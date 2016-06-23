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

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionLexer;
import org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser;
import org.apache.nifi.attribute.expression.language.evaluation.BooleanEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.DateEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.Evaluator;
import org.apache.nifi.attribute.expression.language.evaluation.NumberEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.QueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.StringEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.cast.BooleanCastEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.cast.DateCastEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.cast.NumberCastEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.cast.StringCastEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.functions.AndEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.functions.AppendEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.functions.AttributeEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.functions.ContainsEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.functions.DateToNumberEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.functions.DivideEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.functions.EndsWithEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.functions.EqualsEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.functions.EqualsIgnoreCaseEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.functions.FindEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.functions.FormatEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.functions.GetDelimitedFieldEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.functions.GreaterThanEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.functions.GreaterThanOrEqualEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.functions.HostnameEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.functions.IPEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.functions.InEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.functions.IndexOfEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.functions.IsEmptyEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.functions.IsNullEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.functions.JsonPathEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.functions.LastIndexOfEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.functions.LengthEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.functions.LessThanEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.functions.LessThanOrEqualEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.functions.MatchesEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.functions.MinusEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.functions.ModEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.functions.MultiplyEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.functions.NotEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.functions.NotNullEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.functions.NowEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.functions.NumberToDateEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.functions.OneUpSequenceEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.functions.OrEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.functions.PlusEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.functions.PrependEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.functions.RandomNumberGeneratorEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.functions.ReplaceAllEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.functions.ReplaceEmptyEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.functions.ReplaceEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.functions.ReplaceFirstEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.functions.ReplaceNullEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.functions.StartsWithEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.functions.StringToDateEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.functions.SubstringAfterEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.functions.SubstringAfterLastEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.functions.SubstringBeforeEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.functions.SubstringBeforeLastEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.functions.SubstringEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.functions.ToLowerEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.functions.ToNumberEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.functions.ToRadixEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.functions.ToStringEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.functions.ToUpperEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.functions.TrimEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.functions.UrlDecodeEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.functions.UrlEncodeEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.functions.UuidEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.literals.BooleanLiteralEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.literals.NumberLiteralEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.literals.StringLiteralEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.literals.ToLiteralEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.reduce.CountEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.reduce.JoinEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.reduce.ReduceEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.selection.AllAttributesEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.selection.AnyAttributeEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.selection.DelineatedAttributeEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.selection.IteratingEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.selection.MultiAttributeEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.selection.MultiMatchAttributeEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.selection.MultiNamedAttributeEvaluator;
import org.apache.nifi.attribute.expression.language.exception.AttributeExpressionLanguageException;
import org.apache.nifi.attribute.expression.language.exception.AttributeExpressionLanguageParsingException;
import org.apache.nifi.expression.AttributeExpression.ResultType;
import org.apache.nifi.expression.AttributeValueDecorator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.exception.ProcessException;
import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CharStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.tree.Tree;

import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.ALL_ATTRIBUTES;
import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.ALL_DELINEATED_VALUES;
import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.ALL_MATCHING_ATTRIBUTES;
import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.AND;
import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.ANY_ATTRIBUTE;
import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.ANY_DELINEATED_VALUE;
import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.ANY_MATCHING_ATTRIBUTE;
import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.APPEND;
import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.ATTRIBUTE_REFERENCE;
import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.ATTR_NAME;
import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.CONTAINS;
import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.IN;
import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.COUNT;
import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.DIVIDE;
import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.ENDS_WITH;
import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.EQUALS;
import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.EQUALS_IGNORE_CASE;
import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.EXPRESSION;
import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.FALSE;
import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.FIND;
import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.FORMAT;
import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.GET_DELIMITED_FIELD;
import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.GREATER_THAN;
import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.GREATER_THAN_OR_EQUAL;
import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.HOSTNAME;
import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.INDEX_OF;
import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.IP;
import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.IS_EMPTY;
import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.IS_NULL;
import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.JOIN;
import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.JSON_PATH;
import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.LAST_INDEX_OF;
import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.LENGTH;
import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.LESS_THAN;
import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.LESS_THAN_OR_EQUAL;
import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.MATCHES;
import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.MINUS;
import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.MOD;
import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.MULTIPLY;
import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.MULTI_ATTRIBUTE_REFERENCE;
import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.NEXT_INT;
import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.NOT;
import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.NOT_NULL;
import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.NOW;
import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.NUMBER;
import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.OR;
import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.PLUS;
import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.PREPEND;
import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.REPLACE;
import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.RANDOM;
import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.REPLACE_ALL;
import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.REPLACE_EMPTY;
import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.REPLACE_FIRST;
import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.REPLACE_NULL;
import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.STARTS_WITH;
import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.STRING_LITERAL;
import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.SUBSTRING;
import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.SUBSTRING_AFTER;
import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.SUBSTRING_AFTER_LAST;
import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.SUBSTRING_BEFORE;
import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.SUBSTRING_BEFORE_LAST;
import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.TO_DATE;
import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.TO_LITERAL;
import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.TO_LOWER;
import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.TO_NUMBER;
import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.TO_RADIX;
import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.TO_STRING;
import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.TO_UPPER;
import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.TRIM;
import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.TRUE;
import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.URL_DECODE;
import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.URL_ENCODE;
import static org.apache.nifi.attribute.expression.language.antlr.AttributeExpressionParser.UUID;

import org.apache.nifi.attribute.expression.language.evaluation.selection.MappingEvaluator;
import org.apache.nifi.registry.VariableRegistry;

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

    static String evaluateExpression(final Tree tree, final String queryText, final VariableRegistry registry, final AttributeValueDecorator decorator) throws ProcessException {
        final Object evaluated = Query.fromTree(tree, queryText).evaluate(registry).getValue();
        if (evaluated == null) {
            return null;
        }

        final String value = evaluated.toString();
        final String escaped = value.replace("$$", "$");
        return decorator == null ? escaped : decorator.decorate(escaped);
    }

    static String evaluateExpressions(final String rawValue, VariableRegistry registry) throws ProcessException {
        return evaluateExpressions(rawValue, registry, null);
    }

    static String evaluateExpressions(final String rawValue, VariableRegistry registry, final AttributeValueDecorator decorator) throws ProcessException {
        return Query.prepare(rawValue).evaluateExpressions(registry, decorator);
    }

    private static Evaluator<?> getRootSubjectEvaluator(final Evaluator<?> evaluator) {
        if (evaluator == null) {
            return null;
        }

        final Evaluator<?> subject = evaluator.getSubjectEvaluator();
        if (subject == null) {
            return evaluator;
        }

        return getRootSubjectEvaluator(subject);
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
        return new Query(text, tree, buildEvaluator(tree));
    }

    public static Tree compileTree(final String query) throws AttributeExpressionLanguageParsingException {
        try {
            final CommonTokenStream lexerTokenStream = createTokenStream(query);
            final AttributeExpressionParser parser = new AttributeExpressionParser(lexerTokenStream);
            final Tree ast = (Tree) parser.query().getTree();
            final Tree tree = ast.getChild(0);

            // ensure that we are able to build the evaluators, so that we validate syntax
            final Evaluator<?> evaluator = buildEvaluator(tree);
            verifyMappingEvaluatorReduced(evaluator);
            return tree;
        } catch (final AttributeExpressionLanguageParsingException e) {
            throw e;
        } catch (final Exception e) {
            throw new AttributeExpressionLanguageParsingException(e);
        }
    }

    public static PreparedQuery prepare(final String query) throws AttributeExpressionLanguageParsingException {
        if (query == null) {
            return new EmptyPreparedQuery(null);
        }

        final List<Range> ranges = extractExpressionRanges(query);

        if (ranges.isEmpty()) {
            return new EmptyPreparedQuery(query.replace("$$", "$"));
        }

        try {
            final List<String> substrings = new ArrayList<>();
            final Map<String, Tree> trees = new HashMap<>();

            int lastIndex = 0;
            for (final Range range : ranges) {
                if (range.getStart() > lastIndex) {
                    substrings.add(query.substring(lastIndex, range.getStart()).replace("$$", "$"));
                    lastIndex = range.getEnd() + 1;
                }

                final String treeText = query.substring(range.getStart(), range.getEnd() + 1).replace("$$", "$");
                substrings.add(treeText);
                trees.put(treeText, Query.compileTree(treeText));
                lastIndex = range.getEnd() + 1;
            }

            final Range lastRange = ranges.get(ranges.size() - 1);
            if (lastRange.getEnd() + 1 < query.length()) {
                final String treeText = query.substring(lastRange.getEnd() + 1).replace("$$", "$");
                substrings.add(treeText);
            }

            return new StandardPreparedQuery(substrings, trees);
        } catch (final AttributeExpressionLanguageParsingException e) {
            return new InvalidPreparedQuery(query, e.getMessage());
        }
    }

    public static Query compile(final String query) throws AttributeExpressionLanguageParsingException {
        try {
            final CommonTokenStream lexerTokenStream = createTokenStream(query);
            final AttributeExpressionParser parser = new AttributeExpressionParser(lexerTokenStream);
            final Tree ast = (Tree) parser.query().getTree();
            final Tree tree = ast.getChild(0);

            final Evaluator<?> evaluator = buildEvaluator(tree);
            verifyMappingEvaluatorReduced(evaluator);

            return new Query(query, tree, evaluator);
        } catch (final AttributeExpressionLanguageParsingException e) {
            throw e;
        } catch (final Exception e) {
            throw new AttributeExpressionLanguageParsingException(e);
        }
    }

    private static void verifyMappingEvaluatorReduced(final Evaluator<?> evaluator) {
        final Evaluator<?> rightMostEvaluator;
        if (evaluator instanceof IteratingEvaluator) {
            rightMostEvaluator = ((IteratingEvaluator<?>) evaluator).getLogicEvaluator();
        } else {
            rightMostEvaluator = evaluator;
        }

        Evaluator<?> eval = rightMostEvaluator.getSubjectEvaluator();
        Evaluator<?> lastEval = rightMostEvaluator;
        while (eval != null) {
            if (eval instanceof ReduceEvaluator) {
                throw new AttributeExpressionLanguageParsingException("Expression attempts to call function '" + lastEval.getToken() + "' on the result of '" + eval.getToken() +
                    "'. This is not allowed. Instead, use \"${literal( ${<embedded expression>} ):" + lastEval.getToken() + "(...)}\"");
            }

            lastEval = eval;
            eval = eval.getSubjectEvaluator();
        }

        // if the result type of the evaluator is BOOLEAN, then it will always
        // be reduced when evaluator.
        final ResultType resultType = evaluator.getResultType();
        if (resultType == ResultType.BOOLEAN) {
            return;
        }

        final Evaluator<?> rootEvaluator = getRootSubjectEvaluator(evaluator);
        if (rootEvaluator != null && rootEvaluator instanceof MultiAttributeEvaluator) {
            final MultiAttributeEvaluator multiAttrEval = (MultiAttributeEvaluator) rootEvaluator;
            switch (multiAttrEval.getEvaluationType()) {
                case ALL_ATTRIBUTES:
                case ALL_MATCHING_ATTRIBUTES:
                case ALL_DELINEATED_VALUES: {
                    if (!(evaluator instanceof ReduceEvaluator)) {
                        throw new AttributeExpressionLanguageParsingException("Cannot evaluate expression because it attempts to reference multiple attributes but does not use a reducing function");
                    }
                    break;
                }
                default:
                    throw new AttributeExpressionLanguageParsingException("Cannot evaluate expression because it attempts to reference multiple attributes but does not use a reducing function");
            }
        }
    }

    private static CommonTokenStream createTokenStream(final String expression) throws AttributeExpressionLanguageParsingException {
        final CharStream input = new ANTLRStringStream(expression);
        final AttributeExpressionLexer lexer = new AttributeExpressionLexer(input);
        return new CommonTokenStream(lexer);
    }

    public ResultType getResultType() {
        return evaluator.getResultType();
    }

    QueryResult<?> evaluate(final VariableRegistry registry) {
        if (evaluated.getAndSet(true)) {
            throw new IllegalStateException("A Query cannot be evaluated more than once");
        }

        return evaluator.evaluate(registry.getVariables());
    }

    Tree getTree() {
        return this.tree;
    }

    @Override
    public String toString() {
        return "Query [" + query + "]";
    }

    private static Evaluator<String> newStringLiteralEvaluator(final String literalValue) {
        if (literalValue == null || literalValue.length() < 2) {
            return new StringLiteralEvaluator(literalValue);
        }

        final List<Range> ranges = extractExpressionRanges(literalValue);
        if (ranges.isEmpty()) {
            return new StringLiteralEvaluator(literalValue);
        }

        final List<Evaluator<?>> evaluators = new ArrayList<>();

        int lastIndex = 0;
        for (final Range range : ranges) {
            if (range.getStart() > lastIndex) {
                evaluators.add(newStringLiteralEvaluator(literalValue.substring(lastIndex, range.getStart())));
            }

            final String treeText = literalValue.substring(range.getStart(), range.getEnd() + 1);
            evaluators.add(buildEvaluator(compileTree(treeText)));
            lastIndex = range.getEnd() + 1;
        }

        final Range lastRange = ranges.get(ranges.size() - 1);
        if (lastRange.getEnd() + 1 < literalValue.length()) {
            final String treeText = literalValue.substring(lastRange.getEnd() + 1);
            evaluators.add(newStringLiteralEvaluator(treeText));
        }

        if (evaluators.size() == 1) {
            return toStringEvaluator(evaluators.get(0));
        }

        Evaluator<String> lastEvaluator = toStringEvaluator(evaluators.get(0));
        for (int i = 1; i < evaluators.size(); i++) {
            lastEvaluator = new AppendEvaluator(lastEvaluator, toStringEvaluator(evaluators.get(i)));
        }

        return lastEvaluator;
    }

    private static Evaluator<?> buildEvaluator(final Tree tree) {
        switch (tree.getType()) {
            case EXPRESSION: {
                return buildExpressionEvaluator(tree);
            }
            case ATTRIBUTE_REFERENCE: {
                final Evaluator<?> childEvaluator = buildEvaluator(tree.getChild(0));
                if (childEvaluator instanceof MultiAttributeEvaluator) {
                    return childEvaluator;
                }
                return new AttributeEvaluator(toStringEvaluator(childEvaluator));
            }
            case MULTI_ATTRIBUTE_REFERENCE: {

                final Tree functionTypeTree = tree.getChild(0);
                final int multiAttrType = functionTypeTree.getType();
                if (multiAttrType == ANY_DELINEATED_VALUE || multiAttrType == ALL_DELINEATED_VALUES) {
                    final Evaluator<String> delineatedValueEvaluator = toStringEvaluator(buildEvaluator(tree.getChild(1)));
                    final Evaluator<String> delimiterEvaluator = toStringEvaluator(buildEvaluator(tree.getChild(2)));

                    return new DelineatedAttributeEvaluator(delineatedValueEvaluator, delimiterEvaluator, multiAttrType);
                }

                final List<String> attributeNames = new ArrayList<>();
                for (int i = 1; i < tree.getChildCount(); i++) {  // skip the first child because that's the name of the multi-attribute function
                    attributeNames.add(newStringLiteralEvaluator(tree.getChild(i).getText()).evaluate(null).getValue());
                }

                switch (multiAttrType) {
                    case ALL_ATTRIBUTES:
                        for (final String attributeName : attributeNames) {
                            try {
                                FlowFile.KeyValidator.validateKey(attributeName);
                            } catch (final IllegalArgumentException iae) {
                                throw new AttributeExpressionLanguageParsingException("Invalid Attribute Name: " + attributeName + ". " + iae.getMessage());
                            }
                        }

                        return new MultiNamedAttributeEvaluator(attributeNames, ALL_ATTRIBUTES);
                    case ALL_MATCHING_ATTRIBUTES:
                        return new MultiMatchAttributeEvaluator(attributeNames, ALL_MATCHING_ATTRIBUTES);
                    case ANY_ATTRIBUTE:
                        for (final String attributeName : attributeNames) {
                            try {
                                FlowFile.KeyValidator.validateKey(attributeName);
                            } catch (final IllegalArgumentException iae) {
                                throw new AttributeExpressionLanguageParsingException("Invalid Attribute Name: " + attributeName + ". " + iae.getMessage());
                            }
                        }

                        return new MultiNamedAttributeEvaluator(attributeNames, ANY_ATTRIBUTE);
                    case ANY_MATCHING_ATTRIBUTE:
                        return new MultiMatchAttributeEvaluator(attributeNames, ANY_MATCHING_ATTRIBUTE);
                    default:
                        throw new AssertionError("Illegal Multi-Attribute Reference: " + functionTypeTree.toString());
                }
            }
            case ATTR_NAME: {
                return newStringLiteralEvaluator(tree.getChild(0).getText());
            }
            case NUMBER: {
                return new NumberLiteralEvaluator(tree.getText());
            }
            case STRING_LITERAL: {
                return newStringLiteralEvaluator(tree.getText());
            }
            case TRUE:
            case FALSE:
                return buildBooleanEvaluator(tree);
            case UUID: {
                return new UuidEvaluator();
            }
            case NOW: {
                return new NowEvaluator();
            }
            case TO_LITERAL: {
                final Evaluator<?> argEvaluator = buildEvaluator(tree.getChild(0));
                return new ToLiteralEvaluator(argEvaluator);
            }
            case IP: {
                try {
                    return new IPEvaluator();
                } catch (final UnknownHostException e) {
                    throw new AttributeExpressionLanguageException(e);
                }
            }
            case HOSTNAME: {
                if (tree.getChildCount() == 0) {
                    try {
                        return new HostnameEvaluator(false);
                    } catch (final UnknownHostException e) {
                        throw new AttributeExpressionLanguageException(e);
                    }
                } else if (tree.getChildCount() == 1) {
                    final Tree childTree = tree.getChild(0);
                    try {
                        switch (childTree.getType()) {
                            case TRUE:
                                return new HostnameEvaluator(true);
                            case FALSE:
                                return new HostnameEvaluator(false);
                            default:
                                throw new AttributeExpressionLanguageParsingException("Call to hostname() must take 0 or 1 (boolean) parameter");
                        }
                    } catch (final UnknownHostException e) {
                        throw new AttributeExpressionLanguageException(e);
                    }
                } else {
                    throw new AttributeExpressionLanguageParsingException("Call to hostname() must take 0 or 1 (boolean) parameter");
                }
            }
            case NEXT_INT: {
                return new OneUpSequenceEvaluator();
            }
            case RANDOM: {
                return new RandomNumberGeneratorEvaluator();
            }
            default:
                throw new AttributeExpressionLanguageParsingException("Unexpected token: " + tree.toString());
        }
    }

    private static <T> Evaluator<T> addToken(final Evaluator<T> evaluator, final String token) {
        evaluator.setToken(token);
        return evaluator;
    }

    private static Evaluator<Boolean> buildBooleanEvaluator(final Tree tree) {
        switch (tree.getType()) {
            case TRUE:
                return addToken(new BooleanLiteralEvaluator(true), "true");
            case FALSE:
                return addToken(new BooleanLiteralEvaluator(false), "true");
        }
        throw new AttributeExpressionLanguageParsingException("Cannot build Boolean evaluator from tree " + tree.toString());
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static Evaluator<?> buildExpressionEvaluator(final Tree tree) {
        if (tree.getChildCount() == 0) {
            throw new AttributeExpressionLanguageParsingException("EXPRESSION tree node has no children");
        }

        final Evaluator<?> evaluator;
        if (tree.getChildCount() == 1) {
            evaluator = buildEvaluator(tree.getChild(0));
        } else {
            // we can chain together functions in the form of:
            // ${x:trim():substring(1,2):trim()}
            // in this case, the subject of the right-most function is the function to its left; its
            // subject is the function to its left (the first trim()), and its subject is the value of
            // the 'x' attribute. We accomplish this logic by iterating over all of the children of the
            // tree from the right-most child going left-ward.
            evaluator = buildFunctionExpressionEvaluator(tree, 0);
        }

        Evaluator<?> chosenEvaluator = evaluator;
        final Evaluator<?> rootEvaluator = getRootSubjectEvaluator(evaluator);
        if (rootEvaluator != null) {
            if (rootEvaluator instanceof MultiAttributeEvaluator) {
                final MultiAttributeEvaluator multiAttrEval = (MultiAttributeEvaluator) rootEvaluator;

                switch (multiAttrEval.getEvaluationType()) {
                    case ANY_ATTRIBUTE:
                    case ANY_MATCHING_ATTRIBUTE:
                    case ANY_DELINEATED_VALUE:
                        chosenEvaluator = new AnyAttributeEvaluator((BooleanEvaluator) evaluator, multiAttrEval);
                        break;
                    case ALL_ATTRIBUTES:
                    case ALL_MATCHING_ATTRIBUTES:
                    case ALL_DELINEATED_VALUES: {
                        final ResultType resultType = evaluator.getResultType();
                        if (resultType == ResultType.BOOLEAN) {
                            chosenEvaluator = new AllAttributesEvaluator((BooleanEvaluator) evaluator, multiAttrEval);
                        } else if (evaluator instanceof ReduceEvaluator) {
                            chosenEvaluator = new MappingEvaluator((ReduceEvaluator) evaluator, multiAttrEval);
                        } else {
                            throw new AttributeExpressionLanguageException("Cannot evaluate Expression because it attempts to reference multiple attributes but does not use a reducing function");
                        }
                        break;
                    }
                }

                switch (multiAttrEval.getEvaluationType()) {
                    case ANY_ATTRIBUTE:
                        chosenEvaluator.setToken("anyAttribute");
                        break;
                    case ANY_MATCHING_ATTRIBUTE:
                        chosenEvaluator.setToken("anyMatchingAttribute");
                        break;
                    case ANY_DELINEATED_VALUE:
                        chosenEvaluator.setToken("anyDelineatedValue");
                        break;
                    case ALL_ATTRIBUTES:
                        chosenEvaluator.setToken("allAttributes");
                        break;
                    case ALL_MATCHING_ATTRIBUTES:
                        chosenEvaluator.setToken("allMatchingAttributes");
                        break;
                    case ALL_DELINEATED_VALUES:
                        chosenEvaluator.setToken("allDelineatedValues");
                        break;
                }
            }
        }

        return chosenEvaluator;
    }

    private static Evaluator<?> buildFunctionExpressionEvaluator(final Tree tree, final int offset) {
        if (tree.getChildCount() == 0) {
            throw new AttributeExpressionLanguageParsingException("EXPRESSION tree node has no children");
        }
        final int firstChildIndex = tree.getChildCount() - offset - 1;
        if (firstChildIndex == 0) {
            return buildEvaluator(tree.getChild(0));
        }

        final Tree functionTree = tree.getChild(firstChildIndex);
        final Evaluator<?> subjectEvaluator = buildFunctionExpressionEvaluator(tree, offset + 1);

        final Tree functionNameTree = functionTree.getChild(0);
        final List<Evaluator<?>> argEvaluators = new ArrayList<>();
        for (int i = 1; i < functionTree.getChildCount(); i++) {
            argEvaluators.add(buildEvaluator(functionTree.getChild(i)));
        }
        return buildFunctionEvaluator(functionNameTree, subjectEvaluator, argEvaluators);
    }

    private static List<Evaluator<?>> verifyArgCount(final List<Evaluator<?>> args, final int count, final String functionName) {
        if (args.size() != count) {
            throw new AttributeExpressionLanguageParsingException(functionName + "() function takes " + count + " arguments");
        }
        return args;
    }

    private static Evaluator<String> toStringEvaluator(final Evaluator<?> evaluator) {
        return toStringEvaluator(evaluator, null);
    }

    private static Evaluator<String> toStringEvaluator(final Evaluator<?> evaluator, final String location) {
        if (evaluator.getResultType() == ResultType.STRING) {
            return (StringEvaluator) evaluator;
        }

        return addToken(new StringCastEvaluator(evaluator), evaluator.getToken());
    }

    @SuppressWarnings("unchecked")
    private static Evaluator<Boolean> toBooleanEvaluator(final Evaluator<?> evaluator, final String location) {
        switch (evaluator.getResultType()) {
            case BOOLEAN:
                return (Evaluator<Boolean>) evaluator;
            case STRING:
                return addToken(new BooleanCastEvaluator((StringEvaluator) evaluator), evaluator.getToken());
            default:
                throw new AttributeExpressionLanguageParsingException("Cannot implicitly convert Data Type " + evaluator.getResultType() + " to " + ResultType.BOOLEAN
                    + (location == null ? "" : " at location [" + location + "]"));
        }

    }

    private static Evaluator<Boolean> toBooleanEvaluator(final Evaluator<?> evaluator) {
        return toBooleanEvaluator(evaluator, null);
    }

    private static Evaluator<Long> toNumberEvaluator(final Evaluator<?> evaluator) {
        return toNumberEvaluator(evaluator, null);
    }

    @SuppressWarnings("unchecked")
    private static Evaluator<Long> toNumberEvaluator(final Evaluator<?> evaluator, final String location) {
        switch (evaluator.getResultType()) {
            case NUMBER:
                return (Evaluator<Long>) evaluator;
            case STRING:
                return addToken(new NumberCastEvaluator(evaluator), evaluator.getToken());
            case DATE:
                return addToken(new DateToNumberEvaluator((DateEvaluator) evaluator), evaluator.getToken());
            default:
                throw new AttributeExpressionLanguageParsingException("Cannot implicitly convert Data Type " + evaluator.getResultType() + " to " + ResultType.NUMBER
                    + (location == null ? "" : " at location [" + location + "]"));
        }
    }

    private static DateEvaluator toDateEvaluator(final Evaluator<?> evaluator) {
        return toDateEvaluator(evaluator, null);
    }

    private static DateEvaluator toDateEvaluator(final Evaluator<?> evaluator, final String location) {
        if (evaluator.getResultType() == ResultType.DATE) {
            return (DateEvaluator) evaluator;
        }

        return new DateCastEvaluator(evaluator);
    }

    private static Evaluator<?> buildFunctionEvaluator(final Tree tree, final Evaluator<?> subjectEvaluator, final List<Evaluator<?>> argEvaluators) {
        switch (tree.getType()) {
            case TRIM: {
                verifyArgCount(argEvaluators, 0, "trim");
                return addToken(new TrimEvaluator(toStringEvaluator(subjectEvaluator)), "trim");
            }
            case TO_STRING: {
                verifyArgCount(argEvaluators, 0, "toString");
                return addToken(new ToStringEvaluator(subjectEvaluator), "toString");
            }
            case TO_LOWER: {
                verifyArgCount(argEvaluators, 0, "toLower");
                return addToken(new ToLowerEvaluator(toStringEvaluator(subjectEvaluator)), "toLower");
            }
            case TO_UPPER: {
                verifyArgCount(argEvaluators, 0, "toUpper");
                return addToken(new ToUpperEvaluator(toStringEvaluator(subjectEvaluator)), "toUpper");
            }
            case URL_ENCODE: {
                verifyArgCount(argEvaluators, 0, "urlEncode");
                return addToken(new UrlEncodeEvaluator(toStringEvaluator(subjectEvaluator)), "urlEncode");
            }
            case URL_DECODE: {
                verifyArgCount(argEvaluators, 0, "urlDecode");
                return addToken(new UrlDecodeEvaluator(toStringEvaluator(subjectEvaluator)), "urlDecode");
            }
            case SUBSTRING_BEFORE: {
                verifyArgCount(argEvaluators, 1, "substringBefore");
                return addToken(new SubstringBeforeEvaluator(toStringEvaluator(subjectEvaluator),
                    toStringEvaluator(argEvaluators.get(0), "first argument to substringBefore")), "substringBefore");
            }
            case SUBSTRING_BEFORE_LAST: {
                verifyArgCount(argEvaluators, 1, "substringBeforeLast");
                return addToken(new SubstringBeforeLastEvaluator(toStringEvaluator(subjectEvaluator),
                    toStringEvaluator(argEvaluators.get(0), "first argument to substringBeforeLast")), "substringBeforeLast");
            }
            case SUBSTRING_AFTER: {
                verifyArgCount(argEvaluators, 1, "substringAfter");
                return addToken(new SubstringAfterEvaluator(toStringEvaluator(subjectEvaluator),
                    toStringEvaluator(argEvaluators.get(0), "first argument to substringAfter")), "substringAfter");
            }
            case SUBSTRING_AFTER_LAST: {
                verifyArgCount(argEvaluators, 1, "substringAfterLast");
                return addToken(new SubstringAfterLastEvaluator(toStringEvaluator(subjectEvaluator),
                    toStringEvaluator(argEvaluators.get(0), "first argument to substringAfterLast")), "substringAfterLast");
            }
            case REPLACE_NULL: {
                verifyArgCount(argEvaluators, 1, "replaceNull");
                return addToken(new ReplaceNullEvaluator(toStringEvaluator(subjectEvaluator),
                    toStringEvaluator(argEvaluators.get(0), "first argument to replaceNull")), "replaceNull");
            }
            case REPLACE_EMPTY: {
                verifyArgCount(argEvaluators, 1, "replaceEmtpy");
                return addToken(new ReplaceEmptyEvaluator(toStringEvaluator(subjectEvaluator), toStringEvaluator(argEvaluators.get(0), "first argument to replaceEmpty")), "replaceEmpty");
            }
            case REPLACE: {
                verifyArgCount(argEvaluators, 2, "replace");
                return addToken(new ReplaceEvaluator(toStringEvaluator(subjectEvaluator),
                    toStringEvaluator(argEvaluators.get(0), "first argument to replace"),
                    toStringEvaluator(argEvaluators.get(1), "second argument to replace")), "replace");
            }
            case REPLACE_FIRST: {
                verifyArgCount(argEvaluators, 2, "replaceFirst");
                return addToken(new ReplaceFirstEvaluator(toStringEvaluator(subjectEvaluator),
                        toStringEvaluator(argEvaluators.get(0), "first argument to replaceFirst"),
                        toStringEvaluator(argEvaluators.get(1), "second argument to replaceFirst")), "replaceFirst");
            }
            case REPLACE_ALL: {
                verifyArgCount(argEvaluators, 2, "replaceAll");
                return addToken(new ReplaceAllEvaluator(toStringEvaluator(subjectEvaluator),
                    toStringEvaluator(argEvaluators.get(0), "first argument to replaceAll"),
                    toStringEvaluator(argEvaluators.get(1), "second argument to replaceAll")), "replaceAll");
            }
            case APPEND: {
                verifyArgCount(argEvaluators, 1, "append");
                return addToken(new AppendEvaluator(toStringEvaluator(subjectEvaluator),
                    toStringEvaluator(argEvaluators.get(0), "first argument to append")), "append");
            }
            case PREPEND: {
                verifyArgCount(argEvaluators, 1, "prepend");
                return addToken(new PrependEvaluator(toStringEvaluator(subjectEvaluator),
                    toStringEvaluator(argEvaluators.get(0), "first argument to prepend")), "prepend");
            }
            case SUBSTRING: {
                final int numArgs = argEvaluators.size();
                if (numArgs == 1) {
                    return addToken(new SubstringEvaluator(toStringEvaluator(subjectEvaluator),
                        toNumberEvaluator(argEvaluators.get(0), "first argument to substring")), "substring");
                } else if (numArgs == 2) {
                    return addToken(new SubstringEvaluator(toStringEvaluator(subjectEvaluator),
                        toNumberEvaluator(argEvaluators.get(0), "first argument to substring"),
                        toNumberEvaluator(argEvaluators.get(1), "second argument to substring")), "substring");
                } else {
                    throw new AttributeExpressionLanguageParsingException("substring() function can take either 1 or 2 arguments but cannot take " + numArgs + " arguments");
                }
            }
            case JOIN: {
                verifyArgCount(argEvaluators, 1, "join");
                return addToken(new JoinEvaluator(toStringEvaluator(subjectEvaluator), toStringEvaluator(argEvaluators.get(0))), "join");
            }
            case COUNT: {
                verifyArgCount(argEvaluators, 0, "count");
                return addToken(new CountEvaluator(subjectEvaluator), "count");
            }
            case IS_NULL: {
                verifyArgCount(argEvaluators, 0, "isNull");
                return addToken(new IsNullEvaluator(toStringEvaluator(subjectEvaluator)), "isNull");
            }
            case IS_EMPTY: {
                verifyArgCount(argEvaluators, 0, "isEmpty");
                return addToken(new IsEmptyEvaluator(toStringEvaluator(subjectEvaluator)), "isEmpty");
            }
            case NOT_NULL: {
                verifyArgCount(argEvaluators, 0, "notNull");
                return addToken(new NotNullEvaluator(toStringEvaluator(subjectEvaluator)), "notNull");
            }
            case STARTS_WITH: {
                verifyArgCount(argEvaluators, 1, "startsWith");
                return addToken(new StartsWithEvaluator(toStringEvaluator(subjectEvaluator),
                    toStringEvaluator(argEvaluators.get(0), "first argument to startsWith")), "startsWith");
            }
            case ENDS_WITH: {
                verifyArgCount(argEvaluators, 1, "endsWith");
                return addToken(new EndsWithEvaluator(toStringEvaluator(subjectEvaluator),
                    toStringEvaluator(argEvaluators.get(0), "first argument to endsWith")), "endsWith");
            }
            case CONTAINS: {
                verifyArgCount(argEvaluators, 1, "contains");
                return addToken(new ContainsEvaluator(toStringEvaluator(subjectEvaluator),
                    toStringEvaluator(argEvaluators.get(0), "first argument to contains")), "contains");
            }
            case IN: {
                List<Evaluator<String>> list = new ArrayList<Evaluator<String>>();
                for(int i = 0; i < argEvaluators.size(); i++) {
                    list.add(toStringEvaluator(argEvaluators.get(i), i + "th argument to in"));
                }
                return addToken(new InEvaluator(toStringEvaluator(subjectEvaluator), list), "in");
            }
            case FIND: {
                verifyArgCount(argEvaluators, 1, "find");
                return addToken(new FindEvaluator(toStringEvaluator(subjectEvaluator),
                    toStringEvaluator(argEvaluators.get(0), "first argument to find")), "find");
            }
            case MATCHES: {
                verifyArgCount(argEvaluators, 1, "matches");
                return addToken(new MatchesEvaluator(toStringEvaluator(subjectEvaluator),
                    toStringEvaluator(argEvaluators.get(0), "first argument to matches")), "matches");
            }
            case EQUALS: {
                verifyArgCount(argEvaluators, 1, "equals");
                return addToken(new EqualsEvaluator(subjectEvaluator, argEvaluators.get(0)), "equals");
            }
            case EQUALS_IGNORE_CASE: {
                verifyArgCount(argEvaluators, 1, "equalsIgnoreCase");
                return addToken(new EqualsIgnoreCaseEvaluator(toStringEvaluator(subjectEvaluator),
                    toStringEvaluator(argEvaluators.get(0), "first argument to equalsIgnoreCase")), "equalsIgnoreCase");
            }
            case GREATER_THAN: {
                verifyArgCount(argEvaluators, 1, "gt");
                return addToken(new GreaterThanEvaluator(toNumberEvaluator(subjectEvaluator),
                    toNumberEvaluator(argEvaluators.get(0), "first argument to gt")), "gt");
            }
            case GREATER_THAN_OR_EQUAL: {
                verifyArgCount(argEvaluators, 1, "ge");
                return addToken(new GreaterThanOrEqualEvaluator(toNumberEvaluator(subjectEvaluator),
                    toNumberEvaluator(argEvaluators.get(0), "first argument to ge")), "ge");
            }
            case LESS_THAN: {
                verifyArgCount(argEvaluators, 1, "lt");
                return addToken(new LessThanEvaluator(toNumberEvaluator(subjectEvaluator),
                    toNumberEvaluator(argEvaluators.get(0), "first argument to lt")), "lt");
            }
            case LESS_THAN_OR_EQUAL: {
                verifyArgCount(argEvaluators, 1, "le");
                return addToken(new LessThanOrEqualEvaluator(toNumberEvaluator(subjectEvaluator),
                    toNumberEvaluator(argEvaluators.get(0), "first argument to le")), "le");
            }
            case LENGTH: {
                verifyArgCount(argEvaluators, 0, "length");
                return addToken(new LengthEvaluator(toStringEvaluator(subjectEvaluator)), "length");
            }
            case TO_DATE: {
                if (argEvaluators.isEmpty()) {
                    return addToken(new NumberToDateEvaluator(toNumberEvaluator(subjectEvaluator)), "toDate");
                } else if (subjectEvaluator.getResultType() == ResultType.STRING) {
                    return addToken(new StringToDateEvaluator(toStringEvaluator(subjectEvaluator), toStringEvaluator(argEvaluators.get(0))), "toDate");
                } else {
                    return addToken(new NumberToDateEvaluator(toNumberEvaluator(subjectEvaluator)), "toDate");
                }
            }
            case TO_NUMBER: {
                verifyArgCount(argEvaluators, 0, "toNumber");
                switch (subjectEvaluator.getResultType()) {
                    case STRING:
                        return addToken(new ToNumberEvaluator((StringEvaluator) subjectEvaluator), "toNumber");
                    case DATE:
                        return addToken(new DateToNumberEvaluator((DateEvaluator) subjectEvaluator), "toNumber");
                    default:
                        throw new AttributeExpressionLanguageParsingException(subjectEvaluator + " returns type " + subjectEvaluator.getResultType() + " but expected to get " + ResultType.STRING);
                }
            }
            case TO_RADIX: {
                if (argEvaluators.size() == 1) {
                    return addToken(new ToRadixEvaluator((NumberEvaluator) subjectEvaluator, toNumberEvaluator(argEvaluators.get(0))), "toRadix");
                } else {
                    return addToken(new ToRadixEvaluator((NumberEvaluator) subjectEvaluator, toNumberEvaluator(argEvaluators.get(0)), toNumberEvaluator(argEvaluators.get(1))), "toRadix");
                }
            }
            case MOD: {
                return addToken(new ModEvaluator(toNumberEvaluator(subjectEvaluator), toNumberEvaluator(argEvaluators.get(0))), "mod");
            }
            case PLUS: {
                return addToken(new PlusEvaluator(toNumberEvaluator(subjectEvaluator), toNumberEvaluator(argEvaluators.get(0))), "plus");
            }
            case MINUS: {
                return addToken(new MinusEvaluator(toNumberEvaluator(subjectEvaluator), toNumberEvaluator(argEvaluators.get(0))), "minus");
            }
            case MULTIPLY: {
                return addToken(new MultiplyEvaluator(toNumberEvaluator(subjectEvaluator), toNumberEvaluator(argEvaluators.get(0))), "multiply");
            }
            case DIVIDE: {
                return addToken(new DivideEvaluator(toNumberEvaluator(subjectEvaluator), toNumberEvaluator(argEvaluators.get(0))), "divide");
            }
            case RANDOM : {
                return addToken(new RandomNumberGeneratorEvaluator(), "random");
            }
            case INDEX_OF: {
                verifyArgCount(argEvaluators, 1, "indexOf");
                return addToken(new IndexOfEvaluator(toStringEvaluator(subjectEvaluator),
                    toStringEvaluator(argEvaluators.get(0), "first argument to indexOf")), "indexOf");
            }
            case LAST_INDEX_OF: {
                verifyArgCount(argEvaluators, 1, "lastIndexOf");
                return addToken(new LastIndexOfEvaluator(toStringEvaluator(subjectEvaluator),
                    toStringEvaluator(argEvaluators.get(0), "first argument to lastIndexOf")), "lastIndexOf");
            }
            case FORMAT: {
                return addToken(new FormatEvaluator(toDateEvaluator(subjectEvaluator), toStringEvaluator(argEvaluators.get(0), "first argument of format")), "format");
            }
            case OR: {
                return addToken(new OrEvaluator(toBooleanEvaluator(subjectEvaluator), toBooleanEvaluator(argEvaluators.get(0))), "or");
            }
            case AND: {
                return addToken(new AndEvaluator(toBooleanEvaluator(subjectEvaluator), toBooleanEvaluator(argEvaluators.get(0))), "and");
            }
            case NOT: {
                return addToken(new NotEvaluator(toBooleanEvaluator(subjectEvaluator)), "not");
            }
            case GET_DELIMITED_FIELD: {
                if (argEvaluators.size() == 1) {
                    // Only a single argument - the index to return.
                    return addToken(new GetDelimitedFieldEvaluator(toStringEvaluator(subjectEvaluator),
                        toNumberEvaluator(argEvaluators.get(0), "first argument of getDelimitedField")), "getDelimitedField");
                } else if (argEvaluators.size() == 2) {
                    // two arguments - index and delimiter.
                    return addToken(new GetDelimitedFieldEvaluator(toStringEvaluator(subjectEvaluator),
                        toNumberEvaluator(argEvaluators.get(0), "first argument of getDelimitedField"),
                        toStringEvaluator(argEvaluators.get(1), "second argument of getDelimitedField")),
                        "getDelimitedField");
                } else if (argEvaluators.size() == 3) {
                    // 3 arguments - index, delimiter, quote char.
                    return addToken(new GetDelimitedFieldEvaluator(toStringEvaluator(subjectEvaluator),
                        toNumberEvaluator(argEvaluators.get(0), "first argument of getDelimitedField"),
                        toStringEvaluator(argEvaluators.get(1), "second argument of getDelimitedField"),
                        toStringEvaluator(argEvaluators.get(2), "third argument of getDelimitedField")),
                        "getDelimitedField");
                } else if (argEvaluators.size() == 4) {
                    // 4 arguments - index, delimiter, quote char, escape char
                    return addToken(new GetDelimitedFieldEvaluator(toStringEvaluator(subjectEvaluator),
                        toNumberEvaluator(argEvaluators.get(0), "first argument of getDelimitedField"),
                        toStringEvaluator(argEvaluators.get(1), "second argument of getDelimitedField"),
                        toStringEvaluator(argEvaluators.get(2), "third argument of getDelimitedField"),
                        toStringEvaluator(argEvaluators.get(3), "fourth argument of getDelimitedField")),
                        "getDelimitedField");
                } else {
                    // 5 arguments - index, delimiter, quote char, escape char, strip escape/quote chars flag
                    return addToken(new GetDelimitedFieldEvaluator(toStringEvaluator(subjectEvaluator),
                        toNumberEvaluator(argEvaluators.get(0), "first argument of getDelimitedField"),
                        toStringEvaluator(argEvaluators.get(1), "second argument of getDelimitedField"),
                        toStringEvaluator(argEvaluators.get(2), "third argument of getDelimitedField"),
                        toStringEvaluator(argEvaluators.get(3), "fourth argument of getDelimitedField"),
                        toBooleanEvaluator(argEvaluators.get(4), "fifth argument of getDelimitedField")),
                        "getDelimitedField");
                }
            }
            case JSON_PATH: {
                verifyArgCount(argEvaluators, 1, "jsonPath");
                return addToken(new JsonPathEvaluator(toStringEvaluator(subjectEvaluator),
                        toStringEvaluator(argEvaluators.get(0), "first argument to jsonPath")), "jsonPath");
            }
            default:
                throw new AttributeExpressionLanguageParsingException("Expected a Function-type expression but got " + tree.toString());
            }
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
