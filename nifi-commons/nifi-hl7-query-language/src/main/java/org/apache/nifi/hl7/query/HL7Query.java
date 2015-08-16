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
package org.apache.nifi.hl7.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CharStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.tree.Tree;
import org.apache.nifi.hl7.model.HL7Message;
import org.apache.nifi.hl7.query.evaluator.BooleanEvaluator;
import org.apache.nifi.hl7.query.evaluator.Evaluator;
import org.apache.nifi.hl7.query.evaluator.IntegerEvaluator;
import org.apache.nifi.hl7.query.evaluator.comparison.EqualsEvaluator;
import org.apache.nifi.hl7.query.evaluator.comparison.GreaterThanEvaluator;
import org.apache.nifi.hl7.query.evaluator.comparison.GreaterThanOrEqualEvaluator;
import org.apache.nifi.hl7.query.evaluator.comparison.IsNullEvaluator;
import org.apache.nifi.hl7.query.evaluator.comparison.LessThanEvaluator;
import org.apache.nifi.hl7.query.evaluator.comparison.LessThanOrEqualEvaluator;
import org.apache.nifi.hl7.query.evaluator.comparison.NotEqualsEvaluator;
import org.apache.nifi.hl7.query.evaluator.comparison.NotEvaluator;
import org.apache.nifi.hl7.query.evaluator.comparison.NotNullEvaluator;
import org.apache.nifi.hl7.query.evaluator.literal.IntegerLiteralEvaluator;
import org.apache.nifi.hl7.query.evaluator.literal.StringLiteralEvaluator;
import org.apache.nifi.hl7.query.evaluator.logic.AndEvaluator;
import org.apache.nifi.hl7.query.evaluator.logic.OrEvaluator;
import org.apache.nifi.hl7.query.evaluator.message.DeclaredReferenceEvaluator;
import org.apache.nifi.hl7.query.evaluator.message.DotEvaluator;
import org.apache.nifi.hl7.query.evaluator.message.MessageEvaluator;
import org.apache.nifi.hl7.query.evaluator.message.SegmentEvaluator;
import org.apache.nifi.hl7.query.exception.HL7QueryParsingException;
import org.apache.nifi.hl7.query.result.MissedResult;
import org.apache.nifi.hl7.query.result.StandardQueryResult;

import org.apache.nifi.hl7.query.antlr.HL7QueryLexer;
import org.apache.nifi.hl7.query.antlr.HL7QueryParser;
import static org.apache.nifi.hl7.query.antlr.HL7QueryParser.AND;
import static org.apache.nifi.hl7.query.antlr.HL7QueryParser.DECLARE;
import static org.apache.nifi.hl7.query.antlr.HL7QueryParser.DOT;
import static org.apache.nifi.hl7.query.antlr.HL7QueryParser.EQUALS;
import static org.apache.nifi.hl7.query.antlr.HL7QueryParser.GE;
import static org.apache.nifi.hl7.query.antlr.HL7QueryParser.GT;
import static org.apache.nifi.hl7.query.antlr.HL7QueryParser.IDENTIFIER;
import static org.apache.nifi.hl7.query.antlr.HL7QueryParser.IS_NULL;
import static org.apache.nifi.hl7.query.antlr.HL7QueryParser.LE;
import static org.apache.nifi.hl7.query.antlr.HL7QueryParser.LT;
import static org.apache.nifi.hl7.query.antlr.HL7QueryParser.MESSAGE;
import static org.apache.nifi.hl7.query.antlr.HL7QueryParser.NOT;
import static org.apache.nifi.hl7.query.antlr.HL7QueryParser.NOT_EQUALS;
import static org.apache.nifi.hl7.query.antlr.HL7QueryParser.NOT_NULL;
import static org.apache.nifi.hl7.query.antlr.HL7QueryParser.NUMBER;
import static org.apache.nifi.hl7.query.antlr.HL7QueryParser.OR;
import static org.apache.nifi.hl7.query.antlr.HL7QueryParser.REQUIRED;
import static org.apache.nifi.hl7.query.antlr.HL7QueryParser.SEGMENT_NAME;
import static org.apache.nifi.hl7.query.antlr.HL7QueryParser.SELECT;
import static org.apache.nifi.hl7.query.antlr.HL7QueryParser.STRING_LITERAL;
import static org.apache.nifi.hl7.query.antlr.HL7QueryParser.WHERE;

public class HL7Query {

    private final Tree tree;
    private final String query;
    private final Set<Declaration> declarations = new HashSet<>();

    private final List<Selection> selections;
    private final BooleanEvaluator whereEvaluator;

    private HL7Query(final Tree tree, final String query) {
        this.tree = tree;
        this.query = query;

        List<Selection> select = null;
        BooleanEvaluator where = null;
        for (int i = 0; i < tree.getChildCount(); i++) {
            final Tree child = tree.getChild(i);

            switch (child.getType()) {
                case DECLARE:
                    processDeclare(child);
                    break;
                case SELECT:
                    select = processSelect(child);
                    break;
                case WHERE:
                    where = processWhere(child);
                    break;
                default:
                    throw new HL7QueryParsingException("Found unexpected clause at root level: " + tree.getText());
            }
        }

        this.whereEvaluator = where;
        this.selections = select;
    }

    private void processDeclare(final Tree declare) {
        for (int i = 0; i < declare.getChildCount(); i++) {
            final Tree declarationTree = declare.getChild(i);

            final String identifier = declarationTree.getChild(0).getText();
            final Tree requiredOrOptionalTree = declarationTree.getChild(1);
            final boolean required = requiredOrOptionalTree.getType() == REQUIRED;

            final String segmentName = declarationTree.getChild(2).getText();

            final Declaration declaration = new Declaration() {
                @Override
                public String getAlias() {
                    return identifier;
                }

                @Override
                public boolean isRequired() {
                    return required;
                }

                @Override
                public Object getDeclaredValue(final HL7Message message) {
                    if (message == null) {
                        return null;
                    }

                    return message.getSegments(segmentName);
                }
            };

            declarations.add(declaration);
        }
    }

    private List<Selection> processSelect(final Tree select) {
        final List<Selection> selections = new ArrayList<>();

        for (int i = 0; i < select.getChildCount(); i++) {
            final Tree selectable = select.getChild(i);

            final String alias = getSelectedName(selectable);
            final Evaluator<?> selectionEvaluator = buildReferenceEvaluator(selectable);
            final Selection selection = new Selection(selectionEvaluator, alias);
            selections.add(selection);
        }

        return selections;
    }

    private String getSelectedName(final Tree selectable) {
        if (selectable.getChildCount() == 0) {
            return selectable.getText();
        } else if (selectable.getType() == DOT) {
            return getSelectedName(selectable.getChild(0)) + "." + getSelectedName(selectable.getChild(1));
        } else {
            return selectable.getChild(selectable.getChildCount() - 1).getText();
        }
    }

    private BooleanEvaluator processWhere(final Tree where) {
        return buildBooleanEvaluator(where.getChild(0));
    }

    private Evaluator<?> buildReferenceEvaluator(final Tree tree) {
        switch (tree.getType()) {
            case MESSAGE:
                return new MessageEvaluator();
            case SEGMENT_NAME:
                return new SegmentEvaluator(new StringLiteralEvaluator(tree.getText()));
            case IDENTIFIER:
                return new DeclaredReferenceEvaluator(new StringLiteralEvaluator(tree.getText()));
            case DOT:
                final Tree firstChild = tree.getChild(0);
                final Tree secondChild = tree.getChild(1);
                return new DotEvaluator(buildReferenceEvaluator(firstChild), buildIntegerEvaluator(secondChild));
            case STRING_LITERAL:
                return new StringLiteralEvaluator(tree.getText());
            case NUMBER:
                return new IntegerLiteralEvaluator(Integer.parseInt(tree.getText()));
            default:
                throw new HL7QueryParsingException("Failed to build evaluator for " + tree.getText());
        }
    }

    private IntegerEvaluator buildIntegerEvaluator(final Tree tree) {
        switch (tree.getType()) {
            case NUMBER:
                return new IntegerLiteralEvaluator(Integer.parseInt(tree.getText()));
            default:
                throw new HL7QueryParsingException("Failed to build Integer Evaluator for " + tree.getText());
        }
    }

    private BooleanEvaluator buildBooleanEvaluator(final Tree tree) {
        // TODO: add Date comparisons
        // LT/GT/GE/GE should allow for dates based on Field's Type
        // BETWEEN
        // DATE('2015/01/01')
        // DATE('2015/01/01 12:00:00')
        // DATE('24 HOURS AGO')
        // DATE('YESTERDAY')

        switch (tree.getType()) {
            case EQUALS:
                return new EqualsEvaluator(buildReferenceEvaluator(tree.getChild(0)), buildReferenceEvaluator(tree.getChild(1)));
            case NOT_EQUALS:
                return new NotEqualsEvaluator(buildReferenceEvaluator(tree.getChild(0)), buildReferenceEvaluator(tree.getChild(1)));
            case GT:
                return new GreaterThanEvaluator(buildReferenceEvaluator(tree.getChild(0)), buildReferenceEvaluator(tree.getChild(1)));
            case LT:
                return new LessThanEvaluator(buildReferenceEvaluator(tree.getChild(0)), buildReferenceEvaluator(tree.getChild(1)));
            case GE:
                return new GreaterThanOrEqualEvaluator(buildReferenceEvaluator(tree.getChild(0)), buildReferenceEvaluator(tree.getChild(1)));
            case LE:
                return new LessThanOrEqualEvaluator(buildReferenceEvaluator(tree.getChild(0)), buildReferenceEvaluator(tree.getChild(1)));
            case NOT:
                return new NotEvaluator(buildBooleanEvaluator(tree.getChild(0)));
            case AND:
                return new AndEvaluator(buildBooleanEvaluator(tree.getChild(0)), buildBooleanEvaluator(tree.getChild(1)));
            case OR:
                return new OrEvaluator(buildBooleanEvaluator(tree.getChild(0)), buildBooleanEvaluator(tree.getChild(1)));
            case IS_NULL:
                return new IsNullEvaluator(buildReferenceEvaluator(tree.getChild(0)));
            case NOT_NULL:
                return new NotNullEvaluator(buildReferenceEvaluator(tree.getChild(0)));
            default:
                throw new HL7QueryParsingException("Cannot build boolean evaluator for '" + tree.getText() + "'");
        }
    }

    Tree getTree() {
        return tree;
    }

    public String getQuery() {
        return query;
    }

    @Override
    public String toString() {
        return "HL7Query[" + query + "]";
    }

    public static HL7Query compile(final String query) {
        try {
            final CommonTokenStream lexerTokenStream = createTokenStream(query);
            final HL7QueryParser parser = new HL7QueryParser(lexerTokenStream);
            final Tree tree = (Tree) parser.query().getTree();

            return new HL7Query(tree, query);
        } catch (final HL7QueryParsingException e) {
            throw e;
        } catch (final Exception e) {
            throw new HL7QueryParsingException(e);
        }
    }

    private static CommonTokenStream createTokenStream(final String expression) throws HL7QueryParsingException {
        final CharStream input = new ANTLRStringStream(expression);
        final HL7QueryLexer lexer = new HL7QueryLexer(input);
        return new CommonTokenStream(lexer);
    }

    public List<Class<?>> getReturnTypes() {
        final List<Class<?>> returnTypes = new ArrayList<>();

        for (final Selection selection : selections) {
            returnTypes.add(selection.getEvaluator().getType());
        }

        return returnTypes;
    }

    @SuppressWarnings("unchecked")
    public QueryResult evaluate(final HL7Message message) {

        int totalIterations = 1;
        final LinkedHashMap<String, List<Object>> possibleValueMap = new LinkedHashMap<>();
        for (final Declaration declaration : declarations) {
            final Object value = declaration.getDeclaredValue(message);
            if (value == null && declaration.isRequired()) {
                return new MissedResult(selections);
            }

            final List<Object> possibleValues;
            if (value instanceof List) {
                possibleValues = (List<Object>) value;
            } else if (value instanceof Collection) {
                possibleValues = new ArrayList<Object>((Collection<Object>) value);
            } else {
                possibleValues = new ArrayList<>(1);
                possibleValues.add(value);
            }

            if (possibleValues.isEmpty()) {
                return new MissedResult(selections);
            }

            possibleValueMap.put(declaration.getAlias(), possibleValues);
            totalIterations *= possibleValues.size();
        }

        final Set<Map<String, Object>> resultSet = new HashSet<>();
        for (int i = 0; i < totalIterations; i++) {
            final Map<String, Object> aliasValues = assignAliases(possibleValueMap, i);

            aliasValues.put(Evaluator.MESSAGE_KEY, message);
            if (whereEvaluator == null || Boolean.TRUE.equals(whereEvaluator.evaluate(aliasValues))) {
                final Map<String, Object> resultMap = new HashMap<>();

                for (final Selection selection : selections) {
                    final Object value = selection.getEvaluator().evaluate(aliasValues);
                    resultMap.put(selection.getName(), value);
                }

                resultSet.add(resultMap);
            }
        }

        return new StandardQueryResult(selections, resultSet);
    }

    /**
     * assigns one of the possible values to each alias, based on which iteration this is.
     * require LinkedHashMap just to be very clear and explicit that the order of the Map MUST be guaranteed
     * between multiple invocations of this method.
     * package protected for testing visibility
     */
    static Map<String, Object> assignAliases(final LinkedHashMap<String, List<Object>> possibleValues, final int iteration) {
        final Map<String, Object> aliasMap = new HashMap<>();

        int divisor = 1;
        for (final Map.Entry<String, List<Object>> entry : possibleValues.entrySet()) {
            final String alias = entry.getKey();
            final List<Object> validValues = entry.getValue();

            final int idx = (iteration / divisor) % validValues.size();
            final Object obj = validValues.get(idx);
            aliasMap.put(alias, obj);

            divisor *= validValues.size();
        }

        return aliasMap;
    }

    public String toTreeString() {
        final StringBuilder sb = new StringBuilder();
        toTreeString(tree, sb, 0);
        return sb.toString();
    }

    private void toTreeString(final Tree tree, final StringBuilder sb, final int indentLevel) {
        final String nodeName = tree.getText();
        for (int i = 0; i < indentLevel; i++) {
            sb.append(" ");
        }
        sb.append(nodeName);
        sb.append("\n");

        for (int i = 0; i < tree.getChildCount(); i++) {
            final Tree child = tree.getChild(i);
            toTreeString(child, sb, indentLevel + 2);
        }
    }
}
