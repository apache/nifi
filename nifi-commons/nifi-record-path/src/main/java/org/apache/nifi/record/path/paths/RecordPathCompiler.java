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

package org.apache.nifi.record.path.paths;

import org.antlr.runtime.tree.Tree;
import org.apache.nifi.record.path.NumericRange;
import org.apache.nifi.record.path.exception.RecordPathException;
import org.apache.nifi.record.path.filter.Contains;
import org.apache.nifi.record.path.filter.ContainsRegex;
import org.apache.nifi.record.path.filter.EndsWith;
import org.apache.nifi.record.path.filter.EqualsFilter;
import org.apache.nifi.record.path.filter.GreaterThanFilter;
import org.apache.nifi.record.path.filter.GreaterThanOrEqualFilter;
import org.apache.nifi.record.path.filter.IsBlank;
import org.apache.nifi.record.path.filter.IsEmpty;
import org.apache.nifi.record.path.filter.LessThanFilter;
import org.apache.nifi.record.path.filter.LessThanOrEqualFilter;
import org.apache.nifi.record.path.filter.MatchesRegex;
import org.apache.nifi.record.path.filter.NotEqualsFilter;
import org.apache.nifi.record.path.filter.NotFilter;
import org.apache.nifi.record.path.filter.RecordPathFilter;
import org.apache.nifi.record.path.filter.StartsWith;
import org.apache.nifi.record.path.functions.Anchored;
import org.apache.nifi.record.path.functions.ArrayOf;
import org.apache.nifi.record.path.functions.Base64Decode;
import org.apache.nifi.record.path.functions.Base64Encode;
import org.apache.nifi.record.path.functions.Coalesce;
import org.apache.nifi.record.path.functions.Concat;
import org.apache.nifi.record.path.functions.Count;
import org.apache.nifi.record.path.functions.EscapeJson;
import org.apache.nifi.record.path.functions.FieldName;
import org.apache.nifi.record.path.functions.FilterFunction;
import org.apache.nifi.record.path.functions.Format;
import org.apache.nifi.record.path.functions.Hash;
import org.apache.nifi.record.path.functions.Join;
import org.apache.nifi.record.path.functions.MapOf;
import org.apache.nifi.record.path.functions.PadLeft;
import org.apache.nifi.record.path.functions.PadRight;
import org.apache.nifi.record.path.functions.RecordOf;
import org.apache.nifi.record.path.functions.Replace;
import org.apache.nifi.record.path.functions.ReplaceNull;
import org.apache.nifi.record.path.functions.ReplaceRegex;
import org.apache.nifi.record.path.functions.Substring;
import org.apache.nifi.record.path.functions.SubstringAfter;
import org.apache.nifi.record.path.functions.SubstringAfterLast;
import org.apache.nifi.record.path.functions.SubstringBefore;
import org.apache.nifi.record.path.functions.SubstringBeforeLast;
import org.apache.nifi.record.path.functions.ToBytes;
import org.apache.nifi.record.path.functions.ToDate;
import org.apache.nifi.record.path.functions.ToLowerCase;
import org.apache.nifi.record.path.functions.ToString;
import org.apache.nifi.record.path.functions.ToUpperCase;
import org.apache.nifi.record.path.functions.TrimString;
import org.apache.nifi.record.path.functions.UUID5;
import org.apache.nifi.record.path.functions.UnescapeJson;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

import static org.apache.nifi.record.path.RecordPathParser.ARRAY_INDEX;
import static org.apache.nifi.record.path.RecordPathParser.CHILD_REFERENCE;
import static org.apache.nifi.record.path.RecordPathParser.CURRENT_FIELD;
import static org.apache.nifi.record.path.RecordPathParser.DESCENDANT_REFERENCE;
import static org.apache.nifi.record.path.RecordPathParser.EQUAL;
import static org.apache.nifi.record.path.RecordPathParser.FIELD_NAME;
import static org.apache.nifi.record.path.RecordPathParser.FUNCTION;
import static org.apache.nifi.record.path.RecordPathParser.GREATER_THAN;
import static org.apache.nifi.record.path.RecordPathParser.GREATER_THAN_EQUAL;
import static org.apache.nifi.record.path.RecordPathParser.LESS_THAN;
import static org.apache.nifi.record.path.RecordPathParser.LESS_THAN_EQUAL;
import static org.apache.nifi.record.path.RecordPathParser.MAP_KEY;
import static org.apache.nifi.record.path.RecordPathParser.NOT_EQUAL;
import static org.apache.nifi.record.path.RecordPathParser.NUMBER;
import static org.apache.nifi.record.path.RecordPathParser.NUMBER_LIST;
import static org.apache.nifi.record.path.RecordPathParser.NUMBER_RANGE;
import static org.apache.nifi.record.path.RecordPathParser.PARENT_REFERENCE;
import static org.apache.nifi.record.path.RecordPathParser.PATH;
import static org.apache.nifi.record.path.RecordPathParser.PREDICATE;
import static org.apache.nifi.record.path.RecordPathParser.RELATIVE_PATH;
import static org.apache.nifi.record.path.RecordPathParser.ROOT_REFERENCE;
import static org.apache.nifi.record.path.RecordPathParser.STRING_LIST;
import static org.apache.nifi.record.path.RecordPathParser.STRING_LITERAL;
import static org.apache.nifi.record.path.RecordPathParser.WILDCARD;

public class RecordPathCompiler {

    public static RecordPathSegment compile(final Tree pathTree, final RecordPathSegment root, final boolean absolute) {
        if (pathTree.getType() == FUNCTION) {
            return buildPath(pathTree, null, absolute);
        }

        RecordPathSegment parent = root;
        for (int i = 0; i < pathTree.getChildCount(); i++) {
            final Tree child = pathTree.getChild(i);
            parent = buildPath(child, parent, absolute);
        }

        // If the given path tree is an operator, create a Filter Function that will be responsible for returning true/false based on the provided operation
        return switch (pathTree.getType()) {
            case EQUAL, NOT_EQUAL, LESS_THAN, LESS_THAN_EQUAL, GREATER_THAN, GREATER_THAN_EQUAL -> {
                final RecordPathFilter filter = createFilter(pathTree, null, absolute);
                yield new FilterFunction(pathTree.getText(), filter, absolute);
            }
            default -> parent;
        };

    }

    public static RecordPathSegment buildPath(final Tree tree, final RecordPathSegment parent, final boolean absolute) {
        switch (tree.getType()) {
            case ROOT_REFERENCE: {
                return new RootPath();
            }
            case CHILD_REFERENCE: {
                if (tree.getChildCount() == 0) {
                    return new RootPath();
                }

                final Tree childTree = tree.getChild(0);
                if (childTree == null) {
                    return new RootPath();
                }

                final int childTreeType = childTree.getType();
                if (childTreeType == FIELD_NAME) {
                    final String childName = childTree.getChild(0).getText();
                    return new ChildFieldPath(childName, parent, absolute);
                } else if (childTreeType == WILDCARD) {
                    return new WildcardChildPath(parent, absolute);
                } else {
                    throw new RecordPathException("Expected field name following '/' Token but found " + childTree);
                }
            }
            case ARRAY_INDEX: {
                final Tree indexListTree = tree.getChild(0);
                if (indexListTree.getType() == NUMBER_LIST) {
                    if (indexListTree.getChildCount() == 1 && indexListTree.getChild(0).getType() == NUMBER) {
                        final Tree indexTree = indexListTree.getChild(0);
                        final int index = Integer.parseInt(indexTree.getText());
                        return new ArrayIndexPath(index, parent, absolute);
                    }

                    final List<NumericRange> indexList = new ArrayList<>();

                    for (int i = 0; i < indexListTree.getChildCount(); i++) {
                        final Tree indexTree = indexListTree.getChild(i);
                        if (indexTree.getType() == NUMBER) {
                            final int index = Integer.valueOf(indexTree.getText());
                            indexList.add(new NumericRange(index, index));
                        } else if (indexTree.getType() == NUMBER_RANGE) {
                            final int min = Integer.valueOf(indexTree.getChild(0).getText());
                            final int max = Integer.valueOf(indexTree.getChild(1).getText());
                            indexList.add(new NumericRange(min, max));
                        } else {
                            throw new RecordPathException("Expected Number or Range following '[' Token but found " + indexTree);
                        }
                    }

                    return new MultiArrayIndexPath(indexList, parent, absolute);
                } else {
                    throw new RecordPathException("Expected Number or Range following '[' Token but found " + indexListTree);
                }
            }
            case MAP_KEY: {
                final Tree keyTree = tree.getChild(0);
                if (keyTree.getType() == STRING_LIST) {
                    if (keyTree.getChildCount() == 1) {
                        return new SingularMapKeyPath(keyTree.getChild(0).getText(), parent, absolute);
                    }

                    final List<String> keys = new ArrayList<>(keyTree.getChildCount());
                    for (int i = 0; i < keyTree.getChildCount(); i++) {
                        keys.add(keyTree.getChild(i).getText());
                    }

                    return new MultiMapKeyPath(keys, parent, absolute);
                } else {
                    throw new RecordPathException("Expected Map Key following '[' Token but found " + keyTree);
                }
            }
            case WILDCARD: {
                return new WildcardIndexPath(parent, absolute);
            }
            case DESCENDANT_REFERENCE: {
                final Tree childTree = tree.getChild(0);
                final int childTreeType = childTree.getType();
                if (childTreeType == FIELD_NAME) {
                    final String descendantName = childTree.getChild(0).getText();
                    return new DescendantFieldPath(descendantName, parent, absolute);
                } else if (childTreeType == WILDCARD) {
                    return new WildcardDescendantPath(parent, absolute);
                } else {
                    throw new RecordPathException("Expected field name following '//' Token but found " + childTree);
                }
            }
            case PARENT_REFERENCE: {
                return new ParentPath(parent, absolute);
            }
            case CURRENT_FIELD: {
                return new CurrentFieldPath(parent, absolute);
            }
            case STRING_LITERAL: {
                return new LiteralValuePath(parent, tree.getText(), absolute);
            }
            case NUMBER: {
                final long value = Long.parseLong(tree.getText());
                if (value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE) {
                    // Originally all numbers were treated as ints. Preserving this behavior for compatibility.
                    return new LiteralValuePath(parent, (int) value, absolute);
                }

                return new LiteralValuePath(parent, value, absolute);
            }
            case PREDICATE: {
                final Tree operatorTree = tree.getChild(0);
                final RecordPathFilter filter = createFilter(operatorTree, parent, absolute);
                return new PredicatePath(parent, filter, absolute);
            }
            case RELATIVE_PATH: {
                return compile(tree, parent, absolute);
            }
            case PATH: {
                return compile(tree, new RootPath(), absolute);
            }
            case FUNCTION: {
                final String functionName = tree.getChild(0).getText();
                final Tree argumentListTree = tree.getChild(1);

                switch (functionName) {
                    case "substring": {
                        final RecordPathSegment[] args = getArgPaths(argumentListTree, 3, functionName, absolute);
                        return new Substring(args[0], args[1], args[2], absolute);
                    }
                    case "substringAfter": {
                        final RecordPathSegment[] args = getArgPaths(argumentListTree, 2, functionName, absolute);
                        return new SubstringAfter(args[0], args[1], absolute);
                    }
                    case "substringAfterLast": {
                        final RecordPathSegment[] args = getArgPaths(argumentListTree, 2, functionName, absolute);
                        return new SubstringAfterLast(args[0], args[1], absolute);
                    }
                    case "substringBefore": {
                        final RecordPathSegment[] args = getArgPaths(argumentListTree, 2, functionName, absolute);
                        return new SubstringBefore(args[0], args[1], absolute);
                    }
                    case "substringBeforeLast": {
                        final RecordPathSegment[] args = getArgPaths(argumentListTree, 2, functionName, absolute);
                        return new SubstringBeforeLast(args[0], args[1], absolute);
                    }
                    case "replace": {
                        final RecordPathSegment[] args = getArgPaths(argumentListTree, 3, functionName, absolute);
                        return new Replace(args[0], args[1], args[2], absolute);
                    }
                    case "replaceRegex": {
                        final RecordPathSegment[] args = getArgPaths(argumentListTree, 3, functionName, absolute);
                        return new ReplaceRegex(args[0], args[1], args[2], absolute);
                    }
                    case "replaceNull": {
                        final RecordPathSegment[] args = getArgPaths(argumentListTree, 2, functionName, absolute);
                        return new ReplaceNull(args[0], args[1], absolute);
                    }
                    case "concat": {
                        final int numArgs = argumentListTree.getChildCount();

                        final RecordPathSegment[] argPaths = new RecordPathSegment[numArgs];
                        for (int i = 0; i < numArgs; i++) {
                            argPaths[i] = buildPath(argumentListTree.getChild(i), null, absolute);
                        }

                        return new Concat(argPaths, absolute);
                    }
                    case "arrayOf": {
                        final int numArgs = argumentListTree.getChildCount();

                        final RecordPathSegment[] argPaths = new RecordPathSegment[numArgs];
                        for (int i = 0; i < numArgs; i++) {
                            argPaths[i] = buildPath(argumentListTree.getChild(i), null, absolute);
                        }

                        return new ArrayOf(argPaths, absolute);
                    }
                    case "mapOf": {
                        final int numArgs = argumentListTree.getChildCount();

                        if (numArgs % 2 != 0) {
                            throw new RecordPathException("The mapOf function requires an even number of arguments");
                        }

                        final RecordPathSegment[] argPaths = new RecordPathSegment[numArgs];
                        for (int i = 0; i < numArgs; i++) {
                            argPaths[i] = buildPath(argumentListTree.getChild(i), null, absolute);
                        }

                        return new MapOf(argPaths, absolute);
                    }
                    case "recordOf": {
                        final int numArgs = argumentListTree.getChildCount();

                        if (numArgs % 2 != 0) {
                            throw new RecordPathException("The recordOf function requires an even number of arguments");
                        }

                        final RecordPathSegment[] argPaths = new RecordPathSegment[numArgs];
                        for (int i = 0; i < numArgs; i++) {
                            argPaths[i] = buildPath(argumentListTree.getChild(i), null, absolute);
                        }

                        return new RecordOf(argPaths, absolute);
                    }
                    case "toLowerCase": {
                        final RecordPathSegment[] args = getArgPaths(argumentListTree, 1, functionName, absolute);
                        return new ToLowerCase(args[0], absolute);
                    }
                    case "toUpperCase": {
                        final RecordPathSegment[] args = getArgPaths(argumentListTree, 1, functionName, absolute);
                        return new ToUpperCase(args[0], absolute);
                    }
                    case "trim": {
                        final RecordPathSegment[] args = getArgPaths(argumentListTree, 1, functionName, absolute);
                        return new TrimString(args[0], absolute);
                    }
                    case "fieldName": {
                        final RecordPathSegment[] args = getArgPaths(argumentListTree, 1, functionName, absolute);
                        return new FieldName(args[0], absolute);
                    }
                    case "toDate": {
                        final int numArgs = argumentListTree.getChildCount();

                        if (numArgs == 2) {
                            final RecordPathSegment[] args = getArgPaths(argumentListTree, 2, functionName, absolute);
                            return new ToDate(args[0], args[1], absolute);
                        } else {
                            final RecordPathSegment[] args = getArgPaths(argumentListTree, 3, functionName, absolute);
                            return new ToDate(args[0], args[1], args[2], absolute);
                        }
                    }
                    case "toString": {
                        final RecordPathSegment[] args = getArgPaths(argumentListTree, 2, functionName, absolute);
                        return new ToString(args[0], args[1], absolute);
                    }
                    case "toBytes": {
                        final RecordPathSegment[] args = getArgPaths(argumentListTree, 2, functionName, absolute);
                        return new ToBytes(args[0], args[1], absolute);
                    }
                    case "format": {
                        final int numArgs = argumentListTree.getChildCount();

                        if (numArgs == 2) {
                            final RecordPathSegment[] args = getArgPaths(argumentListTree, 2, functionName, absolute);
                            return new Format(args[0], args[1], absolute);
                        } else {
                            final RecordPathSegment[] args = getArgPaths(argumentListTree, 3, functionName, absolute);
                            return new Format(args[0], args[1], args[2], absolute);
                        }
                    }
                    case "base64Encode": {
                        final RecordPathSegment[] args = getArgPaths(argumentListTree, 1, functionName, absolute);
                        return new Base64Encode(args[0], absolute);
                    }
                    case "base64Decode": {
                        final RecordPathSegment[] args = getArgPaths(argumentListTree, 1, functionName, absolute);
                        return new Base64Decode(args[0], absolute);
                    }
                    case "escapeJson": {
                        final RecordPathSegment[] args = getArgPaths(argumentListTree, 1, functionName, absolute);
                        return new EscapeJson(args[0], absolute);
                    }
                    case "unescapeJson": {
                        final int numArgs = argumentListTree.getChildCount();

                        final RecordPathSegment[] args = getArgPaths(argumentListTree, numArgs, functionName, absolute);
                        final RecordPathSegment convertToRecord = numArgs > 1 ? args[1] : null;
                        final RecordPathSegment recursiveConversion = numArgs > 2 ? args[2] : null;

                        return new UnescapeJson(args[0], convertToRecord, recursiveConversion, absolute);
                    }
                    case "hash": {
                        final RecordPathSegment[] args = getArgPaths(argumentListTree, 2, functionName, absolute);
                        return new Hash(args[0], args[1], absolute);
                    }
                    case "padLeft": {
                        final int numArgs = argumentListTree.getChildCount();

                        if (numArgs == 2) {
                            final RecordPathSegment[] args = getArgPaths(argumentListTree, 2, functionName, absolute);
                            return new PadLeft(args[0], args[1], absolute);
                        } else {
                            final RecordPathSegment[] args = getArgPaths(argumentListTree, 3, functionName, absolute);
                            return new PadLeft(args[0], args[1], args[2], absolute);
                        }
                    }
                    case "padRight": {
                        final int numArgs = argumentListTree.getChildCount();

                        if (numArgs == 2) {
                            final RecordPathSegment[] args = getArgPaths(argumentListTree, 2, functionName, absolute);
                            return new PadRight(args[0], args[1], absolute);
                        } else {
                            final RecordPathSegment[] args = getArgPaths(argumentListTree, 3, functionName, absolute);
                            return new PadRight(args[0], args[1], args[2], absolute);
                        }
                    }
                    case "uuid5": {
                        final int numArgs = argumentListTree.getChildCount();
                        if (numArgs == 2) {
                            final RecordPathSegment[] args = getArgPaths(argumentListTree, 2, functionName, absolute);
                            return new UUID5(args[0], args[1], absolute);
                        } else {
                            final RecordPathSegment[] args = getArgPaths(argumentListTree, 1, functionName, absolute);
                            return new UUID5(args[0], null, absolute);
                        }
                    }
                    case "coalesce": {
                        final int numArgs = argumentListTree.getChildCount();

                        final RecordPathSegment[] argPaths = new RecordPathSegment[numArgs];
                        for (int i = 0; i < numArgs; i++) {
                            argPaths[i] = buildPath(argumentListTree.getChild(i), null, absolute);
                        }

                        return new Coalesce(argPaths, absolute);
                    }
                    case "count": {
                        final RecordPathSegment[] args = getArgPaths(argumentListTree, 1, functionName, absolute);
                        return new Count(args[0], absolute);
                    }
                    case "join": {
                        final int numArgs = argumentListTree.getChildCount();
                        if (numArgs < 2) {
                            throw new RecordPathException("Invalid number of arguments: " + functionName + " function takes 2 or more arguments but got " + numArgs);
                        }

                        final RecordPathSegment[] joinPaths = new RecordPathSegment[numArgs - 1];
                        for (int i = 0; i < numArgs - 1; i++) {
                            joinPaths[i] = buildPath(argumentListTree.getChild(i + 1), null, absolute);
                        }

                        final RecordPathSegment delimiterPath = buildPath(argumentListTree.getChild(0), null, absolute);
                        return new Join(delimiterPath, joinPaths, absolute);
                    }
                    case "anchored": {
                        final RecordPathSegment[] args = getArgPaths(argumentListTree, 2, functionName, absolute);
                        return new Anchored(args[0], args[1], absolute);
                    }
                    case "not":
                    case "contains":
                    case "containsRegex":
                    case "endsWith":
                    case "startsWith":
                    case "isBlank":
                    case "isEmpty":
                    case "matchesRegex": {
                        final RecordPathFilter filter = createFilter(tree, null, absolute);
                        return new FilterFunction(functionName, filter, absolute);
                    }
                    default: {
                        throw new RecordPathException("Invalid function call: The '" + functionName + "' function does not exist or can only "
                            + "be used within a predicate, not as a standalone function");
                    }
                }
            }
        }

        throw new RecordPathException("Encountered unexpected token " + tree);
    }

    private static RecordPathSegment[] getArgumentsForStringFunction(boolean absolute, Tree argumentListTree) {
        final int numArgs = argumentListTree.getChildCount();

        final RecordPathSegment[] argPaths = new RecordPathSegment[numArgs];
        for (int i = 0; i < numArgs; i++) {
            argPaths[i] = buildPath(argumentListTree.getChild(i), null, absolute);
        }

        return argPaths;
    }

    private static RecordPathFilter createFilter(final Tree operatorTree, final RecordPathSegment parent, final boolean absolute) {
        return switch (operatorTree.getType()) {
            case EQUAL -> createBinaryOperationFilter(operatorTree, parent, EqualsFilter::new, absolute);
            case NOT_EQUAL -> createBinaryOperationFilter(operatorTree, parent, NotEqualsFilter::new, absolute);
            case LESS_THAN -> createBinaryOperationFilter(operatorTree, parent, LessThanFilter::new, absolute);
            case LESS_THAN_EQUAL ->
                    createBinaryOperationFilter(operatorTree, parent, LessThanOrEqualFilter::new, absolute);
            case GREATER_THAN -> createBinaryOperationFilter(operatorTree, parent, GreaterThanFilter::new, absolute);
            case GREATER_THAN_EQUAL ->
                    createBinaryOperationFilter(operatorTree, parent, GreaterThanOrEqualFilter::new, absolute);
            case FUNCTION -> createFunctionFilter(operatorTree, absolute);
            default ->
                    throw new RecordPathException("Expected an Expression of form <value> <operator> <value> to follow '[' Token but found " + operatorTree);
        };
    }

    private static RecordPathFilter createBinaryOperationFilter(final Tree operatorTree, final RecordPathSegment parent,
        final BiFunction<RecordPathSegment, RecordPathSegment, RecordPathFilter> function, final boolean absolute) {
        final Tree lhsTree = operatorTree.getChild(0);
        final Tree rhsTree = operatorTree.getChild(1);
        final RecordPathSegment lhsPath = buildPath(lhsTree, parent, absolute);
        final RecordPathSegment rhsPath = buildPath(rhsTree, parent, absolute);
        return function.apply(lhsPath, rhsPath);
    }

    private static RecordPathFilter createFunctionFilter(final Tree functionTree, final boolean absolute) {
        final String functionName = functionTree.getChild(0).getText();
        final Tree argumentListTree = functionTree.getChild(1);

        switch (functionName) {
            case "contains": {
                final RecordPathSegment[] args = getArgPaths(argumentListTree, 2, functionName, absolute);
                return new Contains(args[0], args[1]);
            }
            case "matchesRegex": {
                final RecordPathSegment[] args = getArgPaths(argumentListTree, 2, functionName, absolute);
                return new MatchesRegex(args[0], args[1]);
            }
            case "containsRegex": {
                final RecordPathSegment[] args = getArgPaths(argumentListTree, 2, functionName, absolute);
                return new ContainsRegex(args[0], args[1]);
            }
            case "startsWith": {
                final RecordPathSegment[] args = getArgPaths(argumentListTree, 2, functionName, absolute);
                return new StartsWith(args[0], args[1]);
            }
            case "endsWith": {
                final RecordPathSegment[] args = getArgPaths(argumentListTree, 2, functionName, absolute);
                return new EndsWith(args[0], args[1]);
            }
            case "isEmpty": {
                final RecordPathSegment[] args = getArgPaths(argumentListTree, 1, functionName, absolute);
                return new IsEmpty(args[0]);
            }
            case "isBlank": {
                final RecordPathSegment[] args = getArgPaths(argumentListTree, 1, functionName, absolute);
                return new IsBlank(args[0]);
            }
            case "not": {
                final int numArgs = argumentListTree.getChildCount();
                if (numArgs != 1) {
                    throw new RecordPathException("Invalid number of arguments: " + functionName + " function takes 1 argument but got " + numArgs);
                }

                final Tree childTree = argumentListTree.getChild(0);
                final RecordPathFilter childFilter = createFilter(childTree, null, absolute);
                return new NotFilter(childFilter);
            }
        }

        throw new RecordPathException("Invalid function name: " + functionName);
    }

    private static RecordPathSegment[] getArgPaths(final Tree argumentListTree, final int expectedCount, final String functionName, final boolean absolute) {
        final int numArgs = argumentListTree.getChildCount();
        if (numArgs != expectedCount) {
            throw new RecordPathException("Invalid number of arguments: " + functionName + " function takes " + expectedCount + " arguments but got " + numArgs);
        }

        final RecordPathSegment[] argPaths = new RecordPathSegment[expectedCount];
        for (int i = 0; i < expectedCount; i++) {
            argPaths[i] = buildPath(argumentListTree.getChild(i), null, absolute);
        }

        return argPaths;
    }

    private static RecordPathSegment[] getArgPaths(final Tree argumentListTree, final int minCount, final int maxCount, final String functionName, final boolean absolute) {
        final int numArgs = argumentListTree.getChildCount();
        if (numArgs < minCount || numArgs > maxCount) {
            throw new RecordPathException("Invalid number of arguments: " + functionName + " function takes at least" + minCount
                    + " arguments, and at most " + maxCount + "arguments, but got " + numArgs);
        }

        final List<RecordPathSegment> argPaths = new ArrayList<>();
        for (int i = 0; i < argumentListTree.getChildCount(); i++) {
            argPaths.add(buildPath(argumentListTree.getChild(i), null, absolute));
        }

        return argPaths.toArray(new RecordPathSegment[argPaths.size()]);
    }
}
