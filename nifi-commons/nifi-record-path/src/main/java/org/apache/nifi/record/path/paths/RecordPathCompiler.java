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

import static org.apache.nifi.record.path.RecordPathParser.ARRAY_INDEX;
import static org.apache.nifi.record.path.RecordPathParser.CHILD_REFERENCE;
import static org.apache.nifi.record.path.RecordPathParser.CURRENT_FIELD;
import static org.apache.nifi.record.path.RecordPathParser.DESCENDANT_REFERENCE;
import static org.apache.nifi.record.path.RecordPathParser.EQUAL;
import static org.apache.nifi.record.path.RecordPathParser.FIELD_NAME;
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

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

import org.antlr.runtime.tree.Tree;
import org.apache.nifi.record.path.NumericRange;
import org.apache.nifi.record.path.exception.RecordPathException;
import org.apache.nifi.record.path.filter.EqualsFilter;
import org.apache.nifi.record.path.filter.GreaterThanFilter;
import org.apache.nifi.record.path.filter.GreaterThanOrEqualFilter;
import org.apache.nifi.record.path.filter.LessThanFilter;
import org.apache.nifi.record.path.filter.LessThanOrEqualFilter;
import org.apache.nifi.record.path.filter.NotEqualsFilter;
import org.apache.nifi.record.path.filter.RecordPathFilter;

public class RecordPathCompiler {

    public static RecordPathSegment compile(final Tree pathTree, final RecordPathSegment root, final boolean absolute) {
        RecordPathSegment parent = root;
        for (int i = 0; i < pathTree.getChildCount(); i++) {
            final Tree child = pathTree.getChild(i);
            parent = RecordPathCompiler.buildPath(child, parent, absolute);
        }

        return parent;
    }

    public static RecordPathSegment buildPath(final Tree tree, final RecordPathSegment parent, final boolean absolute) {
        switch (tree.getType()) {
            case ROOT_REFERENCE: {
                return new RootPath();
            }
            case CHILD_REFERENCE: {
                final Tree childTree = tree.getChild(0);
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
                return new LiteralValuePath(parent, Integer.parseInt(tree.getText()), absolute);
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
        }

        throw new RecordPathException("Encountered unexpected token " + tree);
    }

    private static RecordPathFilter createFilter(final Tree operatorTree, final RecordPathSegment parent, final boolean absolute) {
        switch (operatorTree.getType()) {
            case EQUAL:
                return createBinaryOperationFilter(operatorTree, parent, EqualsFilter::new, absolute);
            case NOT_EQUAL:
                return createBinaryOperationFilter(operatorTree, parent, NotEqualsFilter::new, absolute);
            case LESS_THAN:
                return createBinaryOperationFilter(operatorTree, parent, LessThanFilter::new, absolute);
            case LESS_THAN_EQUAL:
                return createBinaryOperationFilter(operatorTree, parent, LessThanOrEqualFilter::new, absolute);
            case GREATER_THAN:
                return createBinaryOperationFilter(operatorTree, parent, GreaterThanFilter::new, absolute);
            case GREATER_THAN_EQUAL:
                return createBinaryOperationFilter(operatorTree, parent, GreaterThanOrEqualFilter::new, absolute);
            default:
                throw new RecordPathException("Expected an Expression of form <value> <operator> <value> to follow '[' Token but found " + operatorTree);
        }
    }

    private static RecordPathFilter createBinaryOperationFilter(final Tree operatorTree, final RecordPathSegment parent,
        final BiFunction<RecordPathSegment, RecordPathSegment, RecordPathFilter> function, final boolean absolute) {
        final Tree lhsTree = operatorTree.getChild(0);
        final Tree rhsTree = operatorTree.getChild(1);
        final RecordPathSegment lhsPath = buildPath(lhsTree, parent, absolute);
        final RecordPathSegment rhsPath = buildPath(rhsTree, parent, absolute);
        return function.apply(lhsPath, rhsPath);
    }
}
