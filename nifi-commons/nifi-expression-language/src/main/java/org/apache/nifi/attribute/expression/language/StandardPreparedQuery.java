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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.expression.AttributeValueDecorator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.exception.ProcessException;

import org.antlr.runtime.tree.Tree;

public class StandardPreparedQuery implements PreparedQuery {

    private final List<String> queryStrings;
    private final Map<String, Tree> trees;

    public StandardPreparedQuery(final List<String> queryStrings, final Map<String, Tree> trees) {
        this.queryStrings = new ArrayList<>(queryStrings);
        this.trees = new HashMap<>(trees);
    }

    @Override
    public String evaluateExpressions(Map<String, String> attributes) throws ProcessException {
        return evaluateExpressions(attributes, null);
    }

    @Override
    public String evaluateExpressions(final Map<String, String> attributes, final AttributeValueDecorator decorator) throws ProcessException {
        final StringBuilder sb = new StringBuilder();
        for (final String val : queryStrings) {
            final Tree tree = trees.get(val);
            if (tree == null) {
                sb.append(val);
            } else {
                final String evaluated = Query.evaluateExpression(tree, val, attributes, decorator);
                if (evaluated != null) {
                    sb.append(evaluated);
                }
            }
        }
        return sb.toString();
    }

    @Override
    public String evaluateExpressions(final FlowFile flowFile, final Map<String, String> additionalAttributes, final AttributeValueDecorator decorator) throws ProcessException {
        final Map<String, String> expressionMap = Query.createExpressionMap(flowFile, additionalAttributes);
        return evaluateExpressions(expressionMap, decorator);
    }

    @Override
    public String evaluateExpressions(final FlowFile flowFile, final AttributeValueDecorator decorator) throws ProcessException {
        final Map<String, String> expressionMap = Query.createExpressionMap(flowFile);
        return evaluateExpressions(expressionMap, decorator);
    }

    @Override
    public String evaluateExpressions() throws ProcessException {
        return evaluateExpressions((FlowFile) null, null);
    }

    @Override
    public String evaluateExpressions(final AttributeValueDecorator decorator) throws ProcessException {
        return evaluateExpressions((FlowFile) null, decorator);
    }

    @Override
    public String evaluateExpressions(final FlowFile flowFile) throws ProcessException {
        return evaluateExpressions(flowFile, null);
    }

}
