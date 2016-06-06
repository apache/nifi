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
import org.apache.nifi.processor.exception.ProcessException;

import org.antlr.runtime.tree.Tree;
import org.apache.nifi.registry.VariableRegistry;

public class StandardPreparedQuery implements PreparedQuery {

    private final List<String> queryStrings;
    private final Map<String, Tree> trees;

    public StandardPreparedQuery(final List<String> queryStrings, final Map<String, Tree> trees) {
        this.queryStrings = new ArrayList<>(queryStrings);
        this.trees = new HashMap<>(trees);
    }


    @Override
    public String evaluateExpressions(final VariableRegistry registry, final AttributeValueDecorator decorator) throws ProcessException {
        final StringBuilder sb = new StringBuilder();
        for (final String val : queryStrings) {
            final Tree tree = trees.get(val);
            if (tree == null) {
                sb.append(val);
            } else {
                final String evaluated = Query.evaluateExpression(tree, val, registry, decorator);
                if (evaluated != null) {
                    sb.append(evaluated);
                }
            }
        }
        return sb.toString();
    }

}
