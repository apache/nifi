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
import org.apache.nifi.attribute.expression.language.evaluation.Evaluator;
import org.apache.nifi.expression.AttributeValueDecorator;

import java.util.Map;
import java.util.Set;

public class SingleEvalExpression implements CompiledExpression {
    private final Evaluator<?> rootEvaluator;
    private final Tree tree;
    private final String expression;
    private final Set<Evaluator<?>> allEvaluators;

    public SingleEvalExpression(final String expression, final Evaluator<?> rootEvaluator, final Tree tree, final Set<Evaluator<?>> allEvaluators) {
        this.rootEvaluator = rootEvaluator;
        this.tree = tree;
        this.expression = expression;
        this.allEvaluators = allEvaluators;
    }

    public Evaluator<?> getRootEvaluator() {
        return rootEvaluator;
    }

    public Tree getTree() {
        return tree;
    }

    public String getExpression() {
        return expression;
    }

    public Set<Evaluator<?>> getAllEvaluators() {
        return allEvaluators;
    }

    @Override
    public String evaluate(final Map<String, String> variables, final AttributeValueDecorator decorator, final Map<String, String> stateVariables) {
        // TODO: Refactor this into something more like:
        // final CompiledQuery query = Query.compile(getTree(), expression);
        // query.evaluate(variables, decorator, stateVariables);
        //
        // where CompiledQuery is an interface that has a single method just like this one: takes variables, decorator, stateVars, returns String
        // Then have different implementations. One that rebuilds the Query object from tree each time, for Evaluators that need it, and another
        // that does not do that and just reuses the Evaluators.
        //
        // Most likely it means that what we really need is for SingleEvalExpression to be split into two implementations of Expression
        // and then have the other getter methods elevated into the Expression interface.
        return Query.evaluateExpression(getTree(), expression, variables, decorator, stateVariables);
    }
}
