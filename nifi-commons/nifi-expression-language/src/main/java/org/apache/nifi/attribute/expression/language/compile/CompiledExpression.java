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

package org.apache.nifi.attribute.expression.language.compile;

import java.util.Set;

import org.antlr.runtime.tree.Tree;
import org.apache.nifi.attribute.expression.language.evaluation.Evaluator;

public class CompiledExpression {
    private final Evaluator<?> rootEvaluator;
    private final Tree tree;
    private final String expression;
    private final Set<Evaluator<?>> allEvaluators;

    public CompiledExpression(final String expression, final Evaluator<?> rootEvaluator, final Tree tree, final Set<Evaluator<?>> allEvaluators) {
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
}
