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

interface CompiledExpression {
    /**
     * Evaluates this Expression against the given variables, attribute decorator, and state variables
     *
     * @param variables variables to be evaluated
     * @param decorator decorator to decorate variable values
     * @param stateVariables state variables to include in evaluation
     * @return the evaluated value
     */
    String evaluate(Map<String, String> variables, AttributeValueDecorator decorator, Map<String, String> stateVariables);

    /**
     * @return the Root Evaluator that is to be used when evaluating the Expression
     */
    Evaluator<?> getRootEvaluator();

    /**
     * @return the Abstract Syntax Tree that was derived from parsing the expression
     */
    Tree getTree();

    /**
     * @return the textual representation of the expression
     */
    String getExpression();

    /**
     * @return the Set of all Evaluators that are used to make up the Expression
     */
    Set<Evaluator<?>> getAllEvaluators();
}
