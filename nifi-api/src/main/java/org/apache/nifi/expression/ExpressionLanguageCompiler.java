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
package org.apache.nifi.expression;

import org.apache.nifi.expression.AttributeExpression.ResultType;

public interface ExpressionLanguageCompiler {

    /**
     * Compiles the given Attribute Expression string into an
     * AttributeExpression that can be evaluated
     *
     * @param expression the Attribute Expression to be compiled
     * @return
     * @throws IllegalArgumentException if the given expression is not valid
     */
    AttributeExpression compile(String expression) throws IllegalArgumentException;

    /**
     * Indicates whether or not the given string is a valid Attribute
     * Expression.
     *
     * @param expression
     * @return
     */
    boolean isValidExpression(String expression);

    /**
     * Attempts to validate the given expression and returns <code>null</code>
     * if the expression is syntactically valid or a String indicating why the
     * expression is invalid otherwise.
     *
     * @param expression
     * @param allowSurroundingCharacters if <code>true</code> allows characters
     * to surround the Expression, otherwise the expression must be exactly
     * equal to a valid Expression. E.g., <code>/${path}</code> is valid if and
     * only if <code>allowSurroundingCharacters</code> is true
     *
     * @return a String indicating the reason that the expression is not
     * syntactically correct, or <code>null</code> if the expression is
     * syntactically correct
     */
    String validateExpression(String expression, boolean allowSurroundingCharacters);

    /**
     * Returns the ResultType that will be returned by the given Expression
     *
     * @param expression the Expression to evaluate
     * @return
     * @throws IllegalArgumentException if the given Expression is not a valid
     * Expression Language Expression; the message of this Exception will
     * indicate the problem if the expression is not syntactically valid.
     */
    ResultType getResultType(String expression) throws IllegalArgumentException;
}
