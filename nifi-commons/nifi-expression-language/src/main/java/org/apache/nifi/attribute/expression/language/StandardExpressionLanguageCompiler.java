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

import org.apache.nifi.attribute.expression.language.exception.AttributeExpressionLanguageParsingException;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageCompiler;
import org.apache.nifi.expression.AttributeExpression.ResultType;
import org.apache.nifi.registry.VariableRegistry;

public class StandardExpressionLanguageCompiler implements ExpressionLanguageCompiler {

    private final VariableRegistry variableRegistry;

    public StandardExpressionLanguageCompiler(final VariableRegistry variableRegistry) {
        this.variableRegistry = variableRegistry;
    }

    @Override
    public AttributeExpression compile(final String expression) throws IllegalArgumentException {
        try {
            return new StandardAttributeExpression(Query.compile(expression),variableRegistry);
        } catch (final AttributeExpressionLanguageParsingException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    @Override
    public boolean isValidExpression(final String expression) {
        return Query.isValidExpression(expression);
    }

    @Override
    public String validateExpression(final String expression, final boolean allowSurroundingCharacters) {
        try {
            Query.validateExpression(expression, allowSurroundingCharacters);
            return null;
        } catch (final AttributeExpressionLanguageParsingException aelpe) {
            return aelpe.getMessage();
        }
    }

    @Override
    public ResultType getResultType(final String expression) throws IllegalArgumentException {
        try {
            return Query.getResultType(expression);
        } catch (final AttributeExpressionLanguageParsingException e) {
            throw new IllegalArgumentException(e);
        }
    }
}
