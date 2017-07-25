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
package org.apache.nifi.attribute.expression.language.evaluation.selection;

import java.util.Map;

import org.apache.nifi.attribute.expression.language.evaluation.Evaluator;
import org.apache.nifi.attribute.expression.language.evaluation.QueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.reduce.ReduceEvaluator;
import org.apache.nifi.expression.AttributeExpression.ResultType;

public class MappingEvaluator<T> implements Evaluator<T> {
    private final ReduceEvaluator<T> mappingEvaluator;
    private final MultiAttributeEvaluator multiAttributeEvaluator;
    private String token;

    public MappingEvaluator(final ReduceEvaluator<T> mappingEvaluator, final MultiAttributeEvaluator multiAttributeEval) {
        this.mappingEvaluator = mappingEvaluator;
        this.multiAttributeEvaluator = multiAttributeEval;
    }

    @Override
    public QueryResult<T> evaluate(final Map<String, String> attributes) {
        QueryResult<T> result = mappingEvaluator.evaluate(attributes);

        while (multiAttributeEvaluator.getEvaluationsRemaining() > 0) {
            result = mappingEvaluator.evaluate(attributes);
        }

        return result;
    }

    @Override
    public ResultType getResultType() {
        return mappingEvaluator.getResultType();
    }

    @Override
    public int getEvaluationsRemaining() {
        return 0;
    }

    @Override
    public Evaluator<?> getSubjectEvaluator() {
        return null;
    }

    @Override
    public String getToken() {
        return token;
    }

    @Override
    public void setToken(final String token) {
        this.token = token;
    }

    public MultiAttributeEvaluator getVariableIteratingEvaluator() {
        return multiAttributeEvaluator;
    }
}
