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
package org.apache.nifi.attribute.expression.language.evaluation.functions;

import org.apache.nifi.attribute.expression.language.EvaluationContext;
import org.apache.nifi.attribute.expression.language.evaluation.BooleanEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.BooleanQueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.Evaluator;
import org.apache.nifi.attribute.expression.language.evaluation.QueryResult;
import org.apache.nifi.expression.AttributeExpression.ResultType;

import java.util.Date;

public class EqualsEvaluator extends BooleanEvaluator {

    private final Evaluator<?> subject;
    private final Evaluator<?> compareTo;

    public EqualsEvaluator(final Evaluator<?> subject, final Evaluator<?> compareTo) {
        this.subject = subject;
        this.compareTo = compareTo;
    }

    @Override
    public QueryResult<Boolean> evaluate(final EvaluationContext evaluationContext) {
        final Object a = subject.evaluate(evaluationContext).getValue();
        if (a == null) {
            return new BooleanQueryResult(false);
        }

        final Object b = compareTo.evaluate(evaluationContext).getValue();
        if (b == null) {
            return new BooleanQueryResult(false);
        }

        if (subject.getResultType() == compareTo.getResultType()) {
            return new BooleanQueryResult(a.equals(b));
        }

        final String normalizedSubjectValue = normalizeValue(subject.getResultType(), a);
        if (normalizedSubjectValue == null) {
            return new BooleanQueryResult(false);
        }

        final String normalizedCompareToValue = normalizeValue(compareTo.getResultType(), b);
        if (normalizedCompareToValue == null) {
            return new BooleanQueryResult(false);
        }

        return new BooleanQueryResult(normalizedSubjectValue.equals(normalizedCompareToValue));
    }

    private String normalizeValue(final ResultType type, final Object value) {
        if (value == null) {
            return null;
        }

        switch (type) {
            case STRING:
                return (String) value;
            case DATE:
                return String.valueOf(((Date) value).getTime());
            case BOOLEAN:
            case NUMBER:
            default:
                return String.valueOf(value);
        }
    }

    @Override
    public Evaluator<?> getSubjectEvaluator() {
        return subject;
    }

}
