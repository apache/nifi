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
package org.apache.nifi.attribute.expression.language.evaluation.cast;

import java.util.Map;
import java.util.regex.Pattern;

import org.apache.nifi.attribute.expression.language.evaluation.DateQueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.Evaluator;
import org.apache.nifi.attribute.expression.language.evaluation.NumberEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.NumberQueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.QueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.StringQueryResult;
import org.apache.nifi.attribute.expression.language.exception.AttributeExpressionLanguageParsingException;
import org.apache.nifi.expression.AttributeExpression.ResultType;

public class NumberCastEvaluator extends NumberEvaluator {

    private final Evaluator<?> subjectEvaluator;
    private static final Pattern NUMBER_PATTERN = Pattern.compile("-?\\d+");

    public NumberCastEvaluator(final Evaluator<?> subjectEvaluator) {
        if (subjectEvaluator.getResultType() == ResultType.BOOLEAN) {
            throw new AttributeExpressionLanguageParsingException("Cannot implicitly convert Data Type " + subjectEvaluator.getResultType() + " to " + ResultType.NUMBER);
        }
        this.subjectEvaluator = subjectEvaluator;
    }

    @Override
    public QueryResult<Long> evaluate(final Map<String, String> attributes) {
        final QueryResult<?> result = subjectEvaluator.evaluate(attributes);
        if (result.getValue() == null) {
            return new NumberQueryResult(null);
        }

        switch (result.getResultType()) {
            case NUMBER:
                return (NumberQueryResult) result;
            case STRING:
                final String trimmed = ((StringQueryResult) result).getValue().trim();
                if (NUMBER_PATTERN.matcher(trimmed).matches()) {
                    return new NumberQueryResult(Long.valueOf(trimmed));
                } else {
                    return new NumberQueryResult(null);
                }
            case DATE:
                return new NumberQueryResult(((DateQueryResult) result).getValue().getTime());
            default:
                return new NumberQueryResult(null);
        }
    }

    @Override
    public Evaluator<?> getSubjectEvaluator() {
        return subjectEvaluator;
    }

}
