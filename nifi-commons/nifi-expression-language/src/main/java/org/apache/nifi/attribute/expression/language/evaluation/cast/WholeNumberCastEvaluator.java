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

import org.apache.nifi.attribute.expression.language.EvaluationContext;
import org.apache.nifi.attribute.expression.language.evaluation.DateQueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.DecimalQueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.Evaluator;
import org.apache.nifi.attribute.expression.language.evaluation.InstantQueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.NumberQueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.QueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.StringQueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.WholeNumberEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.WholeNumberQueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.util.NumberParsing;
import org.apache.nifi.attribute.expression.language.exception.AttributeExpressionLanguageParsingException;
import org.apache.nifi.expression.AttributeExpression.ResultType;

public class WholeNumberCastEvaluator extends WholeNumberEvaluator {

    private final Evaluator<?> subjectEvaluator;

    public WholeNumberCastEvaluator(final Evaluator<?> subjectEvaluator) {
        if (subjectEvaluator.getResultType() == ResultType.BOOLEAN) {
            throw new AttributeExpressionLanguageParsingException("Cannot implicitly convert Data Type " + subjectEvaluator.getResultType() + " to " + ResultType.WHOLE_NUMBER);
        }
        this.subjectEvaluator = subjectEvaluator;
    }

    @Override
    public QueryResult<Long> evaluate(final EvaluationContext evaluationContext) {
        final QueryResult<?> result = subjectEvaluator.evaluate(evaluationContext);
        if (result.getValue() == null) {
            return new WholeNumberQueryResult(null);
        }

        switch (result.getResultType()) {
            case WHOLE_NUMBER:
                return (WholeNumberQueryResult) result;
            case STRING:
                final String trimmed = ((StringQueryResult) result).getValue().trim();
                NumberParsing.ParseResultType parseType = NumberParsing.parse(trimmed);
                switch (parseType) {
                    case DECIMAL:
                        final Double doubleResultValue = Double.valueOf(trimmed);
                        return new WholeNumberQueryResult(doubleResultValue.longValue());
                    case WHOLE_NUMBER:
                        Long longResultValue;
                        try {
                            longResultValue = Long.valueOf(trimmed);
                        } catch (NumberFormatException e) {
                            // Will only occur if trimmed is a hex number
                            longResultValue = Long.decode(trimmed);
                        }
                        return new WholeNumberQueryResult(longResultValue);
                    case NOT_NUMBER:
                        return new WholeNumberQueryResult(null);
                }
                // fallthrough
            case DATE:
                return new WholeNumberQueryResult(((DateQueryResult) result).getValue().getTime());
            case INSTANT:
                return new WholeNumberQueryResult(((InstantQueryResult) result).getValue().toEpochMilli());
            case DECIMAL:
                final Double resultValue = ((DecimalQueryResult) result).getValue();
                return new WholeNumberQueryResult(resultValue.longValue());
            case NUMBER:
                final Number numberValue = ((NumberQueryResult) result).getValue();
                return new WholeNumberQueryResult(numberValue.longValue());
            default:
                return new WholeNumberQueryResult(null);
        }
    }

    @Override
    public Evaluator<?> getSubjectEvaluator() {
        return subjectEvaluator;
    }

}
