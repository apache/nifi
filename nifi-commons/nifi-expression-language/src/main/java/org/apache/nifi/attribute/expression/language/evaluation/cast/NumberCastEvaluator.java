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

import org.apache.nifi.attribute.expression.language.evaluation.DateQueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.DecimalQueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.Evaluator;
import org.apache.nifi.attribute.expression.language.evaluation.NumberEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.NumberQueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.QueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.StringQueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.WholeNumberQueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.util.NumberParsing;
import org.apache.nifi.attribute.expression.language.exception.AttributeExpressionLanguageParsingException;
import org.apache.nifi.expression.AttributeExpression.ResultType;

import java.util.Map;

public class NumberCastEvaluator extends NumberEvaluator {

    private final Evaluator<?> subjectEvaluator;

    public NumberCastEvaluator(final Evaluator<?> subjectEvaluator) {
        if (subjectEvaluator.getResultType() == ResultType.BOOLEAN) {
            throw new AttributeExpressionLanguageParsingException("Cannot implicitly convert Data Type " + subjectEvaluator.getResultType() + " to " + ResultType.WHOLE_NUMBER);
        }
        this.subjectEvaluator = subjectEvaluator;
    }

    @Override
    public QueryResult<Number> evaluate(final Map<String, String> attributes) {
        final QueryResult<?> result = subjectEvaluator.evaluate(attributes);
        if (result.getValue() == null) {
            return new NumberQueryResult(null);
        }

        switch (result.getResultType()) {
            case NUMBER:
                return (NumberQueryResult) result;
            case WHOLE_NUMBER:
                Long longValue = ((WholeNumberQueryResult) result).getValue();
                return new NumberQueryResult(longValue);
            case DECIMAL:
                Double doubleValue = ((DecimalQueryResult) result).getValue();
                return new NumberQueryResult(doubleValue);
            case STRING:
                final String trimmed = ((StringQueryResult) result).getValue().trim();
                NumberParsing.ParseResultType parseType = NumberParsing.parse(trimmed);
                switch (parseType){
                    case DECIMAL:
                        return new NumberQueryResult(Double.valueOf(trimmed));
                    case WHOLE_NUMBER:
                        Long resultValue;
                        try {
                            resultValue = Long.valueOf(trimmed);
                        } catch (NumberFormatException e){
                            // Will only occur if trimmed is a hex number
                            resultValue = Long.decode(trimmed);
                        }
                        return new NumberQueryResult(resultValue);
                    case NOT_NUMBER:
                    default:
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
