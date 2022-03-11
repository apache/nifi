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
import org.apache.nifi.attribute.expression.language.evaluation.Evaluator;
import org.apache.nifi.attribute.expression.language.evaluation.InstantEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.InstantQueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.NumberQueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.QueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.StringQueryResult;
import org.apache.nifi.attribute.expression.language.exception.AttributeExpressionLanguageException;
import org.apache.nifi.attribute.expression.language.exception.AttributeExpressionLanguageParsingException;
import org.apache.nifi.expression.AttributeExpression;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.util.regex.Pattern;

public class InstantCastEvaluator extends InstantEvaluator {

    public static final Pattern NUMBER_PATTERN = Pattern.compile("\\d+");

    private final Evaluator<?> subjectEvaluator;

    public InstantCastEvaluator(final Evaluator<?> subjectEvaluator) {
        if (subjectEvaluator.getResultType() == AttributeExpression.ResultType.BOOLEAN) {
            throw new AttributeExpressionLanguageParsingException("Cannot implicitly convert Data Type " + subjectEvaluator.getResultType() + " to " + AttributeExpression.ResultType.INSTANT);
        }

        this.subjectEvaluator = subjectEvaluator;
    }

    @Override
    public QueryResult<Instant> evaluate(final EvaluationContext evaluationContext) {
        final QueryResult<?> result = subjectEvaluator.evaluate(evaluationContext);
        if (result.getValue() == null) {
            return new InstantQueryResult(null);
        }

        switch (result.getResultType()) {
            case INSTANT:
                return (InstantQueryResult) result;
            case STRING:
                final String value = ((StringQueryResult) result).getValue().trim();
                if (NUMBER_PATTERN.matcher(value).matches()) {
                    return new InstantQueryResult(Instant.ofEpochMilli(Long.parseLong(value)));
                }
                final DateTimeFormatter dtf = new DateTimeFormatterBuilder()
                        .appendOptional(DateTimeFormatter.ISO_INSTANT)
                        .appendOptional(DateTimeFormatter.ISO_OFFSET_DATE_TIME)
                        .appendOptional(DateTimeFormatter.RFC_1123_DATE_TIME)
                        .toFormatter();
                try {
                    Instant instant = dtf.parse(value, Instant::from);
                    return new InstantQueryResult(instant);
                } catch (DateTimeParseException e) {
                    throw new AttributeExpressionLanguageException("Could not implicitly convert input to INSTANT: " + value);
                }
            case WHOLE_NUMBER:
                return new InstantQueryResult(Instant.ofEpochMilli((Long) result.getValue()));
            case DECIMAL:
                Double resultDouble = (Double) result.getValue();
                return new InstantQueryResult(Instant.ofEpochMilli(resultDouble.longValue()));
            case NUMBER:
                final Number numberValue = ((NumberQueryResult) result).getValue();
                return new InstantQueryResult(Instant.ofEpochMilli(numberValue.longValue()));
            default:
                return new InstantQueryResult(null);
        }
    }

    @Override
    public Evaluator<?> getSubjectEvaluator() {
        return subjectEvaluator;
    }
}
