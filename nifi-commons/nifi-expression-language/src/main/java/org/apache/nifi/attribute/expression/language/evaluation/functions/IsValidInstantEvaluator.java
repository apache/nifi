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

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;

/**
 * Evaluator for the {@code isValidInstant()} Expression Language function.
 * Returns {@code true} if the subject parses as a valid {@link java.time.Instant} using
 * ISO_INSTANT, ISO_OFFSET_DATE_TIME, or RFC_1123_DATE_TIME formats; {@code false} otherwise.
 */
public class IsValidInstantEvaluator extends BooleanEvaluator {

    private static final DateTimeFormatter INSTANT_FORMATTER = new DateTimeFormatterBuilder()
            .appendOptional(DateTimeFormatter.ISO_INSTANT)
            .appendOptional(DateTimeFormatter.ISO_OFFSET_DATE_TIME)
            .appendOptional(DateTimeFormatter.RFC_1123_DATE_TIME)
            .toFormatter();

    private final Evaluator<String> subject;

    public IsValidInstantEvaluator(final Evaluator<String> subject) {
        this.subject = subject;
    }

    @Override
    public QueryResult<Boolean> evaluate(final EvaluationContext evaluationContext) {
        final String subjectValue = subject.evaluate(evaluationContext).getValue();
        if (subjectValue == null) {
            return new BooleanQueryResult(false);
        }

        try {
            INSTANT_FORMATTER.parse(subjectValue.trim(), Instant::from);
            return new BooleanQueryResult(true);
        } catch (final DateTimeParseException e) {
            return new BooleanQueryResult(false);
        }
    }

    @Override
    public Evaluator<?> getSubjectEvaluator() {
        return subject;
    }
}
