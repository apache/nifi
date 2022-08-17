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
import org.apache.nifi.attribute.expression.language.evaluation.Evaluator;
import org.apache.nifi.attribute.expression.language.evaluation.InstantEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.InstantQueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.QueryResult;
import org.apache.nifi.attribute.expression.language.exception.IllegalAttributeException;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

public class StringToInstantEvaluator extends InstantEvaluator {

    private final Evaluator<String> subject;
    private final Evaluator<String> format;
    private final Evaluator<String> timeZone;

    public StringToInstantEvaluator(final Evaluator<String> subject, final Evaluator<String> format, final Evaluator<String> timeZone) {
        this.subject = subject;
        this.format = format;
        this.timeZone = timeZone;
    }

    @Override
    public QueryResult<Instant> evaluate(final EvaluationContext evaluationContext) {
        final String subjectValue = subject.evaluate(evaluationContext).getValue();
        final String formatValue = format.evaluate(evaluationContext).getValue();
        if (subjectValue == null || formatValue == null) {
            return new InstantQueryResult(null);
        }

        final QueryResult<String> tzResult = timeZone.evaluate(evaluationContext);
        final String tz = tzResult.getValue();

        DateTimeFormatter dtf = DateTimeFormatter.ofPattern(formatValue, Locale.US)
                .withZone(ZoneId.of(tz));

        try {
            return new InstantQueryResult(dtf.parse(subjectValue, Instant::from));
        } catch (final IllegalArgumentException e) {
            throw new IllegalAttributeException("Invalid instant format: " + formatValue);
        }
    }

    @Override
    public Evaluator<?> getSubjectEvaluator() {
        return subject;
    }
}
