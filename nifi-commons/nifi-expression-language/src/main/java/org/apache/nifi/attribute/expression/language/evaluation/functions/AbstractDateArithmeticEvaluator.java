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
import org.apache.nifi.attribute.expression.language.evaluation.DateEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.DateQueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.Evaluator;
import org.apache.nifi.attribute.expression.language.evaluation.QueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.literals.StringLiteralEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.util.DateAmountParser;
import org.apache.nifi.attribute.expression.language.exception.AttributeExpressionLanguageException;

import java.time.DateTimeException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Date;

/**
 * Shared base for {@link PlusDurationEvaluator} and {@link MinusDurationEvaluator}.
 *
 * <p>Handles literal-argument validation at construction time and the common
 * evaluate-and-convert logic. Subclasses only provide the arithmetic direction
 * via {@link #applyAmount(ZonedDateTime, String)}.</p>
 *
 * <p>An optional timezone evaluator may be provided so that DST-sensitive calendar units
 * (days, weeks, months, years) are applied at the correct wall-clock time rather than
 * as a fixed-length instant offset. When omitted, the system default timezone is used.</p>
 */
abstract class AbstractDateArithmeticEvaluator extends DateEvaluator {

    private final Evaluator<Date> subject;
    private final Evaluator<String> amountEvaluator;
    private final Evaluator<String> timeZoneEvaluator;

    /**
     * @param subject           the date-producing evaluator to operate on
     * @param amountEvaluator   the evaluator producing the amount expression string
     * @param timeZoneEvaluator optional timezone evaluator (e.g. a timezone ID string); may be {@code null}
     */
    protected AbstractDateArithmeticEvaluator(final Evaluator<Date> subject,
                                              final Evaluator<String> amountEvaluator,
                                              final Evaluator<String> timeZoneEvaluator) {
        this.subject = subject;
        this.amountEvaluator = amountEvaluator;
        this.timeZoneEvaluator = timeZoneEvaluator;

        if (amountEvaluator instanceof StringLiteralEvaluator) {
            DateAmountParser.validate(
                    amountEvaluator.evaluate(null).getValue());
        }

        if (timeZoneEvaluator instanceof StringLiteralEvaluator) {
            final String tz = timeZoneEvaluator.evaluate(null).getValue();
            try {
                ZoneId.of(tz);
            } catch (final DateTimeException e) {
                throw new AttributeExpressionLanguageException("Invalid timezone identifier: " + tz, e);
            }
        }
    }

    /** Apply the date arithmetic — plus or minus — to the given date-time. */
    protected abstract ZonedDateTime applyAmount(ZonedDateTime dateTime, String amountExpression);

    @Override
    public QueryResult<Date> evaluate(final EvaluationContext evaluationContext) {
        final Date subjectValue = subject.evaluate(evaluationContext).getValue();
        if (subjectValue == null) {
            return new DateQueryResult(null);
        }

        final String amountExpression = amountEvaluator.evaluate(evaluationContext).getValue();

        ZoneId zoneId = ZoneId.systemDefault();
        if (timeZoneEvaluator != null) {
            final String tz = timeZoneEvaluator.evaluate(evaluationContext).getValue();
            if (tz != null) {
                try {
                    zoneId = ZoneId.of(tz);
                } catch (final DateTimeException e) {
                    throw new AttributeExpressionLanguageException("Invalid timezone identifier: " + tz, e);
                }
            }
        }

        final ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(subjectValue.toInstant(), zoneId);
        final ZonedDateTime result = applyAmount(zonedDateTime, amountExpression);

        return new DateQueryResult(Date.from(result.toInstant()));
    }

    @Override
    public Evaluator<?> getSubjectEvaluator() {
        return subject;
    }
}
