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
import org.apache.nifi.attribute.expression.language.StandardEvaluationContext;
import org.apache.nifi.attribute.expression.language.evaluation.DateEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.DateQueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.Evaluator;
import org.apache.nifi.attribute.expression.language.evaluation.QueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.literals.StringLiteralEvaluator;
import org.apache.nifi.attribute.expression.language.exception.IllegalAttributeException;
import org.apache.nifi.util.FormatUtils;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Collections;
import java.util.Date;

public class StringToDateEvaluator extends DateEvaluator {

    private final Evaluator<String> subject;
    private final Evaluator<String> format;
    private final Evaluator<String> timeZone;

    private final DateTimeFormatter preparedFormatter;
    private final boolean preparedFormatterHasRequestedTimeZone;

    public StringToDateEvaluator(final Evaluator<String> subject, final Evaluator<String> format, final Evaluator<String> timeZone) {
        this.subject = subject;
        this.format = format;
        // if the search string is a literal, we don't need to prepare formatter each time; we can just
        // prepare it once. Otherwise, it must be prepared for each time.
        if (format instanceof StringLiteralEvaluator) {
            String evaluatedFormat = format.evaluate(new StandardEvaluationContext(Collections.emptyMap())).getValue();
            DateTimeFormatter dtf = FormatUtils.prepareLenientCaseInsensitiveDateTimeFormatter(evaluatedFormat);
            if (timeZone == null) {
                preparedFormatter = dtf;
                preparedFormatterHasRequestedTimeZone = true;
            } else if (timeZone instanceof StringLiteralEvaluator) {
                preparedFormatter = dtf.withZone(ZoneId.of(timeZone.evaluate(new StandardEvaluationContext(Collections.emptyMap())).getValue()));
                preparedFormatterHasRequestedTimeZone = true;
            } else {
                preparedFormatter = dtf;
                preparedFormatterHasRequestedTimeZone = false;
            }
        } else {
            preparedFormatter = null;
            preparedFormatterHasRequestedTimeZone = false;
        }
        this.timeZone = timeZone;
    }

    @Override
    public QueryResult<Date> evaluate(final EvaluationContext evaluationContext) {
        final String subjectValue = subject.evaluate(evaluationContext).getValue();
        final String formatValue = format.evaluate(evaluationContext).getValue();
        if (subjectValue == null || formatValue == null) {
            return new DateQueryResult(null);
        }

        DateTimeFormatter dtf;
        if (preparedFormatter != null) {
            dtf = preparedFormatter;
        } else {
            final QueryResult<String> formatResult = format.evaluate(evaluationContext);
            final String format = formatResult.getValue();
            if (format == null) {
                return null;
            }
            dtf = FormatUtils.prepareLenientCaseInsensitiveDateTimeFormatter(format);
        }

        if ((preparedFormatter == null || !preparedFormatterHasRequestedTimeZone) && timeZone != null) {
            final QueryResult<String> tzResult = timeZone.evaluate(evaluationContext);
            final String tz = tzResult.getValue();
            if(tz != null) {
                dtf = dtf.withZone(ZoneId.of(tz));
            }
        }

        try {
            return new DateQueryResult(Date.from(FormatUtils.parseToInstant(dtf, subjectValue)));
        } catch (final DateTimeParseException e) {
            throw new IllegalAttributeException("Cannot parse attribute value as a date; date format: "
                    + formatValue + "; attribute value: " + subjectValue + ". Error: " + e.getMessage());
        } catch (final IllegalArgumentException e) {
            throw new IllegalAttributeException("Invalid date format: " + formatValue);
        }
    }

    @Override
    public Evaluator<?> getSubjectEvaluator() {
        return subject;
    }

}
