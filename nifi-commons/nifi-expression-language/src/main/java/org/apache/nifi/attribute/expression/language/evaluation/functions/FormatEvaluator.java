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
import org.apache.nifi.attribute.expression.language.evaluation.Evaluator;
import org.apache.nifi.attribute.expression.language.evaluation.QueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.StringEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.StringQueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.literals.StringLiteralEvaluator;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Date;
import java.util.Locale;

public class FormatEvaluator extends StringEvaluator {

    private final DateEvaluator subject;
    private final Evaluator<String> format;
    private final Evaluator<String> timeZone;

    private final DateTimeFormatter preparedFormatter;
    private final boolean preparedFormatterHasCorrectZone;

    public FormatEvaluator(final DateEvaluator subject, final Evaluator<String> format, final Evaluator<String> timeZone) {
        this.subject = subject;
        this.format = format;
        // if the search string is a literal, we don't need to prepare formatter each time; we can just
        // prepare it once. Otherwise, it must be prepared for each time.
        if (format instanceof StringLiteralEvaluator) {
            DateTimeFormatter dtf = DateTimeFormatter.ofPattern(format.evaluate(new StandardEvaluationContext(Collections.emptyMap())).getValue(), Locale.US);
            if (timeZone == null) {
                preparedFormatter = dtf;
                preparedFormatterHasCorrectZone = true;
            } else if (timeZone instanceof StringLiteralEvaluator) {
                preparedFormatter = dtf.withZone(ZoneId.of(timeZone.evaluate(new StandardEvaluationContext(Collections.emptyMap())).getValue()));
                preparedFormatterHasCorrectZone = true;
            } else {
                preparedFormatter = dtf;
                preparedFormatterHasCorrectZone = false;
            }
        } else {
            preparedFormatter = null;
            preparedFormatterHasCorrectZone = false;
        }
        this.timeZone = timeZone;
    }

    @Override
    public QueryResult<String> evaluate(final EvaluationContext evaluationContext) {
        final Date subjectValue = subject.evaluate(evaluationContext).getValue();
        if (subjectValue == null) {
            return new StringQueryResult(null);
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
            dtf = DateTimeFormatter.ofPattern(format, Locale.US);
        }

        if ((preparedFormatter == null || !preparedFormatterHasCorrectZone) && timeZone != null) {
            final QueryResult<String> tzResult = timeZone.evaluate(evaluationContext);
            final String tz = tzResult.getValue();
            if(tz != null) {
                dtf = dtf.withZone(ZoneId.of(tz));
            }
        }

        return new StringQueryResult(dtf.format(subjectValue.toInstant().atZone(ZoneId.systemDefault())));
    }

    @Override
    public Evaluator<?> getSubjectEvaluator() {
        return subject;
    }

}
