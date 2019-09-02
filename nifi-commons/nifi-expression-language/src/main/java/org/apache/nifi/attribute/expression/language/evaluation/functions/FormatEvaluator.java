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
import org.apache.nifi.attribute.expression.language.evaluation.Evaluator;
import org.apache.nifi.attribute.expression.language.evaluation.QueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.StringEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.StringQueryResult;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

public class FormatEvaluator extends StringEvaluator {

    private final DateEvaluator subject;
    private final Evaluator<String> format;
    private final Evaluator<String> timeZone;

    public FormatEvaluator(final DateEvaluator subject, final Evaluator<String> format, final Evaluator<String> timeZone) {
        this.subject = subject;
        this.format = format;
        this.timeZone = timeZone;
    }

    @Override
    public QueryResult<String> evaluate(final EvaluationContext evaluationContext) {
        final Date subjectValue = subject.evaluate(evaluationContext).getValue();
        if (subjectValue == null) {
            return new StringQueryResult(null);
        }

        final QueryResult<String> formatResult = format.evaluate(evaluationContext);
        final String format = formatResult.getValue();
        if (format == null) {
            return null;
        }

        final SimpleDateFormat sdf = new SimpleDateFormat(format, Locale.US);

        if(timeZone != null) {
            final QueryResult<String> tzResult = timeZone.evaluate(evaluationContext);
            final String tz = tzResult.getValue();
            if(tz != null && TimeZone.getTimeZone(tz) != null) {
                sdf.setTimeZone(TimeZone.getTimeZone(tz));
            }
        }

        return new StringQueryResult(sdf.format(subjectValue));
    }

    @Override
    public Evaluator<?> getSubjectEvaluator() {
        return subject;
    }

}
