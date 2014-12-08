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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.nifi.attribute.expression.language.evaluation.DateEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.DateQueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.Evaluator;
import org.apache.nifi.attribute.expression.language.evaluation.QueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.StringQueryResult;
import org.apache.nifi.attribute.expression.language.exception.AttributeExpressionLanguageException;
import org.apache.nifi.attribute.expression.language.exception.AttributeExpressionLanguageParsingException;
import org.apache.nifi.expression.AttributeExpression.ResultType;

public class DateCastEvaluator extends DateEvaluator {

    public static final String DATE_TO_STRING_FORMAT = "EEE MMM dd HH:mm:ss zzz yyyy";
    public static final Pattern DATE_TO_STRING_PATTERN = Pattern.compile("(?:[a-zA-Z]{3} ){2}\\d{2} \\d{2}\\:\\d{2}\\:\\d{2} (?:.*?) \\d{4}");

    public static final String ALTERNATE_FORMAT_WITHOUT_MILLIS = "yyyy/MM/dd HH:mm:ss";
    public static final String ALTERNATE_FORMAT_WITH_MILLIS = "yyyy/MM/dd HH:mm:ss.SSS";
    public static final Pattern ALTERNATE_PATTERN = Pattern.compile("\\d{4}/\\d{2}/\\d{2} \\d{2}\\:\\d{2}\\:\\d{2}(\\.\\d{3})?");

    public static final Pattern NUMBER_PATTERN = Pattern.compile("\\d+");

    private final Evaluator<?> subjectEvaluator;

    public DateCastEvaluator(final Evaluator<?> subjectEvaluator) {
        if (subjectEvaluator.getResultType() == ResultType.BOOLEAN) {
            throw new AttributeExpressionLanguageParsingException("Cannot implicitly convert Data Type " + subjectEvaluator.getResultType() + " to " + ResultType.DATE);
        }

        this.subjectEvaluator = subjectEvaluator;
    }

    @Override
    public QueryResult<Date> evaluate(final Map<String, String> attributes) {
        final QueryResult<?> result = subjectEvaluator.evaluate(attributes);
        if (result.getValue() == null) {
            return new DateQueryResult(null);
        }

        switch (result.getResultType()) {
            case DATE:
                return (DateQueryResult) result;
            case STRING:
                final String value = ((StringQueryResult) result).getValue().trim();
                if (DATE_TO_STRING_PATTERN.matcher(value).matches()) {
                    final SimpleDateFormat sdf = new SimpleDateFormat(DATE_TO_STRING_FORMAT);

                    try {
                        final Date date = sdf.parse(value);
                        return new DateQueryResult(date);
                    } catch (final ParseException pe) {
                        throw new AttributeExpressionLanguageException("Could not parse input as date", pe);
                    }
                } else if (NUMBER_PATTERN.matcher(value).matches()) {
                    return new DateQueryResult(new Date(Long.valueOf(value)));
                } else {
                    final Matcher altMatcher = ALTERNATE_PATTERN.matcher(value);
                    if (altMatcher.matches()) {
                        final String millisValue = altMatcher.group(1);

                        final String format;
                        if (millisValue == null) {
                            format = ALTERNATE_FORMAT_WITHOUT_MILLIS;
                        } else {
                            format = ALTERNATE_FORMAT_WITH_MILLIS;
                        }

                        final SimpleDateFormat sdf = new SimpleDateFormat(format);

                        try {
                            final Date date = sdf.parse(value);
                            return new DateQueryResult(date);
                        } catch (final ParseException pe) {
                            throw new AttributeExpressionLanguageException("Could not parse input as date", pe);
                        }
                    } else {
                        throw new AttributeExpressionLanguageException("Could not implicitly convert input to DATE: " + value);
                    }
                }
            case NUMBER:
                return new DateQueryResult(new Date((Long) result.getValue()));
            default:
                return new DateQueryResult(null);
        }
    }

    @Override
    public Evaluator<?> getSubjectEvaluator() {
        return subjectEvaluator;
    }

}
