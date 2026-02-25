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
import org.apache.nifi.attribute.expression.language.evaluation.QueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.StringEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.StringQueryResult;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * UniqueEvaluator removes duplicate values from a delimited string while preserving
 * the order of first occurrence.
 *
 * <p>This evaluator takes a separator as an argument and returns the unique values
 * from the subject string. The order of values is preserved based on their first
 * appearance in the original string.</p>
 *
 * <p>Examples:</p>
 * <ul>
 *   <li>"red,blue,red,green" with separator "," returns "red,blue,green"</li>
 *   <li>"a::b::a::c" with separator "::" returns "a::b::c"</li>
 *   <li>"test" with separator "," returns "test" (no duplicates)</li>
 * </ul>
 *
 * <p>Special cases:</p>
 * <ul>
 *   <li>If the subject is null, returns empty string</li>
 *   <li>If the separator is null or empty, returns the original subject unchanged</li>
 *   <li>Empty strings are treated as distinct values (e.g., "a,,b,," with separator "," returns "a,,b")</li>
 * </ul>
 */
public class UniqueEvaluator extends StringEvaluator {

    private final Evaluator<String> subject;
    private final Evaluator<String> separator;

    /**
     * Constructs a new UniqueEvaluator.
     *
     * @param subject the evaluator that provides the delimited string to process
     * @param separator the evaluator that provides the delimiter to use for splitting and joining
     */
    public UniqueEvaluator(final Evaluator<String> subject, final Evaluator<String> separator) {
        this.subject = subject;
        this.separator = separator;
    }

    @Override
    public QueryResult<String> evaluate(final EvaluationContext evaluationContext) {
        final String subjectValue = subject.evaluate(evaluationContext).getValue();
        if (subjectValue == null) {
            return new StringQueryResult("");
        }

        final String separatorValue = separator.evaluate(evaluationContext).getValue();
        if (separatorValue == null || separatorValue.isEmpty()) {
            return new StringQueryResult(subjectValue);
        }

        // Split the subject by the separator, using Pattern.quote to handle special regex characters
        final String[] parts = subjectValue.split(Pattern.quote(separatorValue), -1);

        // Use LinkedHashSet to maintain insertion order while removing duplicates
        final Set<String> uniqueValues = new LinkedHashSet<>(Arrays.asList(parts));

        // Join the unique values back together with the separator
        final String result = String.join(separatorValue, uniqueValues);
        return new StringQueryResult(result);
    }

    @Override
    public Evaluator<?> getSubjectEvaluator() {
        return subject;
    }
}
