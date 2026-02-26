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
import java.util.regex.Pattern;

/**
 * Removes leading and trailing empty tokens from a delimited string while
 * preserving any interior empty tokens. Non-empty token order is preserved
 * and the result is rejoined using the same delimiter.
 *
 * <p>Examples with delimiter {@code ,}:
 * <ul>
 *   <li>{@code ,a,b,c,,} becomes {@code a,b,c}</li>
 *   <li>{@code ,,a,,b,,} becomes {@code a,,b}</li>
 *   <li>{@code a,,b,,,c} is unchanged (no leading/trailing empties)</li>
 *   <li>{@code ,,,} becomes {@code ""} (empty string)</li>
 * </ul>
 *
 * <p>Usage: {@code ${attribute:trimDelimitedList(',')}}
 */
public class TrimDelimitedListEvaluator extends StringEvaluator {

    private final Evaluator<String> subject;
    private final Evaluator<String> delimiter;

    public TrimDelimitedListEvaluator(final Evaluator<String> subject,
                                      final Evaluator<String> delimiter) {
        this.subject = subject;
        this.delimiter = delimiter;
    }

    @Override
    public QueryResult<String> evaluate(final EvaluationContext evaluationContext) {
        final String subjectValue = subject.evaluate(evaluationContext).getValue();
        if (subjectValue == null) {
            return new StringQueryResult("");
        }

        final String delimiterValue = delimiter.evaluate(evaluationContext).getValue();
        if (delimiterValue == null || delimiterValue.isEmpty()) {
            return new StringQueryResult(subjectValue);
        }

        final String[] tokens = subjectValue.split(Pattern.quote(delimiterValue), -1);

        // Scan forward past leading empty tokens
        int start = 0;
        while (start < tokens.length && tokens[start].isEmpty()) {
            start++;
        }

        // All tokens were empty, nothing to keep
        if (start == tokens.length) {
            return new StringQueryResult("");
        }

        // Scan backward past trailing empty tokens
        int end = tokens.length - 1;
        while (end > start && tokens[end].isEmpty()) {
            end--;
        }

        // Rejoin the slice between start and end, preserving interior empties
        return new StringQueryResult(
                String.join(delimiterValue, Arrays.copyOfRange(tokens, start, end + 1)));
    }

    @Override
    public Evaluator<?> getSubjectEvaluator() {
        return subject;
    }
}
