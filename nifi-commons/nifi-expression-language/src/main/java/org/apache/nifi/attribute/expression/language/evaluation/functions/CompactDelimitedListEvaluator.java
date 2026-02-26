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
import java.util.stream.Collectors;

/**
 * <p>Removes all empty tokens from a delimited string, preserves the order of
 * non-empty tokens, and rejoins them using the same delimiter.</p>
 *
 * <p>Examples with comma delimiter:</p>
 * <ul>
 *   <li>,a,b,c,, becomes a,b,c</li>
 *   <li>a,,b,,,c becomes a,b,c</li>
 *   <li>,,, becomes empty string</li>
 * </ul>
 *
 * <p>Usage: ${attribute:compactDelimitedList(',')}</p>
 */
public class CompactDelimitedListEvaluator extends StringEvaluator {

    private final Evaluator<String> subject;
    private final Evaluator<String> delimiter;

    public CompactDelimitedListEvaluator(final Evaluator<String> subject,
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

        // Split on the delimiter, keeping trailing empties so we can remove them
        final String[] tokens = subjectValue.split(Pattern.quote(delimiterValue), -1);

        // Filter out all empty tokens and rejoin the rest
        final String result = Arrays.stream(tokens)
                .filter(token -> !token.isEmpty())
                .collect(Collectors.joining(delimiterValue));

        return new StringQueryResult(result);
    }

    @Override
    public Evaluator<?> getSubjectEvaluator() {
        return subject;
    }
}
