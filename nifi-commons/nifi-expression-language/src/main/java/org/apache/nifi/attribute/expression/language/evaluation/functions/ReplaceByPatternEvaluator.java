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
import org.apache.nifi.attribute.expression.language.evaluation.Evaluator;
import org.apache.nifi.attribute.expression.language.evaluation.QueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.StringEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.StringQueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.literals.StringLiteralEvaluator;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class ReplaceByPatternEvaluator extends StringEvaluator {

    private final Evaluator<String> subject;
    private final Evaluator<String> search;

    private Map<String, Pattern> compiledPatterns = null;

    public ReplaceByPatternEvaluator(final Evaluator<String> subject, final Evaluator<String> search) {
        this.subject = subject;
        this.search = search;

        // if the search string is a literal, we don't need to evaluate it each time; we can just
        // pre-compile it. Otherwise, it must be compiled every time.
        if (search instanceof StringLiteralEvaluator) {
            this.compiledPatterns = compilePatterns(search.evaluate(new StandardEvaluationContext(Collections.emptyMap())).getValue());
        } else {
            this.compiledPatterns = null;
        }
    }

    @Override
    public QueryResult<String> evaluate(final EvaluationContext evaluationContext) {
        final String subjectValue = subject.evaluate(evaluationContext).getValue();
        if (subjectValue == null) {
            return new StringQueryResult(null);
        }

        final Map<String, Pattern> patterns;
        if (compiledPatterns == null) {
            String expression = search.evaluate(evaluationContext).getValue();
            if (expression == null) {
                return new StringQueryResult(subjectValue);
            }
            patterns = compilePatterns(expression);
        } else {
            patterns = compiledPatterns;
        }

        for (Map.Entry<String, Pattern> entry : patterns.entrySet()) {
            if (entry.getValue().matcher(subjectValue).matches()) {
                return new StringQueryResult(entry.getKey());
            }
        }

        return new StringQueryResult(subjectValue);
    }

    private Map<String, Pattern> compilePatterns(final String argument) {
        final Map<String, Pattern> result = new HashMap<>();
        if (argument == null || argument.trim().isEmpty()) {
            return result;
        }

        final String[] mappings = argument.split(",");
        for (String mapping : mappings) {
            String[] parts = mapping.trim().split(":");
            String streamPattern = parts[0];
            String mappedTo = parts[1];

            if (streamPattern == null || streamPattern.trim().isEmpty()) {
                continue;
            }

            if (mappedTo == null || mappedTo.trim().isEmpty()) {
                continue;
            }

            try {
                result.put(mappedTo, Pattern.compile(streamPattern));
            } catch (Exception e) {
                // ignore
                continue;
            }
        }

        return result;
    }

    @Override
    public Evaluator<?> getSubjectEvaluator() {
        return subject;
    }

}
