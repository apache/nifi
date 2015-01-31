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

import java.util.Map;
import java.util.regex.Pattern;

import org.apache.nifi.attribute.expression.language.evaluation.BooleanEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.BooleanQueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.Evaluator;
import org.apache.nifi.attribute.expression.language.evaluation.QueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.StringEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.literals.StringLiteralEvaluator;

public class MatchesEvaluator extends BooleanEvaluator {

    private final StringEvaluator subject;
    private final StringEvaluator search;

    private final Pattern compiledPattern;

    public MatchesEvaluator(final StringEvaluator subject, final StringEvaluator search) {
        this.subject = subject;
        this.search = search;

        // if the search string is a literal, we don't need to evaluate it each time; we can just
        // pre-compile it. Otherwise, it must be compiled every time.
        if (search instanceof StringLiteralEvaluator) {
            this.compiledPattern = Pattern.compile(search.evaluate(null).getValue());
        } else {
            this.compiledPattern = null;
        }
    }

    @Override
    public QueryResult<Boolean> evaluate(final Map<String, String> attributes) {
        final String subjectValue = subject.evaluate(attributes).getValue();
        if (subjectValue == null) {
            return new BooleanQueryResult(false);
        }
        final Pattern pattern;
        if (compiledPattern == null) {
            pattern = Pattern.compile(search.evaluate(attributes).getValue());
        } else {
            pattern = compiledPattern;
        }

        final boolean matches = pattern.matcher(subjectValue).matches();
        return new BooleanQueryResult(matches);
    }

    @Override
    public Evaluator<?> getSubjectEvaluator() {
        return subject;
    }

}
