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
package org.apache.nifi.attribute.expression.language.evaluation.reduce;

import java.util.Map;

import org.apache.nifi.attribute.expression.language.evaluation.Evaluator;
import org.apache.nifi.attribute.expression.language.evaluation.QueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.StringEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.StringQueryResult;

public class JoinEvaluator extends StringEvaluator implements ReduceEvaluator<String> {

    private final Evaluator<String> subjectEvaluator;
    private final Evaluator<String> delimiterEvaluator;

    private final StringBuilder sb = new StringBuilder();
    private int evalCount = 0;

    public JoinEvaluator(final Evaluator<String> subject, final Evaluator<String> delimiter) {
        this.subjectEvaluator = subject;
        this.delimiterEvaluator = delimiter;
    }

    @Override
    public QueryResult<String> evaluate(final Map<String, String> attributes) {
        String subject = subjectEvaluator.evaluate(attributes).getValue();
        if (subject == null) {
            subject = "";
        }

        final String delimiter = delimiterEvaluator.evaluate(attributes).getValue();
        if (evalCount > 0) {
            sb.append(delimiter);
        }
        sb.append(subject);

        evalCount++;
        return new StringQueryResult(sb.toString());
    }

    @Override
    public Evaluator<?> getSubjectEvaluator() {
        return subjectEvaluator;
    }
}
